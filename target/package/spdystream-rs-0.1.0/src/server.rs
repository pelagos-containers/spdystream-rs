use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::connection::{BoxFuture, Connection};
use crate::error::{Error, Result};
use crate::stream::Stream;

pub struct ServerConfig {
    pub handler: Arc<dyn Fn(Arc<Stream>) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
    pub go_away_timeout: std::time::Duration,
    pub close_timeout: std::time::Duration,
}

impl ServerConfig {
    pub fn new<F>(handler: F) -> Self
    where
        F: Fn(Arc<Stream>) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        Self {
            handler: Arc::new(handler),
            go_away_timeout: std::time::Duration::from_secs(5),
            close_timeout: std::time::Duration::from_secs(5),
        }
    }
}

pub struct UpgradeRequest {
    pub path: String,
    pub headers: http::HeaderMap,
    pub method: String,
}

/// Parse an HTTP/1.1 UPGRADE request from any async reader.
/// Reads until the blank line (CRLFCRLF), then returns the parsed request.
pub async fn parse_upgrade_request<S: AsyncRead + Unpin>(stream: &mut S) -> Result<UpgradeRequest> {
    let mut buf = Vec::with_capacity(1024);
    let mut consecutive_crlf = 0u8;
    loop {
        let b = stream.read_u8().await.map_err(Error::Io)?;
        buf.push(b);
        if b == b'\r' || b == b'\n' {
            consecutive_crlf += 1;
            if consecutive_crlf == 4 {
                break;
            }
        } else {
            consecutive_crlf = 0;
        }
        if buf.len() > 8192 {
            return Err(Error::Protocol("HTTP request too large".to_string()));
        }
    }

    let text = std::str::from_utf8(&buf)
        .map_err(|_| Error::Protocol("non-UTF8 HTTP request".to_string()))?;

    parse_http_request(text)
}

fn parse_http_request(text: &str) -> Result<UpgradeRequest> {
    let mut lines = text.split("\r\n");

    // Parse request line: METHOD path HTTP/1.1
    let request_line = lines
        .next()
        .ok_or_else(|| Error::Protocol("empty HTTP request".to_string()))?;

    let mut parts = request_line.splitn(3, ' ');
    let method = parts
        .next()
        .ok_or_else(|| Error::Protocol("missing method".to_string()))?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| Error::Protocol("missing path".to_string()))?
        .to_string();
    // We don't need the HTTP version part, but it must be present.
    parts
        .next()
        .ok_or_else(|| Error::Protocol("missing HTTP version".to_string()))?;

    // Parse headers
    let mut headers = http::HeaderMap::new();
    let mut has_spdy_upgrade = false;

    for line in lines {
        if line.is_empty() {
            break;
        }
        let colon = line
            .find(':')
            .ok_or_else(|| Error::Protocol(format!("invalid header line: {line}")))?;
        let name = line[..colon].trim();
        let value = line[colon + 1..].trim();

        if name.eq_ignore_ascii_case("upgrade") && value.eq_ignore_ascii_case("spdy/3.1") {
            has_spdy_upgrade = true;
        }

        let header_name = http::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| Error::Protocol(format!("invalid header name: {name}")))?;
        let header_value = http::header::HeaderValue::from_str(value)
            .map_err(|_| Error::Protocol(format!("invalid header value: {value}")))?;
        headers.insert(header_name, header_value);
    }

    if !has_spdy_upgrade {
        return Err(Error::Protocol(
            "missing or incorrect Upgrade: spdy/3.1 header".to_string(),
        ));
    }

    Ok(UpgradeRequest {
        method,
        path,
        headers,
    })
}

/// Send 101 Switching Protocols response to any async writer.
pub async fn send_upgrade_response<S: AsyncWrite + Unpin>(stream: &mut S) -> Result<()> {
    stream
        .write_all(
            b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: spdy/3.1\r\n\r\n",
        )
        .await?;
    Ok(())
}

/// Full accept: parse upgrade, send 101, then serve SPDY.
/// Returns the Arc<Connection> for the established SPDY session.
pub async fn accept(mut stream: TcpStream, config: &ServerConfig) -> Result<Arc<Connection>> {
    parse_upgrade_request(&mut stream).await?;
    send_upgrade_response(&mut stream).await?;
    let handler = Arc::clone(&config.handler);
    Connection::serve(stream, move |s| handler(s)).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_upgrade_handshake() {
        let (mut client, mut server) = tokio::io::duplex(4096);

        // Client writes HTTP UPGRADE request.
        client
            .write_all(b"GET /exec HTTP/1.1\r\nHost: localhost\r\nUpgrade: spdy/3.1\r\nConnection: Upgrade\r\n\r\n")
            .await
            .unwrap();

        // Server parses it.
        let req = parse_upgrade_request(&mut server).await.unwrap();
        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/exec");

        // Server sends 101.
        send_upgrade_response(&mut server).await.unwrap();

        // Client reads 101 by scanning for \r\n\r\n.
        let mut resp = Vec::new();
        loop {
            let b = client.read_u8().await.unwrap();
            resp.push(b);
            if resp.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        assert!(resp.starts_with(b"HTTP/1.1 101"));
    }

    #[tokio::test]
    async fn test_invalid_upgrade() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        client
            .write_all(b"GET /exec HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let result = parse_upgrade_request(&mut server).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_full_echo_server() {
        use crate::frame::{DataFrame, Frame, SynStreamFrame, DATA_FLAG_FIN};
        use crate::framer::Framer;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Server task.
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let config = ServerConfig::new(|stream| {
                Box::pin(async move {
                    stream
                        .send_reply(http::HeaderMap::new(), false)
                        .await
                        .unwrap();
                    if let Ok(Some(data)) = stream.read_data().await {
                        stream.write_data(data, true).await.unwrap();
                    }
                })
            });
            accept(stream, &config).await.unwrap();
        });

        // Client: connect, send HTTP UPGRADE, then send SPDY frames.
        let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        tcp.write_all(b"GET /exec HTTP/1.1\r\nHost: localhost\r\nUpgrade: spdy/3.1\r\nConnection: Upgrade\r\n\r\n")
            .await
            .unwrap();

        // Read 101 response by scanning for \r\n\r\n.
        let mut resp = Vec::new();
        loop {
            let b = tcp.read_u8().await.unwrap();
            resp.push(b);
            if resp.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        assert!(resp.starts_with(b"HTTP/1.1 101"));

        // Now use Framer as a client.
        let (read_half, write_half) = tokio::io::split(tcp);
        let mut enc = Framer::new(write_half, tokio::io::empty());
        let mut dec = Framer::new(tokio::io::sink(), read_half);

        // Client sends SynStream.
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::HOST, "localhost".parse().unwrap());
        enc.write_frame(&Frame::SynStream(SynStreamFrame {
            stream_id: 1,
            associated_stream_id: 0,
            priority: 0,
            headers,
            flags: 0,
        }))
        .await
        .unwrap();

        // Read Settings (sent by server on connect).
        let settings = dec.read_frame().await.unwrap();
        assert!(matches!(settings, Frame::Settings(_)));

        // Read SynReply.
        let reply = dec.read_frame().await.unwrap();
        assert!(matches!(reply, Frame::SynReply(ref r) if r.stream_id == 1));

        // Send data.
        enc.write_frame(&Frame::Data(DataFrame {
            stream_id: 1,
            flags: DATA_FLAG_FIN,
            data: bytes::Bytes::from("hello spdy"),
        }))
        .await
        .unwrap();

        // Read echoed data.
        let data_frame = dec.read_frame().await.unwrap();
        match data_frame {
            Frame::Data(df) => {
                assert_eq!(df.data.as_ref(), b"hello spdy");
                assert_eq!(df.flags & DATA_FLAG_FIN, DATA_FLAG_FIN);
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }
}
