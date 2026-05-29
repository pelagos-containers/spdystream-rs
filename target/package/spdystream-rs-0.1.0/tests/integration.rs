use bytes::Bytes;
use std::collections::HashMap;
use spdystream_rs::{
    frame::{
        DataFrame, Frame, GoAwayFrame, GoAwayStatus, PingFrame, RstStatus, RstStreamFrame,
        SynStreamFrame, DATA_FLAG_FIN,
    },
    framer::Framer,
    server::ServerConfig,
};
use std::sync::Arc;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::time::Duration;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn echo_handler() -> ServerConfig {
    ServerConfig::new(|stream| {
        Box::pin(async move {
            stream
                .send_reply(http::HeaderMap::new(), false)
                .await
                .unwrap();
            loop {
                match stream.read_data().await {
                    Ok(Some(data)) => {
                        stream.write_data(data, false).await.unwrap();
                    }
                    Ok(None) => {
                        // Remote sent FIN — echo our FIN back.
                        stream.write_data(Bytes::new(), true).await.unwrap();
                        break;
                    }
                    Err(_) => break,
                }
            }
        })
    })
}

async fn start_server(config: ServerConfig) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = spdystream_rs::accept(stream, &config).await;
    });
    addr
}

/// Connect to addr, perform HTTP UPGRADE handshake, split the TCP stream into
/// a write-only Framer (enc) and a read-only Framer (dec).
async fn connect_client(
    addr: std::net::SocketAddr,
) -> (
    Framer<tokio::io::WriteHalf<tokio::net::TcpStream>, tokio::io::Empty>,
    Framer<tokio::io::Sink, tokio::io::ReadHalf<tokio::net::TcpStream>>,
) {
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();

    tcp.write_all(
        b"GET /exec HTTP/1.1\r\nHost: localhost\r\nUpgrade: spdy/3.1\r\nConnection: Upgrade\r\n\r\n",
    )
    .await
    .unwrap();

    // Read until \r\n\r\n.
    let mut resp = Vec::new();
    loop {
        let b = tcp.read_u8().await.unwrap();
        resp.push(b);
        if resp.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    assert!(
        resp.starts_with(b"HTTP/1.1 101"),
        "expected 101, got: {:?}",
        std::str::from_utf8(&resp)
    );

    let (read_half, write_half) = split(tcp);
    let enc = Framer::new(write_half, tokio::io::empty());
    let dec = Framer::new(tokio::io::sink(), read_half);
    (enc, dec)
}

fn syn_stream_frame(stream_id: u32) -> Frame {
    Frame::SynStream(SynStreamFrame {
        stream_id,
        associated_stream_id: 0,
        priority: 0,
        headers: http::HeaderMap::new(),
        flags: 0,
    })
}

// ---------------------------------------------------------------------------
// Test 1: single echo stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_echo_single_stream() {
    let addr = start_server(echo_handler()).await;
    let (mut enc, mut dec) = connect_client(addr).await;

    // Skip the Settings frame sent by the server on connect.
    let settings = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout on settings")
        .expect("read settings");
    assert!(
        matches!(settings, Frame::Settings(_)),
        "expected Settings, got {settings:?}"
    );

    // Send SynStream.
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::HOST,
        http::header::HeaderValue::from_static("localhost"),
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        enc.write_frame(&Frame::SynStream(SynStreamFrame {
            stream_id: 1,
            associated_stream_id: 0,
            priority: 0,
            headers,
            flags: 0,
        })),
    )
    .await
    .expect("timeout sending SynStream")
    .expect("write SynStream");

    // Read SynReply.
    let reply = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout on SynReply")
        .expect("read SynReply");
    match reply {
        Frame::SynReply(r) => assert_eq!(r.stream_id, 1),
        other => panic!("expected SynReply, got {other:?}"),
    }

    // Send DATA with FIN.
    tokio::time::timeout(
        Duration::from_secs(5),
        enc.write_frame(&Frame::Data(DataFrame {
            stream_id: 1,
            flags: DATA_FLAG_FIN,
            data: Bytes::from("hello spdy"),
        })),
    )
    .await
    .expect("timeout sending Data")
    .expect("write Data");

    // Read echoed DATA. The handler may send two frames (one with data, one empty FIN),
    // so collect all data frames until we see FIN.
    let mut collected = Bytes::new();
    let mut saw_fin = false;
    while !saw_fin {
        let frame = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
            .await
            .expect("timeout on echoed Data")
            .expect("read echoed Data");
        match frame {
            Frame::Data(df) => {
                assert_eq!(df.stream_id, 1);
                if !df.data.is_empty() {
                    collected = df.data.clone();
                }
                if (df.flags & DATA_FLAG_FIN) != 0 {
                    saw_fin = true;
                }
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }
    assert_eq!(collected, Bytes::from("hello spdy"));
    assert!(saw_fin, "expected FIN flag on echoed data");
}

// ---------------------------------------------------------------------------
// Test 2: five concurrent streams
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_echo_five_concurrent_streams() {
    let addr = start_server(echo_handler()).await;
    let (mut enc, mut dec) = connect_client(addr).await;

    // Skip Settings.
    let settings = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout on settings")
        .expect("read settings");
    assert!(matches!(settings, Frame::Settings(_)));

    let ids: [u32; 5] = [1, 3, 5, 7, 9];

    // Send all SynStreams.
    for &id in &ids {
        tokio::time::timeout(Duration::from_secs(5), enc.write_frame(&syn_stream_frame(id)))
            .await
            .expect("timeout sending SynStream")
            .expect("write SynStream");
    }

    // Send all DATA frames with FIN.
    for &id in &ids {
        tokio::time::timeout(
            Duration::from_secs(5),
            enc.write_frame(&Frame::Data(DataFrame {
                stream_id: id,
                flags: DATA_FLAG_FIN,
                data: Bytes::from(format!("stream-{id}")),
            })),
        )
        .await
        .expect("timeout sending Data")
        .expect("write Data");
    }

    // Read all SynReplies and DATA frames (at least 10; echo produces
    // 2 DATA frames per stream — one with content, one empty FIN).
    let mut replies: HashMap<u32, ()> = HashMap::new();
    let mut received: HashMap<u32, Bytes> = HashMap::new();
    let mut fin_seen: HashMap<u32, bool> = HashMap::new();

    // Loop until every stream has a reply, data, and FIN.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if replies.len() == 5
            && received.len() == 5
            && fin_seen.values().filter(|&&v| v).count() == 5
        {
            break;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "timed out: replies={}, data={}, fins={}",
                replies.len(),
                received.len(),
                fin_seen.values().filter(|&&v| v).count()
            );
        }
        let frame = tokio::time::timeout(remaining, dec.read_frame())
            .await
            .expect("timeout reading frame")
            .expect("read frame");
        match frame {
            Frame::SynReply(r) => {
                replies.insert(r.stream_id, ());
            }
            Frame::Data(d) => {
                let fin = (d.flags & DATA_FLAG_FIN) != 0;
                if !d.data.is_empty() {
                    received.insert(d.stream_id, d.data.clone());
                }
                if fin {
                    fin_seen.insert(d.stream_id, true);
                }
            }
            Frame::Settings(_) => {} // ignore late settings
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    assert_eq!(replies.len(), 5, "expected 5 SynReplies");
    assert_eq!(received.len(), 5, "expected data from 5 streams");
    for &id in &ids {
        assert!(replies.contains_key(&id), "missing reply for stream {id}");
        assert_eq!(
            received.get(&id).cloned().unwrap_or_default(),
            Bytes::from(format!("stream-{id}")),
            "wrong data for stream {id}"
        );
        assert!(
            fin_seen.get(&id).copied().unwrap_or(false),
            "no FIN seen for stream {id}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test 3: settings on connect
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_settings_on_connect() {
    // SETTINGS_MAX_CONCURRENT_STREAMS has ID 4 per SPDY spec.
    const SETTINGS_MAX_CONCURRENT_STREAMS: u32 = 4;

    let addr = start_server(ServerConfig::new(|_| Box::pin(async {}))).await;
    let (_enc, mut dec) = connect_client(addr).await;

    let frame = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout waiting for Settings")
        .expect("read Settings");

    match frame {
        Frame::Settings(s) => {
            let has_max_concurrent = s
                .settings
                .iter()
                .any(|setting| setting.id == SETTINGS_MAX_CONCURRENT_STREAMS);
            assert!(
                has_max_concurrent,
                "Settings frame must include SETTINGS_MAX_CONCURRENT_STREAMS (id=4)"
            );
        }
        other => panic!("expected Settings, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Test 4: ping roundtrip (client-initiated, odd ID)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ping_roundtrip() {
    let addr = start_server(ServerConfig::new(|_| Box::pin(async {}))).await;
    let (mut enc, mut dec) = connect_client(addr).await;

    // Skip Settings.
    let settings = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout on settings")
        .expect("read settings");
    assert!(matches!(settings, Frame::Settings(_)));

    // Send client-initiated Ping (odd ID=1).
    tokio::time::timeout(
        Duration::from_secs(5),
        enc.write_frame(&Frame::Ping(PingFrame { id: 1 })),
    )
    .await
    .expect("timeout sending Ping")
    .expect("write Ping");

    // Server should echo it back.
    let frame = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout waiting for Ping echo")
        .expect("read Ping echo");

    match frame {
        Frame::Ping(p) => assert_eq!(p.id, 1, "echoed ping must have same ID"),
        other => panic!("expected Ping, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Test 5: GoAway closes connection cleanly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_goaway_on_close() {
    let addr = start_server(ServerConfig::new(|_| Box::pin(async {}))).await;
    let (mut enc, mut dec) = connect_client(addr).await;

    // Skip Settings.
    let settings = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout on settings")
        .expect("read settings");
    assert!(matches!(settings, Frame::Settings(_)));

    // Client sends GoAway.
    tokio::time::timeout(
        Duration::from_secs(5),
        enc.write_frame(&Frame::GoAway(GoAwayFrame {
            last_good_stream_id: 0,
            status: GoAwayStatus::Ok,
        })),
    )
    .await
    .expect("timeout sending GoAway")
    .expect("write GoAway");

    // Connection should close: next read returns ConnectionClosed or EOF.
    let result = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout waiting for connection close");

    match result {
        Err(spdystream_rs::Error::ConnectionClosed) => {}
        Err(spdystream_rs::Error::Io(_)) => {}
        Ok(Frame::GoAway(_)) => {
            // Server may echo a GoAway; subsequent read should EOF.
            let result2 = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
                .await
                .expect("timeout waiting for EOF after GoAway");
            match result2 {
                Err(spdystream_rs::Error::ConnectionClosed) | Err(spdystream_rs::Error::Io(_)) => {}
                Ok(other) => panic!("expected EOF/close after GoAway, got {other:?}"),
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        Ok(other) => panic!("expected connection close, got {other:?}"),
        Err(e) => panic!("unexpected error after GoAway: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Test 6: RST_STREAM cancels a stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rst_stream() {
    // Handler: send reply then block waiting for data (never comes due to RST).
    let config = ServerConfig::new(|stream| {
        Box::pin(async move {
            stream.send_reply(http::HeaderMap::new(), false).await.ok();
            // Wait a bit — client will RST before sending data.
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
    });
    let addr = start_server(config).await;
    let (mut enc, mut dec) = connect_client(addr).await;

    // Skip Settings.
    let settings = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout on settings")
        .expect("read settings");
    assert!(matches!(settings, Frame::Settings(_)));

    // Open stream 1.
    tokio::time::timeout(
        Duration::from_secs(5),
        enc.write_frame(&syn_stream_frame(1)),
    )
    .await
    .expect("timeout sending SynStream")
    .expect("write SynStream");

    // Read SynReply.
    let reply = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout on SynReply")
        .expect("read SynReply");
    assert!(
        matches!(reply, Frame::SynReply(ref r) if r.stream_id == 1),
        "expected SynReply for stream 1, got {reply:?}"
    );

    // Send RST_STREAM Cancel.
    tokio::time::timeout(
        Duration::from_secs(5),
        enc.write_frame(&Frame::RstStream(RstStreamFrame {
            stream_id: 1,
            status: RstStatus::Cancel,
        })),
    )
    .await
    .expect("timeout sending RST")
    .expect("write RST");

    // No further DATA frames should arrive for stream 1.
    // Any subsequent frame within a short window must not be data for stream 1.
    let grace = Duration::from_millis(300);
    match tokio::time::timeout(grace, dec.read_frame()).await {
        Err(_timeout) => {} // clean — nothing arrived
        Ok(Ok(Frame::Data(df))) if df.stream_id == 1 => {
            panic!("received Data for cancelled stream 1");
        }
        Ok(Ok(_other)) => {} // some other frame is acceptable
        Ok(Err(_)) => {}     // connection closed is also fine
    }
}

// ---------------------------------------------------------------------------
// Test 7: invalid (decreasing) stream ID is rejected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_invalid_stream_id_rejected() {
    let addr = start_server(ServerConfig::new(|_| Box::pin(async {}))).await;
    let (mut enc, mut dec) = connect_client(addr).await;

    // Skip Settings.
    let settings = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
        .await
        .expect("timeout on settings")
        .expect("read settings");
    assert!(matches!(settings, Frame::Settings(_)));

    // Send stream 5 first (valid).
    tokio::time::timeout(
        Duration::from_secs(5),
        enc.write_frame(&syn_stream_frame(5)),
    )
    .await
    .expect("timeout sending SynStream 5")
    .expect("write SynStream 5");

    // Give server a moment to process it.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send stream 3 (going backward — protocol error).
    tokio::time::timeout(
        Duration::from_secs(5),
        enc.write_frame(&syn_stream_frame(3)),
    )
    .await
    .expect("timeout sending SynStream 3")
    .expect("write SynStream 3");

    // Drain frames until we find an RST for stream 3.
    let rst = loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), dec.read_frame())
            .await
            .expect("timeout waiting for RST")
            .expect("read frame");
        match frame {
            Frame::RstStream(r) if r.stream_id == 3 => break r,
            Frame::SynReply(_) | Frame::Settings(_) => {} // skip
            other => panic!("unexpected frame: {other:?}"),
        }
    };

    assert_eq!(rst.stream_id, 3, "RST should target stream 3");
    assert_eq!(
        rst.status,
        RstStatus::ProtocolError,
        "should be ProtocolError"
    );
}

// ---------------------------------------------------------------------------
// Test 8: non-upgrade request causes connection to close
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_upgrade_wrong_protocol() {
    // Use a server that accepts raw connections and calls `accept`.
    let config = Arc::new(ServerConfig::new(|_| Box::pin(async {})));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        // accept() will fail because the client sends a plain GET (no Upgrade header).
        let _ = spdystream_rs::accept(stream, &config).await;
        // Error is expected; server task exits cleanly.
    });

    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();

    // Send plain GET without Upgrade.
    tcp.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    // Connection should close (EOF or reset) because the server returns an error.
    let mut buf = [0u8; 64];
    let result = tokio::time::timeout(Duration::from_secs(5), tcp.read(&mut buf))
        .await
        .expect("timeout waiting for server close");

    match result {
        Ok(0) => {} // EOF — connection closed cleanly
        Ok(n) => {
            // Some error response bytes may arrive; that's acceptable as long as
            // the connection eventually closes.
            let _ = n;
        }
        Err(_) => {} // connection reset — also acceptable
    }
}
