use std::collections::BTreeMap;

use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use http::{HeaderMap, HeaderName, HeaderValue};
use bytes::Bytes;

use crate::dictionary::SPDY_DICT;
use crate::error::{Error, Result};
use crate::frame::*;

const SPDY_VERSION: u16 = 3;
const TYPE_SYN_STREAM: u16 = 1;
const TYPE_SYN_REPLY: u16 = 2;
const TYPE_RST_STREAM: u16 = 3;
const TYPE_SETTINGS: u16 = 4;
const TYPE_PING: u16 = 6;
const TYPE_GOAWAY: u16 = 7;
const TYPE_HEADERS: u16 = 8;
const TYPE_WINDOW_UPDATE: u16 = 9;

pub struct Framer<W, R> {
    writer: W,
    reader: R,
    compressor: Compress,
    decompressor: Option<Decompress>,
}

impl<W: AsyncWrite + Unpin, R: AsyncRead + Unpin> Framer<W, R> {
    pub fn new(writer: W, reader: R) -> Self {
        let mut compressor = Compress::new(Compression::best(), true);
        compressor.set_dictionary(SPDY_DICT).expect("set compressor dictionary");
        Self {
            writer,
            reader,
            compressor,
            decompressor: None,
        }
    }

    /// Encode headers into the SPDY wire format (uncompressed).
    fn encode_headers(headers: &HeaderMap) -> Vec<u8> {
        // Collect headers, joining multiple values for the same name with NUL.
        let mut map: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for (name, value) in headers.iter() {
            let key = name.as_str().to_owned();
            let val = value.as_bytes();
            map.entry(key)
                .and_modify(|existing| {
                    existing.push(0);
                    existing.extend_from_slice(val);
                })
                .or_insert_with(|| val.to_vec());
        }

        let count = map.len() as u32;
        let mut raw = Vec::new();
        raw.extend_from_slice(&count.to_be_bytes());
        for (name, value) in &map {
            let name_bytes = name.as_bytes();
            raw.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            raw.extend_from_slice(name_bytes);
            raw.extend_from_slice(&(value.len() as u32).to_be_bytes());
            raw.extend_from_slice(value);
        }
        raw
    }

    /// Compress headers using the persistent zlib compressor.
    fn compress_headers(&mut self, headers: &HeaderMap) -> Result<Vec<u8>> {
        let raw = Self::encode_headers(headers);

        // Allocate a generous output buffer.
        let out_size = raw.len() * 2 + 512;
        let mut output = vec![0u8; out_size];

        let before_out = self.compressor.total_out();
        let status = self
            .compressor
            .compress(&raw, &mut output, FlushCompress::Sync)
            .map_err(|e| Error::Compression(e.to_string()))?;

        if status == Status::BufError {
            return Err(Error::Compression("compress buffer error".to_string()));
        }

        let produced = (self.compressor.total_out() - before_out) as usize;
        output.truncate(produced);
        Ok(output)
    }

    /// Decompress a header block using the persistent zlib decompressor.
    fn decompress_headers(&mut self, compressed: &[u8]) -> Result<HeaderMap> {
        let dec = self.decompressor.get_or_insert_with(|| Decompress::new(true));

        let mut output = Vec::with_capacity(compressed.len() * 4 + 512);
        let mut pos = 0usize;

        loop {
            let before_in = dec.total_in();

            // Grow output buffer if needed.
            if output.len() == output.capacity() {
                output.reserve(output.capacity().max(512));
            }

            match dec.decompress_vec(&compressed[pos..], &mut output, FlushDecompress::Sync) {
                Ok(Status::Ok) | Ok(Status::StreamEnd) => {
                    break;
                }
                Ok(Status::BufError) => {
                    // No progress possible, done.
                    break;
                }
                Err(e) => {
                    if e.needs_dictionary().is_some() {
                        // Dict needed — advance pos by what was consumed, set dict, continue.
                        let consumed = (dec.total_in() - before_in) as usize;
                        pos += consumed;
                        dec.set_dictionary(SPDY_DICT)
                            .map_err(|e2| Error::Compression(e2.to_string()))?;
                        // continue loop
                    } else {
                        return Err(Error::Compression(e.to_string()));
                    }
                }
            }

            if pos >= compressed.len() {
                break;
            }
        }

        parse_header_block(&output)
    }

    /// Write a control frame: 8-byte header + payload.
    async fn write_control_frame(
        &mut self,
        frame_type: u16,
        flags: u8,
        payload: &[u8],
    ) -> Result<()> {
        let len = payload.len() as u32;
        // Bytes 0-1: version (3) with control bit
        let word0 = (0x8000u16 | SPDY_VERSION).to_be_bytes();
        let type_bytes = frame_type.to_be_bytes();
        // Bytes 4: flags, bytes 5-7: length (u24)
        let len_bytes = [
            ((len >> 16) & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            (len & 0xff) as u8,
        ];

        let mut header = [0u8; 8];
        header[0] = word0[0];
        header[1] = word0[1];
        header[2] = type_bytes[0];
        header[3] = type_bytes[1];
        header[4] = flags;
        header[5] = len_bytes[0];
        header[6] = len_bytes[1];
        header[7] = len_bytes[2];

        self.writer.write_all(&header).await?;
        self.writer.write_all(payload).await?;
        Ok(())
    }

    /// Write a data frame.
    async fn write_data_frame(&mut self, frame: &DataFrame) -> Result<()> {
        let stream_id = frame.stream_id & 0x7fff_ffff;
        let len = frame.data.len() as u32;

        let mut header = [0u8; 8];
        let sid_bytes = stream_id.to_be_bytes();
        header[0] = sid_bytes[0];
        header[1] = sid_bytes[1];
        header[2] = sid_bytes[2];
        header[3] = sid_bytes[3];
        header[4] = frame.flags;
        header[5] = ((len >> 16) & 0xff) as u8;
        header[6] = ((len >> 8) & 0xff) as u8;
        header[7] = (len & 0xff) as u8;

        self.writer.write_all(&header).await?;
        self.writer.write_all(&frame.data).await?;
        Ok(())
    }

    /// Write any frame.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::SynStream(f) => self.write_syn_stream(f).await,
            Frame::SynReply(f) => self.write_syn_reply(f).await,
            Frame::RstStream(f) => self.write_rst_stream(f).await,
            Frame::Settings(f) => self.write_settings(f).await,
            Frame::Ping(f) => self.write_ping(f).await,
            Frame::GoAway(f) => self.write_goaway(f).await,
            Frame::Headers(f) => self.write_headers(f).await,
            Frame::WindowUpdate(f) => self.write_window_update(f).await,
            Frame::Data(f) => self.write_data_frame(f).await,
        }
    }

    async fn write_syn_stream(&mut self, f: &SynStreamFrame) -> Result<()> {
        let compressed = self.compress_headers(&f.headers)?;
        let mut payload = Vec::with_capacity(10 + compressed.len());
        payload.extend_from_slice(&(f.stream_id & 0x7fff_ffff).to_be_bytes());
        payload.extend_from_slice(&(f.associated_stream_id & 0x7fff_ffff).to_be_bytes());
        payload.push((f.priority & 0x7) << 5);
        payload.push(0); // slot / credential slot
        payload.extend_from_slice(&compressed);
        self.write_control_frame(TYPE_SYN_STREAM, f.flags, &payload).await
    }

    async fn write_syn_reply(&mut self, f: &SynReplyFrame) -> Result<()> {
        let compressed = self.compress_headers(&f.headers)?;
        let mut payload = Vec::with_capacity(4 + compressed.len());
        payload.extend_from_slice(&(f.stream_id & 0x7fff_ffff).to_be_bytes());
        payload.extend_from_slice(&compressed);
        self.write_control_frame(TYPE_SYN_REPLY, f.flags, &payload).await
    }

    async fn write_rst_stream(&mut self, f: &RstStreamFrame) -> Result<()> {
        let mut payload = [0u8; 8];
        payload[..4].copy_from_slice(&(f.stream_id & 0x7fff_ffff).to_be_bytes());
        payload[4..].copy_from_slice(&(f.status as u32).to_be_bytes());
        self.write_control_frame(TYPE_RST_STREAM, 0, &payload).await
    }

    async fn write_settings(&mut self, f: &SettingsFrame) -> Result<()> {
        let mut payload = Vec::with_capacity(4 + f.settings.len() * 8);
        payload.extend_from_slice(&(f.settings.len() as u32).to_be_bytes());
        for s in &f.settings {
            // ID in low 3 bytes, flags in high byte (big-endian: flags | id24)
            let id_flags = ((s.flags as u32) << 24) | (s.id & 0x00ff_ffff);
            payload.extend_from_slice(&id_flags.to_be_bytes());
            payload.extend_from_slice(&s.value.to_be_bytes());
        }
        self.write_control_frame(TYPE_SETTINGS, f.flags, &payload).await
    }

    async fn write_ping(&mut self, f: &PingFrame) -> Result<()> {
        let payload = f.id.to_be_bytes();
        self.write_control_frame(TYPE_PING, 0, &payload).await
    }

    async fn write_goaway(&mut self, f: &GoAwayFrame) -> Result<()> {
        let mut payload = [0u8; 8];
        payload[..4].copy_from_slice(&(f.last_good_stream_id & 0x7fff_ffff).to_be_bytes());
        payload[4..].copy_from_slice(&(f.status as u32).to_be_bytes());
        self.write_control_frame(TYPE_GOAWAY, 0, &payload).await
    }

    async fn write_headers(&mut self, f: &HeadersFrame) -> Result<()> {
        let compressed = self.compress_headers(&f.headers)?;
        let mut payload = Vec::with_capacity(4 + compressed.len());
        payload.extend_from_slice(&(f.stream_id & 0x7fff_ffff).to_be_bytes());
        payload.extend_from_slice(&compressed);
        self.write_control_frame(TYPE_HEADERS, f.flags, &payload).await
    }

    async fn write_window_update(&mut self, f: &WindowUpdateFrame) -> Result<()> {
        let mut payload = [0u8; 8];
        payload[..4].copy_from_slice(&(f.stream_id & 0x7fff_ffff).to_be_bytes());
        payload[4..].copy_from_slice(&(f.delta & 0x7fff_ffff).to_be_bytes());
        self.write_control_frame(TYPE_WINDOW_UPDATE, 0, &payload).await
    }

    /// Read a single frame from the reader.
    pub async fn read_frame(&mut self) -> Result<Frame> {
        let mut header = [0u8; 8];
        self.reader
            .read_exact(&mut header)
            .await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    Error::ConnectionClosed
                } else {
                    Error::Io(e)
                }
            })?;

        let is_control = (header[0] & 0x80) != 0;

        if is_control {
            let version = u16::from_be_bytes([header[0] & 0x7f, header[1]]);
            let frame_type = u16::from_be_bytes([header[2], header[3]]);
            let flags = header[4];
            let length = ((header[5] as u32) << 16)
                | ((header[6] as u32) << 8)
                | (header[7] as u32);

            if version != SPDY_VERSION {
                return Err(Error::Protocol(format!("unsupported SPDY version: {version}")));
            }

            let mut payload = vec![0u8; length as usize];
            self.reader.read_exact(&mut payload).await?;

            self.parse_control_frame(frame_type, flags, &payload)
        } else {
            let stream_id = u32::from_be_bytes([
                header[0] & 0x7f,
                header[1],
                header[2],
                header[3],
            ]);
            let flags = header[4];
            let length = ((header[5] as u32) << 16)
                | ((header[6] as u32) << 8)
                | (header[7] as u32);

            let mut data = vec![0u8; length as usize];
            self.reader.read_exact(&mut data).await?;

            Ok(Frame::Data(DataFrame {
                stream_id,
                flags,
                data: Bytes::from(data),
            }))
        }
    }

    fn parse_control_frame(&mut self, frame_type: u16, flags: u8, payload: &[u8]) -> Result<Frame> {
        match frame_type {
            TYPE_SYN_STREAM => self.parse_syn_stream(flags, payload),
            TYPE_SYN_REPLY => self.parse_syn_reply(flags, payload),
            TYPE_RST_STREAM => parse_rst_stream(payload),
            TYPE_SETTINGS => parse_settings(flags, payload),
            TYPE_PING => parse_ping(payload),
            TYPE_GOAWAY => parse_goaway(payload),
            TYPE_HEADERS => self.parse_headers(flags, payload),
            TYPE_WINDOW_UPDATE => parse_window_update(payload),
            other => Err(Error::UnknownFrameType(other)),
        }
    }

    fn parse_syn_stream(&mut self, flags: u8, payload: &[u8]) -> Result<Frame> {
        if payload.len() < 10 {
            return Err(Error::Protocol("SynStream too short".to_string()));
        }
        let stream_id =
            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
        let associated_stream_id =
            u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) & 0x7fff_ffff;
        let priority = (payload[8] >> 5) & 0x7;
        // payload[9] is slot/credential slot, ignored
        let headers = self.decompress_headers(&payload[10..])?;
        Ok(Frame::SynStream(SynStreamFrame {
            stream_id,
            associated_stream_id,
            priority,
            headers,
            flags,
        }))
    }

    fn parse_syn_reply(&mut self, flags: u8, payload: &[u8]) -> Result<Frame> {
        if payload.len() < 4 {
            return Err(Error::Protocol("SynReply too short".to_string()));
        }
        let stream_id =
            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
        let headers = self.decompress_headers(&payload[4..])?;
        Ok(Frame::SynReply(SynReplyFrame {
            stream_id,
            headers,
            flags,
        }))
    }

    fn parse_headers(&mut self, flags: u8, payload: &[u8]) -> Result<Frame> {
        if payload.len() < 4 {
            return Err(Error::Protocol("Headers frame too short".to_string()));
        }
        let stream_id =
            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
        let headers = self.decompress_headers(&payload[4..])?;
        Ok(Frame::Headers(HeadersFrame {
            stream_id,
            headers,
            flags,
        }))
    }
}

fn parse_rst_stream(payload: &[u8]) -> Result<Frame> {
    if payload.len() < 8 {
        return Err(Error::Protocol("RstStream too short".to_string()));
    }
    let stream_id =
        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
    let status_code = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
    let status = RstStatus::try_from(status_code)
        .map_err(|_| Error::Protocol(format!("unknown RST_STREAM status: {status_code}")))?;
    Ok(Frame::RstStream(RstStreamFrame { stream_id, status }))
}

fn parse_settings(flags: u8, payload: &[u8]) -> Result<Frame> {
    if payload.len() < 4 {
        return Err(Error::Protocol("Settings too short".to_string()));
    }
    let count = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    if payload.len() < 4 + count * 8 {
        return Err(Error::Protocol("Settings payload truncated".to_string()));
    }
    let mut settings = Vec::with_capacity(count);
    for i in 0..count {
        let offset = 4 + i * 8;
        let id_flags = u32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]);
        let setting_flags = ((id_flags >> 24) & 0xff) as u8;
        let setting_id = id_flags & 0x00ff_ffff;
        let value = u32::from_be_bytes([
            payload[offset + 4],
            payload[offset + 5],
            payload[offset + 6],
            payload[offset + 7],
        ]);
        settings.push(Setting {
            id: setting_id,
            flags: setting_flags,
            value,
        });
    }
    Ok(Frame::Settings(SettingsFrame { settings, flags }))
}

fn parse_ping(payload: &[u8]) -> Result<Frame> {
    if payload.len() < 4 {
        return Err(Error::Protocol("Ping too short".to_string()));
    }
    let id = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    Ok(Frame::Ping(PingFrame { id }))
}

fn parse_goaway(payload: &[u8]) -> Result<Frame> {
    if payload.len() < 8 {
        return Err(Error::Protocol("GoAway too short".to_string()));
    }
    let last_good_stream_id =
        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
    let status_code = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
    let status = GoAwayStatus::try_from(status_code)
        .map_err(|_| Error::Protocol(format!("unknown GOAWAY status: {status_code}")))?;
    Ok(Frame::GoAway(GoAwayFrame {
        last_good_stream_id,
        status,
    }))
}

fn parse_window_update(payload: &[u8]) -> Result<Frame> {
    if payload.len() < 8 {
        return Err(Error::Protocol("WindowUpdate too short".to_string()));
    }
    let stream_id =
        u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & 0x7fff_ffff;
    let delta =
        u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]) & 0x7fff_ffff;
    Ok(Frame::WindowUpdate(WindowUpdateFrame { stream_id, delta }))
}

/// Parse a raw (decompressed) header block into an `http::HeaderMap`.
fn parse_header_block(raw: &[u8]) -> Result<HeaderMap> {
    if raw.len() < 4 {
        return Err(Error::Protocol("header block too short".to_string()));
    }
    let count = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let mut map = HeaderMap::new();
    let mut pos = 4usize;

    for _ in 0..count {
        if pos + 4 > raw.len() {
            return Err(Error::Protocol("header name length truncated".to_string()));
        }
        let name_len =
            u32::from_be_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]) as usize;
        pos += 4;
        if pos + name_len > raw.len() {
            return Err(Error::Protocol("header name truncated".to_string()));
        }
        let name_bytes = &raw[pos..pos + name_len];
        pos += name_len;

        if pos + 4 > raw.len() {
            return Err(Error::Protocol("header value length truncated".to_string()));
        }
        let val_len =
            u32::from_be_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]) as usize;
        pos += 4;
        if pos + val_len > raw.len() {
            return Err(Error::Protocol("header value truncated".to_string()));
        }
        let val_bytes = &raw[pos..pos + val_len];
        pos += val_len;

        let name = HeaderName::from_bytes(name_bytes)
            .map_err(|e| Error::Protocol(format!("invalid header name: {e}")))?;

        // Values may be NUL-joined; split and insert each.
        for part in val_bytes.split(|&b| b == 0) {
            let value = HeaderValue::from_bytes(part)
                .map_err(|e| Error::Protocol(format!("invalid header value: {e}")))?;
            map.append(name.clone(), value);
        }
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header;

    /// Create an encoder/decoder pair using an in-memory duplex channel.
    /// The buffer size must be large enough to hold any single frame written.
    fn make_framer_pair_with_buf(buf_size: usize) -> (
        Framer<tokio::io::WriteHalf<tokio::io::DuplexStream>, tokio::io::Empty>,
        Framer<tokio::io::Sink, tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    ) {
        let (client, server) = tokio::io::duplex(buf_size);
        let (server_read, _server_write) = tokio::io::split(server);
        let (_client_read, client_write) = tokio::io::split(client);
        let encoder = Framer::new(client_write, tokio::io::empty());
        let decoder = Framer::new(tokio::io::sink(), server_read);
        (encoder, decoder)
    }

    fn make_framer_pair() -> (
        Framer<tokio::io::WriteHalf<tokio::io::DuplexStream>, tokio::io::Empty>,
        Framer<tokio::io::Sink, tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    ) {
        make_framer_pair_with_buf(65536)
    }

    #[tokio::test]
    async fn test_round_trip_syn_stream() {
        let (mut encoder, mut decoder) = make_framer_pair();

        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "example.com".parse().unwrap());
        headers.insert(header::ACCEPT, "text/html".parse().unwrap());
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

        let frame = Frame::SynStream(SynStreamFrame {
            stream_id: 1,
            associated_stream_id: 0,
            priority: 3,
            headers: headers.clone(),
            flags: FLAG_FIN,
        });

        encoder.write_frame(&frame).await.unwrap();
        let decoded = decoder.read_frame().await.unwrap();
        assert_eq!(frame, decoded);
    }

    #[tokio::test]
    async fn test_round_trip_syn_reply() {
        let (mut encoder, mut decoder) = make_framer_pair();

        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "text/html".parse().unwrap());
        headers.insert(header::CONTENT_LENGTH, "42".parse().unwrap());

        let frame = Frame::SynReply(SynReplyFrame {
            stream_id: 1,
            headers: headers.clone(),
            flags: 0,
        });

        encoder.write_frame(&frame).await.unwrap();
        let decoded = decoder.read_frame().await.unwrap();
        assert_eq!(frame, decoded);
    }

    #[tokio::test]
    async fn test_round_trip_data() {
        let (mut encoder, mut decoder) = make_framer_pair();

        let frame = Frame::Data(DataFrame {
            stream_id: 1,
            flags: DATA_FLAG_FIN,
            data: Bytes::from("Hello, world!"),
        });

        encoder.write_frame(&frame).await.unwrap();
        let decoded = decoder.read_frame().await.unwrap();
        assert_eq!(frame, decoded);
    }

    #[tokio::test]
    async fn test_round_trip_goaway() {
        let (mut encoder, mut decoder) = make_framer_pair();

        let frame = Frame::GoAway(GoAwayFrame {
            last_good_stream_id: 42,
            status: GoAwayStatus::Ok,
        });

        encoder.write_frame(&frame).await.unwrap();
        let decoded = decoder.read_frame().await.unwrap();
        assert_eq!(frame, decoded);
    }

    #[tokio::test]
    async fn test_round_trip_rst_stream() {
        let (mut encoder, mut decoder) = make_framer_pair();

        let frame = Frame::RstStream(RstStreamFrame {
            stream_id: 3,
            status: RstStatus::Cancel,
        });

        encoder.write_frame(&frame).await.unwrap();
        let decoded = decoder.read_frame().await.unwrap();
        assert_eq!(frame, decoded);
    }

    #[tokio::test]
    async fn test_round_trip_ping() {
        let (mut encoder, mut decoder) = make_framer_pair();

        let frame = Frame::Ping(PingFrame { id: 1 });

        encoder.write_frame(&frame).await.unwrap();
        let decoded = decoder.read_frame().await.unwrap();
        assert_eq!(frame, decoded);
    }

    #[tokio::test]
    async fn test_round_trip_window_update() {
        let (mut encoder, mut decoder) = make_framer_pair();

        let frame = Frame::WindowUpdate(WindowUpdateFrame {
            stream_id: 5,
            delta: 65536,
        });

        encoder.write_frame(&frame).await.unwrap();
        let decoded = decoder.read_frame().await.unwrap();
        assert_eq!(frame, decoded);
    }

    #[tokio::test]
    async fn test_multi_frame_header_compression() {
        // Encode multiple frames with the same encoder (stateful compressor),
        // decode with a matching decoder (stateful decompressor).
        let (mut encoder, mut decoder) = make_framer_pair();

        let mut headers1 = HeaderMap::new();
        headers1.insert(header::HOST, "example.com".parse().unwrap());
        headers1.insert(header::ACCEPT, "text/html".parse().unwrap());

        let mut headers2 = HeaderMap::new();
        headers2.insert(header::HOST, "example.com".parse().unwrap());
        headers2.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

        let frame1 = Frame::SynStream(SynStreamFrame {
            stream_id: 1,
            associated_stream_id: 0,
            priority: 0,
            headers: headers1.clone(),
            flags: 0,
        });
        let frame2 = Frame::SynStream(SynStreamFrame {
            stream_id: 3,
            associated_stream_id: 0,
            priority: 0,
            headers: headers2.clone(),
            flags: 0,
        });

        encoder.write_frame(&frame1).await.unwrap();
        encoder.write_frame(&frame2).await.unwrap();

        let decoded1 = decoder.read_frame().await.unwrap();
        let decoded2 = decoder.read_frame().await.unwrap();

        assert_eq!(frame1, decoded1);
        assert_eq!(frame2, decoded2);
    }

    #[tokio::test]
    async fn test_large_data_frame() {
        // Use a large enough buffer (128 KiB data + 8 byte header).
        let (mut encoder, mut decoder) = make_framer_pair_with_buf(256 * 1024);

        let data = vec![0xABu8; 128 * 1024]; // 128 KiB
        let frame = Frame::Data(DataFrame {
            stream_id: 1,
            flags: 0,
            data: Bytes::from(data),
        });

        encoder.write_frame(&frame).await.unwrap();
        let decoded = decoder.read_frame().await.unwrap();
        assert_eq!(frame, decoded);
    }
}
