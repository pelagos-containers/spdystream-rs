use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("compression error: {0}")]
    Compression(String),
    #[error("unknown frame type: {0}")]
    UnknownFrameType(u16),
    #[error("stream closed")]
    StreamClosed,
    #[error("connection closed")]
    ConnectionClosed,
}

pub type Result<T> = std::result::Result<T, Error>;
