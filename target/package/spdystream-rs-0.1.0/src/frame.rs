use bytes::Bytes;
use http::HeaderMap;

pub const FLAG_FIN: u8 = 0x01;
pub const FLAG_UNIDIRECTIONAL: u8 = 0x02;
pub const DATA_FLAG_FIN: u8 = 0x01;

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    SynStream(SynStreamFrame),
    SynReply(SynReplyFrame),
    RstStream(RstStreamFrame),
    Settings(SettingsFrame),
    Ping(PingFrame),
    GoAway(GoAwayFrame),
    Headers(HeadersFrame),
    WindowUpdate(WindowUpdateFrame),
    Data(DataFrame),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SynStreamFrame {
    pub stream_id: u32,
    pub associated_stream_id: u32,
    pub priority: u8,
    pub headers: HeaderMap,
    pub flags: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SynReplyFrame {
    pub stream_id: u32,
    pub headers: HeaderMap,
    pub flags: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RstStreamFrame {
    pub stream_id: u32,
    pub status: RstStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SettingsFrame {
    pub settings: Vec<Setting>,
    pub flags: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Setting {
    pub id: u32,
    pub flags: u8,
    pub value: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PingFrame {
    pub id: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GoAwayFrame {
    pub last_good_stream_id: u32,
    pub status: GoAwayStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeadersFrame {
    pub stream_id: u32,
    pub headers: HeaderMap,
    pub flags: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowUpdateFrame {
    pub stream_id: u32,
    pub delta: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataFrame {
    pub stream_id: u32,
    pub flags: u8,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Copy)]
#[repr(u32)]
pub enum RstStatus {
    ProtocolError = 1,
    InvalidStream = 2,
    RefusedStream = 3,
    UnsupportedVersion = 4,
    Cancel = 5,
    InternalError = 6,
    FlowControlError = 7,
    StreamInUse = 8,
    StreamAlreadyClosed = 9,
    InvalidCredentials = 10,
    FrameTooLarge = 11,
}

impl TryFrom<u32> for RstStatus {
    type Error = u32;
    fn try_from(v: u32) -> Result<Self, u32> {
        match v {
            1 => Ok(Self::ProtocolError),
            2 => Ok(Self::InvalidStream),
            3 => Ok(Self::RefusedStream),
            4 => Ok(Self::UnsupportedVersion),
            5 => Ok(Self::Cancel),
            6 => Ok(Self::InternalError),
            7 => Ok(Self::FlowControlError),
            8 => Ok(Self::StreamInUse),
            9 => Ok(Self::StreamAlreadyClosed),
            10 => Ok(Self::InvalidCredentials),
            11 => Ok(Self::FrameTooLarge),
            _ => Err(v),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
#[repr(u32)]
pub enum GoAwayStatus {
    Ok = 0,
    ProtocolError = 1,
    InternalError = 2,
}

impl TryFrom<u32> for GoAwayStatus {
    type Error = u32;
    fn try_from(v: u32) -> Result<Self, u32> {
        match v {
            0 => Ok(Self::Ok),
            1 => Ok(Self::ProtocolError),
            2 => Ok(Self::InternalError),
            _ => Ok(Self::Ok), // treat unknown as Ok per spec
        }
    }
}
