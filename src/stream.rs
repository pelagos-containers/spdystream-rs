use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, watch, Mutex, Notify};

use crate::error::{Error, Result};
use crate::frame::{DataFrame, Frame, RstStatus, RstStreamFrame, SynReplyFrame, DATA_FLAG_FIN, FLAG_FIN};

pub struct Stream {
    pub stream_id: u32,
    pub headers: http::HeaderMap,
    pub priority: u8,
    data_tx: mpsc::Sender<Bytes>,
    pub(crate) data_rx: Mutex<mpsc::Receiver<Bytes>>,
    write_tx: mpsc::Sender<Frame>,
    pub(crate) local_fin_sent: AtomicBool,
    pub(crate) replied: AtomicBool,
    pub(crate) reply_notify: Notify,
    remote_closed_tx: watch::Sender<bool>,
    pub(crate) remote_closed_rx: watch::Receiver<bool>,
    /// Partial-frame remainder buffer for AsyncRead.
    unread: Mutex<Option<Bytes>>,
}

impl Stream {
    pub(crate) fn new(
        stream_id: u32,
        headers: http::HeaderMap,
        priority: u8,
        write_tx: mpsc::Sender<Frame>,
    ) -> Self {
        let (data_tx, data_rx) = mpsc::channel(64);
        let (remote_closed_tx, remote_closed_rx) = watch::channel(false);
        Self {
            stream_id,
            headers,
            priority,
            data_tx,
            data_rx: Mutex::new(data_rx),
            write_tx,
            local_fin_sent: AtomicBool::new(false),
            replied: AtomicBool::new(false),
            reply_notify: Notify::new(),
            remote_closed_tx,
            remote_closed_rx,
            unread: Mutex::new(None),
        }
    }

    /// Push data received from the remote into this stream's receive buffer.
    pub(crate) fn push_data(&self, data: Bytes) {
        let _ = self.data_tx.try_send(data);
    }

    /// Mark remote side as closed (FIN received).
    pub(crate) fn close_remote(&self) {
        let _ = self.remote_closed_tx.send(true);
    }

    /// Send RST_STREAM.
    pub async fn reset(&self, status: RstStatus) -> Result<()> {
        let frame = Frame::RstStream(RstStreamFrame {
            stream_id: self.stream_id,
            status,
        });
        self.write_tx
            .send(frame)
            .await
            .map_err(|_| Error::ConnectionClosed)
    }

    /// Whether the remote side has closed.
    pub fn is_remote_closed(&self) -> bool {
        *self.remote_closed_rx.borrow()
    }

    /// Whether a local FIN has been sent.
    pub fn is_local_fin_sent(&self) -> bool {
        self.local_fin_sent.load(Ordering::Acquire)
    }

    /// Send SYN_REPLY with optional FIN flag.
    pub async fn send_reply(&self, headers: http::HeaderMap, fin: bool) -> Result<()> {
        let frame = Frame::SynReply(SynReplyFrame {
            stream_id: self.stream_id,
            headers,
            flags: if fin { FLAG_FIN } else { 0 },
        });
        self.write_tx
            .send(frame)
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        self.replied.store(true, Ordering::Release);
        self.reply_notify.notify_waiters();
        if fin {
            self.local_fin_sent.store(true, Ordering::Release);
        }
        Ok(())
    }

    /// Write a DATA frame. Blocks until send_reply has been called.
    pub async fn write_data(&self, data: Bytes, fin: bool) -> Result<()> {
        // Wait until we have sent a reply.
        while !self.replied.load(Ordering::Acquire) {
            // notify_waiters fires on the *current* batch of waiters; we need
            // notified() before re-checking, so we use a loop with notified().
            let notified = self.reply_notify.notified();
            if self.replied.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }

        if self.local_fin_sent.load(Ordering::Acquire) {
            return Err(Error::StreamClosed);
        }
        if fin {
            self.local_fin_sent.store(true, Ordering::Release);
        }
        let frame = Frame::Data(DataFrame {
            stream_id: self.stream_id,
            flags: if fin { DATA_FLAG_FIN } else { 0 },
            data,
        });
        self.write_tx
            .send(frame)
            .await
            .map_err(|_| Error::ConnectionClosed)
    }

    /// Read a DATA frame from the remote. Returns None on EOF.
    pub async fn read_data(&self) -> Result<Option<Bytes>> {
        let mut rx = self.data_rx.lock().await;

        // Non-blocking attempt first.
        match rx.try_recv() {
            Ok(data) => return Ok(Some(data)),
            Err(mpsc::error::TryRecvError::Disconnected) => return Ok(None),
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        // Check if remote already closed.
        if *self.remote_closed_rx.borrow() {
            return Ok(None);
        }

        // Wait for data or remote close.
        let mut closed_rx = self.remote_closed_rx.clone();
        tokio::select! {
            data = rx.recv() => {
                Ok(data.map(Some).unwrap_or(None))
            }
            result = closed_rx.changed() => {
                let _ = result;
                // Drain any data that arrived just before close.
                match rx.try_recv() {
                    Ok(data) => Ok(Some(data)),
                    _ => Ok(None),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncRead
// ---------------------------------------------------------------------------

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        // Check unread remainder buffer.
        if let Ok(mut unread) = this.unread.try_lock() {
            if let Some(ref mut bytes) = *unread {
                let n = bytes.len().min(buf.remaining());
                buf.put_slice(&bytes[..n]);
                if n == bytes.len() {
                    *unread = None;
                } else {
                    *bytes = bytes.slice(n..);
                }
                return Poll::Ready(Ok(()));
            }
        }

        // Poll the data channel.
        let mut rx = match this.data_rx.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        };

        match rx.poll_recv(cx) {
            Poll::Ready(Some(data)) => {
                let n = data.len().min(buf.remaining());
                buf.put_slice(&data[..n]);
                if n < data.len() {
                    if let Ok(mut unread) = this.unread.try_lock() {
                        *unread = Some(data.slice(n..));
                    }
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => {
                if *this.remote_closed_rx.borrow() {
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AsyncWrite
// ---------------------------------------------------------------------------

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match this.write_tx.try_send(Frame::Data(DataFrame {
            stream_id: this.stream_id,
            flags: 0,
            data: Bytes::copy_from_slice(buf),
        })) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(mpsc::error::TrySendError::Full(_)) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Poll::Ready(Err(
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "connection closed"),
            )),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.local_fin_sent.load(Ordering::Acquire) {
            return Poll::Ready(Ok(()));
        }
        match this.write_tx.try_send(Frame::Data(DataFrame {
            stream_id: this.stream_id,
            flags: DATA_FLAG_FIN,
            data: Bytes::new(),
        })) {
            Ok(()) => {
                this.local_fin_sent.store(true, Ordering::Release);
                Poll::Ready(Ok(()))
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Poll::Ready(Err(
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "connection closed"),
            )),
        }
    }
}
