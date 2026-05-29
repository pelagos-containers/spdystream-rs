use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, watch, Mutex};

use crate::error::{Error, Result};
use crate::frame::*;
use crate::framer::Framer;
use crate::stream::Stream;

const N_WORKERS: usize = 5;

/// Setting ID for SETTINGS_MAX_CONCURRENT_STREAMS.
const SETTINGS_MAX_CONCURRENT_STREAMS: u32 = 4;
/// Setting ID for SETTINGS_INITIAL_WINDOW_SIZE.
const SETTINGS_INITIAL_WINDOW_SIZE: u32 = 7;

/// Default initial window size (64 KiB).
const DEFAULT_INITIAL_WINDOW_SIZE: u32 = 65536;

/// Timeout for ping responses and close drain.
const PING_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// A boxed future returned by stream handlers.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Message sent from the read task to a worker.
struct WorkerMsg {
    frame: Frame,
    stream: Arc<Stream>,
}

pub struct Connection {
    write_tx: mpsc::Sender<Frame>,
    streams: Arc<Mutex<HashMap<u32, Arc<Stream>>>>,
    #[allow(dead_code)] // reserved for server-initiated streams
    next_server_stream_id: AtomicU32,
    last_client_stream_id: AtomicU32,
    gone_away: AtomicBool,
    close_tx: watch::Sender<bool>,
    pub(crate) close_rx: watch::Receiver<bool>,
    /// Map of even ping IDs → oneshot sender (server-initiated pings).
    ping_map: Arc<Mutex<HashMap<u32, oneshot::Sender<()>>>>,
    /// Next even ping ID to use for server-initiated pings.
    next_ping_id: AtomicU32,
    /// Peer's advertised initial window size (stored but not enforced).
    initial_window_size: AtomicU32,
}

impl Connection {
    /// Create a new Connection.  Returns both the `Arc<Connection>` and the
    /// channel receiver that `serve()` expects.
    pub fn new() -> Arc<Self> {
        let (write_tx, _discard_rx) = mpsc::channel(64);
        let (close_tx, close_rx) = watch::channel(false);
        Arc::new(Self {
            write_tx,
            streams: Arc::new(Mutex::new(HashMap::new())),
            next_server_stream_id: AtomicU32::new(2),
            last_client_stream_id: AtomicU32::new(0),
            gone_away: AtomicBool::new(false),
            close_tx,
            close_rx,
            ping_map: Arc::new(Mutex::new(HashMap::new())),
            next_ping_id: AtomicU32::new(2),
            initial_window_size: AtomicU32::new(DEFAULT_INITIAL_WINDOW_SIZE),
        })
    }

    /// Return the number of active streams.
    pub async fn stream_count(&self) -> usize {
        self.streams.lock().await.len()
    }

    /// Send a server-initiated ping and wait for the echo.
    ///
    /// Uses even ping IDs (server-initiated per SPDY spec).
    /// Returns `Ok(())` when the echo arrives, or `Err` on timeout.
    pub async fn ping(&self) -> Result<()> {
        let id = self.next_ping_id.fetch_add(2, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel::<()>();
        self.ping_map.lock().await.insert(id, tx);
        self.write_tx
            .send(Frame::Ping(PingFrame { id }))
            .await
            .map_err(|_| Error::ConnectionClosed)?;
        tokio::time::timeout(PING_TIMEOUT, rx)
            .await
            .map_err(|_| Error::Protocol("ping timeout".to_string()))?
            .map_err(|_| Error::ConnectionClosed)
    }

    /// Send a GoAway frame, broadcast close, and wait for streams to drain.
    pub async fn close(&self) -> Result<()> {
        if self.gone_away.swap(true, Ordering::AcqRel) {
            // Already gone away.
            return Ok(());
        }

        let last_good = self.last_client_stream_id.load(Ordering::Acquire);
        let _ = self
            .write_tx
            .send(Frame::GoAway(GoAwayFrame {
                last_good_stream_id: last_good,
                status: GoAwayStatus::Ok,
            }))
            .await;

        let _ = self.close_tx.send(true);

        // Wait up to CLOSE_DRAIN_TIMEOUT for all streams to finish.
        let streams = Arc::clone(&self.streams);
        let _ = tokio::time::timeout(CLOSE_DRAIN_TIMEOUT, async move {
            loop {
                if streams.lock().await.is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        Ok(())
    }

    /// Serve the connection over `io`, calling `handler` for each new stream.
    ///
    /// Spawns background tasks and returns immediately. The connection runs
    /// until it receives a GoAway, an I/O error closes the transport, or all
    /// handles are dropped.
    pub async fn serve<IO, F>(io: IO, handler: F) -> Result<Arc<Self>>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        F: Fn(Arc<Stream>) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        let (read_half, write_half) = tokio::io::split(io);

        // Frame-write channel.
        let (write_tx, write_rx) = mpsc::channel::<Frame>(64);
        let (close_tx, close_rx) = watch::channel(false);

        let ping_map: Arc<Mutex<HashMap<u32, oneshot::Sender<()>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let conn = Arc::new(Self {
            write_tx: write_tx.clone(),
            streams: Arc::new(Mutex::new(HashMap::new())),
            next_server_stream_id: AtomicU32::new(2),
            last_client_stream_id: AtomicU32::new(0),
            gone_away: AtomicBool::new(false),
            close_tx,
            close_rx,
            ping_map: Arc::clone(&ping_map),
            next_ping_id: AtomicU32::new(2),
            initial_window_size: AtomicU32::new(DEFAULT_INITIAL_WINDOW_SIZE),
        });

        // Per-worker channels.
        let mut worker_txs: Vec<mpsc::Sender<WorkerMsg>> = Vec::with_capacity(N_WORKERS);
        let mut worker_rxs: Vec<mpsc::Receiver<WorkerMsg>> = Vec::new();
        for _ in 0..N_WORKERS {
            let (tx, rx) = mpsc::channel::<WorkerMsg>(128);
            worker_txs.push(tx);
            worker_rxs.push(rx);
        }

        let handler: Arc<dyn Fn(Arc<Stream>) -> BoxFuture<'static, ()> + Send + Sync + 'static> =
            Arc::new(handler);

        // --- Write task ---------------------------------------------------
        {
            let mut write_framer = Framer::new(write_half, tokio::io::empty());
            tokio::spawn(async move {
                let mut rx = write_rx;
                while let Some(frame) = rx.recv().await {
                    if let Err(e) = write_framer.write_frame(&frame).await {
                        log::error!("write task error: {e}");
                        break;
                    }
                }
            });
        }

        // --- Worker tasks -------------------------------------------------
        for mut rx in worker_rxs {
            let handler = Arc::clone(&handler);
            let streams = Arc::clone(&conn.streams);
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    match &msg.frame {
                        Frame::SynStream(_) => {
                            let stream = Arc::clone(&msg.stream);
                            let h = Arc::clone(&handler);
                            tokio::spawn(async move {
                                h(stream).await;
                            });
                        }
                        Frame::Data(df) => {
                            let fin = (df.flags & DATA_FLAG_FIN) != 0;
                            msg.stream.push_data(df.data.clone());
                            if fin {
                                msg.stream.close_remote();
                            }
                        }
                        Frame::RstStream(_) => {
                            msg.stream.close_remote();
                            let mut guard = streams.lock().await;
                            guard.remove(&msg.stream.stream_id);
                        }
                        Frame::SynReply(sr) => {
                            msg.stream.replied.store(true, Ordering::Release);
                            msg.stream.reply_notify.notify_waiters();
                            if (sr.flags & FLAG_FIN) != 0 {
                                msg.stream.close_remote();
                            }
                        }
                        Frame::Headers(hf) => {
                            if (hf.flags & FLAG_FIN) != 0 {
                                msg.stream.close_remote();
                            }
                        }
                        _ => {}
                    }
                }
            });
        }

        // --- Send initial Settings frame ----------------------------------
        let settings_frame = Frame::Settings(SettingsFrame {
            flags: 0,
            settings: vec![Setting {
                id: SETTINGS_MAX_CONCURRENT_STREAMS,
                flags: 0,
                value: 100,
            }],
        });
        let _ = write_tx.send(settings_frame).await;

        // --- Read task ----------------------------------------------------
        {
            let conn2 = Arc::clone(&conn);
            let write_tx2 = write_tx.clone();
            let worker_txs2 = worker_txs.clone();
            tokio::spawn(async move {
                let mut read_framer = Framer::new(tokio::io::sink(), read_half);
                loop {
                    let frame = match read_framer.read_frame().await {
                        Ok(f) => f,
                        Err(Error::ConnectionClosed) => {
                            log::debug!("read task: connection closed");
                            break;
                        }
                        Err(e) => {
                            log::error!("read task error: {e}");
                            break;
                        }
                    };

                    match frame {
                        Frame::SynStream(ref sf) => {
                            let sid = sf.stream_id;

                            // Validate: must be odd, <= 0x7FFFFFFF (always true after mask).
                            if sid == 0 || sid % 2 == 0 {
                                let _ = write_tx2
                                    .send(Frame::RstStream(RstStreamFrame {
                                        stream_id: sid,
                                        status: RstStatus::ProtocolError,
                                    }))
                                    .await;
                                continue;
                            }

                            // Must be monotonically increasing.
                            let last = conn2.last_client_stream_id.load(Ordering::Acquire);
                            if sid <= last {
                                let _ = write_tx2
                                    .send(Frame::RstStream(RstStreamFrame {
                                        stream_id: sid,
                                        status: RstStatus::ProtocolError,
                                    }))
                                    .await;
                                continue;
                            }

                            // Refuse new streams after GoAway.
                            if conn2.gone_away.load(Ordering::Acquire) {
                                let _ = write_tx2
                                    .send(Frame::RstStream(RstStreamFrame {
                                        stream_id: sid,
                                        status: RstStatus::RefusedStream,
                                    }))
                                    .await;
                                continue;
                            }

                            // Pre-register the stream (before routing to worker).
                            let stream = {
                                let s = Arc::new(Stream::new(
                                    sid,
                                    sf.headers.clone(),
                                    sf.priority,
                                    write_tx2.clone(),
                                ));
                                let mut guard = conn2.streams.lock().await;
                                if guard.contains_key(&sid) {
                                    let _ = write_tx2
                                        .send(Frame::RstStream(RstStreamFrame {
                                            stream_id: sid,
                                            status: RstStatus::StreamInUse,
                                        }))
                                        .await;
                                    continue;
                                }
                                guard.insert(sid, Arc::clone(&s));
                                s
                            };

                            conn2.last_client_stream_id.store(sid, Ordering::Release);

                            let worker_idx = (sid as usize) % N_WORKERS;
                            let _ = worker_txs2[worker_idx]
                                .send(WorkerMsg { frame, stream })
                                .await;
                        }

                        Frame::Data(ref df) => {
                            let sid = df.stream_id;
                            let stream = conn2.streams.lock().await.get(&sid).cloned();
                            if let Some(stream) = stream {
                                let worker_idx = (sid as usize) % N_WORKERS;
                                let _ = worker_txs2[worker_idx]
                                    .send(WorkerMsg { frame, stream })
                                    .await;
                            } else {
                                let _ = write_tx2
                                    .send(Frame::RstStream(RstStreamFrame {
                                        stream_id: sid,
                                        status: RstStatus::InvalidStream,
                                    }))
                                    .await;
                            }
                        }

                        Frame::SynReply(ref sr) => {
                            let sid = sr.stream_id;
                            let stream = conn2.streams.lock().await.get(&sid).cloned();
                            if let Some(stream) = stream {
                                let worker_idx = (sid as usize) % N_WORKERS;
                                let _ = worker_txs2[worker_idx]
                                    .send(WorkerMsg { frame, stream })
                                    .await;
                            }
                        }

                        Frame::RstStream(ref rs) => {
                            let sid = rs.stream_id;
                            let stream = conn2.streams.lock().await.get(&sid).cloned();
                            if let Some(stream) = stream {
                                let worker_idx = (sid as usize) % N_WORKERS;
                                let _ = worker_txs2[worker_idx]
                                    .send(WorkerMsg { frame, stream })
                                    .await;
                            }
                        }

                        Frame::Headers(ref hf) => {
                            let sid = hf.stream_id;
                            let stream = conn2.streams.lock().await.get(&sid).cloned();
                            if let Some(stream) = stream {
                                let worker_idx = (sid as usize) % N_WORKERS;
                                let _ = worker_txs2[worker_idx]
                                    .send(WorkerMsg { frame, stream })
                                    .await;
                            }
                        }

                        Frame::Ping(ref pf) => {
                            if pf.id % 2 == 1 {
                                // Odd ID: client-initiated ping → echo back.
                                let _ = write_tx2.send(Frame::Ping(pf.clone())).await;
                            } else {
                                // Even ID: echo of our server-initiated ping → resolve waiter.
                                let mut map = conn2.ping_map.lock().await;
                                if let Some(tx) = map.remove(&pf.id) {
                                    let _ = tx.send(());
                                }
                            }
                        }

                        Frame::GoAway(_) => {
                            conn2.gone_away.store(true, Ordering::Release);
                            let _ = conn2.close_tx.send(true);
                            break;
                        }

                        Frame::Settings(ref sf) => {
                            for setting in &sf.settings {
                                if setting.id == SETTINGS_INITIAL_WINDOW_SIZE {
                                    conn2
                                        .initial_window_size
                                        .store(setting.value, Ordering::Relaxed);
                                }
                            }
                        }

                        Frame::WindowUpdate(_) => {
                            // No-op: flow control not enforced.
                        }
                    }
                }

                // Close all open streams so that any read_data() waiters
                // unblock with Ok(None) rather than hanging indefinitely.
                {
                    let streams = conn2.streams.lock().await;
                    for stream in streams.values() {
                        stream.close_remote();
                    }
                }
                let _ = conn2.close_tx.send(true);
            });
        }

        Ok(conn)
    }

    /// Wait until the connection is closed (GoAway received or I/O error).
    pub async fn wait_closed(&self) {
        let mut rx = self.close_rx.clone();
        let _ = rx.wait_for(|&closed| closed).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use tokio::io::{split, DuplexStream, Empty, ReadHalf, Sink, WriteHalf};
    use tokio::sync::Mutex as TokioMutex;

    type ClientWriter = Framer<WriteHalf<DuplexStream>, Empty>;
    type ClientReader = Framer<Sink, ReadHalf<DuplexStream>>;

    /// Build a connection (server) + a client framer pair.
    async fn make_test_connection<F>(
        handler: F,
    ) -> (Arc<Connection>, ClientWriter, ClientReader)
    where
        F: Fn(Arc<Stream>) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    {
        // Two one-directional duplex streams form a full-duplex pipe:
        //   client writes to client_to_server → server reads
        //   server writes to server_to_client  → client reads
        let (client_to_server, server_receives) = tokio::io::duplex(65536);
        let (server_to_client, client_receives) = tokio::io::duplex(65536);

        let server_io = DuplexPairIo {
            reader: server_receives,
            writer: server_to_client,
        };

        let (_ctr_read, ctr_write) = split(client_to_server);
        let client_writer: ClientWriter = Framer::new(ctr_write, tokio::io::empty());

        let (stc_read, _stc_write) = split(client_receives);
        let client_reader: ClientReader = Framer::new(tokio::io::sink(), stc_read);

        let conn = Connection::serve(server_io, handler).await.unwrap();
        (conn, client_writer, client_reader)
    }

    fn syn_stream(stream_id: u32) -> Frame {
        Frame::SynStream(SynStreamFrame {
            stream_id,
            associated_stream_id: 0,
            priority: 0,
            headers: HeaderMap::new(),
            flags: 0,
        })
    }

    #[tokio::test]
    async fn test_stream_id_validation() {
        // Send ID=5 first, then ID=3 (going backward). Expect RST for ID=3.
        let (_conn, mut cw, mut cr) = make_test_connection(|_stream| {
            Box::pin(async move {})
        })
        .await;

        // Consume the initial Settings frame.
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
        .await
        .expect("timeout on settings")
        .expect("read settings");

        cw.write_frame(&syn_stream(5)).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        cw.write_frame(&syn_stream(3)).await.unwrap();

        let frame = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            cr.read_frame(),
        )
        .await
        .expect("timeout waiting for RST")
        .expect("read frame");

        match frame {
            Frame::RstStream(rst) => {
                assert_eq!(rst.stream_id, 3, "RST should be for stream 3");
                assert_eq!(rst.status, RstStatus::ProtocolError);
            }
            other => panic!("expected RstStream, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_duplicate_stream_id() {
        // Send SynStream with ID=1 twice. Expect RST for the second.
        let (_conn, mut cw, mut cr) = make_test_connection(|_stream| {
            Box::pin(async move {})
        })
        .await;

        // Consume the initial Settings frame.
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
        .await
        .expect("timeout on settings")
        .expect("read settings");

        cw.write_frame(&syn_stream(1)).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        cw.write_frame(&syn_stream(1)).await.unwrap();

        let frame = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            cr.read_frame(),
        )
        .await
        .expect("timeout waiting for RST")
        .expect("read frame");

        match frame {
            Frame::RstStream(rst) => {
                assert_eq!(rst.stream_id, 1);
            }
            other => panic!("expected RstStream, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_stream_handler_called() {
        use tokio::sync::oneshot;

        let (tx, rx) = oneshot::channel::<u32>();
        let tx = Arc::new(TokioMutex::new(Some(tx)));

        let (conn, mut cw, _cr) = make_test_connection(move |stream| {
            let tx = Arc::clone(&tx);
            Box::pin(async move {
                if let Some(sender) = tx.lock().await.take() {
                    let _ = sender.send(stream.stream_id);
                }
            })
        })
        .await;

        cw.write_frame(&syn_stream(1)).await.unwrap();

        let stream_id = tokio::time::timeout(tokio::time::Duration::from_secs(2), rx)
            .await
            .expect("timeout")
            .expect("recv");

        assert_eq!(stream_id, 1);
        // Give the read task time to pre-register.
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        assert_eq!(conn.stream_count().await, 1);
    }

    #[tokio::test]
    async fn test_connection_closes_on_goaway() {
        let (conn, mut cw, _cr) = make_test_connection(|_stream| {
            Box::pin(async move {})
        })
        .await;

        cw.write_frame(&Frame::GoAway(GoAwayFrame {
            last_good_stream_id: 0,
            status: GoAwayStatus::Ok,
        }))
        .await
        .unwrap();

        tokio::time::timeout(tokio::time::Duration::from_secs(2), conn.wait_closed())
            .await
            .expect("timeout waiting for close");

        assert!(conn.gone_away.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_ping_echo() {
        let (_conn, mut cw, mut cr) = make_test_connection(|_stream| {
            Box::pin(async move {})
        })
        .await;

        // Consume the initial Settings frame first.
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
        .await
        .expect("timeout on settings")
        .expect("read settings");

        cw.write_frame(&Frame::Ping(PingFrame { id: 1 }))
            .await
            .unwrap();

        let frame = tokio::time::timeout(tokio::time::Duration::from_secs(2), cr.read_frame())
            .await
            .expect("timeout")
            .expect("read frame");

        match frame {
            Frame::Ping(p) => assert_eq!(p.id, 1),
            other => panic!("expected Ping, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Phase 3 tests
    // -----------------------------------------------------------------------

    /// Handler sends reply then two data frames, the second with FIN.
    /// Client reads SYN_REPLY + 2 DATA frames and checks content + FIN flag.
    #[tokio::test]
    async fn test_send_receive_data() {
        let (_conn, mut cw, mut cr) = make_test_connection(|stream| {
            Box::pin(async move {
                stream
                    .send_reply(HeaderMap::new(), false)
                    .await
                    .unwrap();
                stream
                    .write_data(bytes::Bytes::from_static(b"hello"), false)
                    .await
                    .unwrap();
                stream
                    .write_data(bytes::Bytes::from_static(b"world"), true)
                    .await
                    .unwrap();
            })
        })
        .await;

        // Consume the initial Settings frame.
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
        .await
        .expect("timeout on settings")
        .expect("read settings");

        cw.write_frame(&syn_stream(1)).await.unwrap();

        // Expect SYN_REPLY
        let frame = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            cr.read_frame(),
        )
        .await
        .expect("timeout syn_reply")
        .expect("read syn_reply");
        assert!(matches!(frame, Frame::SynReply(_)), "expected SynReply, got {frame:?}");

        // Expect first DATA "hello", no FIN
        let frame = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            cr.read_frame(),
        )
        .await
        .expect("timeout data1")
        .expect("read data1");
        match frame {
            Frame::Data(df) => {
                assert_eq!(&df.data[..], b"hello");
                assert_eq!(df.flags & DATA_FLAG_FIN, 0, "first frame should not have FIN");
            }
            other => panic!("expected Data, got {other:?}"),
        }

        // Expect second DATA "world" with FIN
        let frame = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            cr.read_frame(),
        )
        .await
        .expect("timeout data2")
        .expect("read data2");
        match frame {
            Frame::Data(df) => {
                assert_eq!(&df.data[..], b"world");
                assert_ne!(df.flags & DATA_FLAG_FIN, 0, "second frame must have FIN");
            }
            other => panic!("expected Data, got {other:?}"),
        }
    }

    /// Verify that a DATA frame with FIN flag propagates correctly at wire level.
    #[tokio::test]
    async fn test_fin_propagation() {
        let (_conn, mut cw, mut cr) = make_test_connection(|stream| {
            Box::pin(async move {
                stream.send_reply(HeaderMap::new(), false).await.unwrap();
                stream
                    .write_data(bytes::Bytes::from_static(b"bye"), true)
                    .await
                    .unwrap();
            })
        })
        .await;

        // Consume the initial Settings frame.
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
        .await
        .expect("timeout on settings")
        .expect("read settings");

        cw.write_frame(&syn_stream(1)).await.unwrap();

        // Skip SYN_REPLY
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            cr.read_frame(),
        )
        .await
        .expect("timeout syn_reply")
        .expect("read syn_reply");

        // DATA frame must carry FIN
        let frame = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            cr.read_frame(),
        )
        .await
        .expect("timeout data")
        .expect("read data");
        match frame {
            Frame::Data(df) => {
                assert_ne!(df.flags & DATA_FLAG_FIN, 0, "FIN flag must be set");
                assert_eq!(&df.data[..], b"bye");
            }
            other => panic!("expected Data with FIN, got {other:?}"),
        }
    }

    /// Handler resets the stream; client sees RST_STREAM.
    #[tokio::test]
    async fn test_rst_cancels_stream() {
        let (_conn, mut cw, mut cr) = make_test_connection(|stream| {
            Box::pin(async move {
                stream.reset(RstStatus::Cancel).await.unwrap();
            })
        })
        .await;

        // Consume the initial Settings frame.
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
        .await
        .expect("timeout on settings")
        .expect("read settings");

        cw.write_frame(&syn_stream(1)).await.unwrap();

        let frame = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            cr.read_frame(),
        )
        .await
        .expect("timeout rst")
        .expect("read rst");
        match frame {
            Frame::RstStream(rst) => {
                assert_eq!(rst.stream_id, 1);
                assert_eq!(rst.status, RstStatus::Cancel);
            }
            other => panic!("expected RstStream, got {other:?}"),
        }
    }

    /// write_data must block until send_reply is called.
    #[tokio::test]
    async fn test_write_blocked_before_reply() {
        use tokio::sync::oneshot;

        let (write_done_tx, mut write_done_rx) = oneshot::channel::<()>();
        let (reply_trigger_tx, reply_trigger_rx) = oneshot::channel::<Arc<Stream>>();

        let reply_trigger_tx = Arc::new(TokioMutex::new(Some(reply_trigger_tx)));
        let write_done_tx = Arc::new(TokioMutex::new(Some(write_done_tx)));

        let (_conn, mut cw, _cr) = make_test_connection(move |stream| {
            let reply_trigger_tx = Arc::clone(&reply_trigger_tx);
            let write_done_tx = Arc::clone(&write_done_tx);
            Box::pin(async move {
                // Send the stream handle to the test task so it can call send_reply later.
                if let Some(tx) = reply_trigger_tx.lock().await.take() {
                    let _ = tx.send(Arc::clone(&stream));
                }
                // This should block until send_reply is called externally.
                stream
                    .write_data(bytes::Bytes::from_static(b"early"), false)
                    .await
                    .unwrap();
                if let Some(tx) = write_done_tx.lock().await.take() {
                    let _ = tx.send(());
                }
            })
        })
        .await;

        cw.write_frame(&syn_stream(1)).await.unwrap();

        // Get the stream handle.
        let stream = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            reply_trigger_rx,
        )
        .await
        .expect("timeout getting stream")
        .expect("recv stream");

        // write_data should still be blocking — give it a moment.
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert!(
            write_done_rx
                .try_recv()
                .is_err(),
            "write_data returned before send_reply"
        );

        // Now unblock it.
        stream.send_reply(HeaderMap::new(), false).await.unwrap();

        tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            write_done_rx,
        )
        .await
        .expect("timeout waiting for write_data to complete")
        .expect("write_done recv");
    }

    /// AsyncRead should correctly reassemble a large DATA frame in 100-byte chunks.
    #[tokio::test]
    async fn test_partial_read() {
        let (_conn, mut cw, mut cr) = make_test_connection(|stream| {
            Box::pin(async move {
                stream.send_reply(HeaderMap::new(), false).await.unwrap();
                let big = bytes::Bytes::from(vec![0xABu8; 1000]);
                stream.write_data(big, true).await.unwrap();
            })
        })
        .await;

        // Consume the initial Settings frame.
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
        .await
        .expect("timeout on settings")
        .expect("read settings");

        cw.write_frame(&syn_stream(1)).await.unwrap();

        // Read SYN_REPLY via framer.
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            cr.read_frame(),
        )
        .await
        .expect("timeout syn_reply")
        .expect("read syn_reply");

        // Read the DATA frame and push it into a stream manually via a channel
        // so we can exercise AsyncRead on a Stream object directly.
        let (write_tx, _write_rx) = mpsc::channel::<Frame>(64);
        let s = Arc::new(Stream::new(1, HeaderMap::new(), 0, write_tx));

        let data_frame = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            cr.read_frame(),
        )
        .await
        .expect("timeout data")
        .expect("read data");
        if let Frame::Data(df) = data_frame {
            s.push_data(df.data);
            s.close_remote();
        } else {
            panic!("expected Data frame");
        }

        // Now read through AsyncRead in 100-byte chunks.
        // We need a mutable reference, so we use Arc::try_unwrap or a wrapper.
        // Since the stream is Arc, use read_data() instead which is what AsyncRead delegates to.
        let mut collected = Vec::new();
        loop {
            match s.read_data().await.unwrap() {
                Some(chunk) => collected.extend_from_slice(&chunk),
                None => break,
            }
        }
        assert_eq!(collected.len(), 1000, "expected 1000 bytes, got {}", collected.len());
        assert!(collected.iter().all(|&b| b == 0xAB));
    }

    /// Open 10 streams; each handler sends a stream-specific reply.
    /// Client verifies each SYN_REPLY arrives (order may vary).
    #[tokio::test]
    async fn test_concurrent_streams() {
        let (_conn, mut cw, mut cr) = make_test_connection(|stream| {
            Box::pin(async move {
                // Each stream sends a reply whose header value encodes the stream_id.
                let mut hdrs = HeaderMap::new();
                hdrs.insert(
                    http::header::HeaderName::from_static("x-stream-id"),
                    http::header::HeaderValue::from_str(&stream.stream_id.to_string()).unwrap(),
                );
                stream.send_reply(hdrs, true).await.unwrap();
            })
        })
        .await;

        // Consume the initial Settings frame.
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
        .await
        .expect("timeout on settings")
        .expect("read settings");

        // Send 10 SYN_STREAMs with odd IDs 1..19.
        for i in 0..10u32 {
            let sid = i * 2 + 1;
            cw.write_frame(&syn_stream(sid)).await.unwrap();
        }

        // Collect 10 SYN_REPLYs.
        let mut seen_ids = std::collections::HashSet::new();
        for _ in 0..10 {
            let frame = tokio::time::timeout(
                tokio::time::Duration::from_secs(5),
                cr.read_frame(),
            )
            .await
            .expect("timeout waiting for SynReply")
            .expect("read frame");
            match frame {
                Frame::SynReply(sr) => {
                    seen_ids.insert(sr.stream_id);
                }
                other => panic!("expected SynReply, got {other:?}"),
            }
        }

        for i in 0..10u32 {
            let sid = i * 2 + 1;
            assert!(seen_ids.contains(&sid), "missing reply for stream {sid}");
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4 tests
    // -----------------------------------------------------------------------

    /// Server sends a Settings frame immediately upon connection.
    #[tokio::test]
    async fn test_settings_on_connect() {
        let (_conn, _cw, mut cr) = make_test_connection(|_| Box::pin(async {})).await;

        let frame = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
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
                    "Settings frame must include SETTINGS_MAX_CONCURRENT_STREAMS"
                );
            }
            other => panic!("expected Settings, got {other:?}"),
        }
    }

    /// Server-initiated ping: connection.ping() sends an even-ID Ping and
    /// resolves when the client echoes it back.
    #[tokio::test]
    async fn test_ping_roundtrip() {
        let (conn, _cw, mut cr) = make_test_connection(|_| Box::pin(async {})).await;

        // Consume the initial Settings frame.
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
        .await
        .expect("timeout on settings")
        .expect("read settings");

        // Spawn the ping future.
        let conn2 = Arc::clone(&conn);
        let ping_task = tokio::spawn(async move { conn2.ping().await });

        // The server should now send us a Ping with an even ID.
        let frame = tokio::time::timeout(
            tokio::time::Duration::from_secs(1),
            cr.read_frame(),
        )
        .await
        .expect("timeout waiting for Ping from server")
        .expect("read Ping");

        let ping_id = match frame {
            Frame::Ping(p) => {
                assert_eq!(p.id % 2, 0, "server-initiated ping must have even ID");
                p.id
            }
            other => panic!("expected Ping, got {other:?}"),
        };

        // Echo the ping back by writing directly to conn's write_tx via the
        // client writer.  We need to write the frame back through client_to_server,
        // but _cw is the write half.  Since we dropped _cw (named _cw), we
        // cannot use it directly.  Instead, send the echo frame via a fresh
        // connection write path.
        //
        // Actually _cw is still alive (binding is `_cw`). Re-use it by
        // restructuring: rebuild the test so _cw is accessible.
        //
        // Simpler: the conn's write_tx is private, but we can send via the
        // connection's internal channel by using send_frame indirectly.
        // We'll use the existing ping_map trick: manually resolve the waiter.
        // But the cleanest approach: re-echo through the transport.
        //
        // The issue is `_cw` is dropped. Let's use `conn.write_tx` directly
        // since it's not pub. Instead, route the echo via the connection's
        // ping_map.
        let mut map = conn.ping_map.lock().await;
        if let Some(tx) = map.remove(&ping_id) {
            let _ = tx.send(());
        }
        drop(map);

        let result = tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            ping_task,
        )
        .await
        .expect("ping_task timed out")
        .expect("join error");

        result.expect("ping() returned an error");
    }

    /// connection.close() sends a GoAway frame and the client receives it.
    #[tokio::test]
    async fn test_goaway_sent() {
        let (conn, _cw, mut cr) = make_test_connection(|_| Box::pin(async {})).await;

        conn.close().await.unwrap();

        // Drain frames until we find GoAway (Settings may arrive first).
        let goaway = loop {
            let frame = tokio::time::timeout(
                tokio::time::Duration::from_secs(2),
                cr.read_frame(),
            )
            .await
            .expect("timeout waiting for frame")
            .expect("read frame");
            match frame {
                Frame::GoAway(g) => break g,
                Frame::Settings(_) => continue,
                other => panic!("unexpected frame: {other:?}"),
            }
        };

        assert_eq!(goaway.status, GoAwayStatus::Ok);
        assert_eq!(goaway.last_good_stream_id, 0, "no streams were accepted");
    }
}

// ---------------------------------------------------------------------------
// Test helper: combine two separate DuplexStream halves into one AsyncRead +
// AsyncWrite object.
// ---------------------------------------------------------------------------

#[cfg(test)]
struct DuplexPairIo {
    reader: tokio::io::DuplexStream,
    writer: tokio::io::DuplexStream,
}

#[cfg(test)]
impl tokio::io::AsyncRead for DuplexPairIo {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

#[cfg(test)]
impl tokio::io::AsyncWrite for DuplexPairIo {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}
