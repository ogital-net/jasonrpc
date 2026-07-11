//! Multiplexing transport over a single shared framed connection.
//!
//! [`MultiplexTransport`] lets many concurrent calls share one byte stream (a
//! `UnixStream`, `TcpStream`, ...) instead of opening a connection per request.
//!
//! # How it works
//!
//! - A background **reader task** owns the read half of the connection. It loops
//!   decoding frames, parses the `id` out of each response, and routes the raw
//!   bytes to whoever is waiting for that id.
//! - A **pending map** (`HashMap<Id, oneshot::Sender<Vec<u8>>>`) records, for
//!   each in-flight request, a one-shot channel to deliver its reply. Each call
//!   registers its id *before* writing, so a fast reply can't arrive before the
//!   waiter is recorded.
//! - The **write half** is behind a mutex so concurrent calls serialize whole
//!   frames onto the wire without interleaving bytes.
//! - Correlation is **order-independent**: the server may answer out of order;
//!   the reader delivers each reply to the matching waiter by id.
//! - If the connection closes or errors, the reader drops every pending sender,
//!   so all waiters resolve to [`MultiplexError::ConnectionClosed`] instead of
//!   hanging.
//!
//! Notifications carry no `id` and get no reply, so
//! [`send_notification`](Transport::send_notification) just writes the frame and
//! returns without registering a waiter.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::sync::{oneshot, Mutex as AsyncMutex};

use super::Transport;
use crate::json;
use crate::protocol::Id;
use crate::transport::io::{FramedReader, FramedWriter};
use crate::transport::Framing;

/// Errors produced by [`MultiplexTransport`].
#[derive(Debug)]
#[non_exhaustive]
pub enum MultiplexError {
    /// The outgoing request bytes did not contain a usable `id`. Calls must be
    /// requests (with an id); use `send_notification` for id-less messages.
    MissingId,
    /// The underlying connection closed or errored while a call was in flight.
    ConnectionClosed,
    /// A framing or I/O error on the shared connection.
    Transport(crate::error::TransportError),
}

impl std::fmt::Display for MultiplexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MultiplexError::MissingId => {
                write!(f, "outgoing request has no id to correlate on")
            }
            MultiplexError::ConnectionClosed => {
                write!(f, "connection closed before a response arrived")
            }
            MultiplexError::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for MultiplexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MultiplexError::Transport(e) => Some(e),
            MultiplexError::MissingId | MultiplexError::ConnectionClosed => None,
        }
    }
}

impl From<crate::error::TransportError> for MultiplexError {
    fn from(e: crate::error::TransportError) -> Self {
        MultiplexError::Transport(e)
    }
}

/// The map of in-flight requests: `id -> reply channel`.
type Pending = Arc<Mutex<HashMap<Id, oneshot::Sender<Vec<u8>>>>>;

/// RAII guard that removes a call's id from the pending map when dropped.
///
/// A `round_trip` future can be dropped before its reply arrives — most
/// commonly when a caller-side timeout (`Client::with_request_timeout`) or
/// cancellation fires while awaiting the oneshot. Without this guard the
/// registered sender would linger in the map forever, since the reader only
/// removes an id when a matching reply actually arrives. On a long-lived
/// multiplexed connection that is an unbounded leak. Dropping the guard on
/// every exit path — success, error, or cancellation — keeps the map bounded
/// by the number of genuinely in-flight calls.
struct PendingGuard<'a> {
    pending: &'a Pending,
    id: Option<Id>,
}

impl<'a> PendingGuard<'a> {
    fn new(pending: &'a Pending, id: Id) -> Self {
        Self {
            pending,
            id: Some(id),
        }
    }

    /// Disarm the guard once the reply has been received, so a normal
    /// completion doesn't do a redundant (already-removed) map lookup.
    fn disarm(mut self) {
        self.id = None;
    }
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.pending.lock().unwrap().remove(&id);
        }
    }
}

/// A multiplexing transport over a single shared, framed connection.
///
/// Cheap to clone: clones share the same underlying connection, writer lock,
/// and pending map. Build one with [`MultiplexTransport::new`] and hand clones
/// to as many concurrent callers as you like.
pub struct MultiplexTransport<W, F> {
    writer: Arc<AsyncMutex<FramedWriter<W, F>>>,
    pending: Pending,
    /// `true` while the background reader is alive. Flipped to `false` when the
    /// reader loop exits (EOF or error), so callers can detect a dead
    /// connection and fail fast instead of writing into a socket whose replies
    /// will never be read.
    connected: Arc<AtomicBool>,
}

impl<W, F> std::fmt::Debug for MultiplexTransport<W, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiplexTransport")
            .field("connected", &self.connected.load(Ordering::Relaxed))
            .finish()
    }
}

impl<W, F> Clone for MultiplexTransport<W, F> {
    fn clone(&self) -> Self {
        Self {
            writer: Arc::clone(&self.writer),
            pending: Arc::clone(&self.pending),
            connected: Arc::clone(&self.connected),
        }
    }
}

/// A [`MultiplexTransport`] built over the split halves of a stream `S`.
pub type MultiplexOver<S, F> = MultiplexTransport<WriteHalf<S>, F>;

impl<S, F> MultiplexTransport<WriteHalf<S>, F>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    ReadHalf<S>: Unpin + Send,
    WriteHalf<S>: Unpin + Send,
    F: Framing + Clone + Send + Sync + 'static,
{
    /// Build a multiplexing transport over a full-duplex stream.
    ///
    /// Splits the stream into read/write halves, spawns the background reader,
    /// and returns the cloneable transport. The reader task lives until the
    /// connection ends (EOF or error), at which point all pending and future
    /// calls fail with [`MultiplexError::ConnectionClosed`].
    pub fn new(stream: S, framing: F) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let connected = Arc::new(AtomicBool::new(true));

        let writer = Arc::new(AsyncMutex::new(FramedWriter::new(
            write_half,
            framing.clone(),
        )));

        // Spawn the reader task: decode frames, route by id.
        let reader = FramedReader::new(read_half, framing);
        tokio::spawn(reader_loop(
            reader,
            Arc::clone(&pending),
            Arc::clone(&connected),
        ));

        MultiplexTransport {
            writer,
            pending,
            connected,
        }
    }
}

/// The background reader: pull frames, parse ids, deliver to waiters. On any
/// end-of-stream or error, drop all waiters (their receivers resolve to Err).
async fn reader_loop<R, F>(
    mut reader: FramedReader<R, F>,
    pending: Pending,
    connected: Arc<AtomicBool>,
) where
    R: AsyncRead + Unpin,
    F: Framing,
{
    loop {
        if let Ok(Some(frame)) = reader.recv().await {
            if let Some(id) = parse_id(&frame) {
                let waiter = pending.lock().unwrap().remove(&id);
                if let Some(tx) = waiter {
                    // Ignore send error: the caller may have gone away.
                    let _ = tx.send(frame);
                }
                // No waiter: a stray/duplicate response; drop it.
            }
            // A frame with no id (e.g. a parse-error response with null id
            // that we can't attribute) is dropped; the affected caller will
            // time out or fail when the connection ends.
        } else {
            // Clean EOF or error: mark the connection dead so future calls fail
            // fast, then fail everyone still waiting.
            connected.store(false, Ordering::Release);
            pending.lock().unwrap().clear();
            return;
        }
    }
}

/// Parse just the `id` field out of a response frame, without fully typing it.
fn parse_id(frame: &[u8]) -> Option<Id> {
    #[derive(serde::Deserialize)]
    struct IdOnly {
        id: Id,
    }
    json::from_slice::<IdOnly>(frame).ok().map(|x| x.id)
}

impl<W, F> Transport for MultiplexTransport<W, F>
where
    W: AsyncWrite + Unpin + Send,
    F: Framing + Send + Sync,
{
    type Error = MultiplexError;

    async fn round_trip(&self, request: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        // If the reader has already exited, no reply can ever arrive; fail now
        // rather than writing into a dead socket and hanging on the oneshot.
        if !self.is_connected() {
            return Err(MultiplexError::ConnectionClosed);
        }

        // The id must be recoverable from the request so we can correlate the
        // reply. `Client` always includes one for calls.
        let id = parse_id(&request).ok_or(MultiplexError::MissingId)?;

        let (tx, rx) = oneshot::channel();

        // Register the waiter BEFORE writing, so a fast reply can't race us.
        // The guard removes our id from the pending map on every exit path,
        // including if this future is dropped mid-await (timeout/cancellation).
        self.pending.lock().unwrap().insert(id.clone(), tx);
        let guard = PendingGuard::new(&self.pending, id);

        // Write the frame while holding the writer lock, then release it so
        // other calls can proceed while we await our reply. On write failure
        // the guard cleans up the pending entry as it drops.
        self.write_frame(&request).await?;

        // Await the reply; an Err means the reader dropped our sender.
        let reply = rx.await.map_err(|_| MultiplexError::ConnectionClosed);
        guard.disarm();
        reply
    }

    async fn send_notification(&self, notification: Vec<u8>) -> Result<(), Self::Error> {
        if !self.is_connected() {
            return Err(MultiplexError::ConnectionClosed);
        }
        // No id, no waiter -- just write it.
        self.write_frame(&notification).await
    }
}

impl<W, F> MultiplexTransport<W, F>
where
    W: AsyncWrite + Unpin + Send,
    F: Framing + Send + Sync,
{
    async fn write_frame(&self, frame: &[u8]) -> Result<(), MultiplexError> {
        let mut writer = self.writer.lock().await;
        writer.send(frame).await.map_err(MultiplexError::from)
    }

    /// Whether the background reader is still alive. Returns `false` once the
    /// connection has closed or errored, after which every call fails with
    /// [`MultiplexError::ConnectionClosed`].
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// The number of in-flight calls currently registered in the pending map.
    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

#[cfg(all(test, feature = "netstring"))]
mod tests {
    use super::*;
    use crate::client::Client;
    use crate::transport::io::{FramedReader, FramedWriter};
    use crate::transport::Netstring;

    /// A mock server over one end of an in-memory duplex that echoes each call
    /// back as a result, optionally after a per-method delay so replies can be
    /// forced to come back out of order.
    async fn run_mock_server(server: tokio::io::DuplexStream) {
        let (r, w) = tokio::io::split(server);
        let mut reader = FramedReader::new(r, Netstring);
        let writer = Arc::new(AsyncMutex::new(FramedWriter::new(w, Netstring)));

        while let Ok(Some(frame)) = reader.recv().await {
            let writer = Arc::clone(&writer);
            tokio::spawn(async move {
                // Parse the id and the method to decide the reply + delay.
                #[derive(serde::Deserialize)]
                struct Req {
                    method: String,
                    id: Option<Id>,
                }
                let req: Req = match json::from_slice(&frame) {
                    Ok(r) => r,
                    Err(_) => return,
                };
                // Notifications get no reply.
                let Some(id) = req.id else { return };

                // "slow" replies last, so a slow call issued first still comes
                // back after a fast call issued second.
                if req.method == "slow" {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }

                let resp = crate::protocol::Response::result(id, crate::json::string(&req.method));
                let bytes = crate::json::to_vec(&resp).unwrap();
                let _ = writer.lock().await.send(&bytes).await;
            });
        }
    }

    #[tokio::test]
    async fn concurrent_out_of_order_calls_correlate() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        tokio::spawn(run_mock_server(server_io));

        let transport = MultiplexTransport::new(client_io, Netstring);
        let client = Arc::new(Client::new(transport));

        // Issue a slow call and a fast call concurrently; the slow one is sent
        // first but the server answers it last. Correlation must still be right.
        let c1 = Arc::clone(&client);
        let slow = tokio::spawn(async move { c1.call::<_, String>("slow", ()).await });
        let c2 = Arc::clone(&client);
        let fast = tokio::spawn(async move { c2.call::<_, String>("fast", ()).await });

        let slow = slow.await.unwrap().unwrap();
        let fast = fast.await.unwrap().unwrap();
        assert_eq!(slow, "slow");
        assert_eq!(fast, "fast");
    }

    #[tokio::test]
    async fn many_concurrent_calls_all_correlate() {
        let (client_io, server_io) = tokio::io::duplex(256 * 1024);
        tokio::spawn(run_mock_server(server_io));

        let client = Arc::new(Client::new(MultiplexTransport::new(client_io, Netstring)));

        let mut handles = Vec::new();
        for i in 0..50 {
            let c = Arc::clone(&client);
            let method = format!("m{i}");
            handles.push(tokio::spawn(async move {
                let got: String = c.call(&method, ()).await.unwrap();
                assert_eq!(got, method);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn connection_close_fails_pending_calls() {
        let (client_io, server_io) = tokio::io::duplex(1024);
        // Drop the server end immediately so the reader sees EOF.
        drop(server_io);

        let client = Client::new(MultiplexTransport::new(client_io, Netstring));
        let err = client.call::<_, String>("whatever", ()).await.unwrap_err();
        // Should surface as a transport error, not hang.
        assert!(matches!(err, crate::error::ClientError::Transport(_)));
    }

    #[tokio::test]
    async fn dropped_round_trip_drains_pending_map() {
        // A server that reads frames but never replies, so every call hangs
        // awaiting its oneshot.
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let (r, _w) = tokio::io::split(server_io);
            let mut reader = FramedReader::new(r, Netstring);
            while let Ok(Some(_frame)) = reader.recv().await {
                // Swallow the request; never answer.
            }
        });

        let transport = MultiplexTransport::new(client_io, Netstring);

        // Issue a call and cancel it via a timeout. The round_trip future is
        // dropped mid-await; the guard must remove the pending entry.
        let fut = transport.round_trip(br#"{"jsonrpc":"2.0","method":"m","id":1}"#.to_vec());
        let timed_out = tokio::time::timeout(std::time::Duration::from_millis(50), fut).await;
        assert!(timed_out.is_err(), "call should have timed out");

        // Give the drop a moment to run, then confirm nothing leaked.
        tokio::task::yield_now().await;
        assert_eq!(
            transport.pending_len(),
            0,
            "pending map must be empty after a cancelled call"
        );
    }

    #[test]
    fn parse_id_numeric() {
        let frame = br#"{"jsonrpc":"2.0","result":1,"id":42}"#;
        assert_eq!(parse_id(frame), Some(Id::Number(42)));
    }

    #[test]
    fn parse_id_string() {
        let frame = br#"{"jsonrpc":"2.0","result":1,"id":"abc"}"#;
        assert_eq!(parse_id(frame), Some(Id::String("abc".into())));
    }

    #[test]
    fn parse_id_null_is_not_a_notification() {
        // Null id in a response is a valid id.
        let frame = br#"{"jsonrpc":"2.0","result":1,"id":null}"#;
        assert_eq!(parse_id(frame), Some(Id::Null));
    }

    #[test]
    fn parse_id_missing_is_none() {
        // A notification response frame (shouldn't happen normally).
        let frame = br#"{"jsonrpc":"2.0","result":1}"#;
        assert_eq!(parse_id(frame), None);
    }

    #[test]
    fn parse_id_not_json() {
        assert_eq!(parse_id(b"not json"), None);
    }

    #[test]
    fn parse_id_error_response() {
        let frame = br#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"nope"},"id":7}"#;
        assert_eq!(parse_id(frame), Some(Id::Number(7)));
    }
}
