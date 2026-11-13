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
//! - A **pending map** (`HashMap<Id, oneshot::Sender<ReceivedResponse>>`) records, for
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

use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::sync::{oneshot, watch, Mutex as AsyncMutex};

use super::{ReceivedResponse, Transport};
use crate::json;
use crate::protocol::Id;
use crate::transport::io::{FramedReader, FramedWriter, DEFAULT_MAX_FRAME_SIZE};
use crate::transport::Framing;

/// Errors produced by [`MultiplexTransport`].
#[derive(Debug)]
#[non_exhaustive]
pub enum MultiplexError {
    /// The outgoing request bytes did not contain a usable `id`. Calls must be
    /// requests (with an id); use `send_notification` for id-less messages.
    MissingId,
    /// Another request with the same id is already awaiting a response.
    DuplicateId,
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
            MultiplexError::DuplicateId => {
                write!(f, "another request with the same id is already in flight")
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
            MultiplexError::MissingId
            | MultiplexError::DuplicateId
            | MultiplexError::ConnectionClosed => None,
        }
    }
}

impl From<crate::error::TransportError> for MultiplexError {
    fn from(e: crate::error::TransportError) -> Self {
        MultiplexError::Transport(e)
    }
}

struct ConnectionState {
    connected: bool,
    pending: HashMap<Id, oneshot::Sender<ReceivedResponse>>,
    closed: watch::Sender<bool>,
}

impl ConnectionState {
    fn disconnect(&mut self) {
        self.connected = false;
        self.pending.clear();
        self.closed.send_replace(true);
    }
}

type SharedState = Arc<Mutex<ConnectionState>>;

struct ReaderStateGuard(SharedState);

impl Drop for ReaderStateGuard {
    fn drop(&mut self) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.disconnect();
    }
}

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
    state: &'a SharedState,
    id: Option<Id>,
}

impl<'a> PendingGuard<'a> {
    fn new(state: &'a SharedState, id: Id) -> Self {
        Self {
            state,
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
            self.state.lock().unwrap().pending.remove(&id);
        }
    }
}

struct WriteGuard<'a> {
    state: &'a SharedState,
    completed: bool,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            let mut state = self.state.lock().unwrap();
            state.disconnect();
        }
    }
}

struct ReaderTaskGuard(tokio::task::JoinHandle<()>);

impl Drop for ReaderTaskGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A multiplexing transport over a single shared, framed connection.
///
/// Cheap to clone: clones share the same underlying connection, writer lock,
/// and pending map. Build one with [`MultiplexTransport::new`] and hand clones
/// to as many concurrent callers as you like.
/// Dropping the last transport handle aborts the reader task and closes the
/// connection.
///
/// Cancelling an in-progress frame write invalidates the connection and fails
/// all pending calls with [`MultiplexError::ConnectionClosed`]. Cancelling while
/// waiting for the writer lock or a response leaves the connection usable.
pub struct MultiplexTransport<W, F> {
    writer: Arc<AsyncMutex<FramedWriter<W, F>>>,
    state: SharedState,
    reader_task: Arc<ReaderTaskGuard>,
}

impl<W, F> std::fmt::Debug for MultiplexTransport<W, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiplexTransport")
            .field("connected", &self.state.lock().unwrap().connected)
            .finish()
    }
}

impl<W, F> Clone for MultiplexTransport<W, F> {
    fn clone(&self) -> Self {
        Self {
            writer: Arc::clone(&self.writer),
            state: Arc::clone(&self.state),
            reader_task: Arc::clone(&self.reader_task),
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
    /// connection ends (EOF or error) or the last transport handle is dropped.
    /// When the connection ends, all pending and future calls fail with
    /// [`MultiplexError::ConnectionClosed`].
    pub fn new(stream: S, framing: F) -> Self {
        Self::new_with_max_frame_size(stream, framing, DEFAULT_MAX_FRAME_SIZE)
    }

    /// Build a multiplexing transport with a maximum incoming frame size.
    ///
    /// The limit includes framing bytes and does not constrain outgoing writes.
    /// Set to zero to disable the limit. Unbounded buffering is not recommended
    /// for network-facing clients. Other behavior matches [`Self::new`].
    pub fn new_with_max_frame_size(stream: S, framing: F, max_frame_size: usize) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);
        let state = Arc::new(Mutex::new(ConnectionState {
            connected: true,
            pending: HashMap::new(),
            closed: watch::channel(false).0,
        }));

        let writer = Arc::new(AsyncMutex::new(FramedWriter::new(
            write_half,
            framing.clone(),
        )));

        // Spawn the reader task: decode frames, route by id.
        let reader = FramedReader::new(read_half, framing).with_max_frame_size(max_frame_size);
        let reader_task = Arc::new(ReaderTaskGuard(tokio::spawn(reader_loop(
            reader,
            ReaderStateGuard(Arc::clone(&state)),
        ))));

        MultiplexTransport {
            writer,
            state,
            reader_task,
        }
    }
}

/// The background reader: pull frames, parse ids, deliver to waiters. On any
/// end-of-stream or error, drop all waiters (their receivers resolve to Err).
async fn reader_loop<R, F>(mut reader: FramedReader<R, F>, guard: ReaderStateGuard)
where
    R: AsyncRead + Unpin,
    F: Framing,
{
    let state = &guard.0;
    loop {
        if let Ok(Some(frame)) = reader.recv_bytes().await {
            let response = ReceivedResponse::parsed(frame);
            let fallback_id = if response.routing_id().is_none() {
                parse_id(response.as_bytes())
            } else {
                None
            };
            if let Some(id) = response.routing_id().or(fallback_id.as_ref()) {
                let waiter = state.lock().unwrap().pending.remove(id);
                if let Some(tx) = waiter {
                    // Ignore send error: the caller may have gone away.
                    let _ = tx.send(response);
                }
                // No waiter: a stray/duplicate response; drop it.
            }
            // A frame with no id (e.g. a parse-error response with null id
            // that we can't attribute) is dropped; the affected caller will
            // time out or fail when the connection ends.
        } else {
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
        // The id must be recoverable from the request so we can correlate the
        // reply. `Client` always includes one for calls.
        let id = parse_id(&request).ok_or(MultiplexError::MissingId)?;

        self.round_trip_with_id(request, id).await
    }

    async fn round_trip_with_id(&self, request: Vec<u8>, id: Id) -> Result<Vec<u8>, Self::Error> {
        self.round_trip_response(request, id)
            .await
            .map(ReceivedResponse::into_bytes)
    }

    async fn round_trip_response(
        &self,
        request: Vec<u8>,
        id: Id,
    ) -> Result<ReceivedResponse, Self::Error> {
        let (tx, rx) = oneshot::channel();

        // Register the waiter BEFORE writing, so a fast reply can't race us.
        // The guard removes our id from the pending map on every exit path,
        // including if this future is dropped mid-await (timeout/cancellation).
        {
            let mut state = self.state.lock().unwrap();
            if !state.connected {
                return Err(MultiplexError::ConnectionClosed);
            }
            match state.pending.entry(id.clone()) {
                Entry::Occupied(_) => return Err(MultiplexError::DuplicateId),
                Entry::Vacant(entry) => {
                    entry.insert(tx);
                }
            }
        }
        let guard = PendingGuard::new(&self.state, id);

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
        if !self.is_connected() {
            return Err(MultiplexError::ConnectionClosed);
        }

        let mut guard = WriteGuard {
            state: &self.state,
            completed: false,
        };
        writer.send(frame).await.map_err(MultiplexError::from)?;
        guard.completed = true;
        Ok(())
    }

    /// Whether the shared connection is still usable. Returns `false` once the
    /// connection closes, errors, or an in-progress write is cancelled, after
    /// which every call fails with [`MultiplexError::ConnectionClosed`].
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.state.lock().unwrap().connected
    }

    /// Wait until the connection becomes unusable through EOF, a framing or
    /// I/O error, cancellation of an in-progress frame write, or termination of
    /// the background reader task.
    ///
    /// Returns immediately if already closed. Multiple concurrent waiters are
    /// supported, and cancelling this wait does not affect the connection.
    pub async fn closed(&self) {
        let mut closed = self.state.lock().unwrap().closed.subscribe();
        let _ = closed.wait_for(|closed| *closed).await;
    }

    /// The number of in-flight calls currently registered in the pending map.
    #[cfg(all(test, feature = "netstring"))]
    fn pending_len(&self) -> usize {
        self.state.lock().unwrap().pending.len()
    }
}

#[cfg(all(test, feature = "netstring"))]
mod tests {
    use super::*;
    use crate::client::Client;
    use crate::transport::io::{FramedReader, FramedWriter};
    use crate::transport::Netstring;

    async fn assert_pending(mut future: std::pin::Pin<&mut impl std::future::Future>) {
        std::future::poll_fn(|context| {
            assert!(future.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }

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
    async fn known_ids_correlate() {
        use crate::transport::io::FramedConn;

        for id in [
            Id::Number(-1),
            Id::Unsigned(i64::MAX as u64 + 1),
            Id::String("known".into()),
            Id::Null,
        ] {
            let (client_io, server_io) = tokio::io::duplex(1024);
            let transport = MultiplexTransport::new(client_io, Netstring);
            let (request, id) = crate::client::encode_call("m", &(), id).unwrap();
            let expected_request = request.clone();
            let response = json::to_vec(&crate::Response::result(id.clone(), 7)).unwrap();
            let server = tokio::spawn(async move {
                let mut conn = FramedConn::new(server_io, Netstring);
                assert_eq!(conn.recv().await.unwrap().unwrap(), expected_request);
                conn.send(&response).await.unwrap();
            });

            let reply = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                transport.round_trip_with_id(request, id.clone()),
            )
            .await
            .expect("known-id calls must receive their response")
            .unwrap();
            assert_eq!(crate::client::decode_result::<i64>(&reply, &id).unwrap(), 7);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn raw_round_trip_preserves_malformed_response_bytes() {
        use crate::transport::io::FramedConn;

        for response in [
            br#"{ "id":7, "result":42, "jsonrpc":"2.0" }"#.as_slice(),
            br#"{"jsonrpc":"1.0","result":42,"id":7}"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":42,"error":null,"id":7}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":7}"#.as_slice(),
        ] {
            let (client_io, server_io) = tokio::io::duplex(1024);
            let transport = MultiplexTransport::new(client_io, Netstring);
            let server = tokio::spawn(async move {
                let mut conn = FramedConn::new(server_io, Netstring);
                conn.recv().await.unwrap().unwrap();
                conn.send(response).await.unwrap();
            });
            let reply = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                transport.round_trip(br#"{"jsonrpc":"2.0","method":"m","id":7}"#.to_vec()),
            )
            .await
            .expect("recoverable ids must route malformed envelopes")
            .unwrap();
            assert_eq!(reply, response);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn dropping_last_transport_closes_connection() {
        use tokio::io::AsyncReadExt;

        let (client_io, mut server_io) = tokio::io::duplex(64);
        let transport = MultiplexTransport::new(client_io, Netstring);
        let state = Arc::downgrade(&transport.state);
        drop(transport);

        let mut byte = [0; 1];
        let read =
            tokio::time::timeout(std::time::Duration::from_secs(1), server_io.read(&mut byte))
                .await
                .expect("dropping the last transport must close the peer connection")
                .unwrap();
        assert_eq!(read, 0);
        assert!(state.upgrade().is_none());
    }

    #[tokio::test]
    async fn dropping_one_clone_keeps_connection_usable() {
        let (client_io, server_io) = tokio::io::duplex(1024);
        let server = tokio::spawn(run_mock_server(server_io));
        let transport = MultiplexTransport::new(client_io, Netstring);
        let state = Arc::downgrade(&transport.state);
        let client =
            Client::new(transport.clone()).with_request_timeout(std::time::Duration::from_secs(1));

        assert_eq!(
            client.call::<_, String>("before", ()).await.unwrap(),
            "before"
        );
        drop(transport);
        assert_eq!(
            client.call::<_, String>("after", ()).await.unwrap(),
            "after"
        );
        drop(client);

        tokio::time::timeout(std::time::Duration::from_secs(1), server)
            .await
            .expect("dropping the last client must close its connection")
            .unwrap();
        assert!(state.upgrade().is_none());
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
    async fn invalid_utf8_response_does_not_strand_pending_calls() {
        use crate::transport::io::FramedConn;

        let (client_io, server_io) = tokio::io::duplex(1024);
        let transport = MultiplexTransport::new(client_io, Netstring);
        let mut first = Box::pin(transport.round_trip_response(
            br#"{"jsonrpc":"2.0","method":"m","id":1}"#.to_vec(),
            Id::Number(1),
        ));
        let mut second = Box::pin(transport.round_trip_response(
            br#"{"jsonrpc":"2.0","method":"m","id":2}"#.to_vec(),
            Id::Number(2),
        ));
        assert_pending(first.as_mut()).await;
        assert_pending(second.as_mut()).await;
        assert_eq!(transport.pending_len(), 2);

        let mut peer = FramedConn::new(server_io, Netstring);
        peer.recv().await.unwrap().unwrap();
        peer.recv().await.unwrap().unwrap();
        peer.send(b"{\"jsonrpc\":\"2.0\",\"result\":\"\xff\",\"id\":1}")
            .await
            .unwrap();
        drop(peer);

        let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(first, second)
        })
        .await
        .expect("invalid UTF-8 followed by EOF must not strand pending calls");
        match first {
            Ok(response) => assert!(matches!(
                response.decode::<String>(&Id::Number(1)),
                Err(crate::error::ClientError::Protocol(_))
            )),
            Err(error) => assert!(matches!(error, MultiplexError::ConnectionClosed)),
        }
        assert!(matches!(second, Err(MultiplexError::ConnectionClosed)));
        assert!(!transport.is_connected());
        assert_eq!(transport.pending_len(), 0);
        tokio::time::timeout(std::time::Duration::from_secs(1), transport.closed())
            .await
            .expect("invalid UTF-8 followed by EOF must notify closure");
    }

    #[tokio::test]
    async fn reader_cancellation_disconnects_and_fails_pending_calls() {
        for poll_reader in [false, true] {
            let (client_io, _server_io) = tokio::io::duplex(1024);
            let transport = MultiplexTransport::new(client_io, Netstring);
            let mut call = Box::pin(transport.round_trip_with_id(
                br#"{"jsonrpc":"2.0","method":"m","id":1}"#.to_vec(),
                Id::Number(1),
            ));
            assert_pending(call.as_mut()).await;
            assert_eq!(transport.pending_len(), 1);
            if poll_reader {
                tokio::task::yield_now().await;
            }
            transport.reader_task.0.abort();

            let error = tokio::time::timeout(std::time::Duration::from_secs(1), call)
                .await
                .expect("reader cancellation must wake pending calls")
                .unwrap_err();
            assert!(matches!(error, MultiplexError::ConnectionClosed));
            assert!(!transport.is_connected());
            assert_eq!(transport.pending_len(), 0);
            tokio::time::timeout(std::time::Duration::from_secs(1), transport.closed())
                .await
                .expect("reader cancellation must notify closure");
        }
    }

    #[tokio::test]
    async fn reader_panic_disconnects_and_fails_pending_calls() {
        use crate::transport::io::FramedConn;

        #[derive(Clone)]
        struct PanickingFraming;

        impl Framing for PanickingFraming {
            fn encode(&self, payload: &[u8], dst: &mut Vec<u8>) {
                Netstring.encode(payload, dst);
            }

            fn decode(
                &self,
                src: &[u8],
            ) -> Result<Option<(Vec<u8>, usize)>, crate::error::TransportError> {
                assert!(src.is_empty(), "injected reader panic");
                Ok(None)
            }
        }

        let (client_io, server_io) = tokio::io::duplex(1024);
        let transport = MultiplexTransport::new(client_io, PanickingFraming);
        let mut call = Box::pin(transport.round_trip_with_id(
            br#"{"jsonrpc":"2.0","method":"m","id":1}"#.to_vec(),
            Id::Number(1),
        ));
        assert_pending(call.as_mut()).await;
        assert_eq!(transport.pending_len(), 1);
        let mut peer = FramedConn::new(server_io, Netstring);
        peer.recv().await.unwrap().unwrap();
        peer.send(b"panic").await.unwrap();

        let error = tokio::time::timeout(std::time::Duration::from_secs(1), call)
            .await
            .expect("reader panic must wake pending calls")
            .unwrap_err();
        assert!(matches!(error, MultiplexError::ConnectionClosed));
        assert!(!transport.is_connected());
        assert_eq!(transport.pending_len(), 0);
        tokio::time::timeout(std::time::Duration::from_secs(1), transport.closed())
            .await
            .expect("reader panic must notify closure");
    }

    #[tokio::test]
    async fn configured_frame_limit_controls_incoming_responses_only() {
        for limit in [64, 256, 0] {
            let (client_io, server_io) = tokio::io::duplex(1024);
            tokio::spawn(run_mock_server(server_io));
            let transport =
                MultiplexTransport::new_with_max_frame_size(client_io, Netstring, limit);
            let client =
                Client::new(transport).with_request_timeout(std::time::Duration::from_secs(1));
            let method = "x".repeat(128);
            let response = client.call::<_, String>(&method, ()).await;
            if limit == 64 {
                assert!(matches!(
                    response,
                    Err(crate::error::ClientError::Transport(_))
                ));
            } else {
                assert_eq!(response.unwrap(), method);
            }
        }
    }

    #[tokio::test]
    async fn closed_notifies_all_waiters_and_remembers_eof() {
        let (client_io, server_io) = tokio::io::duplex(1024);
        let transport = MultiplexTransport::new(client_io, Netstring);
        let first = transport.closed();
        let second = transport.closed();
        tokio::pin!(first, second);
        assert_pending(first.as_mut()).await;
        assert_pending(second.as_mut()).await;

        drop(server_io);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(first, second);
            transport.closed().await;
        })
        .await
        .expect("closure must wake existing and future waiters");
        assert!(!transport.is_connected());
    }

    #[tokio::test]
    async fn closed_notifies_waiters_after_cancelled_partial_write() {
        let (client_io, _server_io) = tokio::io::duplex(16);
        let transport = MultiplexTransport::new(client_io, Netstring);
        let closed = transport.closed();
        tokio::pin!(closed);
        assert_pending(closed.as_mut()).await;

        let notification =
            crate::client::encode_notification("large", &("x".repeat(4096),)).unwrap();
        let mut sending = Box::pin(transport.send_notification(notification));
        assert_pending(sending.as_mut()).await;
        drop(sending);

        tokio::time::timeout(std::time::Duration::from_secs(1), closed)
            .await
            .expect("a cancelled partial write must notify closure");
        assert!(!transport.is_connected());
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
        let fut = transport.round_trip_with_id(
            br#"{"jsonrpc":"2.0","method":"m","id":1}"#.to_vec(),
            Id::Number(1),
        );
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

    #[tokio::test]
    async fn partial_write_timeout_disconnects() {
        use tokio::io::AsyncReadExt;

        let (client_io, mut server_io) = tokio::io::duplex(16);
        let transport = MultiplexTransport::new(client_io, Netstring);
        let client = Client::new(transport.clone())
            .with_request_timeout(std::time::Duration::from_millis(10));

        let error = client
            .call::<_, String>("large", ("x".repeat(4096),))
            .await
            .unwrap_err();
        assert!(matches!(error, crate::error::ClientError::Timeout(_)));

        let mut partial_frame = [0; 16];
        server_io.read_exact(&mut partial_frame).await.unwrap();
        assert!(partial_frame.contains(&b':'));
        assert!(!transport.is_connected());
        assert_eq!(transport.pending_len(), 0);

        let error = transport
            .round_trip(br#"{"jsonrpc":"2.0","method":"next","id":2}"#.to_vec())
            .await
            .unwrap_err();
        assert!(matches!(error, MultiplexError::ConnectionClosed));

        let error = transport
            .send_notification(br#"{"jsonrpc":"2.0","method":"next"}"#.to_vec())
            .await
            .unwrap_err();
        assert!(matches!(error, MultiplexError::ConnectionClosed));
    }

    #[tokio::test]
    async fn partial_flush_timeout_disconnects() {
        use tokio::io::AsyncReadExt;

        let (client_io, mut server_io) = tokio::io::duplex(16);
        let buffered_io = tokio::io::BufStream::with_capacity(1024, 8192, client_io);
        let transport = MultiplexTransport::new(buffered_io, Netstring);
        let client = Client::new(transport.clone())
            .with_request_timeout(std::time::Duration::from_millis(10));

        let error = client
            .call::<_, String>("large", ("x".repeat(4096),))
            .await
            .unwrap_err();
        assert!(matches!(error, crate::error::ClientError::Timeout(_)));

        let mut partial_frame = [0; 16];
        server_io.read_exact(&mut partial_frame).await.unwrap();
        assert!(partial_frame.contains(&b':'));
        assert!(!transport.is_connected());
        assert_eq!(transport.pending_len(), 0);
    }

    #[tokio::test]
    async fn cancelled_partial_write_fails_pending_and_queued_sends() {
        use tokio::io::AsyncReadExt;

        let (client_io, mut server_io) = tokio::io::duplex(128);
        let transport = MultiplexTransport::new(client_io, Netstring);

        let mut waiting_reply = Box::pin(
            transport.round_trip(br#"{"jsonrpc":"2.0","method":"first","id":100}"#.to_vec()),
        );
        assert_pending(waiting_reply.as_mut()).await;

        let (request, _) =
            crate::client::encode_call("large", &("x".repeat(4096),), Id::Number(1)).unwrap();
        let mut writing = Box::pin(transport.round_trip(request));
        assert_pending(writing.as_mut()).await;

        let mut queued_call =
            Box::pin(transport.round_trip(br#"{"jsonrpc":"2.0","method":"next","id":2}"#.to_vec()));
        assert_pending(queued_call.as_mut()).await;

        let mut queued_notification =
            Box::pin(transport.send_notification(br#"{"jsonrpc":"2.0","method":"next"}"#.to_vec()));
        assert_pending(queued_notification.as_mut()).await;
        assert_eq!(transport.pending_len(), 3);

        let mut partial_frame = [0; 128];
        server_io.read_exact(&mut partial_frame).await.unwrap();
        drop(writing);
        assert!(!transport.is_connected());
        assert_eq!(transport.pending_len(), 0);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            assert!(matches!(
                waiting_reply.await,
                Err(MultiplexError::ConnectionClosed)
            ));
            assert!(matches!(
                queued_call.await,
                Err(MultiplexError::ConnectionClosed)
            ));
            assert!(matches!(
                queued_notification.await,
                Err(MultiplexError::ConnectionClosed)
            ));
        })
        .await
        .expect("pending and queued sends must fail promptly");
    }

    #[tokio::test]
    async fn cancelled_partial_notification_disconnects() {
        let (client_io, _server_io) = tokio::io::duplex(16);
        let transport = MultiplexTransport::new(client_io, Netstring);
        let notification =
            crate::client::encode_notification("large", &("x".repeat(4096),)).unwrap();
        let mut sending = Box::pin(transport.send_notification(notification));
        assert_pending(sending.as_mut()).await;
        drop(sending);

        assert!(!transport.is_connected());
        assert_eq!(transport.pending_len(), 0);
    }

    #[tokio::test]
    async fn cancelled_writer_wait_keeps_connection_usable() {
        let (client_io, server_io) = tokio::io::duplex(1024);
        let transport = MultiplexTransport::new(client_io, Netstring);
        let writer_lock = transport.writer.lock().await;
        let mut queued = Box::pin(transport.round_trip_with_id(
            br#"{"jsonrpc":"2.0","method":"first","id":100}"#.to_vec(),
            Id::Number(100),
        ));
        assert_pending(queued.as_mut()).await;
        assert_eq!(transport.pending_len(), 1);
        drop(queued);
        drop(writer_lock);

        assert!(transport.is_connected());
        assert_eq!(transport.pending_len(), 0);

        tokio::spawn(run_mock_server(server_io));
        let client = Client::new(transport).with_request_timeout(std::time::Duration::from_secs(1));
        assert_eq!(client.call::<_, String>("fast", ()).await.unwrap(), "fast");
    }

    #[tokio::test]
    async fn cancelled_response_wait_keeps_connection_usable() {
        let (client_io, server_io) = tokio::io::duplex(1024);
        let transport = MultiplexTransport::new(client_io, Netstring);
        let mut waiting_reply = Box::pin(transport.round_trip_with_id(
            br#"{"jsonrpc":"2.0","method":"first","id":100}"#.to_vec(),
            Id::Number(100),
        ));
        assert_pending(waiting_reply.as_mut()).await;
        drop(waiting_reply);

        assert!(transport.is_connected());
        assert_eq!(transport.pending_len(), 0);

        tokio::spawn(run_mock_server(server_io));
        let client = Client::new(transport).with_request_timeout(std::time::Duration::from_secs(1));
        assert_eq!(client.call::<_, String>("fast", ()).await.unwrap(), "fast");
    }

    #[tokio::test]
    async fn duplicate_in_flight_id_is_rejected() {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let (r, _w) = tokio::io::split(server_io);
            let mut reader = FramedReader::new(r, Netstring);
            while let Ok(Some(_frame)) = reader.recv().await {}
        });

        let transport = MultiplexTransport::new(client_io, Netstring);
        let first_transport = transport.clone();
        let request = br#"{"jsonrpc":"2.0","method":"m","id":1}"#.to_vec();
        let first_request = request.clone();
        let first = tokio::spawn(async move {
            first_transport
                .round_trip_with_id(first_request, Id::Number(1))
                .await
        });

        while transport.pending_len() == 0 {
            tokio::task::yield_now().await;
        }

        let err = transport.round_trip(request).await.unwrap_err();
        assert!(matches!(err, MultiplexError::DuplicateId));
        assert_eq!(transport.pending_len(), 1);

        first.abort();
        let _ = first.await;
        tokio::task::yield_now().await;
        assert_eq!(transport.pending_len(), 0);
    }

    #[tokio::test]
    async fn write_failure_disconnects_all_pending_calls() {
        struct FailSecondWrite {
            writes: usize,
        }

        impl tokio::io::AsyncRead for FailSecondWrite {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                _buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Pending
            }
        }

        impl tokio::io::AsyncWrite for FailSecondWrite {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<Result<usize, std::io::Error>> {
                if self.writes == 0 {
                    self.writes += 1;
                    std::task::Poll::Ready(Ok(buf.len()))
                } else {
                    std::task::Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "write failed",
                    )))
                }
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), std::io::Error>> {
                std::task::Poll::Ready(Ok(()))
            }

            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), std::io::Error>> {
                std::task::Poll::Ready(Ok(()))
            }
        }

        let transport = MultiplexTransport::new(FailSecondWrite { writes: 0 }, Netstring);
        let first_transport = transport.clone();
        let first = tokio::spawn(async move {
            first_transport
                .round_trip(br#"{"jsonrpc":"2.0","method":"m","id":1}"#.to_vec())
                .await
        });

        while transport.pending_len() == 0 {
            tokio::task::yield_now().await;
        }

        let second = transport
            .round_trip(br#"{"jsonrpc":"2.0","method":"m","id":2}"#.to_vec())
            .await;
        assert!(matches!(second, Err(MultiplexError::Transport(_))));
        assert!(matches!(
            first.await.unwrap(),
            Err(MultiplexError::ConnectionClosed)
        ));
        assert!(!transport.is_connected());
        assert_eq!(transport.pending_len(), 0);
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
