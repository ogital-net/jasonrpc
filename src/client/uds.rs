//! Unix-domain-socket client.
//!
//! [`UdsClient`] is a [`Client`] over a [`ReconnectingUds`] transport backed by
//! a [`UnixStream`]. The [`connect`](UdsClient::connect) constructor
//! establishes the connection and installs the framing in one call.
//!
//! # Automatic reconnection
//!
//! A Unix socket connection is long-lived, and the peer can go away underneath
//! it — most commonly when the server process restarts. Rather than leaving the
//! client permanently broken (every subsequent call failing), the UDS client
//! transparently re-dials the socket when it notices the connection has died.
//!
//! Recovery is governed by a [`RetryPolicy`] (bounded attempts with exponential
//! backoff). The semantics are deliberately conservative:
//!
//! - A call that is **awaiting its reply** when the connection drops **fails**
//!   with [`MultiplexError::ConnectionClosed`]. JSON-RPC methods are not assumed
//!   idempotent, so a request that may already have reached the server is never
//!   silently replayed.
//! - The **next** call observes the dead connection, re-dials (with backoff),
//!   and is sent over the fresh connection. Because that request never left the
//!   client, sending it on a new connection is safe.
//!
//! This module is available on `unix` targets with the `uds` feature enabled.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::UnixStream;

use super::{Client, MultiplexError, MultiplexOver, MultiplexTransport, Transport};
use crate::error::TransportError;
use crate::transport::Framing;

/// Policy governing how the UDS client re-dials a dropped connection.
///
/// Reconnection makes at most [`max_attempts`](RetryPolicy::max_attempts) dials.
/// The first attempt happens immediately (a restarted server is often already
/// back); subsequent attempts wait an exponentially growing delay, starting at
/// [`base_backoff`](RetryPolicy::base_backoff) and doubling up to
/// [`max_backoff`](RetryPolicy::max_backoff). If every attempt fails, the call
/// that triggered the reconnect surfaces the connection error.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of dial attempts before giving up. Must be at least 1.
    pub max_attempts: u32,
    /// Delay before the second attempt; doubles each subsequent attempt.
    pub base_backoff: Duration,
    /// Upper bound on the backoff delay between attempts.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    /// A policy that never reconnects: a single dial attempt, and once the
    /// connection dies it stays dead (matching the pre-reconnection behavior).
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }
}

/// A self-healing multiplexing transport over a Unix-domain socket.
///
/// Wraps a [`MultiplexTransport`] and, when it detects the connection has
/// closed, re-dials the socket according to a [`RetryPolicy`]. Cheap to clone:
/// clones share the same live connection and reconnection state.
///
/// You rarely construct this directly — [`UdsClient::connect`] builds a
/// [`Client`] around it for you.
#[derive(Clone)]
pub struct ReconnectingUds<F> {
    path: Arc<PathBuf>,
    framing: F,
    policy: RetryPolicy,
    /// The current live transport. Cloned out under the lock (cheaply, no
    /// awaits held) and replaced wholesale on reconnect.
    current: Arc<Mutex<MultiplexOver<UnixStream, F>>>,
    /// Serializes reconnection so a burst of failing calls triggers a single
    /// re-dial rather than a thundering herd.
    reconnect_gate: Arc<tokio::sync::Mutex<()>>,
}

impl<F> std::fmt::Debug for ReconnectingUds<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReconnectingUds")
            .field("path", &self.path)
            .field("policy", &self.policy)
            .finish()
    }
}

impl<F> ReconnectingUds<F>
where
    F: Framing + Clone + Send + Sync + 'static,
{
    /// Dial `path` once and build a reconnecting transport with `policy`.
    async fn connect(
        path: impl AsRef<std::path::Path>,
        framing: F,
        policy: RetryPolicy,
    ) -> Result<Self, std::io::Error> {
        let path = Arc::new(path.as_ref().to_path_buf());
        let stream = UnixStream::connect(path.as_ref()).await?;
        let current = MultiplexTransport::new(stream, framing.clone());
        Ok(Self {
            path,
            framing,
            policy,
            current: Arc::new(Mutex::new(current)),
            reconnect_gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Clone out the current transport handle without holding the lock across
    /// an await.
    fn snapshot(&self) -> MultiplexOver<UnixStream, F> {
        self.current.lock().unwrap().clone()
    }

    /// Return a live transport, re-dialing first if the current one is dead.
    async fn ensure_connected(&self) -> Result<MultiplexOver<UnixStream, F>, MultiplexError> {
        let current = self.snapshot();
        if current.is_connected() {
            return Ok(current);
        }
        self.reconnect().await
    }

    /// Re-dial the socket according to the retry policy and install the fresh
    /// connection. Only one task dials at a time; others reuse its result.
    async fn reconnect(&self) -> Result<MultiplexOver<UnixStream, F>, MultiplexError> {
        let _gate = self.reconnect_gate.lock().await;

        // Another task may have reconnected while we waited on the gate.
        let current = self.snapshot();
        if current.is_connected() {
            return Ok(current);
        }

        let mut backoff = self.policy.base_backoff;
        let mut last_err: Option<std::io::Error> = None;

        for attempt in 0..self.policy.max_attempts.max(1) {
            if attempt > 0 {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(self.policy.max_backoff);
            }
            match UnixStream::connect(self.path.as_ref()).await {
                Ok(stream) => {
                    let fresh = MultiplexTransport::new(stream, self.framing.clone());
                    *self.current.lock().unwrap() = fresh.clone();
                    return Ok(fresh);
                }
                Err(e) => last_err = Some(e),
            }
        }

        Err(last_err
            .map(|e| MultiplexError::Transport(TransportError::Io(e)))
            .unwrap_or(MultiplexError::ConnectionClosed))
    }
}

impl<F> Transport for ReconnectingUds<F>
where
    F: Framing + Clone + Send + Sync + 'static,
{
    type Error = MultiplexError;

    async fn round_trip(&self, request: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        // Re-dial up front if the connection is known dead; this request never
        // left the client, so sending it on a fresh connection is safe.
        let transport = self.ensure_connected().await?;
        // If the connection dies while this specific call is awaiting its reply,
        // `round_trip` yields `ConnectionClosed`. We propagate it rather than
        // replaying: the request may already have executed on the server. The
        // next call will observe the dead connection and reconnect.
        transport.round_trip(request).await
    }

    async fn send_notification(&self, notification: Vec<u8>) -> Result<(), Self::Error> {
        let transport = self.ensure_connected().await?;
        transport.send_notification(notification).await
    }
}

/// A multiplexed, self-reconnecting JSON-RPC client over a Unix-domain socket.
///
/// A type alias for the [`Client`] produced by
/// [`connect`](UdsClient::connect); all [`Client`] methods (`call`, `notify`,
/// `round_trip_raw`, `with_request_timeout`) apply. Calls are multiplexed over
/// a single connection and correlated by id, so concurrent calls may be issued
/// by wrapping the client in an [`Arc`](std::sync::Arc) and sharing it across
/// tasks.
///
/// If the connection drops (for example, the server restarts), the client
/// re-dials the socket on the next call according to its [`RetryPolicy`]. See
/// [`ReconnectingUds`] for the exact recovery semantics.
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// use jasonrpc::client::UdsClient;
/// use jasonrpc::transport::Netstring;
///
/// let client = UdsClient::connect("/run/app.sock", Netstring).await?;
/// let sum: i64 = client.call("add", (1, 2)).await?;
/// assert_eq!(sum, 3);
/// # Ok(())
/// # }
/// ```
pub type UdsClient<F> = Client<ReconnectingUds<F>>;

impl<F> Client<ReconnectingUds<F>>
where
    F: Framing + Clone + Send + Sync + 'static,
{
    /// Connects to the Unix socket at `path` and returns a client that frames
    /// messages with `framing` and reconnects automatically using the
    /// [default](RetryPolicy::default) [`RetryPolicy`].
    ///
    /// The connection is long-lived: a background task reads replies and
    /// correlates them to callers by id, so the returned client (and any
    /// clones sharing it) multiplex their calls over it. If the connection
    /// drops, the next call transparently re-dials. Use
    /// [`with_request_timeout`](Client::with_request_timeout) to bound a call
    /// (including any reconnection time) against an unresponsive peer.
    ///
    /// # Errors
    ///
    /// Returns the [`std::io::Error`] from [`UnixStream::connect`] if the
    /// socket cannot be reached for the initial connection.
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::time::Duration;
    /// use jasonrpc::client::UdsClient;
    /// use jasonrpc::transport::Netstring;
    ///
    /// let client = UdsClient::connect("/run/app.sock", Netstring)
    ///     .await?
    ///     .with_request_timeout(Duration::from_secs(5));
    /// # let _ = client;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(
        path: impl AsRef<std::path::Path>,
        framing: F,
    ) -> Result<Self, std::io::Error> {
        Self::connect_with_policy(path, framing, RetryPolicy::default()).await
    }

    /// Like [`connect`](Self::connect), but with an explicit reconnection
    /// [`RetryPolicy`]. Pass [`RetryPolicy::none`] to opt out of reconnection.
    ///
    /// # Errors
    ///
    /// Returns the [`std::io::Error`] from [`UnixStream::connect`] if the
    /// socket cannot be reached for the initial connection.
    pub async fn connect_with_policy(
        path: impl AsRef<std::path::Path>,
        framing: F,
        policy: RetryPolicy,
    ) -> Result<Self, std::io::Error> {
        let transport = ReconnectingUds::connect(path, framing, policy).await?;
        Ok(Self::new(transport))
    }
}

#[cfg(all(test, feature = "netstring", feature = "server"))]
mod tests {
    use super::*;
    use crate::server::Router;
    use crate::transport::io::FramedConn;
    use crate::transport::Netstring;
    use crate::{Error, Request};
    use std::sync::Arc;
    use tokio::net::UnixListener;

    /// A process-wide counter keeps socket names short and unique (the OS caps
    /// `sockaddr_un` paths at ~104 bytes, so a nanosecond timestamp is too long).
    static SOCK_SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

    /// The `add` router shared by every test server instance.
    fn test_router() -> Router {
        Router::new().register("add", |_, req: Request| async move {
            let (a, b): (i64, i64) = req.params_as().ok_or_else(Error::invalid_params)?;
            Ok(a + b)
        })
    }

    /// Bind a server on an existing `path` and serve until the returned handle
    /// is aborted (or dropped). Used to simulate a server restart by aborting
    /// one instance and binding a fresh one on the same path.
    ///
    /// Connection tasks are tracked in a `JoinSet` owned by the accept-loop
    /// task, so aborting the returned handle drops the set and cancels every
    /// live connection too — genuinely dropping the client's socket, the way a
    /// real server restart would.
    fn serve_at(path: std::path::PathBuf) -> tokio::task::JoinHandle<()> {
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        let router = test_router();
        tokio::spawn(async move {
            let mut conns = tokio::task::JoinSet::new();
            while let Ok((stream, _)) = listener.accept().await {
                let router = router.clone();
                conns.spawn(async move {
                    let mut conn = FramedConn::new(stream, Netstring);
                    while let Ok(Some(frame)) = conn.recv().await {
                        let out = router.handle_bytes(&frame).await;
                        if let Ok(Some(bytes)) = out.to_bytes() {
                            if conn.send(&bytes).await.is_err() {
                                break;
                            }
                        }
                    }
                });
            }
        })
    }

    /// Allocate a unique socket path (does not bind).
    fn unique_sock() -> std::path::PathBuf {
        let seq = SOCK_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("jrpc-uds-{}-{seq}.sock", std::process::id()))
    }

    /// A minimal netstring-framed UDS server backed by a `Router`, used to
    /// exercise the client end to end.
    async fn spawn_server() -> std::path::PathBuf {
        let sock = unique_sock();
        serve_at(sock.clone());
        sock
    }

    #[tokio::test]
    async fn connect_and_call() {
        let sock = spawn_server().await;
        let client = UdsClient::connect(&sock, Netstring).await.unwrap();
        let sum: i64 = client.call("add", (20, 22)).await.unwrap();
        assert_eq!(sum, 42);
        let _ = std::fs::remove_file(&sock);
    }

    #[tokio::test]
    async fn connect_missing_socket_errors() {
        let missing = std::env::temp_dir().join("jasonrpc-nonexistent-xyz.sock");
        let _ = std::fs::remove_file(&missing);
        let err = UdsClient::connect(&missing, Netstring).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn shared_client_multiplexes_concurrent_calls() {
        let sock = spawn_server().await;
        let client = Arc::new(UdsClient::connect(&sock, Netstring).await.unwrap());
        // Many concurrent calls over the one shared connection, correlated by id.
        let mut handles = Vec::new();
        for i in 0..16 {
            let c = Arc::clone(&client);
            handles.push(tokio::spawn(async move {
                let got: i64 = c.call("add", (i, 100)).await.unwrap();
                assert_eq!(got, i + 100);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let _ = std::fs::remove_file(&sock);
    }

    #[tokio::test]
    async fn reconnects_after_server_restart() {
        let sock = unique_sock();
        let server = serve_at(sock.clone());

        let client = UdsClient::connect(&sock, Netstring).await.unwrap();
        let sum: i64 = client.call("add", (1, 2)).await.unwrap();
        assert_eq!(sum, 3);

        // "Restart" the server: abort the current instance so the client's
        // connection drops, then bind a fresh instance on the same path.
        server.abort();
        let _ = server.await;
        // Give the client's reader loop time to observe EOF.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let server2 = serve_at(sock.clone());

        // The next call should transparently re-dial and succeed.
        let sum: i64 = client.call("add", (10, 20)).await.unwrap();
        assert_eq!(sum, 30);

        server2.abort();
        let _ = std::fs::remove_file(&sock);
    }

    #[tokio::test]
    async fn reconnect_gives_up_when_server_stays_down() {
        let sock = unique_sock();
        let server = serve_at(sock.clone());

        let policy = RetryPolicy {
            max_attempts: 3,
            base_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(20),
        };
        let client = UdsClient::connect_with_policy(&sock, Netstring, policy)
            .await
            .unwrap();
        let sum: i64 = client.call("add", (1, 2)).await.unwrap();
        assert_eq!(sum, 3);

        // Take the server down for good and remove the socket file.
        server.abort();
        let _ = server.await;
        let _ = std::fs::remove_file(&sock);
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Reconnection is attempted but every dial fails; the call errors.
        let err = client.call::<_, i64>("add", (1, 2)).await.unwrap_err();
        assert!(matches!(err, crate::error::ClientError::Transport(_)));
    }
}
