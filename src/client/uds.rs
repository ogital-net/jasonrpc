//! Unix-domain-socket client.
//!
//! [`UdsClient`] is a [`Client`] over a [`MultiplexTransport`] backed by a
//! [`UnixStream`]. The [`connect`](UdsClient::connect) constructor establishes
//! the connection and installs the framing in one call.
//!
//! This module is available on `unix` targets with the `uds` feature enabled.

use tokio::net::UnixStream;

use super::{Client, MultiplexOver};
use crate::transport::Framing;

/// A multiplexed JSON-RPC client over a Unix-domain socket.
///
/// A type alias for the [`Client`] produced by
/// [`connect`](UdsClient::connect); all [`Client`] methods (`call`, `notify`,
/// `round_trip_raw`, `with_request_timeout`) apply. Calls are multiplexed over
/// a single connection and correlated by id, so concurrent calls may be issued
/// by wrapping the client in an [`Arc`](std::sync::Arc) and sharing it across
/// tasks.
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
pub type UdsClient<F> = Client<MultiplexOver<UnixStream, F>>;

impl<F> Client<MultiplexOver<UnixStream, F>>
where
    F: Framing + Clone + Send + Sync + 'static,
{
    /// Connects to the Unix socket at `path` and returns a client that frames
    /// messages with `framing`.
    ///
    /// The connection is long-lived: a background task reads replies and
    /// correlates them to callers by id, so the returned client (and any
    /// clones sharing it) multiplex their calls over it. Use
    /// [`with_request_timeout`](Client::with_request_timeout) to bound a call
    /// against an unresponsive peer.
    ///
    /// # Errors
    ///
    /// Returns the [`std::io::Error`] from [`UnixStream::connect`] if the
    /// socket cannot be reached.
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
        let stream = UnixStream::connect(path).await?;
        Ok(Self::new(super::MultiplexTransport::new(stream, framing)))
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

    /// A minimal netstring-framed UDS server backed by a `Router`, used to
    /// exercise the client end to end.
    async fn spawn_server() -> std::path::PathBuf {
        let seq = SOCK_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let sock = std::env::temp_dir().join(format!("jrpc-uds-{}-{seq}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock).unwrap();

        let router = Router::new().register("add", |_, req: Request| async move {
            let (a, b): (i64, i64) = req.params_as().ok_or_else(Error::invalid_params)?;
            Ok(a + b)
        });

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let router = router.clone();
                tokio::spawn(async move {
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
        });

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
}
