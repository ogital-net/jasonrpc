//! JSON-RPC over WebSocket.
//!
//! A WebSocket connection carries discrete messages, and in JSON-RPC over
//! WebSocket each message is one JSON-RPC request, response, or notification.
//! [`serve`] drives a [`Router`] over such a connection: it reads each text (or
//! binary) message, dispatches it through the router, and writes any response
//! back as a text message. Ping/pong and close frames are handled by the
//! underlying library.
//!
//! This suits any bidirectional JSON-RPC-over-WebSocket workload — a browser
//! subscribing to a live stats feed, an agent reporting telemetry, or a
//! device-management protocol such as OpenWiFi's uCentral (where a device
//! opens a WebSocket to a controller and the two exchange JSON-RPC messages).
//! Notifications flow in one direction and request/response pairs in either.
//! [`serve`] handles the inbound dispatch (messages arriving on the socket);
//! to *send* a request to the peer and await its reply, hold the write half and
//! correlate replies by id in a handler, or drive the socket directly.
//!
//! WebSocket is a protocol, not a byte framing, so this lives here rather than
//! as a [`Framing`](crate::transport::Framing) codec: the library already
//! yields whole messages, leaving only JSON-RPC dispatch — which the
//! transport-agnostic [`Router`] already does.
//!
//! Built on [`fastwebsockets`]. The [`upgrade`] re-export performs the
//! server-side HTTP upgrade using the `hyper` stack this crate already depends
//! on.

use fastwebsockets::{FragmentCollector, Frame, OpCode, Payload, WebSocket, WebSocketError};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::server::{RequestContext, Router};

/// Re-export of the `fastwebsockets` server-side upgrade helper.
///
/// Call [`upgrade`](upgrade::upgrade) on an incoming `hyper` request to obtain
/// the HTTP `101 Switching Protocols` response plus a future that resolves to
/// the [`WebSocket`] once the response has been sent. Pass that socket to
/// [`serve`].
pub use fastwebsockets::upgrade;

/// Errors from driving a [`Router`] over a WebSocket connection.
#[derive(Debug)]
#[non_exhaustive]
pub enum WsError {
    /// A WebSocket protocol or I/O error.
    WebSocket(WebSocketError),
}

impl std::fmt::Display for WsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WsError::WebSocket(e) => write!(f, "websocket error: {e}"),
        }
    }
}

impl std::error::Error for WsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WsError::WebSocket(e) => Some(e),
        }
    }
}

impl From<WebSocketError> for WsError {
    fn from(e: WebSocketError) -> Self {
        WsError::WebSocket(e)
    }
}

/// Drive a [`Router`] over an established WebSocket connection until the peer
/// closes it or an error occurs.
///
/// Each inbound text or binary message is dispatched through the router with
/// [`handle_bytes`](Router::handle_bytes); any response is written back as a
/// text message. Notifications (and all-notification batches) produce no
/// response, matching the JSON-RPC spec. Ping frames are answered with pong and
/// close frames end the loop (both handled by `fastwebsockets`).
///
/// The `socket` is typically obtained from [`upgrade`] on the server side. Auto
/// ping/pong and close handling are enabled on the passed socket.
///
/// Fragmented messages are reassembled before dispatch, so a handler always
/// sees one complete JSON-RPC message even if the peer split it across several
/// WebSocket frames.
///
/// # Message size limit
///
/// `fastwebsockets` enforces a maximum message size (default 64 MiB) and closes
/// the connection with an error if a peer exceeds it, which bounds per-message
/// memory. To pick your own limit, call
/// [`WebSocket::set_max_message_size`](fastwebsockets::WebSocket::set_max_message_size)
/// on the socket before passing it here. Note the cap applies per *frame*; a
/// peer sending many fragments of a single message can still accumulate beyond
/// it, so set a limit appropriate to your workload for untrusted peers.
///
/// # Errors
///
/// Returns a [`WsError`] if the WebSocket connection fails. A clean close
/// returns `Ok(())`.
pub async fn serve<S, State>(socket: WebSocket<S>, router: &Router<State>) -> Result<(), WsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    State: Clone + Send + Sync + 'static,
{
    serve_with_context(socket, router, &RequestContext::default).await
}

/// Like [`serve`], but builds a fresh [`RequestContext`] for each message via
/// `make_ctx`.
///
/// Use this to pass transport-level metadata — for example an authenticated
/// session or client identity derived from the TLS certificate, or headers
/// captured during the upgrade — to handlers registered with
/// [`register_with_context`](Router::register_with_context). `make_ctx` is
/// called once per inbound message.
///
/// # Errors
///
/// Returns a [`WsError`] if the WebSocket connection fails. A clean close
/// returns `Ok(())`.
pub async fn serve_with_context<S, State, F>(
    mut socket: WebSocket<S>,
    router: &Router<State>,
    make_ctx: &F,
) -> Result<(), WsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    State: Clone + Send + Sync + 'static,
    F: Fn() -> RequestContext,
{
    socket.set_auto_close(true);
    socket.set_auto_pong(true);

    // Wrap in a `FragmentCollector` so a message split across several frames
    // (fin=false plus `Continuation` frames) is reassembled into one payload
    // before dispatch. `WebSocket::read_frame` alone yields raw frames and
    // would hand a partial message to the router.
    let mut socket = FragmentCollector::new(socket);

    loop {
        // `read_frame` handles obligated sends (auto-pong / auto-close) itself.
        let frame = match socket.read_frame().await {
            Ok(frame) => frame,
            Err(WebSocketError::ConnectionClosed) => return Ok(()),
            Err(e) => return Err(WsError::WebSocket(e)),
        };

        match frame.opcode {
            OpCode::Close => return Ok(()),
            OpCode::Text | OpCode::Binary => {
                let output = router
                    .handle_bytes_with_context(&frame.payload, make_ctx())
                    .await;
                // `to_bytes` only errors if our own response fails to serialize
                // (a handler result-type bug); fall back to an internal-error
                // body so the peer still gets a well-formed reply.
                let reply_bytes = match output.to_bytes() {
                    Ok(Some(bytes)) => Some(bytes),
                    Ok(None) => None, // notification / all-notification batch
                    Err(_) => Some(internal_error_body()),
                };
                if let Some(bytes) = reply_bytes {
                    socket
                        .write_frame(Frame::text(Payload::Owned(bytes)))
                        .await?;
                }
            }
            // Ping / Pong are handled by the library; Continuation frames are
            // consumed by the `FragmentCollector` and never surface here.
            _ => {}
        }
    }
}

/// Like [`serve`], but stops cleanly when `shutdown` resolves.
///
/// This is the graceful-shutdown entry point. It drives the [`Router`] exactly
/// like [`serve`], but races each read against the `shutdown` future. When
/// `shutdown` resolves first, the loop stops accepting new messages, sends a
/// WebSocket Close frame (normal-closure `1000`) to the peer, and returns
/// `Ok(())`. A response already being written is allowed to finish first, so no
/// reply is truncated mid-flight.
///
/// `shutdown` is any `Future`, so it composes with whatever signal the
/// application already uses — a [`tokio::sync::oneshot`] receiver, a
/// `broadcast` receiver, `tokio_util`'s `CancellationToken::cancelled`, or a
/// timer. No extra dependency is required.
///
/// # Cancellation safety
///
/// `shutdown` is polled inside a [`tokio::select!`] and may be dropped without
/// completing if the peer closes first. Pass a future that tolerates being
/// dropped (all of the primitives named above do).
///
/// # Errors
///
/// Returns a [`WsError`] if the WebSocket connection fails. A clean close —
/// whether initiated by the peer or by `shutdown` — returns `Ok(())`.
///
/// ```no_run
/// use jasonrpc::integration::websocket::{self, upgrade};
/// use jasonrpc::server::Router;
/// use tokio::sync::watch;
///
/// # async fn run<S>(socket: fastwebsockets::WebSocket<S>, router: Router<()>, mut shutdown_rx: watch::Receiver<bool>)
/// # where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {
/// // Resolve when the shared flag flips to `true`.
/// let shutdown = async move { let _ = shutdown_rx.wait_for(|stop| *stop).await; };
/// websocket::serve_with_shutdown(socket, &router, shutdown).await.unwrap();
/// # }
/// ```
pub async fn serve_with_shutdown<S, State, Fut>(
    socket: WebSocket<S>,
    router: &Router<State>,
    shutdown: Fut,
) -> Result<(), WsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    State: Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()>,
{
    serve_with_context_shutdown(socket, router, &RequestContext::default, shutdown).await
}

/// Like [`serve_with_context`], but stops cleanly when `shutdown` resolves.
///
/// Combines the per-message [`RequestContext`] construction of
/// [`serve_with_context`] with the graceful-shutdown behavior of
/// [`serve_with_shutdown`]. See those functions for details on `make_ctx` and
/// `shutdown` respectively.
///
/// # Errors
///
/// Returns a [`WsError`] if the WebSocket connection fails. A clean close —
/// whether initiated by the peer or by `shutdown` — returns `Ok(())`.
pub async fn serve_with_context_shutdown<S, State, F, Fut>(
    mut socket: WebSocket<S>,
    router: &Router<State>,
    make_ctx: &F,
    shutdown: Fut,
) -> Result<(), WsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    State: Clone + Send + Sync + 'static,
    F: Fn() -> RequestContext,
    Fut: std::future::Future<Output = ()>,
{
    socket.set_auto_close(true);
    socket.set_auto_pong(true);

    // Reassemble fragmented messages before dispatch. See `serve_with_context`.
    let mut socket = FragmentCollector::new(socket);

    // Pin the shutdown future so it holds state across loop iterations: each
    // `select!` re-polls the *same* future rather than restarting it.
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        let frame = tokio::select! {
            biased;
            // Prefer draining a ready frame over an equally-ready shutdown,
            // so an already-arrived request is still answered.
            frame = socket.read_frame() => frame,
            () = shutdown.as_mut() => {
                // Send a normal-closure Close frame and stop. Ignore the
                // write result: the peer may already be gone.
                let _ = socket
                    .write_frame(Frame::close(1000, b"server shutting down"))
                    .await;
                return Ok(());
            }
        };

        // `read_frame` handles obligated sends (auto-pong / auto-close) itself.
        let frame = match frame {
            Ok(frame) => frame,
            Err(WebSocketError::ConnectionClosed) => return Ok(()),
            Err(e) => return Err(WsError::WebSocket(e)),
        };

        match frame.opcode {
            OpCode::Close => return Ok(()),
            OpCode::Text | OpCode::Binary => {
                let output = router
                    .handle_bytes_with_context(&frame.payload, make_ctx())
                    .await;
                let reply_bytes = match output.to_bytes() {
                    Ok(Some(bytes)) => Some(bytes),
                    Ok(None) => None,
                    Err(_) => Some(internal_error_body()),
                };
                if let Some(bytes) = reply_bytes {
                    socket
                        .write_frame(Frame::text(Payload::Owned(bytes)))
                        .await?;
                }
            }
            _ => {}
        }
    }
}

/// A JSON body for a top-level internal error (`-32603`) with a `Null` id.
fn internal_error_body() -> Vec<u8> {
    let err = crate::protocol::Response::error(
        crate::protocol::Id::Null,
        crate::protocol::Error::internal_error(),
    );
    crate::json::to_vec(&err).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, Request};
    use fastwebsockets::Role;

    fn router() -> Router<()> {
        Router::new()
            .register("add", |_, req: Request| async move {
                let (a, b): (i64, i64) = req.params_as().ok_or_else(Error::invalid_params)?;
                Ok(a + b)
            })
            // A notification handler: accepts and records, no reply.
            .register("notify", |_, _req: Request| async move { Ok(()) })
    }

    /// Drive `serve` over one end of an in-memory duplex; talk to it over the
    /// other end as a WebSocket client.
    #[tokio::test]
    async fn dispatches_call_and_suppresses_notification() {
        let (server_io, client_io) = tokio::io::duplex(64 * 1024);

        let server = tokio::spawn(async move {
            let ws = WebSocket::after_handshake(server_io, Role::Server);
            serve(ws, &router()).await
        });

        let mut client = WebSocket::after_handshake(client_io, Role::Client);

        // A call gets a response.
        let call = br#"{"jsonrpc":"2.0","method":"add","params":[40,2],"id":1}"#;
        client
            .write_frame(Frame::text(Payload::Borrowed(call)))
            .await
            .unwrap();
        let reply = client.read_frame().await.unwrap();
        assert_eq!(reply.opcode, OpCode::Text);
        let s = String::from_utf8(reply.payload.to_vec()).unwrap();
        assert!(s.contains("\"result\":42"), "{s}");
        assert!(s.contains("\"id\":1"), "{s}");

        // A notification gets no reply: the next thing the client sees is the
        // response to a *following* call, proving the notification was silent.
        let notif = br#"{"jsonrpc":"2.0","method":"notify","params":[]}"#;
        client
            .write_frame(Frame::text(Payload::Borrowed(notif)))
            .await
            .unwrap();
        let call2 = br#"{"jsonrpc":"2.0","method":"add","params":[1,1],"id":2}"#;
        client
            .write_frame(Frame::text(Payload::Borrowed(call2)))
            .await
            .unwrap();
        let reply = client.read_frame().await.unwrap();
        let s = String::from_utf8(reply.payload.to_vec()).unwrap();
        assert!(s.contains("\"id\":2"), "notification leaked a reply: {s}");
        assert!(s.contains("\"result\":2"), "{s}");

        // Close from the client; the server loop should end cleanly.
        client.write_frame(Frame::close(1000, b"")).await.unwrap();
        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn unknown_method_returns_error_frame() {
        let (server_io, client_io) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let ws = WebSocket::after_handshake(server_io, Role::Server);
            serve(ws, &router()).await
        });

        let mut client = WebSocket::after_handshake(client_io, Role::Client);
        client
            .write_frame(Frame::text(Payload::Borrowed(
                br#"{"jsonrpc":"2.0","method":"nope","id":"x"}"#,
            )))
            .await
            .unwrap();
        let reply = client.read_frame().await.unwrap();
        let s = String::from_utf8(reply.payload.to_vec()).unwrap();
        assert!(s.contains("-32601"), "{s}");
        assert!(s.contains("\"id\":\"x\""), "{s}");

        client.write_frame(Frame::close(1000, b"")).await.unwrap();
        let _ = server.await.unwrap();
    }

    /// A single JSON-RPC message split across multiple WebSocket frames
    /// (an initial non-final Text frame plus `Continuation` frames) is
    /// reassembled and dispatched as one request.
    #[tokio::test]
    async fn reassembles_fragmented_message() {
        let (server_io, client_io) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let ws = WebSocket::after_handshake(server_io, Role::Server);
            serve(ws, &router()).await
        });

        let mut client = WebSocket::after_handshake(client_io, Role::Client);

        // Split a valid request into three byte-chunks sent as fragments.
        let full = br#"{"jsonrpc":"2.0","method":"add","params":[40,2],"id":7}"#;
        let (a, rest) = full.split_at(20);
        let (b, c) = rest.split_at(20);

        // First frame: Text, fin=false. Middle: Continuation, fin=false.
        // Last: Continuation, fin=true.
        client
            .write_frame(Frame::new(false, OpCode::Text, None, Payload::Borrowed(a)))
            .await
            .unwrap();
        client
            .write_frame(Frame::new(
                false,
                OpCode::Continuation,
                None,
                Payload::Borrowed(b),
            ))
            .await
            .unwrap();
        client
            .write_frame(Frame::new(
                true,
                OpCode::Continuation,
                None,
                Payload::Borrowed(c),
            ))
            .await
            .unwrap();

        let reply = client.read_frame().await.unwrap();
        assert_eq!(reply.opcode, OpCode::Text);
        let s = String::from_utf8(reply.payload.to_vec()).unwrap();
        assert!(s.contains("\"result\":42"), "{s}");
        assert!(s.contains("\"id\":7"), "{s}");

        client.write_frame(Frame::close(1000, b"")).await.unwrap();
        let _ = server.await.unwrap();
    }

    /// `serve_with_shutdown` returns `Ok(())` and sends a Close frame when the
    /// shutdown signal fires, even with no peer activity.
    #[tokio::test]
    async fn shutdown_signal_closes_cleanly() {
        let (server_io, client_io) = tokio::io::duplex(64 * 1024);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            let ws = WebSocket::after_handshake(server_io, Role::Server);
            let shutdown = async move {
                let _ = rx.await;
            };
            serve_with_shutdown(ws, &router(), shutdown).await
        });

        let mut client = WebSocket::after_handshake(client_io, Role::Client);
        // Don't auto-echo the Close: the server socket is already gone by then.
        client.set_auto_close(false);

        // Trigger shutdown; the server should close on its own.
        tx.send(()).unwrap();

        let frame = client.read_frame().await.unwrap();
        assert_eq!(frame.opcode, OpCode::Close);
        assert!(server.await.unwrap().is_ok());
    }

    /// A request already on the wire when shutdown fires is still answered
    /// (biased select drains readable frames before honoring shutdown).
    #[tokio::test]
    async fn shutdown_still_answers_pending_request() {
        let (server_io, client_io) = tokio::io::duplex(64 * 1024);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            let ws = WebSocket::after_handshake(server_io, Role::Server);
            let shutdown = async move {
                let _ = rx.await;
            };
            serve_with_shutdown(ws, &router(), shutdown).await
        });

        let mut client = WebSocket::after_handshake(client_io, Role::Client);
        client.set_auto_close(false);
        let call = br#"{"jsonrpc":"2.0","method":"add","params":[40,2],"id":1}"#;
        client
            .write_frame(Frame::text(Payload::Borrowed(call)))
            .await
            .unwrap();

        let reply = client.read_frame().await.unwrap();
        assert_eq!(reply.opcode, OpCode::Text);
        let s = String::from_utf8(reply.payload.to_vec()).unwrap();
        assert!(s.contains("\"result\":42"), "{s}");

        // Now shut down; expect a Close frame next.
        tx.send(()).unwrap();
        let frame = client.read_frame().await.unwrap();
        assert_eq!(frame.opcode, OpCode::Close);
        assert!(server.await.unwrap().is_ok());
    }
}
