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

use fastwebsockets::{Frame, OpCode, Payload, WebSocket, WebSocketError};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::server::{RequestContext, Router};

/// Re-export of the `fastwebsockets` server-side upgrade helper.
///
/// Call [`upgrade`](upgrade::upgrade) on an incoming `hyper` request to obtain
/// the HTTP `101 Switching Protocols` response plus a future that resolves to
/// the [`WebSocket`] once the response has been sent. Pass that socket to
/// [`serve`].
pub use fastwebsockets::upgrade;

/// Default maximum WebSocket message size: 16 MiB.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// WebSocket serving limits.
#[derive(Clone, Copy, Debug)]
pub struct ServeConfig {
    max_message_size: usize,
}

impl ServeConfig {
    /// Set the maximum aggregate text or binary message size in bytes. A limit
    /// of 0 accepts only empty messages.
    #[must_use]
    pub const fn with_max_message_size(mut self, limit: usize) -> Self {
        self.max_message_size = limit;
        self
    }
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

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

enum MessageRead<'a> {
    Frame(Frame<'a>),
    TooLarge,
}

struct MessageSocket<S> {
    socket: WebSocket<S>,
    fragments: Option<(OpCode, Vec<u8>)>,
    max_message_size: usize,
}

impl<S> MessageSocket<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn new(mut socket: WebSocket<S>, config: ServeConfig) -> Self {
        socket.set_auto_close(true);
        socket.set_auto_pong(true);
        // Control frames may carry up to 125 bytes and are not application
        // messages, so small configured limits must not reject valid control
        // traffic in the underlying per-frame check.
        socket.set_max_message_size(config.max_message_size.saturating_add(1).max(126));
        Self {
            socket,
            fragments: None,
            max_message_size: config.max_message_size,
        }
    }

    async fn read_frame(&mut self) -> Result<MessageRead<'_>, WebSocketError> {
        loop {
            let frame = match self.socket.read_frame().await {
                Ok(frame) => frame,
                Err(WebSocketError::FrameTooLarge) => return Ok(MessageRead::TooLarge),
                Err(error) => return Err(error),
            };

            match frame.opcode {
                OpCode::Text | OpCode::Binary => {
                    if self.fragments.is_some() {
                        return Err(WebSocketError::InvalidFragment);
                    }
                    if frame.payload.len() > self.max_message_size {
                        return Ok(MessageRead::TooLarge);
                    }
                    if frame.fin {
                        return Ok(MessageRead::Frame(frame));
                    }
                    self.fragments = Some((frame.opcode, frame.payload.to_vec()));
                }
                OpCode::Continuation => {
                    let Some((_, payload)) = self.fragments.as_mut() else {
                        return Err(WebSocketError::InvalidContinuationFrame);
                    };
                    let Some(new_len) = payload.len().checked_add(frame.payload.len()) else {
                        return Ok(MessageRead::TooLarge);
                    };
                    if new_len > self.max_message_size {
                        return Ok(MessageRead::TooLarge);
                    }
                    payload.extend_from_slice(&frame.payload);
                    if frame.fin {
                        let (opcode, payload) = self.fragments.take().unwrap();
                        if opcode == OpCode::Text && std::str::from_utf8(&payload).is_err() {
                            return Err(WebSocketError::InvalidUTF8);
                        }
                        return Ok(MessageRead::Frame(Frame::new(
                            true,
                            opcode,
                            None,
                            payload.into(),
                        )));
                    }
                }
                _ => return Ok(MessageRead::Frame(frame)),
            }
        }
    }

    async fn write_frame(&mut self, frame: Frame<'_>) -> Result<(), WebSocketError> {
        self.socket.write_frame(frame).await
    }

    async fn close_too_large(&mut self) {
        let _ = self
            .write_frame(Frame::close(1009, b"message too large"))
            .await;
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
/// Messages default to [`DEFAULT_MAX_MESSAGE_SIZE`], enforced across all
/// fragments before dispatch. Use [`serve_with_config`] and
/// [`ServeConfig::with_max_message_size`] to tune it. Oversized messages close
/// the connection with status code `1009` (Message Too Big).
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
    serve_with_config(socket, router, ServeConfig::default()).await
}

/// Like [`serve`], with explicit serving limits.
pub async fn serve_with_config<S, State>(
    socket: WebSocket<S>,
    router: &Router<State>,
    config: ServeConfig,
) -> Result<(), WsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    State: Clone + Send + Sync + 'static,
{
    serve_with_context_config(socket, router, &RequestContext::default, config).await
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
    socket: WebSocket<S>,
    router: &Router<State>,
    make_ctx: &F,
) -> Result<(), WsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    State: Clone + Send + Sync + 'static,
    F: Fn() -> RequestContext,
{
    serve_with_context_config(socket, router, make_ctx, ServeConfig::default()).await
}

/// Like [`serve_with_context`], with explicit serving limits.
pub async fn serve_with_context_config<S, State, F>(
    socket: WebSocket<S>,
    router: &Router<State>,
    make_ctx: &F,
    config: ServeConfig,
) -> Result<(), WsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    State: Clone + Send + Sync + 'static,
    F: Fn() -> RequestContext,
{
    let mut socket = MessageSocket::new(socket, config);

    loop {
        // `read_frame` handles obligated sends (auto-pong / auto-close) itself.
        let frame = match socket.read_frame().await {
            Ok(MessageRead::Frame(frame)) => frame,
            Ok(MessageRead::TooLarge) => {
                socket.close_too_large().await;
                return Ok(());
            }
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
            // consumed by `MessageSocket` and never surface here.
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
    serve_with_shutdown_config(socket, router, shutdown, ServeConfig::default()).await
}

/// Like [`serve_with_shutdown`], with explicit serving limits.
pub async fn serve_with_shutdown_config<S, State, Fut>(
    socket: WebSocket<S>,
    router: &Router<State>,
    shutdown: Fut,
    config: ServeConfig,
) -> Result<(), WsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    State: Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()>,
{
    serve_with_context_shutdown_config(socket, router, &RequestContext::default, shutdown, config)
        .await
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
    socket: WebSocket<S>,
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
    serve_with_context_shutdown_config(socket, router, make_ctx, shutdown, ServeConfig::default())
        .await
}

/// Like [`serve_with_context_shutdown`], with explicit serving limits.
pub async fn serve_with_context_shutdown_config<S, State, F, Fut>(
    socket: WebSocket<S>,
    router: &Router<State>,
    make_ctx: &F,
    shutdown: Fut,
    config: ServeConfig,
) -> Result<(), WsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    State: Clone + Send + Sync + 'static,
    F: Fn() -> RequestContext,
    Fut: std::future::Future<Output = ()>,
{
    let mut socket = MessageSocket::new(socket, config);

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
            Ok(MessageRead::Frame(frame)) => frame,
            Ok(MessageRead::TooLarge) => {
                socket.close_too_large().await;
                return Ok(());
            }
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
                let (a, b): (i64, i64) = req
                    .params_as()
                    .map_err(|error| Error::invalid_params().with_data(error.to_string()))?;
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

    #[tokio::test]
    async fn oversized_fragmented_message_closes_with_1009() {
        let (server_io, client_io) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let ws = WebSocket::after_handshake(server_io, Role::Server);
            serve_with_config(
                ws,
                &router(),
                ServeConfig::default().with_max_message_size(8),
            )
            .await
        });

        let mut client = WebSocket::after_handshake(client_io, Role::Client);
        client.set_auto_close(false);
        client
            .write_frame(Frame::new(
                false,
                OpCode::Text,
                None,
                Payload::Borrowed(b"12345"),
            ))
            .await
            .unwrap();
        client
            .write_frame(Frame::new(
                true,
                OpCode::Continuation,
                None,
                Payload::Borrowed(b"6789"),
            ))
            .await
            .unwrap();

        let close = client.read_frame().await.unwrap();
        assert_eq!(close.opcode, OpCode::Close);
        assert_eq!(&close.payload[..2], &1009_u16.to_be_bytes());
        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn fragmented_message_at_limit_is_accepted() {
        let (server_io, client_io) = tokio::io::duplex(64 * 1024);
        let full = br#"{"jsonrpc":"2.0","method":"add","params":[40,2],"id":7}"#;
        let limit = full.len();
        let server = tokio::spawn(async move {
            let ws = WebSocket::after_handshake(server_io, Role::Server);
            serve_with_config(
                ws,
                &router(),
                ServeConfig::default().with_max_message_size(limit),
            )
            .await
        });

        let mut client = WebSocket::after_handshake(client_io, Role::Client);
        let (first, second) = full.split_at(20);
        client
            .write_frame(Frame::new(
                false,
                OpCode::Text,
                None,
                Payload::Borrowed(first),
            ))
            .await
            .unwrap();
        client
            .write_frame(Frame::new(
                true,
                OpCode::Continuation,
                None,
                Payload::Borrowed(second),
            ))
            .await
            .unwrap();

        let reply = client.read_frame().await.unwrap();
        assert_eq!(reply.opcode, OpCode::Text);
        assert!(String::from_utf8(reply.payload.to_vec())
            .unwrap()
            .contains("\"result\":42"));

        client.write_frame(Frame::close(1000, b"")).await.unwrap();
        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn control_frame_may_exceed_message_limit() {
        let (server_io, client_io) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            let ws = WebSocket::after_handshake(server_io, Role::Server);
            serve_with_config(
                ws,
                &router(),
                ServeConfig::default().with_max_message_size(1),
            )
            .await
        });

        let mut client = WebSocket::after_handshake(client_io, Role::Client);
        client
            .write_frame(Frame::new(
                true,
                OpCode::Ping,
                None,
                Payload::Borrowed(b"control"),
            ))
            .await
            .unwrap();

        let pong = client.read_frame().await.unwrap();
        assert_eq!(pong.opcode, OpCode::Pong);
        assert_eq!(pong.payload.as_ref(), b"control");

        client.write_frame(Frame::close(1000, b"")).await.unwrap();
        assert!(server.await.unwrap().is_ok());
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
