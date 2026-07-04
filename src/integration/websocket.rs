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
            // Ping / Pong / Continuation are handled by the library.
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
}
