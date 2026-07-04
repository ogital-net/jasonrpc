//! A WebSocket controller: a `hyper` server that accepts device WebSocket
//! connections and dispatches their JSON-RPC messages through a [`Router`].
//!
//! Run with:
//!
//! ```sh
//! cargo run --example ws_controller --features "server,websocket,tokio,hyper"
//! ```
//!
//! This mirrors the shape of device-management protocols such as OpenWiFi's
//! uCentral, where a device opens a WebSocket to the controller and sends
//! JSON-RPC notifications (`connect`, `state`, `healthcheck`, ...). Here the
//! example both stands up the controller and drives it with a simple built-in
//! device client so it runs end to end.
//!
//! The controller side is the interesting part:
//!
//! 1. `hyper` accepts the HTTP connection.
//! 2. `integration::websocket::upgrade` performs the WebSocket handshake.
//! 3. [`serve`](jasonrpc::integration::websocket::serve) drives the [`Router`]
//!    over the upgraded socket: each inbound message is dispatched by `method`,
//!    and any response is written back.

#![allow(clippy::ignored_unit_patterns)]

use std::convert::Infallible;

use http_body_util::Empty;
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request as HttpRequest, Response as HttpResponse};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;

use jasonrpc::integration::websocket::{self, upgrade};
use jasonrpc::server::Router;
use jasonrpc::{Error, Request};

// --- Device message payloads (a subset of the uCentral protocol) ----------

#[derive(Debug, Serialize, Deserialize)]
struct Connect {
    serial: String,
    firmware: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Ping {
    serial: String,
}

#[derive(Debug, Serialize)]
struct PingReply {
    serial: String,
    #[serde(rename = "deviceUTCTime")]
    device_utc_time: i64,
}

// --- Controller: the Router that handles device messages -------------------

fn controller_router() -> Router<()> {
    Router::new()
        // Device connection notification (no reply).
        .register("connect", |_, req: Request| async move {
            let c: Connect = req.params_as().ok_or_else(Error::invalid_params)?;
            println!(
                "[controller] device {} connected (fw {})",
                c.serial, c.firmware
            );
            Ok(())
        })
        // Periodic state notification (no reply).
        .register("state", |_, _req: Request| async move {
            println!("[controller] received state report");
            Ok(())
        })
        // A keepalive the controller answers, to show request/response too.
        .register("ping", |_, req: Request| async move {
            let p: Ping = req.params_as().ok_or_else(Error::invalid_params)?;
            println!("[controller] ping from {}", p.serial);
            Ok(PingReply {
                serial: p.serial,
                device_utc_time: 0,
            })
        })
}

/// One accepted HTTP connection: upgrade to WebSocket, then serve the router.
async fn handle(req: HttpRequest<Incoming>) -> Result<HttpResponse<Empty<Bytes>>, Infallible> {
    let (response, fut) = match upgrade::upgrade(req) {
        Ok(pair) => pair,
        Err(_) => {
            // Not a WebSocket upgrade request; reply 400.
            let mut resp = HttpResponse::new(Empty::new());
            *resp.status_mut() = hyper::StatusCode::BAD_REQUEST;
            return Ok(resp);
        }
    };

    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ws) => ws,
            Err(e) => {
                eprintln!("[controller] upgrade failed: {e}");
                return;
            }
        };
        if let Err(e) = websocket::serve(ws, &controller_router()).await {
            eprintln!("[controller] connection ended: {e}");
        }
    });

    Ok(response)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Start the controller (WebSocket server).
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service_fn(handle))
                    .with_upgrades()
                    .await;
            });
        }
    });

    // 2. Drive it with a device client that opens a WebSocket and sends a few
    //    uCentral-style messages.
    let ws = connect_device(addr).await?;
    let mut ws = fastwebsockets::FragmentCollector::new(ws);

    // connect notification (no reply expected)
    send(
        &mut ws,
        br#"{"jsonrpc":"2.0","method":"connect","params":{"serial":"aabbccddeeff","firmware":"1.2.3"}}"#,
    )
    .await?;

    // state notification (no reply expected)
    send(
        &mut ws,
        br#"{"jsonrpc":"2.0","method":"state","params":{"serial":"aabbccddeeff"}}"#,
    )
    .await?;

    // ping call (expects a reply)
    send(
        &mut ws,
        br#"{"jsonrpc":"2.0","method":"ping","params":{"serial":"aabbccddeeff"},"id":1}"#,
    )
    .await?;
    let reply = ws.read_frame().await?;
    println!(
        "[device] ping reply: {}",
        String::from_utf8_lossy(&reply.payload)
    );

    ws.write_frame(fastwebsockets::Frame::close(1000, b""))
        .await?;
    Ok(())
}

// --- Device-side WebSocket client (for the demo) ---------------------------

async fn connect_device(
    addr: std::net::SocketAddr,
) -> Result<
    fastwebsockets::WebSocket<TokioIo<hyper::upgrade::Upgraded>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    use hyper::header::{CONNECTION, UPGRADE};

    let stream = TcpStream::connect(addr).await?;
    let req = HttpRequest::builder()
        .method("GET")
        .uri(format!("http://{addr}/"))
        .header("Host", addr.to_string())
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header(
            "Sec-WebSocket-Key",
            fastwebsockets::handshake::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())?;

    let (ws, _) = fastwebsockets::handshake::client(&SpawnExecutor, req, stream).await?;
    Ok(ws)
}

async fn send<S>(
    ws: &mut fastwebsockets::FragmentCollector<S>,
    msg: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    ws.write_frame(fastwebsockets::Frame::text(
        fastwebsockets::Payload::Borrowed(msg),
    ))
    .await?;
    Ok(())
}

/// Ties hyper's executor to the tokio runtime for the client handshake.
struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: std::future::Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}
