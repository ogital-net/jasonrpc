//! End-to-end raw Unix-domain-socket client + server example.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example uds_demo --features "server,client,netstring,tokio"
//! ```
//!
//! This is the non-HTTP path the gateway depends on: no `hyper`, no HTTP, just
//! JSON-RPC messages framed with `netstring` over a `UnixStream`. It exercises:
//!
//! - the `transport::Framing` + `FramedConn` I/O helpers on the server,
//!   including per-read timeouts and max frame size for resilience
//! - a client `Transport` impl over UDS, including the `send_notification`
//!   override (a raw framed socket gets *no* reply frame for a notification, so
//!   it must send-only rather than wait for a response)
//! - `Router` dispatch identical to the HTTP case (dispatch is by `method`)
//! - `Client::with_request_timeout` for call-level deadline enforcement
//!
//! Written the way a real user would, so it doubles as an API smoke test.

#![allow(clippy::ignored_unit_patterns)]

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::{UnixListener, UnixStream};

use jasonrpc::client::{Client, Transport};
use jasonrpc::server::Router;
use jasonrpc::transport::io::FramedConn;
use jasonrpc::transport::Netstring;
use jasonrpc::{Error, Request};

// --- Method payloads -------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct EchoParams {
    text: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct EchoResult {
    echoed: String,
    len: usize,
}

// --- Client transport over UDS --------------------------------------------
//
// One connection per round trip keeps the example simple and is perfectly
// serviceable for a control socket. A pooled/multiplexed variant would reuse a
// single `FramedConn` and correlate by id using the `client::decode_result`
// helper -- that's the shape the gateway will want.

struct UdsTransport {
    path: Arc<std::path::PathBuf>,
}

impl UdsTransport {
    fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            path: Arc::new(path.into()),
        }
    }
}

impl Transport for UdsTransport {
    type Error = jasonrpc::error::TransportError;

    async fn round_trip(&self, request: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        let stream = UnixStream::connect(self.path.as_path()).await?;
        let mut conn = FramedConn::new(stream, Netstring);
        conn.send(&request).await?;
        match conn.recv().await? {
            Some(bytes) => Ok(bytes),
            None => Err(jasonrpc::error::TransportError::Frame(
                "connection closed before a response frame".into(),
            )),
        }
    }

    // A notification gets no reply frame on a raw socket, so send and return
    // without waiting on `recv` (the default impl would block forever here).
    async fn send_notification(&self, notification: Vec<u8>) -> Result<(), Self::Error> {
        let stream = UnixStream::connect(self.path.as_path()).await?;
        let mut conn = FramedConn::new(stream, Netstring);
        conn.send(&notification).await?;
        Ok(())
    }
}

// --- Server ----------------------------------------------------------------

fn build_router() -> Arc<Router<()>> {
    Arc::new(
        Router::new()
            .register("echo", |_, req: Request| async move {
                let p: EchoParams = req.params_as().ok_or_else(Error::invalid_params)?;
                Ok(EchoResult {
                    len: p.text.chars().count(),
                    echoed: p.text,
                })
            })
            .register("add", |_, req: Request| async move {
                let (a, b): (i64, i64) = req.params_as().ok_or_else(Error::invalid_params)?;
                Ok(a + b)
            })
            .register("boom", |_, _req: Request| async move {
                Err::<(), _>(Error::server_error(-32010, "intentional failure"))
            }),
    )
}

/// Serve framed JSON-RPC over one accepted connection until the peer hangs up.
///
/// Applies a per-read timeout and max frame size so a misbehaving client
/// cannot keep the connection alive indefinitely or exhaust server memory.
async fn serve_conn(router: Arc<Router<()>>, stream: UnixStream) {
    let mut conn = FramedConn::new(stream, Netstring)
        .with_read_timeout(Duration::from_secs(30))
        .with_max_frame_size(16 * 1024 * 1024); // 16 MiB
                                                // Exit the loop on clean EOF (`Ok(None)`) or any framing/I/O error.
    while let Ok(Some(frame)) = conn.recv().await {
        let output = router.handle_bytes(&frame).await;
        // Notifications (and all-notification batches) produce no bytes: send
        // nothing back, exactly as the spec requires.
        if let Ok(Some(bytes)) = output.to_bytes() {
            if conn.send(&bytes).await.is_err() {
                return;
            }
        }
    }
}

// --- main ------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // A unique socket path under the OS temp dir.
    let sock = std::env::temp_dir().join(format!("jasonrpc-uds-demo-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);

    let listener = UnixListener::bind(&sock)?;
    let router = build_router();

    // Accept loop in the background.
    let server_router = Arc::clone(&router);
    let accept = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let r = Arc::clone(&server_router);
            tokio::spawn(serve_conn(r, stream));
        }
    });

    // Drive it with the client. A 5-second timeout per call ensures the demo
    // doesn't hang indefinitely if something goes wrong.
    let client =
        Client::new(UdsTransport::new(sock.clone())).with_request_timeout(Duration::from_secs(5));

    let sum: i64 = client.call("add", (20, 22)).await?;
    println!("add(20, 22) = {sum}");

    let echo: EchoResult = client
        .call(
            "echo",
            EchoParams {
                text: "hello".into(),
            },
        )
        .await?;
    println!("echo -> {echo:?}");

    match client.call::<_, ()>("boom", ()).await {
        Ok(_) => println!("unexpected ok"),
        Err(e) => println!("boom -> error as expected: {e}"),
    }

    // Notification: send-only, no reply frame. Would hang with the default
    // round-trip behavior; the transport overrides `send_notification`.
    client
        .notify(
            "echo",
            EchoParams {
                text: "no reply please".into(),
            },
        )
        .await?;
    println!("sent notification (no response expected)");

    accept.abort();
    let _ = std::fs::remove_file(&sock);
    Ok(())
}
