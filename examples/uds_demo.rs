//! End-to-end Unix-domain-socket client + server example: no HTTP, just
//! JSON-RPC messages framed with `netstring` over a `UnixStream`.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example uds_demo --features "server,client,uds,netstring,tokio"
//! ```
//!
//! It shows:
//!
//! - the server side built from the `transport::Framing` + `FramedConn` I/O
//!   helpers, with a per-read timeout and max frame size for resilience
//! - the client side using [`UdsClient`], which connects and frames in one
//!   call and multiplexes calls over a single connection
//! - `Router` dispatch (by `method`, identical to the HTTP integrations)
//! - `Client::with_request_timeout` for call-level deadline enforcement

#![allow(clippy::ignored_unit_patterns)]

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::{UnixListener, UnixStream};

use jasonrpc::client::UdsClient;
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

// --- Server ----------------------------------------------------------------

fn build_router() -> Router<()> {
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
        })
}

/// Serve framed JSON-RPC over one accepted connection until the peer hangs up.
///
/// Applies a per-read timeout and max frame size so a misbehaving client
/// cannot keep the connection alive indefinitely or exhaust server memory.
///
/// Takes the router by value; `Router` is cheap to clone (an `Arc` bump on the
/// shared method table), so each connection gets its own handle.
async fn serve_conn(router: Router<()>, stream: UnixStream) {
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

    // Accept loop in the background. Each connection gets a cheap clone of the
    // router (shared method table behind an `Arc`).
    let accept = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(serve_conn(router.clone(), stream));
        }
    });

    // Drive it with the client. A 5-second timeout per call ensures the demo
    // doesn't hang indefinitely if something goes wrong.
    // `UdsClient::connect` dials the socket, installs the framing, and returns a
    // ready client that multiplexes calls over the one connection. A per-call
    // timeout keeps the demo from hanging if something goes wrong.
    let client = UdsClient::connect(&sock, Netstring)
        .await?
        .with_request_timeout(Duration::from_secs(5));

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

    // Notification: send-only, no reply expected. The multiplexed transport
    // writes the frame and returns without registering a response waiter.
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
