//! End-to-end hyper client + server example with a few methods.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example hyper_demo --features "hyper,http-client"
//! ```
//!
//! This spins up a `jasonrpc` server on a hyper connection, then drives it with
//! the `jasonrpc` client over the built-in [`HttpTransport`]. It exercises:
//!
//! - typed handler params (positional `add`, by-name `greet`)
//! - handlers returning plain types and a `#[derive(Serialize)]` struct
//!   directly (no manual `to_value`)
//! - shared server state (a call counter)
//! - a domain error mapped to a JSON-RPC error object
//! - a notification (no response)
//! - `Client::with_request_timeout` for call-level deadlines
//!
//! It's intentionally written the way a real user would write it, so it doubles
//! as an API smoke test.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use jasonrpc::client::{Client, HttpTransport};
use jasonrpc::integration::hyper::HyperService;
use jasonrpc::server::Router;
use jasonrpc::{Error, Request};

// --- Method payloads -------------------------------------------------------

/// Params for `greet`, taken by name. `Serialize` is for the client side,
/// `Deserialize` for the server side.
#[derive(Debug, Serialize, Deserialize)]
struct GreetParams {
    name: String,
    #[serde(default)]
    formal: bool,
}

/// Result of `greet`.
#[derive(Debug, Serialize, Deserialize)]
struct Greeting {
    message: String,
    /// How many times the server has been called overall.
    call_count: u64,
}

// --- Shared server state ---------------------------------------------------

#[derive(Clone, Default)]
struct AppState {
    calls: Arc<AtomicU64>,
}

// --- main ------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = AppState::default();

    let router = Router::with_state(state)
        // Positional params `[a, b]`, returning a plain `i64` directly.
        .register("add", |st: AppState, req: Request| async move {
            st.calls.fetch_add(1, Ordering::Relaxed);
            let (a, b): (i64, i64) = req.params_as().ok_or_else(Error::invalid_params)?;
            Ok(a + b)
        })
        // Fallible method: division that rejects a zero divisor with a
        // domain-specific server error.
        .register("divide", |st: AppState, req: Request| async move {
            st.calls.fetch_add(1, Ordering::Relaxed);
            let (a, b): (i64, i64) = req.params_as().ok_or_else(Error::invalid_params)?;
            if b == 0 {
                return Err(Error::server_error(-32001, "division by zero"));
            }
            Ok(a / b)
        })
        // By-name params returning a struct directly, reading shared state.
        .register("greet", |st: AppState, req: Request| async move {
            let n = st.calls.fetch_add(1, Ordering::Relaxed) + 1;
            let p: GreetParams = req.params_as().ok_or_else(Error::invalid_params)?;
            let hello = if p.formal { "Good day" } else { "Hi" };
            Ok(Greeting {
                message: format!("{hello}, {}!", p.name),
                call_count: n,
            })
        });

    // Bind and serve in the background.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let server_router = router.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let svc = HyperService::new(server_router.clone());
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });

    // Drive it with the client over the built-in HTTP transport.
    let endpoint = format!("http://{addr}/");
    let client =
        Client::new(HttpTransport::new(&endpoint)?).with_request_timeout(Duration::from_secs(10));

    let sum: i64 = client.call("add", (40, 2)).await?;
    println!("add(40, 2) = {sum}");

    let quotient: i64 = client.call("divide", (10, 2)).await?;
    println!("divide(10, 2) = {quotient}");

    let greeting: Greeting = client
        .call(
            "greet",
            GreetParams {
                name: "Ada".into(),
                formal: true,
            },
        )
        .await?;
    println!("greet -> {greeting:?}");

    // A fallible call that trips the domain error.
    match client.call::<_, i64>("divide", (1, 0)).await {
        Ok(v) => println!("unexpected ok: {v}"),
        Err(e) => println!("divide(1, 0) -> error as expected: {e}"),
    }

    // A notification: no response is produced.
    client.notify("add", (1, 1)).await?;
    println!("sent notification (no response expected)");

    Ok(())
}
