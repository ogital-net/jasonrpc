//! HTTP -> UDS gateway: one representative use case.
//!
//! This example ties the layers together into a proxy that rewrites and
//! forwards raw request bytes over a shared multiplexed connection — a
//! demanding workload that motivated the transport-free client split. It is
//! one of many ways to use the crate, not the only one.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example gateway_demo \
//!     --features "server,http-client,hyper,uds,netstring,tokio"
//! ```
//!
//! Topology:
//!
//! ```text
//!   HTTP client --POST /rpc--> Gateway (AAA) --netstring/UDS--> Upstream
//!    (Bearer token)           rewrites ids      one shared,       Router
//!                             proxies bytes     multiplexed conn
//! ```
//!
//! Three parts run in one process:
//!
//! 1. **Upstream** -- a plain `jasonrpc` server on a Unix socket, `netstring`
//!    framed. It knows nothing about HTTP or auth.
//! 2. **Gateway** -- a `hyper` HTTP front door. It performs AAA (checks a Bearer
//!    token), then proxies the JSON-RPC body to the upstream over a single
//!    long-lived, multiplexed UDS connection shared by all HTTP clients.
//! 3. **Client** -- the built-in [`HttpTransport`] with an `Authorization`
//!    header, calling the gateway as if it were the service.
//!
//! The **id rewriting** is the load-bearing detail. Every HTTP client starts its
//! own id sequence at 1, but they all share one multiplexed upstream connection
//! whose correlation map is keyed by id. So the gateway rewrites each inbound id
//! to a process-unique id before forwarding, then restores the original id on
//! the way back. The upstream connection is a [`UdsClient`]; its raw byte
//! passthrough ([`round_trip_raw`](jasonrpc::client::Client::round_trip_raw))
//! forwards a request without re-parsing it.

#![allow(clippy::ignored_unit_patterns)]

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request as HttpRequest, Response as HttpResponse, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, UnixListener};

use jasonrpc::client::{Client, HttpTransport, UdsClient};
use jasonrpc::protocol::{Id, Request, Response};
use jasonrpc::server::Router;
use jasonrpc::transport::io::FramedConn;
use jasonrpc::transport::Netstring;
use jasonrpc::{json, Error};

const TOKEN: &str = "s3cret-token";

// --- Method payloads -------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct WhoAmI {
    service: String,
    pid: u32,
}

// ===========================================================================
// 1. Upstream: a plain jasonrpc server over a netstring-framed Unix socket.
// ===========================================================================

fn upstream_router() -> Router<()> {
    Router::new()
        .register("add", |_, req: Request| async move {
            let (a, b): (i64, i64) = req.params_as().ok_or_else(Error::invalid_params)?;
            Ok(a + b)
        })
        .register("whoami", |_, _req: Request| async move {
            Ok(WhoAmI {
                service: "upstream".into(),
                pid: std::process::id(),
            })
        })
        .register("boom", |_, _req: Request| async move {
            Err::<(), _>(Error::server_error(-32020, "upstream failure"))
        })
}

async fn run_upstream(listener: UnixListener) {
    let router = upstream_router();
    while let Ok((stream, _)) = listener.accept().await {
        // Each connection gets a cheap clone (shared method table behind an Arc).
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
}

// ===========================================================================
// 2. Gateway: HTTP front door with AAA, proxying to the upstream over a single
//    multiplexed UDS connection.
// ===========================================================================

/// Shared gateway state: the multiplexed client to the upstream plus a
/// process-unique id allocator for rewriting.
#[derive(Clone)]
struct Gateway {
    // `UdsClient<Netstring>` is the multiplexed Unix-socket client; it is not
    // itself `Clone` (each client owns its id sequence), so share it via `Arc`.
    upstream: Arc<UdsClient<Netstring>>,
    next_id: Arc<AtomicI64>,
}

impl Gateway {
    /// Proxy one already-authenticated JSON-RPC request value to the upstream,
    /// rewriting its id into our shared id space and restoring it on return.
    ///
    /// Returns `None` for a notification (nothing to send back).
    async fn proxy_one(&self, mut req: Request) -> Option<Response> {
        let original = req.id().cloned();

        // Notification: forward send-only, no response.
        let Some(orig_id) = original.clone() else {
            let bytes = json::to_vec(&req).unwrap_or_default();
            // Best-effort; a failed notification is not reported to the caller.
            let _ = self.upstream.send_raw_notification(bytes).await;
            return None;
        };

        // Rewrite to a process-unique id so concurrent clients don't collide on
        // the shared upstream connection's correlation map.
        let gw_id = Id::Number(self.next_id.fetch_add(1, Ordering::Relaxed));
        req.set_id(gw_id.clone());

        let request_bytes = json::to_vec(&req).unwrap_or_default();
        match self.upstream.round_trip_raw(request_bytes).await {
            Ok(reply) => {
                // Restore the caller's original id before returning.
                match json::from_slice::<Response>(&reply) {
                    Ok(mut resp) => {
                        resp.set_id(orig_id);
                        Some(resp)
                    }
                    Err(_) => Some(Response::error(
                        original.unwrap_or(Id::Null),
                        Error::internal_error(),
                    )),
                }
            }
            Err(_) => Some(Response::error(orig_id, Error::internal_error())),
        }
    }
}

/// The gateway's HTTP handler: AAA, then proxy.
async fn gateway_handle(
    gw: Gateway,
    req: HttpRequest<Incoming>,
) -> Result<HttpResponse<Full<Bytes>>, std::convert::Infallible> {
    // --- Authentication: require a matching Bearer token. ---
    let authed = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .is_some_and(|t| t == TOKEN);

    if !authed {
        return Ok(HttpResponse::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Full::new(Bytes::from_static(b"unauthorized")))
            .unwrap());
    }

    // --- Read the JSON-RPC body and proxy it. ---
    let body = if let Ok(c) = req.into_body().collect().await {
        c.to_bytes()
    } else {
        let err = Response::error(Id::Null, Error::parse_error());
        return Ok(json_response(json::to_vec(&err).unwrap_or_default()));
    };

    // Single request only in this demo (batches would map each entry the same
    // way). Parse, proxy, respond.
    let response = if let Ok(rpc_req) = json::from_slice::<Request>(&body) {
        gw.proxy_one(rpc_req).await
    } else {
        Some(Response::error(Id::Null, Error::invalid_request()))
    };

    match response {
        Some(resp) => Ok(json_response(json::to_vec(&resp).unwrap_or_default())),
        None => Ok(HttpResponse::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap()),
    }
}

fn json_response(body: Vec<u8>) -> HttpResponse<Full<Bytes>> {
    HttpResponse::builder()
        .status(StatusCode::OK)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

// ===========================================================================
// main: wire the three parts together.
// ===========================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Start the upstream on a Unix socket.
    let sock = std::env::temp_dir().join(format!("jasonrpc-gw-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);
    let uds_listener = UnixListener::bind(&sock)?;
    tokio::spawn(run_upstream(uds_listener));

    // 2. Dial the upstream once and multiplex all gateway traffic over it.
    //    Set a request timeout so a hung upstream doesn't block all callers.
    let upstream = UdsClient::connect(&sock, Netstring)
        .await?
        .with_request_timeout(Duration::from_secs(10));
    let gw = Gateway {
        upstream: Arc::new(upstream),
        next_id: Arc::new(AtomicI64::new(1)),
    };

    // 3. Start the HTTP gateway.
    let http_listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let gw_addr = http_listener.local_addr()?;
    tokio::spawn(async move {
        while let Ok((stream, _)) = http_listener.accept().await {
            let gw = gw.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req| gateway_handle(gw.clone(), req));
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });

    // --- Drive it as an HTTP client with a Bearer token. ---
    let endpoint = format!("http://{gw_addr}/rpc");
    let client = Client::new(
        HttpTransport::new(&endpoint)?.with_header("authorization", format!("Bearer {TOKEN}"))?,
    )
    .with_request_timeout(Duration::from_secs(5));

    let sum: i64 = client.call("add", (3, 4)).await?;
    println!("add(3, 4) via gateway = {sum}");

    let who: WhoAmI = client.call("whoami", ()).await?;
    println!("whoami via gateway = {who:?}");

    // Fire many calls concurrently: they share one HTTP client and, downstream,
    // one multiplexed UDS connection. The gateway's id rewriting keeps their
    // replies from crossing wires.
    let client = Arc::new(client);
    let mut handles = Vec::new();
    for i in 0..20 {
        let c = Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            let got: i64 = c.call("add", (i, 100)).await.unwrap();
            assert_eq!(got, i + 100);
            got
        }));
    }
    let mut total = 0;
    for h in handles {
        total += h.await?;
    }
    println!("20 concurrent add() calls through the gateway summed to {total}");

    match client.call::<_, ()>("boom", ()).await {
        Ok(_) => println!("unexpected ok"),
        Err(e) => println!("boom via gateway -> error as expected: {e}"),
    }

    // --- A request with the wrong token is rejected at the gateway. ---
    let bad =
        Client::new(HttpTransport::new(&endpoint)?.with_header("authorization", "Bearer wrong")?)
            .with_request_timeout(Duration::from_secs(5));
    match bad.call::<_, i64>("add", (1, 1)).await {
        Ok(v) => println!("unexpected ok: {v}"),
        Err(e) => println!("bad token -> rejected as expected: {e}"),
    }

    let _ = std::fs::remove_file(&sock);
    Ok(())
}
