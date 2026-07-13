//! OAuth 2.0 client-credentials authorization for the HTTP transport.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example oauth_client_credentials --features "hyper,http-client,tokio"
//! ```
//!
//! `jasonrpc` intentionally ships **no** OAuth implementation: token endpoints,
//! client-auth methods, caching, and refresh strategy are consumer policy. What
//! it provides is the [`Authorizer`] hook -- a small async trait the
//! [`HttpTransport`] calls to obtain (and refresh) the `Authorization` header
//! per request.
//!
//! This example implements that trait with a realistic client-credentials token
//! manager:
//!
//! - **Caching**: a fetched token is reused until it nears expiry.
//! - **Proactive refresh**: the token is refreshed a little *before* its
//!   `expires_in` elapses (a skew margin), so requests don't race the clock.
//! - **Single-flight**: a dedicated refresh lock ensures a burst of requests
//!   that all find the token stale triggers exactly one token fetch, not a
//!   stampede against the authorization server.
//! - **Reactive refresh**: if the resource server still answers `401` (early
//!   revocation, clock skew), [`Authorizer::on_unauthorized`] invalidates the
//!   cache and the transport retries once.
//!
//! In production you would replace the hand-rolled token POST in `fetch_token`
//! with the [`oauth2`](https://docs.rs/oauth2) crate's
//! `BasicClient::exchange_client_credentials()`, keeping the same caching shell
//! shown here. The point of this file is the caching/refresh *shape* around the
//! transport-provided [`Authorizer`] trait.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use http::HeaderValue;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request as HyperRequest, Response as HyperResponse, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use jasonrpc::client::{AuthError, Authorizer, Client, HttpTransport};
use jasonrpc::server::Router;
use jasonrpc::Request;

// --- The token manager: a consumer-side Authorizer ------------------------

/// A cached OAuth access token and the instant it should be refreshed at.
struct CachedToken {
    header: HeaderValue,
    refresh_at: Instant,
}

/// The token endpoint's JSON response (RFC 6749 section 5.1, trimmed).
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

/// Client-credentials token manager. Fetches a bearer token from `token_url`,
/// caches it, and refreshes it before expiry. Cheap to `clone` (shared state).
#[derive(Clone)]
struct ClientCredentials {
    http: HyperClient<HttpConnector, Full<Bytes>>,
    token_url: String,
    client_id: String,
    client_secret: String,
    /// Refresh this long *before* the advertised expiry, to absorb clock skew
    /// and in-flight latency.
    skew: Duration,
    cache: Arc<Mutex<Option<CachedToken>>>,
    /// Serializes refreshes so concurrent stale-token callers share one fetch.
    refresh_lock: Arc<Mutex<()>>,
}

impl ClientCredentials {
    fn new(token_url: String, client_id: String, client_secret: String) -> Self {
        Self {
            http: HyperClient::builder(TokioExecutor::new()).build(HttpConnector::new()),
            token_url,
            client_id,
            client_secret,
            // Small margin for the demo's short-lived (2s) tokens. In production
            // this is typically tens of seconds against tokens that live for
            // minutes/hours.
            skew: Duration::from_millis(500),
            cache: Arc::new(Mutex::new(None)),
            refresh_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Return a still-valid cached token, or `None` if absent / due for refresh.
    async fn cached_valid(&self) -> Option<HeaderValue> {
        let guard = self.cache.lock().await;
        guard
            .as_ref()
            .filter(|tok| Instant::now() < tok.refresh_at)
            .map(|tok| tok.header.clone())
    }

    /// POST `grant_type=client_credentials` and parse the token response. This
    /// is the spot to swap in the `oauth2` crate in real code.
    async fn fetch_token(&self) -> Result<TokenResponse, AuthError> {
        let form = format!(
            "grant_type=client_credentials&client_id={}&client_secret={}",
            self.client_id, self.client_secret
        );
        let req = HyperRequest::builder()
            .method("POST")
            .uri(&self.token_url)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Full::new(Bytes::from(form)))?;
        let resp = self.http.request(req).await?;
        if resp.status() != StatusCode::OK {
            return Err(format!("token endpoint returned {}", resp.status()).into());
        }
        let body = resp.into_body().collect().await?.to_bytes();
        Ok(serde_json::from_slice(&body)?)
    }
}

impl Authorizer for ClientCredentials {
    async fn authorize(&self) -> Result<Option<HeaderValue>, AuthError> {
        // Fast path: a cached token that isn't due for refresh yet.
        if let Some(header) = self.cached_valid().await {
            return Ok(Some(header));
        }

        // Slow path: acquire the refresh lock so only one task fetches.
        let _refresh = self.refresh_lock.lock().await;
        // Re-check: another task may have refreshed while we waited on the lock.
        if let Some(header) = self.cached_valid().await {
            return Ok(Some(header));
        }

        let resp = self.fetch_token().await?;
        let header = HeaderValue::try_from(format!("Bearer {}", resp.access_token))?;
        let refresh_at = Instant::now() + Duration::from_secs(resp.expires_in) - self.skew;
        *self.cache.lock().await = Some(CachedToken {
            header: header.clone(),
            refresh_at,
        });
        Ok(Some(header))
    }

    async fn on_unauthorized(&self) -> bool {
        // The resource server rejected our token even though we thought it was
        // valid. Drop it so the retry mints a fresh one, and ask for one retry.
        *self.cache.lock().await = None;
        true
    }
}

// --- Mock authorization server (issues short-lived, unique tokens) --------

async fn spawn_token_server(issued: Arc<AtomicU64>) -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let issued = Arc::clone(&issued);
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |_req| {
                    let issued = Arc::clone(&issued);
                    async move {
                        let n = issued.fetch_add(1, Ordering::SeqCst) + 1;
                        let body = format!(
                            r#"{{"access_token":"tok-{n}","token_type":"Bearer","expires_in":2}}"#
                        );
                        Ok::<_, std::convert::Infallible>(HyperResponse::new(Full::new(
                            Bytes::from(body),
                        )))
                    }
                });
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });
    format!("http://{addr}/token")
}

// --- JSON-RPC resource server (requires a Bearer token) -------------------

async fn spawn_api_server(router: Router) -> String {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let router = router.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: HyperRequest<hyper::body::Incoming>| {
                    let router = router.clone();
                    async move {
                        // Reject anything without a plausible Bearer token.
                        let authed = req
                            .headers()
                            .get(http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .is_some_and(|v| v.starts_with("Bearer tok-"));
                        if !authed {
                            return Ok::<_, std::convert::Infallible>(
                                HyperResponse::builder()
                                    .status(StatusCode::UNAUTHORIZED)
                                    .body(Full::new(Bytes::new()))
                                    .unwrap(),
                            );
                        }
                        // Dispatch the JSON-RPC request body via the router.
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        let out = router.handle_bytes(&body).await;
                        let bytes = out.to_bytes().ok().flatten().unwrap_or_default();
                        Ok(HyperResponse::new(Full::new(Bytes::from(bytes))))
                    }
                });
                let _ = http1::Builder::new().serve_connection(io, svc).await;
            });
        }
    });
    format!("http://{addr}/")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let issued = Arc::new(AtomicU64::new(0));
    let token_url = spawn_token_server(Arc::clone(&issued)).await;

    let router =
        Router::new().register("whoami", |_, _req: Request| async move { Ok("authenticated") });
    let api_url = spawn_api_server(router).await;

    // The client: HTTP transport + client-credentials authorizer.
    let authorizer = ClientCredentials::new(token_url, "my-client".into(), "s3cr3t".into());
    let transport = HttpTransport::new(&api_url)?.with_authorizer(authorizer);
    let client = Client::new(transport).with_request_timeout(Duration::from_secs(5));

    // Call 1: cache empty -> fetch a token, then succeed.
    let who: String = client.call("whoami", ()).await?;
    println!(
        "call 1 -> {who} (tokens issued: {})",
        issued.load(Ordering::SeqCst)
    );

    // Call 2: reuses the cached token, no new fetch.
    let who: String = client.call("whoami", ()).await?;
    println!(
        "call 2 -> {who} (tokens issued: {})",
        issued.load(Ordering::SeqCst)
    );

    // Wait until the token is within the refresh skew window (2s expiry minus
    // 0.5s skew = refresh at ~1.5s) so the next call refreshes proactively.
    tokio::time::sleep(Duration::from_millis(1600)).await;
    let who: String = client.call("whoami", ()).await?;
    println!(
        "call 3 -> {who} (tokens issued: {})",
        issued.load(Ordering::SeqCst)
    );

    Ok(())
}
