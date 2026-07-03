//! Hyper integration.
//!
//! Small services that use `hyper` directly are supported directly. This
//! module keeps the adapter minimal: a [`Router`] plus a request body becomes
//! a JSON-RPC response body, and [`HyperService`] adapts a router into a
//! `hyper::service::Service` you can hand to a connection.
//!
//! The design deliberately does no routing on the HTTP path/verb: a JSON-RPC
//! endpoint is a single POST target and all dispatch happens by `method`. AAA
//! (authn/authz/accounting) belongs in a layer *around* this -- see the gateway
//! example -- so this stays a pure protocol adapter.

use std::convert::Infallible;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::Service;
use hyper::{Request as HttpRequest, Response as HttpResponse, StatusCode};

use crate::integration::http::{self, HttpOutcome};
use crate::server::{RequestContext, Router};

/// The HTTP body type produced by this integration.
pub type ResponseBody = Full<Bytes>;

/// Collect an HTTP request body, dispatch it through the router, and build the
/// HTTP response.
///
/// Per JSON-RPC-over-HTTP convention: a normal result (single or batch) returns
/// `200 OK` with a JSON body; an all-notification request returns `204 No
/// Content` with an empty body. Body-read failures map to a `-32700` parse
/// error response.
///
/// HTTP request extensions are forwarded to handlers via [`RequestContext`].
///
/// # Panics
///
/// Panics if response construction fails (should never happen with valid
/// status codes and headers).
///
/// # Errors
///
/// This function is infallible (`Error = Infallible`); the `Result` return
/// type is for compatibility with the `Service` trait.
pub(crate) async fn serve_request<S>(
    router: Router<S>,
    req: HttpRequest<Incoming>,
) -> Result<HttpResponse<ResponseBody>, Infallible>
where
    S: Clone + Send + Sync + 'static,
{
    // Split into parts and body so we can extract extensions and headers
    // before consuming the body.
    let (parts, body) = req.into_parts();
    let ctx = RequestContext {
        headers: parts.headers,
        extensions: parts.extensions,
    };

    let body = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return Ok(json_response(StatusCode::OK, http::parse_error_body())),
    };

    let HttpOutcome { status, body } = http::dispatch_with_context(&router, &body, ctx).await;
    if status == 204 {
        Ok(HttpResponse::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .expect("valid 204 response"))
    } else {
        let status = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
        Ok(json_response(status, body))
    }
}

/// Build a JSON `200 OK` response with a JSON content type.
///
/// # Panics
///
/// Panics if response construction fails (should never happen with valid
/// status codes and headers).
fn json_response(status: StatusCode, body: Vec<u8>) -> HttpResponse<ResponseBody> {
    HttpResponse::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("valid json response")
}

/// A cloneable `hyper` service wrapping a [`Router`].
///
/// Hand this to `hyper::server::conn::*` per connection. Cloning is cheap: the
/// router's method table is shared behind an `Arc`, so a clone only bumps that
/// reference count and clones the state handle.
///
/// A full accept loop against hyper 1.x looks like this — clone the service
/// per connection and drive it with `serve_connection`:
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use hyper::server::conn::http1;
/// use hyper_util::rt::TokioIo;
/// use tokio::net::TcpListener;
/// use jasonrpc::server::Router;
/// use jasonrpc::integration::hyper::HyperService;
///
/// let router = Router::new(); // register your methods here
/// let service = HyperService::new(router);
///
/// let listener = TcpListener::bind(("127.0.0.1", 8080)).await?;
/// loop {
///     let (stream, _) = listener.accept().await?;
///     let service = service.clone(); // cheap: Arc bump + state clone
///     tokio::spawn(async move {
///         let io = TokioIo::new(stream);
///         let _ = http1::Builder::new().serve_connection(io, service).await;
///     });
/// }
/// # }
/// ```
///
/// See the `hyper_demo` example for a complete client + server program.
pub struct HyperService<S> {
    router: Router<S>,
}

impl<S: Clone> Clone for HyperService<S> {
    fn clone(&self) -> Self {
        Self {
            router: self.router.clone(),
        }
    }
}

impl<S> std::fmt::Debug for HyperService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperService").finish()
    }
}

impl<S> HyperService<S> {
    /// Wrap a router.
    pub fn new(router: Router<S>) -> Self {
        Self { router }
    }
}

impl<S> Service<HttpRequest<Incoming>> for HyperService<S>
where
    S: Clone + Send + Sync + 'static,
{
    type Response = HttpResponse<ResponseBody>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn call(&self, req: HttpRequest<Incoming>) -> Self::Future {
        let router = self.router.clone();
        Box::pin(serve_request(router, req))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::Router;
    use crate::Request;

    /// Build a hyper `Request<Full<Bytes>>` matching what `serve_request` accepts
    /// (the generic is erased via `into_body().collect()` inside `serve_request`).
    type TestBody = Full<Bytes>;

    fn json_req(body: &[u8]) -> HttpRequest<TestBody> {
        HttpRequest::builder()
            .method("POST")
            .uri("/")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::copy_from_slice(body)))
            .unwrap()
    }

    /// A version of `serve_request` that accepts any `Body` (for testing).
    /// The real `serve_request` takes `Incoming` specifically; this test helper
    /// exercises the same internal `http::dispatch` path.
    async fn serve_test_request<S>(
        router: &Router<S>,
        req: HttpRequest<TestBody>,
    ) -> HttpResponse<ResponseBody>
    where
        S: Clone + Send + Sync + 'static,
    {
        let body = match req.into_body().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(_) => return json_response(StatusCode::OK, http::parse_error_body()),
        };
        let HttpOutcome { status, body } = http::dispatch(router, &body).await;
        if status == 204 {
            HttpResponse::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Full::new(Bytes::new()))
                .expect("valid 204 response")
        } else {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
            json_response(status, body)
        }
    }

    #[tokio::test]
    async fn valid_call_returns_200() {
        let router = Router::new().register("ping", |(), _req: Request| async move {
            Ok::<_, crate::Error>("pong")
        });

        let body = br#"{"jsonrpc":"2.0","method":"ping","id":1}"#;
        let resp = serve_test_request(&router, json_req(body)).await;
        assert_eq!(resp.status(), 200);

        let collected = resp.into_body().collect().await.unwrap().to_bytes();
        let val = crate::json::from_slice::<crate::json::Value>(&collected).unwrap();
        assert_eq!(val["result"], "pong");
    }

    #[tokio::test]
    async fn notification_returns_204() {
        let router = Router::new().register("log", |(), _req: Request| async move {
            Ok::<_, crate::Error>(())
        });

        let body = br#"{"jsonrpc":"2.0","method":"log"}"#;
        let resp = serve_test_request(&router, json_req(body)).await;
        assert_eq!(resp.status(), 204);
    }

    #[tokio::test]
    async fn invalid_json_returns_parse_error() {
        let router = Router::new();

        let resp = serve_test_request(&router, json_req(b"not json")).await;
        assert_eq!(resp.status(), 200);

        let collected = resp.into_body().collect().await.unwrap().to_bytes();
        let val = crate::json::from_slice::<crate::json::Value>(&collected).unwrap();
        assert_eq!(val["error"]["code"], -32700);
    }

    #[tokio::test]
    async fn unknown_method_returns_error() {
        let router = Router::new();

        let body = br#"{"jsonrpc":"2.0","method":"nope","id":1}"#;
        let resp = serve_test_request(&router, json_req(body)).await;
        assert_eq!(resp.status(), 200);

        let collected = resp.into_body().collect().await.unwrap().to_bytes();
        let val = crate::json::from_slice::<crate::json::Value>(&collected).unwrap();
        assert_eq!(val["error"]["code"], -32601);
    }
}
