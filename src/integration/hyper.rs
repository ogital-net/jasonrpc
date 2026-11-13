//! Hyper integration.
//!
//! [`HyperService`] adapts a [`Router`] into a `hyper::service::Service`: it
//! collects a request body, dispatches it through the router, and builds the
//! JSON-RPC response body.
//!
//! Dispatch does not consider the HTTP path or verb. A JSON-RPC endpoint is a
//! single POST target and all dispatch happens by `method`, so authentication,
//! authorization, and accounting belong in a layer around this service (see the
//! gateway example) rather than within it.
//!
//! Request bodies default to a 16 MiB limit. Use
//! [`HyperService::with_max_request_body_size`] to tune it.

use std::convert::Infallible;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::service::Service;
use hyper::{Request as HttpRequest, Response as HttpResponse, StatusCode};

use crate::integration::http::{self, BodyCollection, HttpOutcome};
use crate::integration::DEFAULT_MAX_REQUEST_BODY_SIZE;
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
pub(crate) async fn serve_request<S, B>(
    router: Router<S>,
    req: HttpRequest<B>,
    max_request_body_size: usize,
) -> Result<HttpResponse<ResponseBody>, Infallible>
where
    S: Clone + Send + Sync + 'static,
    B: hyper::body::Body,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    // Split into parts and body so we can extract extensions and headers
    // before consuming the body.
    let (parts, body) = req.into_parts();
    let ctx = RequestContext {
        headers: parts.headers,
        extensions: parts.extensions,
    };

    let body = match http::collect_body(body, max_request_body_size).await {
        BodyCollection::Bytes(body) => body,
        BodyCollection::TooLarge => return Ok(empty_response(StatusCode::PAYLOAD_TOO_LARGE)),
        BodyCollection::Failed => {
            return Ok(json_response(StatusCode::OK, http::parse_error_body()))
        }
    };

    let HttpOutcome { status, body } = http::dispatch_with_context(&router, body, ctx).await;
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

fn empty_response(status: StatusCode) -> HttpResponse<ResponseBody> {
    HttpResponse::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("valid empty response")
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
    max_request_body_size: usize,
}

impl<S: Clone> Clone for HyperService<S> {
    fn clone(&self) -> Self {
        Self {
            router: self.router.clone(),
            max_request_body_size: self.max_request_body_size,
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
        Self {
            router,
            max_request_body_size: DEFAULT_MAX_REQUEST_BODY_SIZE,
        }
    }

    /// Set the maximum accepted request body size in bytes. A limit of 0
    /// accepts only empty bodies.
    #[must_use]
    pub fn with_max_request_body_size(mut self, limit: usize) -> Self {
        self.max_request_body_size = limit;
        self
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
        let max_request_body_size = self.max_request_body_size;
        Box::pin(serve_request(router, req, max_request_body_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::Router;
    use crate::Request;
    use http_body_util::BodyExt;

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

    async fn serve_test_request<S>(
        router: &Router<S>,
        req: HttpRequest<TestBody>,
    ) -> HttpResponse<ResponseBody>
    where
        S: Clone + Send + Sync + 'static,
    {
        serve_request(router.clone(), req, DEFAULT_MAX_REQUEST_BODY_SIZE)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn http_body_is_shared_with_borrowed_params() {
        #[derive(serde::Deserialize)]
        struct Params<'a> {
            data: &'a str,
        }
        let body = Bytes::from_static(
            br#"{"jsonrpc":"2.0","method":"ingest","params":{"data":"YWJj"},"id":1}"#,
        );
        let offset = body.windows(4).position(|bytes| bytes == b"YWJj").unwrap();
        let expected = body[offset..].as_ptr() as usize;
        let router =
            Router::with_state(expected).register("ingest", |expected, req: Request| async move {
                let params: Params<'_> = req
                    .params_as_borrowed()
                    .map_err(|error| crate::Error::invalid_params().with_data(error.to_string()))?;
                assert_eq!(params.data.as_ptr() as usize, expected);
                tokio::task::yield_now().await;
                Ok(params.data.len())
            });
        let request = HttpRequest::builder().body(Full::new(body)).unwrap();
        let response = serve_test_request(&router, request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let response: crate::Response = crate::json::from_slice(&body).unwrap();
        assert_eq!(response.result_as::<usize>().unwrap(), 4);
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

    #[tokio::test]
    async fn oversized_body_returns_413() {
        let router = Router::new();
        let req = json_req(br#"{"jsonrpc":"2.0","method":"ping","id":1}"#);
        let resp = serve_request(router, req, 8).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn body_at_limit_is_accepted() {
        let router = Router::new().register("ping", |(), _req: Request| async move {
            Ok::<_, crate::Error>("pong")
        });
        let body = br#"{"jsonrpc":"2.0","method":"ping","id":1}"#;
        let resp = serve_request(router, json_req(body), body.len())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
