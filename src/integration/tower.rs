//! Tower / axum integration.
//!
//! [`RpcService`] is a [`tower_service::Service`] that maps an
//! `http::Request<B>` to an `http::Response<Full<Bytes>>` by dispatching the
//! request body through a [`Router`]. Because it speaks the generic `http` and
//! `http-body` traits (not hyper's concrete types), it slots straight into an
//! axum `Router` via `route_service`, or anywhere a tower `Service` is expected.
//!
//! Kept dependency-light on purpose: this pulls in only `tower-service`, `http`,
//! and `http-body`, not the whole `tower` crate. AAA and other cross-cutting
//! concerns are added as tower layers *around* this service, not inside it.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ::http::{header, Request as HttpRequest, Response as HttpResponse, StatusCode};
use bytes::Bytes;
use http_body::Body;
use http_body_util::{BodyExt, Full};
use tower_service::Service;

use crate::integration::http::{self, HttpOutcome};
use crate::server::{RequestContext, Router};

/// The response body type produced by this integration.
pub type ResponseBody = Full<Bytes>;

/// A cloneable tower [`Service`] that dispatches JSON-RPC over HTTP.
///
/// Wraps a [`Router`]; cloning is cheap (the router's method table is shared
/// behind an `Arc`). Dispatch is by JSON-RPC `method` — the HTTP path and verb
/// are ignored, so mount it at a single route.
///
/// ```
/// use jasonrpc::server::Router;
/// use jasonrpc::integration::tower::RpcService;
///
/// let _service = RpcService::new(Router::new());
/// ```
pub struct RpcService<S> {
    router: Router<S>,
}

impl<S> std::fmt::Debug for RpcService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcService").finish()
    }
}

impl<S> RpcService<S> {
    /// Wrap a router in a tower service.
    pub fn new(router: Router<S>) -> Self {
        Self { router }
    }
}

impl<S: Clone> Clone for RpcService<S> {
    fn clone(&self) -> Self {
        Self {
            router: self.router.clone(),
        }
    }
}

impl<S, ReqBody> Service<HttpRequest<ReqBody>> for RpcService<S>
where
    S: Clone + Send + Sync + 'static,
    ReqBody: Body + Send + 'static,
    ReqBody::Data: Send,
{
    type Response = HttpResponse<ResponseBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let router = self.router.clone();
        Box::pin(async move {
            // Split to preserve extensions and headers before collecting the body.
            let (parts, body) = req.into_parts();
            let ctx = RequestContext {
                headers: parts.headers,
                extensions: parts.extensions,
            };

            let body = match body.collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(_) => return Ok(json_response(StatusCode::OK, http::parse_error_body())),
            };

            let HttpOutcome { status, body } =
                http::dispatch_with_context(&router, &body, ctx).await;
            if status == 204 {
                Ok(HttpResponse::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(Full::new(Bytes::new()))
                    .expect("valid 204 response"))
            } else {
                let status = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
                Ok(json_response(status, body))
            }
        })
    }
}

fn json_response(status: StatusCode, body: Vec<u8>) -> HttpResponse<ResponseBody> {
    HttpResponse::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .expect("valid json response")
}

#[cfg(test)]
#[allow(clippy::ignored_unit_patterns)]
mod tests {
    use super::*;
    use crate::json::Value;
    use crate::protocol::Request;

    fn router() -> Router<()> {
        Router::new().register(
            "ping",
            |_, _req: Request| async move { Ok(Value::from("pong")) },
        )
    }

    #[tokio::test]
    async fn dispatches_via_tower_service() {
        let mut svc = RpcService::new(router());
        let req = HttpRequest::builder()
            .method("POST")
            .body(Full::new(Bytes::from_static(
                br#"{"jsonrpc":"2.0","method":"ping","id":1}"#,
            )))
            .unwrap();

        let resp = svc.call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let s = String::from_utf8(body.to_vec()).unwrap();
        assert!(s.contains("\"result\":\"pong\""), "{s}");
    }

    #[tokio::test]
    async fn notification_yields_204() {
        let mut svc = RpcService::new(router());
        let req = HttpRequest::builder()
            .body(Full::new(Bytes::from_static(
                br#"{"jsonrpc":"2.0","method":"ping"}"#,
            )))
            .unwrap();

        let resp = svc.call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }
}
