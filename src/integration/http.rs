//! Shared HTTP dispatch logic, independent of any particular HTTP library.
//!
//! Both the `hyper` and `tower` integrations collect a request body into bytes,
//! run it through the [`Router`], and turn the result into an HTTP status plus a
//! response body. That middle step is identical regardless of the surrounding
//! HTTP types, so it lives here and both adapters call it.

use crate::server::{RequestContext, Router};

/// The outcome of dispatching an HTTP request body through the router: an HTTP
/// status code and the raw response body bytes (empty for `204`).
pub(crate) struct HttpOutcome {
    /// The HTTP status code to send (as a `u16` to stay library-neutral).
    pub(crate) status: u16,
    /// The response body bytes. Empty when `status` is `204 No Content`.
    pub(crate) body: Vec<u8>,
}

/// Dispatch already-collected request-body bytes through the router.
///
/// Follows the JSON-RPC-over-HTTP convention: a normal result (single or batch)
/// is `200 OK` with a JSON body; a request that produces no response (e.g. an
/// all-notification batch) is `204 No Content` with an empty body. A
/// serialization failure on our side becomes a `200` carrying an internal-error
/// response object.
#[allow(dead_code)] // used by integration test helpers
pub(crate) async fn dispatch<S>(router: &Router<S>, body: &[u8]) -> HttpOutcome
where
    S: Clone + Send + Sync + 'static,
{
    dispatch_with_context(router, body, RequestContext::default()).await
}

/// Dispatch with per-request context (HTTP headers, auth claims, etc.).
pub(crate) async fn dispatch_with_context<S>(
    router: &Router<S>,
    body: &[u8],
    ctx: RequestContext,
) -> HttpOutcome
where
    S: Clone + Send + Sync + 'static,
{
    let output = router.handle_bytes_with_context(body, ctx).await;
    match output.to_bytes() {
        Ok(Some(bytes)) => HttpOutcome {
            status: 200,
            body: bytes,
        },
        Ok(None) => HttpOutcome {
            status: 204,
            body: Vec::new(),
        },
        Err(_) => HttpOutcome {
            status: 200,
            body: internal_error_body(),
        },
    }
}

/// Build a JSON body for a top-level parse error (`-32700`) with a `Null` id.
///
/// Used when the HTTP body itself could not be read.
pub(crate) fn parse_error_body() -> Vec<u8> {
    let err = crate::protocol::Response::error(
        crate::protocol::Id::Null,
        crate::protocol::Error::parse_error(),
    );
    crate::json::to_vec(&err).unwrap_or_default()
}

/// Build a JSON body for a top-level internal error (`-32603`) with a `Null` id.
pub(crate) fn internal_error_body() -> Vec<u8> {
    let err = crate::protocol::Response::error(
        crate::protocol::Id::Null,
        crate::protocol::Error::internal_error(),
    );
    crate::json::to_vec(&err).unwrap_or_default()
}
