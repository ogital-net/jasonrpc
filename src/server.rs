//! Method router and request dispatch.
//!
//! The [`Router`] is the server core. Register async handlers
//! by method name, then feed it either raw bytes (from any transport) or
//! already-parsed [`Request`]s. It handles the spec details:
//! notification suppression, batch fan-out, and the parse/invalid-request error
//! responses with a `Null` id.
//!
//! It is transport-agnostic: use it directly behind a raw `hyper` service, a
//! UDS listener, or anything else. HTTP-specific adapters live in the integration
//! modules and are a thin wrapper over [`Router::handle_bytes`].
//!
//! # Per-request context
//!
//! Handlers that need transport-level metadata (HTTP headers, auth claims,
//! trace IDs) can be registered with [`register_with_context`](Router::register_with_context).
//! These receive a [`RequestContext`] alongside the state and request. The
//! typical pattern is for middleware (e.g. a tower `Service` doing auth) to
//! insert typed data into the HTTP request's extensions before the adapter
//! passes them through to the router.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::json::{self, JsonRawValue, Value};
use crate::protocol::{Error, Id, Request, Response};

/// Per-request context passed from the transport layer to handlers.
///
/// When using HTTP integrations (`hyper` or `tower` feature), this carries
/// request `headers` and typed `extensions` populated by middleware. For
/// non-HTTP transports it is an empty struct.
///
/// Handlers that need this data register with
/// [`Router::register_with_context`].
///
/// ```
/// use jasonrpc::server::RequestContext;
///
/// let ctx = RequestContext::default();
/// // When http features are enabled:
/// // ctx.headers.get("authorization");
/// // ctx.extensions.get::<AuthClaims>();
/// ```
///
/// *Not* `Send + Sync` — it is consumed by one handler, not shared.
#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    /// Request headers (available with `hyper` or `tower` feature).
    #[cfg(any(feature = "hyper", feature = "tower"))]
    pub headers: http::HeaderMap,
    /// Typed extensions map (available with `hyper` or `tower` feature).
    #[cfg(any(feature = "hyper", feature = "tower"))]
    pub extensions: http::Extensions,
}

/// The output of processing one or more requests.
///
/// A single request yields a single response (or nothing, for a notification).
/// A batch yields an array (or nothing, if every entry was a notification).
/// This distinction is preserved so callers frame the wire bytes correctly.
#[derive(Debug)]
#[non_exhaustive]
pub enum Output {
    /// No response should be sent (e.g. a lone notification, or an
    /// all-notification batch).
    Empty,
    /// A single response object.
    Single(Response),
    /// A batch of response objects. Never empty (empties become [`Output::Empty`]).
    Batch(Vec<Response>),
}

impl Output {
    /// Serialize this output to wire bytes.
    ///
    /// The two-layer return type distinguishes three outcomes:
    ///
    /// - `Ok(Some(bytes))` — a response to send.
    /// - `Ok(None)` — nothing to send ([`Output::Empty`]: a notification or an
    ///   all-notification batch). Send no bytes; for HTTP reply `204 No Content`.
    /// - `Err(_)` — serialization failed (see below).
    ///
    /// ```
    /// # fn main() -> Result<(), jasonrpc::protocol::Error> {
    /// use jasonrpc::server::Output;
    /// use jasonrpc::{Id, Response};
    ///
    /// let single = Output::Single(Response::result(Id::Number(1), 19_i64));
    /// assert!(single.to_bytes()?.is_some());
    ///
    /// assert!(Output::Empty.to_bytes()?.is_none());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an internal error (`-32603`) if serialization fails.
    pub fn to_bytes(&self) -> Result<Option<Vec<u8>>, Error> {
        let map_err = |e: crate::json::JsonError| Error::internal_error().with_data(e.message());
        match self {
            Output::Empty => Ok(None),
            Output::Single(r) => json::to_vec(r).map(Some).map_err(map_err),
            Output::Batch(rs) => json::to_vec(rs).map(Some).map_err(map_err),
        }
    }
}

/// A boxed future returned by a handler.
///
/// Resolves to the handler's result already serialized to a raw JSON value
/// (serialized exactly once), or a protocol [`Error`].
pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<JsonRawValue, Error>> + Send>>;

/// A registered method handler.
///
/// Receives the shared state and the parsed [`Request`], and resolves to either
/// a result value or a protocol [`Error`]. Handlers are invoked for both calls
/// and notifications; the router discards the output for notifications.
///
/// You rarely implement this trait directly: any async closure
/// `Fn(S, Request) -> impl Future<Output = Result<T, Error>>` where `T:
/// Serialize` is a handler. The result is serialized for you, so a handler can
/// return an `i64`, a `String`, a `#[derive(Serialize)]` struct, or a
/// [`Value`] directly.
pub trait Handler<S>: Send + Sync {
    /// Invoke the handler, resolving to the JSON result value or an error.
    fn call(&self, state: S, req: Request) -> HandlerFuture;
}

impl<S, F, Fut, T> Handler<S> for F
where
    F: Fn(S, Request) -> Fut + Send + Sync,
    Fut: Future<Output = Result<T, Error>> + Send + 'static,
    T: serde::Serialize + 'static,
{
    fn call(&self, state: S, req: Request) -> HandlerFuture {
        let fut = (self)(state, req);
        Box::pin(async move {
            let value = fut.await?;
            // Serialize once, straight to a raw JSON value (no intermediate
            // DOM). Serialization failure here is a bug in the handler's
            // result type, surfaced as an internal error (-32603).
            crate::json::to_raw_value(&value)
                .map_err(|e| Error::internal_error().with_data(e.message()))
        })
    }
}

/// A handler that receives per-request context alongside state.
///
/// Like [`Handler`], but with a [`RequestContext`] argument. Use
/// [`Router::register_with_context`] to register these. The context carries
/// transport-level metadata such as HTTP headers and auth claims inserted by
/// middleware.
pub trait HandlerWithContext<S>: Send + Sync {
    /// Invoke the handler with state, request, and context.
    fn call(&self, state: S, req: Request, ctx: RequestContext) -> HandlerFuture;
}

impl<S, F, Fut, T> HandlerWithContext<S> for F
where
    F: Fn(S, Request, RequestContext) -> Fut + Send + Sync,
    Fut: Future<Output = Result<T, Error>> + Send + 'static,
    T: serde::Serialize + 'static,
{
    fn call(&self, state: S, req: Request, ctx: RequestContext) -> HandlerFuture {
        let fut = (self)(state, req, ctx);
        Box::pin(async move {
            let value = fut.await?;
            crate::json::to_raw_value(&value)
                .map_err(|e| Error::internal_error().with_data(e.message()))
        })
    }
}

/// A method registry with shared state `S`.
///
/// `S` is cloned once per dispatched request, so make it cheap to clone
/// (typically an `Arc<...>` or a handle). For a stateless server use `()`.
///
/// # Cloning
///
/// [`Router`] is cheap to clone when `S` is: the method table lives behind an
/// [`Arc`], so a clone only bumps that reference count and clones the state
/// handle. This makes it easy to hand a router to many connections or tasks
/// without wrapping it in an extra `Arc` yourself:
///
/// ```
/// use jasonrpc::server::Router;
/// use jasonrpc::{Error, Request};
///
/// let router = Router::new()
///     .register("ping", |_, _req: Request| async move {
///         Ok::<_, Error>("pong")
///     });
///
/// let per_connection = router.clone(); // just an Arc bump + state clone
/// # let _ = per_connection;
/// ```
pub struct Router<S = ()> {
    state: S,
    methods: Arc<HashMap<String, Method<S>>>,
    /// Maximum number of entries allowed in a single batch, or `None` for no
    /// limit. Batches larger than this are rejected with a single Invalid
    /// Request response.
    max_batch_len: Option<usize>,
}

impl<S: Clone> Clone for Router<S> {
    /// Cheap clone: bumps the method table's [`Arc`] and clones the state
    /// handle. Does not duplicate the registered handlers.
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            methods: Arc::clone(&self.methods),
            max_batch_len: self.max_batch_len,
        }
    }
}

/// A registered method — either context-free or context-aware.
enum Method<S> {
    Handler(Arc<dyn Handler<S>>),
    WithContext(Arc<dyn HandlerWithContext<S>>),
}

// Hand-written so cloning a `Method` (an `Arc` bump) never requires `S: Clone`.
impl<S> Clone for Method<S> {
    fn clone(&self) -> Self {
        match self {
            Method::Handler(h) => Method::Handler(Arc::clone(h)),
            Method::WithContext(h) => Method::WithContext(Arc::clone(h)),
        }
    }
}

impl<S> Method<S> {
    async fn call(
        &self,
        state: S,
        req: Request,
        ctx: RequestContext,
    ) -> Result<JsonRawValue, Error> {
        match self {
            Method::Handler(h) => h.call(state, req).await,
            Method::WithContext(h) => h.call(state, req, ctx).await,
        }
    }
}

impl<S> std::fmt::Debug for Router<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router")
            .field("methods", &self.methods.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl<S: Clone + Send + Sync + 'static> Router<S> {
    /// Create a router with the given shared state.
    #[must_use]
    pub fn with_state(state: S) -> Self {
        Self {
            state,
            methods: Arc::new(HashMap::new()),
            max_batch_len: None,
        }
    }

    /// Borrow the router's shared state.
    ///
    /// Handy when the state handle (typically an `Arc<...>`) is needed outside
    /// the router — for example to drive background work with the same shared
    /// state the handlers see. Clone it if you need an owned copy.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use jasonrpc::server::Router;
    ///
    /// let router = Router::with_state(Arc::new(42_i64));
    /// assert_eq!(**router.state(), 42);
    /// let handle = Arc::clone(router.state());
    /// # let _ = handle;
    /// ```
    #[must_use]
    pub fn state(&self) -> &S {
        &self.state
    }

    /// Cap the number of entries accepted in a single batch request.
    ///
    /// Because batch elements are dispatched sequentially, an unbounded batch
    /// lets one request force arbitrarily many handler invocations — a cheap
    /// denial-of-service vector for a network-facing server. When set, a batch
    /// with more than `max` entries is rejected up front with a single Invalid
    /// Request (`-32600`) response and no handlers run.
    ///
    /// Defaults to no limit. A limit of 0 rejects every batch (single requests
    /// are unaffected).
    ///
    /// ```
    /// use jasonrpc::server::Router;
    ///
    /// let router = Router::new().with_max_batch_len(100);
    /// ```
    #[must_use]
    pub fn with_max_batch_len(mut self, max: usize) -> Self {
        self.max_batch_len = Some(max);
        self
    }

    /// Register a handler for `method`.
    ///
    /// A handler is any async closure `Fn(S, Request) -> impl
    /// Future<Output = Result<T, Error>>` where `T: Serialize`. The state
    /// argument's type usually can't be inferred from the closure alone, so
    /// annotate it — `|state: MyState, req: Request| ...` — when the compiler
    /// asks. The [`Request`] parameter's type annotation is likewise required.
    ///
    /// # Panics
    ///
    /// Panics if `method` is already registered. Use unique method names.
    #[must_use]
    pub fn register<H: Handler<S> + 'static>(
        mut self,
        method: impl Into<String>,
        handler: H,
    ) -> Self {
        let name = method.into();
        // Copy-on-write: mutate in place while uniquely held (the common
        // builder case), otherwise clone the shared table first.
        if Arc::make_mut(&mut self.methods)
            .insert(name.clone(), Method::Handler(Arc::new(handler)))
            .is_some()
        {
            panic!("method {name:?} registered twice");
        }
        self
    }

    /// Register a handler that receives [`RequestContext`] alongside the
    /// request.
    ///
    /// Use this when handlers need transport-level metadata (HTTP headers,
    /// auth claims from middleware, trace IDs). The context is provided by
    /// the integration adapter.
    ///
    /// # Panics
    ///
    /// Panics if `method` is already registered. Use unique method names.
    #[must_use]
    pub fn register_with_context<H: HandlerWithContext<S> + 'static>(
        mut self,
        method: impl Into<String>,
        handler: H,
    ) -> Self {
        let name = method.into();
        if Arc::make_mut(&mut self.methods)
            .insert(name.clone(), Method::WithContext(Arc::new(handler)))
            .is_some()
        {
            panic!("method {name:?} registered twice");
        }
        self
    }

    /// Parse raw bytes and process them, returning wire-ready output.
    ///
    /// Handles the top-level spec cases: invalid JSON and non-array/empty-array
    /// batches produce a single Invalid Request / Parse error response with a
    /// `Null` id.
    ///
    /// Batch elements are dispatched **sequentially**, in order. The spec places
    /// no ordering requirement on batch responses, but running them one at a
    /// time keeps a single request from fanning out into unbounded concurrent
    /// handler execution. If you need batch elements to run concurrently, split
    /// the batch yourself and drive [`handle_request`](Self::handle_request) on
    /// your own executor.
    ///
    /// If you have per-request context (HTTP headers, auth claims), use
    /// [`handle_bytes_with_context`](Self::handle_bytes_with_context) instead.
    pub async fn handle_bytes(&self, bytes: &[u8]) -> Output {
        self.handle_bytes_with_context(bytes, RequestContext::default())
            .await
    }

    /// Parse raw bytes and process them with per-request context.
    ///
    /// Like [`handle_bytes`](Self::handle_bytes) but passes `ctx` to handlers
    /// registered with [`register_with_context`](Self::register_with_context).
    pub async fn handle_bytes_with_context(&self, bytes: &[u8], ctx: RequestContext) -> Output {
        // Peek at the first non-whitespace byte to distinguish a batch (array,
        // `[`) from a single request (object, `{`) without a full parse.
        let first = match bytes.iter().find(|b| !b.is_ascii_whitespace()) {
            Some(b) => *b,
            None => return Output::Single(Response::error(Id::Null, Error::invalid_request())),
        };
        match first {
            b'[' => self.handle_batch_bytes_with_context(bytes, ctx).await,
            b'{' => {
                if let Ok(req) = json::from_slice::<Request>(bytes) {
                    if let Some(resp) = self.handle_request_with_context(req, ctx).await {
                        Output::Single(resp)
                    } else {
                        Output::Empty
                    }
                } else {
                    let is_json = json::from_slice::<Value>(bytes).is_ok();
                    Output::Single(Response::error(
                        Id::Null,
                        if is_json {
                            Error::invalid_request()
                        } else {
                            Error::parse_error()
                        },
                    ))
                }
            }
            _ => {
                let is_json = json::from_slice::<Value>(bytes).is_ok();
                Output::Single(Response::error(
                    Id::Null,
                    if is_json {
                        Error::invalid_request()
                    } else {
                        Error::parse_error()
                    },
                ))
            }
        }
    }

    /// Process a single already-parsed [`Request`], returning `None` for a
    /// notification.
    ///
    /// Use [`handle_request_with_context`](Self::handle_request_with_context) if
    /// you have per-request context.
    pub async fn handle_request(&self, req: Request) -> Option<Response> {
        self.handle_request_with_context(req, RequestContext::default())
            .await
    }

    /// Process a single request with per-request context.
    ///
    /// Returns `None` for a notification.
    pub async fn handle_request_with_context(
        &self,
        req: Request,
        ctx: RequestContext,
    ) -> Option<Response> {
        let is_notification = req.is_notification();
        let id = req.id().cloned();

        let result = match self.methods.get(req.method()) {
            Some(handler) => handler.call(self.state.clone(), req, ctx).await,
            None => Err(Error::method_not_found()),
        };

        if is_notification {
            return None;
        }

        let id = id.unwrap_or(Id::Null);
        Some(match result {
            Ok(raw) => Response::from_raw_result(id, raw),
            Err(error) => Response::error(id, error),
        })
    }

    /// Process one batch element, given its raw JSON bytes, with context.
    async fn handle_element_bytes_with_context(
        &self,
        bytes: &[u8],
        ctx: &RequestContext,
    ) -> Option<Response> {
        match json::from_slice::<Request>(bytes) {
            Ok(req) => {
                // Each batch element gets its own clone of the context.
                self.handle_request_with_context(req, ctx.clone()).await
            }
            Err(_) => Some(Response::error(Id::Null, Error::invalid_request())),
        }
    }

    /// Process a batch with context.
    async fn handle_batch_bytes_with_context(&self, bytes: &[u8], ctx: RequestContext) -> Output {
        let items: Vec<Vec<u8>> = if let Ok(items) = json::split_array(bytes) {
            items
        } else {
            let is_json = json::from_slice::<Value>(bytes).is_ok();
            return Output::Single(Response::error(
                Id::Null,
                if is_json {
                    Error::invalid_request()
                } else {
                    Error::parse_error()
                },
            ));
        };

        if items.is_empty() {
            return Output::Single(Response::error(Id::Null, Error::invalid_request()));
        }

        // Reject oversized batches before running any handler, so a single
        // request can't fan out into unbounded sequential work.
        if let Some(max) = self.max_batch_len {
            if items.len() > max {
                return Output::Single(Response::error(
                    Id::Null,
                    Error::invalid_request().with_data(format!(
                        "batch of {} entries exceeds maximum of {max}",
                        items.len()
                    )),
                ));
            }
        }

        let mut responses = Vec::new();
        for item in items {
            if let Some(resp) = self.handle_element_bytes_with_context(&item, &ctx).await {
                responses.push(resp);
            }
        }

        if responses.is_empty() {
            Output::Empty
        } else {
            Output::Batch(responses)
        }
    }
}

impl Router<()> {
    /// Create a stateless router.
    #[must_use]
    pub fn new() -> Self {
        Self::with_state(())
    }
}

impl Default for Router<()> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::ignored_unit_patterns)]
mod tests {
    use super::*;

    fn router() -> Router<()> {
        Router::new()
            .register("subtract", |_, req: Request| async move {
                let [a, b]: [i64; 2] = req.params_as().ok_or_else(Error::invalid_params)?;
                Ok(a - b)
            })
            .register("notify", |_, _req: Request| async move {
                Ok(crate::json::null())
            })
    }

    #[tokio::test]
    async fn positional_call() {
        let out = router()
            .handle_bytes(br#"{"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":1}"#)
            .await;
        let bytes = out.to_bytes().unwrap().unwrap();
        let s = String::from_utf8(bytes).unwrap();
        assert!(s.contains("\"result\":19"), "{s}");
    }

    #[tokio::test]
    async fn notification_yields_nothing() {
        let out = router()
            .handle_bytes(br#"{"jsonrpc":"2.0","method":"notify","params":[1]}"#)
            .await;
        assert!(matches!(out, Output::Empty));
        assert!(out.to_bytes().unwrap().is_none());
    }

    #[tokio::test]
    async fn unknown_method() {
        let out = router()
            .handle_bytes(br#"{"jsonrpc":"2.0","method":"nope","id":"1"}"#)
            .await;
        let s = String::from_utf8(out.to_bytes().unwrap().unwrap()).unwrap();
        assert!(s.contains("-32601"), "{s}");
        assert!(s.contains("\"id\":\"1\""), "{s}");
    }

    #[tokio::test]
    async fn invalid_json_is_parse_error_with_null_id() {
        let out = router().handle_bytes(b"{not json").await;
        let s = String::from_utf8(out.to_bytes().unwrap().unwrap()).unwrap();
        assert!(s.contains("-32700"), "{s}");
        assert!(s.contains("\"id\":null"), "{s}");
    }

    #[tokio::test]
    async fn empty_batch_is_single_invalid_request() {
        let out = router().handle_bytes(b"[]").await;
        let s = String::from_utf8(out.to_bytes().unwrap().unwrap()).unwrap();
        assert!(
            s.starts_with('{'),
            "empty batch must yield a single object: {s}"
        );
        assert!(s.contains("-32600"), "{s}");
    }

    #[tokio::test]
    async fn batch_of_invalids() {
        let out = router().handle_bytes(b"[1,2,3]").await;
        let s = String::from_utf8(out.to_bytes().unwrap().unwrap()).unwrap();
        assert!(s.starts_with('['), "{s}");
        assert_eq!(s.matches("-32600").count(), 3, "{s}");
    }

    #[tokio::test]
    async fn mixed_batch_drops_notifications() {
        let out = router()
            .handle_bytes(
                br#"[
                    {"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":"1"},
                    {"jsonrpc":"2.0","method":"notify","params":[7]},
                    {"jsonrpc":"2.0","method":"nope","id":"2"}
                ]"#,
            )
            .await;
        let s = String::from_utf8(out.to_bytes().unwrap().unwrap()).unwrap();
        // Two responses: the call result and the method-not-found; not the notification.
        assert_eq!(s.matches("\"id\"").count(), 2, "{s}");
    }

    #[tokio::test]
    async fn all_notification_batch_yields_nothing() {
        let out = router()
            .handle_bytes(
                br#"[
                    {"jsonrpc":"2.0","method":"notify","params":[1]},
                    {"jsonrpc":"2.0","method":"notify","params":[2]}
                ]"#,
            )
            .await;
        assert!(matches!(out, Output::Empty));
    }

    #[tokio::test]
    async fn oversized_batch_is_rejected() {
        let r = router().with_max_batch_len(2);
        let out = r
            .handle_bytes(
                br#"[
                    {"jsonrpc":"2.0","method":"subtract","params":[3,1],"id":1},
                    {"jsonrpc":"2.0","method":"subtract","params":[5,1],"id":2},
                    {"jsonrpc":"2.0","method":"subtract","params":[9,1],"id":3}
                ]"#,
            )
            .await;
        // Single Invalid Request response, no handler results.
        let s = String::from_utf8(out.to_bytes().unwrap().unwrap()).unwrap();
        assert!(s.starts_with('{'), "must be a single object: {s}");
        assert!(s.contains("-32600"), "{s}");
        assert!(!s.contains("\"result\""), "no handlers should run: {s}");
    }

    #[tokio::test]
    async fn batch_at_limit_is_allowed() {
        let r = router().with_max_batch_len(2);
        let out = r
            .handle_bytes(
                br#"[
                    {"jsonrpc":"2.0","method":"subtract","params":[3,1],"id":1},
                    {"jsonrpc":"2.0","method":"subtract","params":[5,1],"id":2}
                ]"#,
            )
            .await;
        let s = String::from_utf8(out.to_bytes().unwrap().unwrap()).unwrap();
        assert!(s.starts_with('['), "{s}");
        assert_eq!(s.matches("\"result\"").count(), 2, "{s}");
    }

    #[tokio::test]
    async fn batch_limit_does_not_affect_single_requests() {
        // Even a zero limit only affects batches; single requests still work.
        let r = router().with_max_batch_len(0);
        let out = r
            .handle_bytes(br#"{"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":1}"#)
            .await;
        let s = String::from_utf8(out.to_bytes().unwrap().unwrap()).unwrap();
        assert!(s.contains("\"result\":19"), "{s}");
    }

    #[tokio::test]
    async fn cloned_router_shares_handlers_and_dispatches() {
        let r = router().with_max_batch_len(5);
        let clone = r.clone();

        // The clone shares the same method table (Arc bump, not a deep copy).
        assert!(Arc::ptr_eq(&r.methods, &clone.methods));
        // ...and config is carried over.
        assert_eq!(clone.max_batch_len, Some(5));

        // Both dispatch identically.
        let out = clone
            .handle_bytes(br#"{"jsonrpc":"2.0","method":"subtract","params":[42,23],"id":1}"#)
            .await;
        let s = String::from_utf8(out.to_bytes().unwrap().unwrap()).unwrap();
        assert!(s.contains("\"result\":19"), "{s}");
    }

    #[test]
    fn clone_then_register_is_copy_on_write() {
        // Registering on a clone must not mutate the original's shared table.
        let base = router();
        let extended = base
            .clone()
            .register("extra", |_, _req: Request| async move { Ok("ok") });

        // The two now point at distinct tables.
        assert!(!Arc::ptr_eq(&base.methods, &extended.methods));
        assert!(base.methods.get("extra").is_none());
        assert!(extended.methods.get("extra").is_some());
    }

    #[tokio::test]
    async fn router_clone_works_with_arc_state() {
        // A realistic server: shared state behind an Arc, router cloned per task.
        let router = Router::with_state(Arc::new(7_i64)).register(
            "get",
            |state: Arc<i64>, _req: Request| async move { Ok(*state) },
        );

        let mut handles = Vec::new();
        for _ in 0..4 {
            let r = router.clone();
            handles.push(tokio::spawn(async move {
                let out = r
                    .handle_bytes(br#"{"jsonrpc":"2.0","method":"get","id":1}"#)
                    .await;
                String::from_utf8(out.to_bytes().unwrap().unwrap()).unwrap()
            }));
        }
        for h in handles {
            assert!(h.await.unwrap().contains("\"result\":7"));
        }
    }

    #[test]
    fn state_accessor_returns_shared_handle() {
        let router = Router::with_state(Arc::new(99_i64));
        assert_eq!(**router.state(), 99);
        // The accessor hands back the same Arc the handlers would clone.
        let handle = Arc::clone(router.state());
        assert_eq!(*handle, 99);
    }
}
