//! HTTP client transport.
//!
//! [`HttpTransport`] implements [`Transport`](super::Transport) by `POST`ing the
//! request bytes to a single JSON-RPC endpoint URL. It is built on `hyper` and
//! `hyper-util`'s pooled `Client`, without higher-level HTTP machinery such as
//! redirects, cookies, or TLS.
//!
//! The transport speaks plain HTTP. The public surface is deliberately small,
//! leaving room to add TLS or Unix-socket connectors by generalizing over the
//! `hyper_util` connector without a breaking change.
//!
//! # Authorization
//!
//! For a static credential (e.g. an API key that never changes) set it once
//! with [`with_header`](HttpTransport::with_header). For a credential with a
//! *lifecycle* -- most notably an OAuth 2.0 bearer token from the
//! client-credentials flow, which expires and must be refreshed -- implement the
//! [`Authorizer`] trait and attach it with
//! [`with_authorizer`](HttpTransport::with_authorizer). The transport asks the
//! authorizer for a fresh `Authorization` header value before every request and
//! can transparently refresh-and-retry once on a `401`.
//!
//! This crate deliberately does **not** implement any OAuth flow itself: token
//! endpoints, client-auth methods, caching, and refresh strategy are consumer
//! policy. The [`Authorizer`] trait is the seam where you plug in the `oauth2`
//! crate or your own token manager. See `examples/oauth_client_credentials.rs`.

use std::error::Error as StdError;
use std::future::Future;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;

use super::Transport;

/// A boxed error returned by an [`Authorizer`] when it cannot produce a
/// credential (for example, the token endpoint is unreachable).
pub type AuthError = Box<dyn StdError + Send + Sync>;

/// Supplies the `Authorization` header for outgoing HTTP requests, managing any
/// credential lifecycle (caching, expiry, refresh) internally.
///
/// This is the hook for dynamic credentials such as OAuth 2.0 bearer tokens.
/// The transport calls [`authorize`](Authorizer::authorize) before each request
/// to obtain the current header value; the implementation is responsible for
/// returning a still-valid token, refreshing proactively as needed, and
/// collapsing concurrent refreshes (single-flight) so a burst of calls doesn't
/// stampede the token endpoint.
///
/// # Example
///
/// A trivial static-token authorizer (a real one would cache a token and
/// refresh it before `expires_in` elapses):
///
/// ```
/// # #[cfg(feature = "http-client")] {
/// use jasonrpc::client::{Authorizer, AuthError};
/// use http::HeaderValue;
///
/// #[derive(Clone)]
/// struct StaticToken(String);
///
/// impl Authorizer for StaticToken {
///     async fn authorize(&self) -> Result<Option<HeaderValue>, AuthError> {
///         let value = HeaderValue::try_from(format!("Bearer {}", self.0))?;
///         Ok(Some(value))
///     }
/// }
/// # }
/// ```
pub trait Authorizer: Send + Sync {
    /// Produce the `Authorization` header value to send with the next request,
    /// or `None` to send no `Authorization` header.
    ///
    /// Called once per request (and again before a retry after a `401`). The
    /// implementation should return a currently-valid credential, refreshing
    /// internally if the cached one is expired or near expiry.
    fn authorize(&self) -> impl Future<Output = Result<Option<HeaderValue>, AuthError>> + Send;

    /// Called after the server answers `401 Unauthorized`. Return `true` to
    /// have the transport refresh (via another [`authorize`](Self::authorize))
    /// and retry the request exactly once; return `false` to surface the `401`.
    ///
    /// Implementations should invalidate their cached credential here so the
    /// following `authorize` mints a fresh one. This covers the case where a
    /// token expired earlier than its advertised lifetime (clock skew, server
    /// revocation). The default returns `false` (no retry).
    fn on_unauthorized(&self) -> impl Future<Output = bool> + Send {
        async { false }
    }
}

/// The default [`Authorizer`]: attaches no credential. Sending no
/// `Authorization` header and never retrying on `401`.
#[derive(Clone, Debug, Default)]
pub struct NoAuth;

impl Authorizer for NoAuth {
    async fn authorize(&self) -> Result<Option<HeaderValue>, AuthError> {
        Ok(None)
    }
}

/// Default maximum response body size: 16 MiB.
///
/// Matches the transport layer's default frame cap. A JSON-RPC response larger
/// than this from an upstream is almost certainly a bug or an attack; the read
/// is aborted rather than buffered without bound.
pub const DEFAULT_MAX_RESPONSE_SIZE: usize = 16 * 1024 * 1024;

/// A pooled HTTP transport that POSTs JSON-RPC messages to one endpoint.
///
/// Cloning shares the underlying connection pool, so a single transport can be
/// constructed and reused (or wrapped in a [`Client`](super::Client)).
///
/// Static default headers set via [`with_header`](Self::with_header) are sent on
/// every request. For a credential with a lifecycle (e.g. an OAuth bearer
/// token), attach an [`Authorizer`] with
/// [`with_authorizer`](Self::with_authorizer); the type parameter `A` defaults
/// to [`NoAuth`] so the common no-auth case needs no annotation.
#[derive(Clone, Debug)]
pub struct HttpTransport<A = NoAuth> {
    client: HyperClient<HttpConnector, Full<Bytes>>,
    uri: Uri,
    headers: HeaderMap,
    max_response_size: usize,
    authorizer: A,
}

/// Errors produced by [`HttpTransport`].
#[derive(Debug)]
#[non_exhaustive]
pub enum HttpTransportError {
    /// The endpoint string was not a valid URI.
    InvalidUri(String),
    /// A connection or protocol error from the underlying client.
    Connect(Box<dyn StdError + Send + Sync>),
    /// Reading the response body failed.
    Body(Box<dyn StdError + Send + Sync>),
    /// The server answered with a non-success HTTP status.
    Status(http::StatusCode),
    /// A default header name or value was invalid.
    InvalidHeader(String),
    /// The response body exceeded the configured maximum size.
    TooLarge(usize),
    /// The [`Authorizer`] failed to produce a credential.
    Auth(AuthError),
}

impl std::fmt::Display for HttpTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpTransportError::InvalidUri(u) => write!(f, "invalid endpoint uri: {u}"),
            HttpTransportError::Connect(e) => write!(f, "http connect error: {e}"),
            HttpTransportError::Body(e) => write!(f, "http body error: {e}"),
            HttpTransportError::Status(s) => write!(f, "unexpected http status: {s}"),
            HttpTransportError::InvalidHeader(h) => write!(f, "invalid default header: {h}"),
            HttpTransportError::TooLarge(limit) => {
                write!(f, "response body exceeded max size of {limit} bytes")
            }
            HttpTransportError::Auth(e) => write!(f, "authorization error: {e}"),
        }
    }
}

impl StdError for HttpTransportError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            HttpTransportError::Connect(e) | HttpTransportError::Body(e) => Some(e.as_ref()),
            HttpTransportError::Auth(e) => Some(e.as_ref()),
            HttpTransportError::InvalidUri(_)
            | HttpTransportError::Status(_)
            | HttpTransportError::InvalidHeader(_)
            | HttpTransportError::TooLarge(_) => None,
        }
    }
}

impl HttpTransport<NoAuth> {
    /// Build a transport targeting `endpoint` (e.g. `http://127.0.0.1:8080/`).
    ///
    /// The transport starts with no [`Authorizer`]; attach one later with
    /// [`with_authorizer`](Self::with_authorizer) for OAuth-style credentials.
    ///
    /// # Errors
    ///
    /// Returns [`HttpTransportError::InvalidUri`] if `endpoint` is not a valid
    /// URI.
    pub fn new(endpoint: impl AsRef<str>) -> Result<Self, HttpTransportError> {
        let uri: Uri = endpoint
            .as_ref()
            .parse()
            .map_err(|_| HttpTransportError::InvalidUri(endpoint.as_ref().to_owned()))?;
        let client = HyperClient::builder(TokioExecutor::new()).build(HttpConnector::new());
        Ok(Self {
            client,
            uri,
            headers: HeaderMap::new(),
            max_response_size: DEFAULT_MAX_RESPONSE_SIZE,
            authorizer: NoAuth,
        })
    }
}

impl<A> HttpTransport<A> {
    /// Set the maximum response body size in bytes. Responses larger than this
    /// are rejected with [`HttpTransportError::TooLarge`] rather than buffered.
    ///
    /// Defaults to [`DEFAULT_MAX_RESPONSE_SIZE`] (16 MiB). Set to 0 to disable
    /// the limit (not recommended when talking to an untrusted upstream).
    #[must_use]
    pub fn with_max_response_size(mut self, limit: usize) -> Self {
        self.max_response_size = limit;
        self
    }

    /// Add a static default header sent on every request.
    ///
    /// Use this for credentials that never change (e.g. a fixed API key). For a
    /// credential with a lifecycle, prefer
    /// [`with_authorizer`](Self::with_authorizer). Both the name and value are
    /// validated eagerly.
    ///
    /// # Errors
    ///
    /// Returns [`HttpTransportError::InvalidHeader`] if the name or value is
    /// not a valid HTTP header.
    pub fn with_header(
        mut self,
        name: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> Result<Self, HttpTransportError> {
        let name = HeaderName::try_from(name.as_ref())
            .map_err(|_| HttpTransportError::InvalidHeader(name.as_ref().to_owned()))?;
        let value = HeaderValue::try_from(value.as_ref())
            .map_err(|_| HttpTransportError::InvalidHeader(value.as_ref().to_owned()))?;
        self.headers.insert(name, value);
        Ok(self)
    }

    /// Attach an [`Authorizer`] that supplies (and refreshes) the
    /// `Authorization` header per request.
    ///
    /// This swaps the transport's authorizer type, so it consumes `self` and
    /// returns an `HttpTransport<A2>`. The authorizer is consulted before every
    /// request; if the server answers `401`, the transport calls
    /// [`Authorizer::on_unauthorized`] and retries once when that returns
    /// `true`.
    #[must_use]
    pub fn with_authorizer<A2>(self, authorizer: A2) -> HttpTransport<A2> {
        HttpTransport {
            client: self.client,
            uri: self.uri,
            headers: self.headers,
            max_response_size: self.max_response_size,
            authorizer,
        }
    }
}

impl<A: Authorizer> HttpTransport<A> {
    /// Build one POST request, attaching the static headers plus the
    /// `Authorization` value the authorizer produced for this attempt.
    fn build_request(
        &self,
        request: &[u8],
        auth: Option<&HeaderValue>,
    ) -> Result<http::Request<Full<Bytes>>, HttpTransportError> {
        let mut builder = http::Request::builder()
            .method(Method::POST)
            .uri(self.uri.clone())
            .header(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
        if let Some(headers) = builder.headers_mut() {
            // Static default headers first (e.g. a fixed API key)...
            for (name, value) in &self.headers {
                headers.insert(name, value.clone());
            }
            // ...then the dynamic Authorization, which wins over any static one.
            if let Some(value) = auth {
                headers.insert(http::header::AUTHORIZATION, value.clone());
            }
        }
        builder
            .body(Full::new(Bytes::from(request.to_vec())))
            .map_err(|e| HttpTransportError::Connect(Box::new(e)))
    }

    /// Read and size-bound the response body.
    async fn read_body(
        &self,
        resp: http::Response<Incoming>,
    ) -> Result<Vec<u8>, HttpTransportError> {
        // Bound the body read so a hostile or buggy upstream can't OOM us.
        // `Limited` short-circuits the stream once the cap is exceeded; a limit
        // of 0 means unbounded.
        let limit = if self.max_response_size == 0 {
            usize::MAX
        } else {
            self.max_response_size
        };
        let body = Limited::new(resp.into_body(), limit)
            .collect()
            .await
            .map_err(|e| {
                // `Limited` reports an over-limit body as a `LengthLimitError`;
                // anything else is a genuine transport/body failure.
                if e.downcast_ref::<http_body_util::LengthLimitError>()
                    .is_some()
                {
                    HttpTransportError::TooLarge(self.max_response_size)
                } else {
                    HttpTransportError::Body(e)
                }
            })?
            .to_bytes();
        Ok(body.to_vec())
    }
}

impl<A: Authorizer> Transport for HttpTransport<A> {
    type Error = HttpTransportError;

    async fn round_trip(&self, request: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        // Ask the authorizer for the current credential (it refreshes as needed).
        let auth = self
            .authorizer
            .authorize()
            .await
            .map_err(HttpTransportError::Auth)?;

        let req = self.build_request(&request, auth.as_ref())?;
        let resp = self
            .client
            .request(req)
            .await
            .map_err(|e| HttpTransportError::Connect(Box::new(e)))?;

        // On 401, give the authorizer a chance to invalidate + refresh, then
        // retry the request exactly once with the fresh credential.
        let resp = if resp.status() == StatusCode::UNAUTHORIZED
            && self.authorizer.on_unauthorized().await
        {
            let auth = self
                .authorizer
                .authorize()
                .await
                .map_err(HttpTransportError::Auth)?;
            let req = self.build_request(&request, auth.as_ref())?;
            self.client
                .request(req)
                .await
                .map_err(|e| HttpTransportError::Connect(Box::new(e)))?
        } else {
            resp
        };

        let status = resp.status();
        if !status.is_success() && status != StatusCode::NO_CONTENT {
            return Err(HttpTransportError::Status(status));
        }

        self.read_body(resp).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use http_body_util::Full;
    use hyper::body::Bytes as HyperBytes;
    use hyper::service::service_fn;
    use hyper::{Request as HyperRequest, Response as HyperResponse};
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;

    /// What the test server should do with a request, based on its
    /// `Authorization` header. Returns `(status, echoed-auth-header)`.
    type Decide = Arc<dyn Fn(Option<String>) -> (StatusCode, String) + Send + Sync>;

    /// Spawn a one-endpoint HTTP server whose behavior is decided per request by
    /// `decide`. Returns the bound `http://addr/` endpoint.
    async fn spawn_http_server(decide: Decide) -> String {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let decide = Arc::clone(&decide);
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req: HyperRequest<hyper::body::Incoming>| {
                        let decide = Arc::clone(&decide);
                        async move {
                            let auth = req
                                .headers()
                                .get(http::header::AUTHORIZATION)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_owned);
                            let (status, body) = decide(auth);
                            Ok::<_, std::convert::Infallible>(
                                HyperResponse::builder()
                                    .status(status)
                                    .body(Full::new(HyperBytes::from(body)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .await;
                });
            }
        });
        format!("http://{addr}/")
    }

    /// A static bearer-token authorizer.
    #[derive(Clone)]
    struct StaticToken(&'static str);

    impl Authorizer for StaticToken {
        async fn authorize(&self) -> Result<Option<HeaderValue>, AuthError> {
            Ok(Some(HeaderValue::try_from(format!("Bearer {}", self.0))?))
        }
    }

    /// An authorizer that hands out "stale" first, then "fresh" after a 401,
    /// counting how many times it minted a token.
    #[derive(Clone)]
    struct RefreshingToken {
        mints: Arc<AtomicU32>,
        invalidated: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Authorizer for RefreshingToken {
        async fn authorize(&self) -> Result<Option<HeaderValue>, AuthError> {
            self.mints.fetch_add(1, Ordering::SeqCst);
            let token = if self.invalidated.load(Ordering::SeqCst) {
                "fresh"
            } else {
                "stale"
            };
            Ok(Some(HeaderValue::try_from(format!("Bearer {token}"))?))
        }

        async fn on_unauthorized(&self) -> bool {
            self.invalidated.store(true, Ordering::SeqCst);
            true
        }
    }

    #[tokio::test]
    async fn no_auth_sends_no_authorization_header() {
        let endpoint = spawn_http_server(Arc::new(|auth: Option<String>| {
            let body = format!("{{\"auth\":{}}}", auth.is_some());
            (StatusCode::OK, body)
        }))
        .await;

        let transport = HttpTransport::new(&endpoint).unwrap();
        let body = transport.round_trip(b"{}".to_vec()).await.unwrap();
        assert_eq!(body, br#"{"auth":false}"#);
    }

    #[tokio::test]
    async fn authorizer_attaches_bearer_token() {
        let endpoint = spawn_http_server(Arc::new(|auth: Option<String>| {
            (StatusCode::OK, auth.unwrap_or_default())
        }))
        .await;

        let transport = HttpTransport::new(&endpoint)
            .unwrap()
            .with_authorizer(StaticToken("abc123"));
        let body = transport.round_trip(b"{}".to_vec()).await.unwrap();
        assert_eq!(body, b"Bearer abc123");
    }

    #[tokio::test]
    async fn refreshes_and_retries_once_on_401() {
        // Server accepts only "Bearer fresh"; anything else is 401.
        let endpoint = spawn_http_server(Arc::new(|auth: Option<String>| {
            if auth.as_deref() == Some("Bearer fresh") {
                (StatusCode::OK, "ok".to_owned())
            } else {
                (StatusCode::UNAUTHORIZED, String::new())
            }
        }))
        .await;

        let mints = Arc::new(AtomicU32::new(0));
        let authorizer = RefreshingToken {
            mints: Arc::clone(&mints),
            invalidated: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let transport = HttpTransport::new(&endpoint)
            .unwrap()
            .with_authorizer(authorizer);

        let body = transport.round_trip(b"{}".to_vec()).await.unwrap();
        assert_eq!(body, b"ok");
        // Minted twice: once stale (rejected), once fresh (accepted).
        assert_eq!(mints.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn surfaces_401_when_retry_declined() {
        // Server always 401s; NoAuth never retries, so we see the status error.
        let endpoint = spawn_http_server(Arc::new(|_auth: Option<String>| {
            (StatusCode::UNAUTHORIZED, String::new())
        }))
        .await;

        let transport = HttpTransport::new(&endpoint).unwrap();
        let err = transport.round_trip(b"{}".to_vec()).await.unwrap_err();
        assert!(matches!(
            err,
            HttpTransportError::Status(StatusCode::UNAUTHORIZED)
        ));
    }
}
