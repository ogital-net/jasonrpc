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

use std::error::Error as StdError;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Uri};
use http_body_util::{BodyExt, Full, Limited};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;

use super::Transport;

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
/// Default headers set via [`with_header`](Self::with_header) are sent on every
/// request, for example a static `Authorization` token.
#[derive(Clone, Debug)]
pub struct HttpTransport {
    client: HyperClient<HttpConnector, Full<Bytes>>,
    uri: Uri,
    headers: HeaderMap,
    max_response_size: usize,
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
        }
    }
}

impl StdError for HttpTransportError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            HttpTransportError::Connect(e) | HttpTransportError::Body(e) => Some(e.as_ref()),
            HttpTransportError::InvalidUri(_)
            | HttpTransportError::Status(_)
            | HttpTransportError::InvalidHeader(_)
            | HttpTransportError::TooLarge(_) => None,
        }
    }
}

impl HttpTransport {
    /// Build a transport targeting `endpoint` (e.g. `http://127.0.0.1:8080/`).
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
        })
    }

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

    /// Add a default header sent on every request.
    ///
    /// Typically used to attach a static `Authorization` token. Both the name
    /// and value are validated eagerly.
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
}

impl Transport for HttpTransport {
    type Error = HttpTransportError;

    async fn round_trip(&self, request: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        let mut builder = http::Request::builder()
            .method(Method::POST)
            .uri(self.uri.clone())
            .header(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
        // Attach any configured default headers (e.g. Authorization).
        if let Some(headers) = builder.headers_mut() {
            for (name, value) in &self.headers {
                headers.insert(name, value.clone());
            }
        }
        let req = builder
            .body(Full::new(Bytes::from(request)))
            .map_err(|e| HttpTransportError::Connect(Box::new(e)))?;

        let resp = self
            .client
            .request(req)
            .await
            .map_err(|e| HttpTransportError::Connect(Box::new(e)))?;

        let status = resp.status();
        if !status.is_success() && status != http::StatusCode::NO_CONTENT {
            return Err(HttpTransportError::Status(status));
        }

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
