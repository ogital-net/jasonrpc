//! Client helpers: build requests, correlate responses, surface typed results.
//!
//! The client is split into two pieces:
//!
//! - [`encode_call`] / [`decode_result`] / [`IdGen`] -- pure, transport-free
//!   construction of request bytes and matching of response bytes back to typed
//!   results.
//! - [`Client`] -- a driver over a [`Transport`] that performs a round trip.
//!
//! This split keeps the correlation logic usable even when you drive the I/O
//! yourself — for example multiplexing over a shared connection, or proxying
//! raw request bytes without re-parsing them at every hop.

use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "tokio")]
use std::time::Duration;

use crate::error::ClientError;
use crate::json;
use crate::protocol::{Id, Request, Response};

#[cfg(feature = "http-client")]
mod http;

#[cfg(feature = "http-client")]
pub use http::{HttpTransport, HttpTransportError, DEFAULT_MAX_RESPONSE_SIZE};

#[cfg(feature = "tokio")]
mod multiplex;

#[cfg(feature = "tokio")]
pub use multiplex::{MultiplexError, MultiplexOver, MultiplexTransport};

/// Monotonic request-id allocator. Cheap to clone via shared reference.
#[derive(Debug, Default)]
pub struct IdGen {
    next: AtomicU64,
}

impl IdGen {
    /// Create a new id generator starting at 1.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate the next numeric id.
    pub fn next_id(&self) -> Id {
        // Cast is safe: JSON-RPC ids are small; wrapping after 2^63 calls is
        // acceptable (and the id would be a String at that point anyway).
        #[allow(clippy::cast_possible_wrap)]
        Id::Number(self.next.fetch_add(1, Ordering::Relaxed) as i64)
    }
}

/// Serialize a call request to bytes, returning the id used for correlation.
///
/// # Errors
///
/// Returns a [`ClientError::Protocol`] error if the params or the request
/// cannot be serialized.
pub fn encode_call<P: serde::Serialize>(
    method: &str,
    params: &P,
    id: Id,
) -> Result<(Vec<u8>, Id), ClientError> {
    // `try_call` surfaces a params serialization failure instead of silently
    // sending a param-less request.
    let req = Request::try_call(method, params, id.clone()).map_err(ClientError::Protocol)?;
    let bytes = json::to_vec(&req).map_err(ClientError::Protocol)?;
    Ok((bytes, id))
}

/// Serialize a notification to bytes.
///
/// # Errors
///
/// Returns a [`ClientError::Protocol`] error if the params or the notification
/// cannot be serialized.
pub fn encode_notification<P: serde::Serialize>(
    method: &str,
    params: &P,
) -> Result<Vec<u8>, ClientError> {
    let req = Request::try_notification(method, params).map_err(ClientError::Protocol)?;
    json::to_vec(&req).map_err(ClientError::Protocol)
}

/// Parse response bytes, verify the id matches, and deserialize the result.
///
/// ```
/// # fn main() -> Result<(), jasonrpc::error::ClientError> {
/// use jasonrpc::client::decode_result;
/// use jasonrpc::Id;
///
/// let raw = br#"{"jsonrpc":"2.0","result":42,"id":1}"#;
/// let answer: i64 = decode_result(raw, &Id::Number(1))?;
/// assert_eq!(answer, 42);
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns a [`ClientError`] if:
/// - The response is not valid JSON ([`ClientError::Protocol`])
/// - The response id does not match `expected` ([`ClientError::IdMismatch`])
/// - The response contains a JSON-RPC error object ([`ClientError::Rpc`])
/// - The response has neither/both of `result` and `error`
///   ([`ClientError::MalformedResponse`])
pub fn decode_result<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    expected: &Id,
) -> Result<T, ClientError> {
    let resp: Response = json::from_slice(bytes).map_err(ClientError::Protocol)?;
    if resp.id() != expected {
        return Err(ClientError::IdMismatch);
    }
    match (resp.result_raw(), resp.error_obj()) {
        (Some(raw), None) => json::from_raw_value(raw).map_err(ClientError::Protocol),
        (None, Some(e)) => Err(ClientError::Rpc(e.clone())),
        _ => Err(ClientError::MalformedResponse),
    }
}

/// A byte-in / byte-out transport for a single request/response round trip.
///
/// Implementors handle framing and I/O. The `newline`/`netstring` codecs plus a
/// tokio stream make this trivial for UDS; an HTTP client backs the gateway's
/// upstream calls.
///
/// The associated `Error` need only be convertible into a boxed error, so the
/// ergonomic `Box<dyn std::error::Error + Send + Sync>` works directly as a
/// transport error type, as does any concrete `std::error::Error`.
///
/// ```
/// use jasonrpc::client::Transport;
///
/// // Minimal transport that round-trips through a byte buffer (test double).
/// struct EchoTransport;
///
/// impl Transport for EchoTransport {
///     type Error = std::convert::Infallible;
///
///     async fn round_trip(
///         &self,
///         request: Vec<u8>,
///     ) -> Result<Vec<u8>, Self::Error> {
///         Ok(request) // echo bytes back as-is
///     }
///
///     async fn send_notification(
///         &self,
///         _request: Vec<u8>,
///     ) -> Result<(), Self::Error> {
///         Ok(())
///     }
/// }
/// ```
pub trait Transport {
    /// The error type produced by this transport.
    type Error: Into<Box<dyn std::error::Error + Send + Sync>>;

    /// Send request bytes and return the raw response bytes.
    fn round_trip(
        &self,
        request: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, Self::Error>> + Send;

    /// Send notification bytes for which no response is expected.
    ///
    /// The default implementation performs a full [`round_trip`](Self::round_trip)
    /// and discards the reply. That is correct for request/response transports
    /// such as HTTP, where even a notification yields an (empty) response. A
    /// streaming transport where a notification produces *no* reply frame -- a
    /// raw framed UDS socket, for instance -- MUST override this to send only,
    /// otherwise it will block waiting for a frame that never arrives.
    fn send_notification(
        &self,
        notification: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Sync,
    {
        async move {
            self.round_trip(notification).await?;
            Ok(())
        }
    }
}

/// A high-level client over a [`Transport`], with automatic id allocation.
///
/// ```
/// # use jasonrpc::client::Transport;
/// # struct EchoTransport;
/// # impl Transport for EchoTransport {
/// #     type Error = std::convert::Infallible;
/// #     async fn round_trip(&self, req: Vec<u8>) -> Result<Vec<u8>, Self::Error> { Ok(req) }
/// # }
/// use jasonrpc::client::Client;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = Client::new(EchoTransport);
///
/// // Send and receive raw bytes — useful for proxies and gateways.
/// let reply = client.round_trip_raw(b"hello".to_vec()).await?;
/// assert_eq!(reply, b"hello");
/// # Ok(())
/// # }
/// ```
pub struct Client<T> {
    transport: T,
    ids: IdGen,
    #[cfg(feature = "tokio")]
    timeout: Option<Duration>,
}

impl<T> std::fmt::Debug for Client<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Client");
        s.field("next_id", &self.ids);
        #[cfg(feature = "tokio")]
        s.field("timeout", &self.timeout);
        s.finish_non_exhaustive()
    }
}

impl<T: Transport> Client<T> {
    /// Wrap a transport in a client.
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            ids: IdGen::new(),
            #[cfg(feature = "tokio")]
            timeout: None,
        }
    }

    /// Set a request timeout. Calls that don't complete within `timeout` will
    /// return [`ClientError::Timeout`].
    ///
    /// Requires the `tokio` feature (`tokio::time::timeout` backs it). Without
    /// tokio there is no runtime timer to drive the deadline, so the method is
    /// not available — wrap calls in your own runtime's timeout instead.
    #[cfg(feature = "tokio")]
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Perform a typed method call and await the typed result.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError`] on transport failure, serialization failure,
    /// timeout, or if the server returns a JSON-RPC error.
    pub async fn call<P, R>(&self, method: &str, params: P) -> Result<R, ClientError>
    where
        P: serde::Serialize,
        R: serde::de::DeserializeOwned,
    {
        let (bytes, id) = encode_call(method, &params, self.ids.next_id())?;
        let resp = self.round_trip_inner(bytes).await?;
        decode_result(&resp, &id)
    }

    /// Fire a notification (no response expected).
    ///
    /// Requires `T: Sync` because the [`Transport::send_notification`] default
    /// holds `&self` across an await. Streaming transports that override it are
    /// typically `Sync` anyway; the bound is a no-op for them.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError`] on transport failure, serialization failure,
    /// or timeout.
    pub async fn notify<P: serde::Serialize>(
        &self,
        method: &str,
        params: P,
    ) -> Result<(), ClientError>
    where
        T: Sync,
    {
        let bytes = encode_notification(method, &params)?;
        self.notify_inner(bytes).await
    }

    /// Send pre-formed request bytes through the transport and return the raw
    /// reply bytes, bypassing the typed encode/decode layer.
    ///
    /// This is the proxy/gateway entry point: forward an already-serialized
    /// JSON-RPC request (whose `id` you control) over the transport -- including
    /// a multiplexed one -- and get the raw response back to relay onward. The
    /// caller is responsible for id allocation and correlation semantics.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError::Transport`] or [`ClientError::Timeout`] error
    /// if the underlying transport fails or the timeout expires.
    pub async fn round_trip_raw(&self, request: Vec<u8>) -> Result<Vec<u8>, ClientError> {
        self.round_trip_inner(request).await
    }

    /// Send pre-formed notification bytes through the transport with no reply.
    ///
    /// The raw counterpart to [`round_trip_raw`](Self::round_trip_raw) for
    /// id-less messages a proxy forwards send-only.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError::Transport`] or [`ClientError::Timeout`] error
    /// if the underlying transport fails or the timeout expires.
    pub async fn send_raw_notification(&self, notification: Vec<u8>) -> Result<(), ClientError>
    where
        T: Sync,
    {
        self.notify_inner(notification).await
    }

    /// Internal: perform a round trip with optional timeout.
    async fn round_trip_inner(&self, request: Vec<u8>) -> Result<Vec<u8>, ClientError> {
        let fut = self.transport.round_trip(request);
        self.wrap_timeout(fut).await
    }

    /// Internal: send a notification with optional timeout.
    async fn notify_inner(&self, notification: Vec<u8>) -> Result<(), ClientError>
    where
        T: Sync,
    {
        let fut = self.transport.send_notification(notification);
        self.wrap_timeout_notify(fut).await
    }

    /// Wrap a transport future returning `Vec<u8>` with the configured timeout.
    async fn wrap_timeout<F, E>(&self, fut: F) -> Result<Vec<u8>, ClientError>
    where
        F: std::future::Future<Output = Result<Vec<u8>, E>>,
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        #[cfg(feature = "tokio")]
        if let Some(to) = self.timeout {
            return tokio::time::timeout(to, fut)
                .await
                .map_err(|_| ClientError::Timeout(to))?
                .map_err(|e| ClientError::Transport(e.into()));
        }
        fut.await.map_err(|e| ClientError::Transport(e.into()))
    }

    /// Wrap a notification future with the configured timeout.
    async fn wrap_timeout_notify<F, E>(&self, fut: F) -> Result<(), ClientError>
    where
        F: std::future::Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        #[cfg(feature = "tokio")]
        if let Some(to) = self.timeout {
            return tokio::time::timeout(to, fut)
                .await
                .map_err(|_| ClientError::Timeout(to))?
                .map_err(|e| ClientError::Transport(e.into()));
        }
        fut.await.map_err(|e| ClientError::Transport(e.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_matches_id_and_result() {
        let bytes = br#"{"jsonrpc":"2.0","result":19,"id":7}"#;
        let v: i64 = decode_result(bytes, &Id::Number(7)).unwrap();
        assert_eq!(v, 19);
    }

    #[test]
    fn encode_call_propagates_param_serialization_error() {
        struct Bad;
        impl serde::Serialize for Bad {
            fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("nope"))
            }
        }
        let err = encode_call("m", &Bad, Id::Number(1)).unwrap_err();
        assert!(matches!(err, ClientError::Protocol(_)), "got {err:?}");
        let err = encode_notification("m", &Bad).unwrap_err();
        assert!(matches!(err, ClientError::Protocol(_)), "got {err:?}");
    }

    #[test]
    fn decode_rejects_id_mismatch() {
        let bytes = br#"{"jsonrpc":"2.0","result":19,"id":8}"#;
        let e = decode_result::<i64>(bytes, &Id::Number(7)).unwrap_err();
        assert!(matches!(e, ClientError::IdMismatch));
    }

    #[test]
    fn decode_surfaces_rpc_error() {
        let bytes =
            br#"{"jsonrpc":"2.0","error":{"code":-32601,"message":"Method not found"},"id":1}"#;
        let e = decode_result::<i64>(bytes, &Id::Number(1)).unwrap_err();
        match e {
            ClientError::Rpc(err) => assert_eq!(err.code(), -32601),
            other => panic!("expected rpc error, got {other:?}"),
        }
    }

    #[test]
    fn decode_reports_malformed_response() {
        // Both result and error present.
        let bytes = br#"{"jsonrpc":"2.0","result":1,"error":{"code":0,"message":""},"id":1}"#;
        let e = decode_result::<i64>(bytes, &Id::Number(1)).unwrap_err();
        assert!(matches!(e, ClientError::MalformedResponse), "got {e:?}");

        // Neither present.
        let bytes = br#"{"jsonrpc":"2.0","id":1}"#;
        let e = decode_result::<i64>(bytes, &Id::Number(1)).unwrap_err();
        assert!(matches!(e, ClientError::MalformedResponse), "got {e:?}");
    }

    #[test]
    fn encode_notification_round_trips() {
        let bytes = encode_notification("my_method", &42i64).unwrap();
        let parsed: crate::Request = json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.method(), "my_method");
        assert!(parsed.is_notification());
        let result: i64 = parsed.params_as().unwrap();
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn client_round_trip_raw() {
        // A mock transport that echoes the request id back.
        struct Echo;
        impl Transport for Echo {
            type Error = std::convert::Infallible;
            async fn round_trip(&self, req: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
                Ok(req)
            }
        }
        let client = Client::new(Echo);
        let raw = br#"{"jsonrpc":"2.0","method":"m","id":7}"#.to_vec();
        let reply = client.round_trip_raw(raw).await.unwrap();
        assert_eq!(reply, br#"{"jsonrpc":"2.0","method":"m","id":7}"#);
    }

    #[tokio::test]
    async fn client_send_raw_notification() {
        use std::marker::PhantomData;
        struct Sink(PhantomData<fn()>);
        // fn() is always Sync, so Sink is Sync.
        impl Transport for Sink {
            type Error = std::convert::Infallible;
            async fn round_trip(&self, _req: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
                panic!("notification must not round-trip");
            }
            async fn send_notification(&self, _notification: Vec<u8>) -> Result<(), Self::Error> {
                Ok(())
            }
        }
        let client = Client::new(Sink(PhantomData));
        client
            .send_raw_notification(b"garbage".to_vec())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn client_timeout_on_call() {
        // A transport that never responds.
        struct Hangs;
        impl Transport for Hangs {
            type Error = std::convert::Infallible;
            async fn round_trip(&self, _req: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
                std::future::pending::<()>().await;
                unreachable!()
            }
        }
        let client = Client::new(Hangs).with_request_timeout(Duration::from_millis(10));
        let err = client.call::<_, String>("method", ()).await.unwrap_err();
        assert!(matches!(err, ClientError::Timeout(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn client_timeout_on_round_trip_raw() {
        struct Hangs;
        impl Transport for Hangs {
            type Error = std::convert::Infallible;
            async fn round_trip(&self, _req: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
                std::future::pending::<()>().await;
                unreachable!()
            }
        }
        let client = Client::new(Hangs).with_request_timeout(Duration::from_millis(10));
        let err = client.round_trip_raw(b"{}".to_vec()).await.unwrap_err();
        assert!(matches!(err, ClientError::Timeout(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn client_timeout_on_notify() {
        use std::marker::PhantomData;
        struct HangsNotify(PhantomData<fn()>);
        impl Transport for HangsNotify {
            type Error = std::convert::Infallible;
            async fn round_trip(&self, _req: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
                unreachable!()
            }
            async fn send_notification(&self, _n: Vec<u8>) -> Result<(), Self::Error> {
                std::future::pending::<()>().await;
                unreachable!()
            }
        }
        let client =
            Client::new(HangsNotify(PhantomData)).with_request_timeout(Duration::from_millis(10));
        let err = client.notify("m", ()).await.unwrap_err();
        assert!(matches!(err, ClientError::Timeout(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn client_no_timeout_succeeds() {
        struct Echo;
        impl Transport for Echo {
            type Error = std::convert::Infallible;
            async fn round_trip(&self, req: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
                // Echo the request as the reply.
                Ok(req)
            }
        }
        // No timeout set — should still work.
        let client = Client::new(Echo);
        let raw = br#"{"jsonrpc":"2.0","method":"m","id":7}"#.to_vec();
        let reply = client.round_trip_raw(raw).await.unwrap();
        assert!(!reply.is_empty());
    }
}
