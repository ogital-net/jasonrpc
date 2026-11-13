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
use crate::protocol::{Error, Id, Response, Version};

#[cfg(feature = "http-client")]
mod http;

#[cfg(feature = "http-client")]
pub use http::{
    AuthError, Authorizer, HttpTransport, HttpTransportError, NoAuth, DEFAULT_MAX_RESPONSE_SIZE,
};

#[cfg(feature = "tokio")]
mod multiplex;

#[cfg(feature = "tokio")]
pub use multiplex::{MultiplexError, MultiplexOver, MultiplexTransport};

#[cfg(all(feature = "uds", unix))]
mod uds;

#[cfg(all(feature = "uds", unix))]
pub use uds::{ReconnectingUds, RetryPolicy, UdsClient};

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
        Id::Unsigned(self.next.fetch_add(1, Ordering::Relaxed))
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
    let bytes = encode_request(method, params, Some(&id)).map_err(ClientError::Protocol)?;
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
    encode_request(method, params, None).map_err(ClientError::Protocol)
}

fn encode_request<P: serde::Serialize>(
    method: &str,
    params: &P,
    id: Option<&Id>,
) -> Result<Vec<u8>, json::JsonError> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(br#"{"jsonrpc":"2.0","method":"#);
    json::to_writer(&mut bytes, &method)?;
    let params_field = bytes.len();
    bytes.extend_from_slice(br#","params":"#);
    let params_start = bytes.len();
    json::to_writer(&mut bytes, params)?;
    match json::classify_raw_value(&bytes[params_start..]) {
        json::RawValueKind::Null => bytes.truncate(params_field),
        json::RawValueKind::Structured => {}
        json::RawValueKind::Scalar => {
            bytes.insert(params_start, b'[');
            bytes.push(b']');
        }
    }
    if let Some(id) = id {
        bytes.extend_from_slice(br#","id":"#);
        json::to_writer(&mut bytes, id)?;
    }
    bytes.push(b'}');
    Ok(bytes)
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
/// - The response contains invalid UTF-8 or cannot be deserialized, including
///   an explicit `error: null`
///   ([`ClientError::Protocol`])
/// - The response id does not match `expected` ([`ClientError::IdMismatch`])
/// - The response contains a JSON-RPC error object ([`ClientError::Rpc`])
/// - The response has neither/both of `result` and `error`
///   ([`ClientError::MalformedResponse`])
pub fn decode_result<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    expected: &Id,
) -> Result<T, ClientError> {
    validate_response_utf8(bytes).map_err(ClientError::Protocol)?;
    if let Ok(response) = json::from_slice::<TypedResponse<T>>(bytes) {
        if &response.id != expected {
            return Err(ClientError::IdMismatch);
        }
        return match (response.result, response.error) {
            (Some(result), None) => Ok(result),
            (None, Some(error)) => Err(ClientError::Rpc(error)),
            _ => Err(ClientError::MalformedResponse),
        };
    }

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

fn validate_response_utf8(bytes: &[u8]) -> Result<(), json::JsonError> {
    std::str::from_utf8(bytes)
        .map(|_| ())
        .map_err(serde::de::Error::custom)
}

#[derive(serde::Deserialize)]
#[serde(bound(deserialize = "T: serde::Deserialize<'de>"))]
struct TypedResponse<T> {
    #[serde(rename = "jsonrpc")]
    _version: Version,
    #[serde(default, deserialize_with = "deserialize_present")]
    result: Option<T>,
    #[serde(default, deserialize_with = "deserialize_present")]
    error: Option<Error>,
    id: Id,
}

fn deserialize_present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// Received response bytes with optional cached envelope metadata.
///
/// Raw forwarding preserves the exact original bytes. Multiplexed transports
/// can cache the routing envelope without deserializing the caller's result
/// type in the shared reader task.
#[derive(Debug)]
pub struct ReceivedResponse {
    bytes: bytes::Bytes,
    #[cfg(feature = "tokio")]
    envelope: Option<CachedEnvelope>,
}

#[cfg(feature = "tokio")]
#[derive(Debug)]
struct CachedEnvelope {
    id: Id,
    result: Option<ResultBytes>,
    error: Option<Error>,
}

#[cfg(feature = "tokio")]
#[derive(Debug)]
enum ResultBytes {
    Range(std::ops::Range<usize>),
    Owned(Vec<u8>),
}

impl ReceivedResponse {
    /// Wrap bytes without parsing them.
    pub fn from_bytes(bytes: impl Into<bytes::Bytes>) -> Self {
        Self {
            bytes: bytes.into(),
            #[cfg(feature = "tokio")]
            envelope: None,
        }
    }

    /// Borrow the exact received bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Return the exact received bytes, without reserialization.
    pub fn into_bytes(self) -> Vec<u8> {
        Vec::from(self.bytes)
    }

    /// Return the shared received bytes without copying the payload.
    pub fn into_shared_bytes(self) -> bytes::Bytes {
        self.bytes
    }

    /// Validate correlation and decode the typed result, reusing any cache.
    ///
    /// # Errors
    ///
    /// Returns the same validation, protocol, and RPC errors as [`decode_result`].
    pub fn decode<T: serde::de::DeserializeOwned>(self, expected: &Id) -> Result<T, ClientError> {
        #[cfg(feature = "tokio")]
        if let Some(envelope) = self.envelope {
            if &envelope.id != expected {
                return Err(ClientError::IdMismatch);
            }
            return match (envelope.result, envelope.error) {
                (Some(result), None) => {
                    let bytes = match &result {
                        ResultBytes::Range(range) => &self.bytes[range.clone()],
                        ResultBytes::Owned(bytes) => bytes.as_slice(),
                    };
                    json::from_slice(bytes).map_err(ClientError::Protocol)
                }
                (None, Some(error)) => Err(ClientError::Rpc(error)),
                _ => Err(ClientError::MalformedResponse),
            };
        }
        decode_result(&self.bytes, expected)
    }

    #[cfg(feature = "tokio")]
    fn parsed(bytes: impl Into<bytes::Bytes>) -> Self {
        let bytes = bytes.into();
        let envelope = Self::parse_envelope(&bytes).ok();
        Self { bytes, envelope }
    }

    #[cfg(feature = "tokio")]
    fn routing_id(&self) -> Option<&Id> {
        self.envelope.as_ref().map(|envelope| &envelope.id)
    }

    #[cfg(feature = "tokio")]
    fn parse_envelope(bytes: &[u8]) -> Result<CachedEnvelope, json::JsonError> {
        validate_response_utf8(bytes)?;
        #[derive(serde::Deserialize)]
        struct Envelope<'a> {
            #[serde(rename = "jsonrpc")]
            _version: Version,
            #[serde(borrow, default, deserialize_with = "deserialize_present")]
            result: Option<json::BorrowedRawValue<'a>>,
            #[serde(default, deserialize_with = "deserialize_present")]
            error: Option<Error>,
            id: Id,
        }

        let envelope: Envelope<'_> = json::from_slice_borrowed(bytes)?;
        let result = envelope.result.map(|raw| {
            let text = json::borrowed_raw_text(&raw);
            let range = (text.as_ptr() as usize)
                .checked_sub(bytes.as_ptr() as usize)
                .and_then(|start| start.checked_add(text.len()).map(|end| start..end))
                .filter(|range| range.end <= bytes.len());
            match range {
                Some(range) => ResultBytes::Range(range),
                None => ResultBytes::Owned(text.as_bytes().to_vec()),
            }
        });
        Ok(CachedEnvelope {
            id: envelope.id,
            result,
            error: envelope.error,
        })
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

    /// Send request bytes with a known id and return the raw response bytes.
    ///
    /// The default ignores the id and delegates to [`round_trip`](Self::round_trip).
    /// Multiplexed transports can override this to avoid parsing the outgoing
    /// request to recover its id. The caller must supply the same id that is
    /// encoded in the request; transports need not validate this correspondence.
    fn round_trip_with_id(
        &self,
        request: Vec<u8>,
        _id: Id,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, Self::Error>> + Send {
        self.round_trip(request)
    }

    /// Send a call and return response bytes with optional cached metadata.
    ///
    /// The default delegates to [`round_trip_with_id`](Self::round_trip_with_id)
    /// and wraps the bytes without parsing them. Multiplexed transports can
    /// override this to reuse the envelope parsed for response routing. The id
    /// must match the one encoded in the request.
    fn round_trip_response(
        &self,
        request: Vec<u8>,
        id: Id,
    ) -> impl std::future::Future<Output = Result<ReceivedResponse, Self::Error>> + Send {
        let response = self.round_trip_with_id(request, id);
        async move { response.await.map(ReceivedResponse::from_bytes) }
    }

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
        let response = self.transport.round_trip_response(bytes, id.clone());
        self.wrap_timeout(response).await?.decode(&id)
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

    /// Send pre-formed request bytes with a known id and return the raw reply.
    ///
    /// Like [`round_trip_raw`](Self::round_trip_raw), but lets multiplexed
    /// transports avoid parsing the outgoing request. The id must match the id
    /// encoded in the request. This method does not validate the response id or
    /// decode the response.
    ///
    /// # Errors
    ///
    /// Returns a [`ClientError::Transport`] or [`ClientError::Timeout`] error
    /// if the underlying transport fails or the timeout expires.
    pub async fn round_trip_raw_with_id(
        &self,
        request: Vec<u8>,
        id: Id,
    ) -> Result<Vec<u8>, ClientError> {
        let fut = self.transport.round_trip_with_id(request, id);
        self.wrap_timeout(fut).await
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

    /// Wrap a transport future with the configured timeout.
    async fn wrap_timeout<F, E, R>(&self, fut: F) -> Result<R, ClientError>
    where
        F: std::future::Future<Output = Result<R, E>>,
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
    use crate::protocol::Request;

    #[test]
    fn encoding_matches_request_constructors() {
        fn check<P: serde::Serialize>(params: &P) {
            let method = "quote: \" slash: \\ newline: \n";
            for id in [
                Id::Null,
                Id::Number(-1),
                Id::Unsigned(u64::MAX),
                Id::String("quote: \" slash: \\ newline: \n".into()),
            ] {
                let expected = Request::try_call(method, params, id.clone()).unwrap();
                let (bytes, returned_id) = encode_call(method, params, id.clone()).unwrap();
                assert_eq!(returned_id, id);
                assert_eq!(bytes, json::to_vec(&expected).unwrap());
                let _: Request = json::from_slice(&bytes).unwrap();
            }
            let expected = Request::try_notification(method, params).unwrap();
            let bytes = encode_notification(method, params).unwrap();
            assert_eq!(bytes, json::to_vec(&expected).unwrap());
            let _: Request = json::from_slice(&bytes).unwrap();
        }

        check(&());
        check(&None::<u64>);
        check(&true);
        check(&42);
        check(&"scalar \"string\"");
        check(&Some(42));
        check(&[1, 2, 3]);
        check(&Vec::<u64>::new());
        check(&std::collections::BTreeMap::from([("name", "value")]));
    }

    #[test]
    fn encoding_serializes_params_once() {
        struct Counted(std::cell::Cell<usize>);
        impl serde::Serialize for Counted {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                self.0.set(self.0.get() + 1);
                serializer.serialize_u64(42)
            }
        }

        let params = Counted(std::cell::Cell::new(0));
        encode_call("m", &params, Id::Number(1)).unwrap();
        assert_eq!(params.0.get(), 1);
        encode_notification("m", &params).unwrap();
        assert_eq!(params.0.get(), 2);
    }

    #[test]
    fn decode_matches_id_and_result() {
        let bytes = br#"{"jsonrpc":"2.0","result":19,"id":7}"#;
        let v: i64 = decode_result(bytes, &Id::Number(7)).unwrap();
        assert_eq!(v, 19);
    }

    #[test]
    fn decode_rejects_invalid_utf8_in_ignored_fields() {
        #[derive(Debug, serde::Deserialize)]
        struct Count {
            count: i64,
        }

        let valid =
            b"{\"jsonrpc\":\"2.0\",\"result\":{\"count\":1,\"extra\":\"\xe2\x82\xac\"},\"id\":7}";
        assert_eq!(
            decode_result::<Count>(valid, &Id::Number(7)).unwrap().count,
            1
        );

        for bytes in [
            b"{\"jsonrpc\":\"2.0\",\"result\":{\"count\":1,\"extra\":\"\xff\"},\"id\":7}"
                .as_slice(),
            b"{\"jsonrpc\":\"2.0\",\"result\":{\"count\":1},\"extra\":\"\xff\",\"id\":7}"
                .as_slice(),
            b"{\"jsonrpc\":\"2.0\",\"result\":\"\xff\",\"id\":7}".as_slice(),
        ] {
            assert!(matches!(
                decode_result::<Count>(bytes, &Id::Number(7)),
                Err(ClientError::Protocol(_))
            ));
            assert!(matches!(
                decode_result::<serde::de::IgnoredAny>(bytes, &Id::Number(7)),
                Err(ClientError::Protocol(_))
            ));

            #[cfg(feature = "tokio")]
            {
                let response = ReceivedResponse::parsed(bytes.to_vec());
                assert!(response.envelope.is_none());
                assert_eq!(response.as_bytes(), bytes);
                assert!(matches!(
                    response.decode::<Count>(&Id::Number(7)),
                    Err(ClientError::Protocol(_))
                ));
            }
        }
    }

    #[test]
    fn decode_null_result() {
        for bytes in [
            br#"{"jsonrpc":"2.0","result":null,"id":1}"#.to_vec(),
            json::to_vec(&Response::result(Id::Number(1), ())).unwrap(),
            json::to_vec(&Response::result(Id::Number(1), None::<String>)).unwrap(),
        ] {
            assert!(decode_result::<()>(&bytes, &Id::Number(1)).is_ok());
            assert_eq!(
                decode_result::<Option<String>>(&bytes, &Id::Number(1)).unwrap(),
                None
            );
        }
    }

    #[test]
    fn decode_rejects_null_result_with_error() {
        let bytes = br#"{"jsonrpc":"2.0","result":null,"error":{"code":-32601,"message":"Method not found"},"id":1}"#;
        let error = decode_result::<()>(bytes, &Id::Number(1)).unwrap_err();
        assert!(
            matches!(error, ClientError::MalformedResponse),
            "got {error:?}"
        );
    }

    #[test]
    fn decode_rejects_explicit_null_error() {
        for bytes in [
            br#"{"jsonrpc":"2.0","result":42,"error":null,"id":1}"#.as_slice(),
            br#"{"jsonrpc":"2.0","error":null,"result":42,"id":1}"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":null,"error":null,"id":1}"#.as_slice(),
            br#"{"jsonrpc":"2.0","error":null,"id":1}"#.as_slice(),
        ] {
            let error = decode_result::<Option<i64>>(bytes, &Id::Number(1)).unwrap_err();
            assert!(matches!(error, ClientError::Protocol(_)), "got {error:?}");
        }
    }

    #[test]
    fn id_gen_uses_unsigned_ids_across_signed_boundary() {
        let ids = IdGen {
            next: AtomicU64::new(i64::MAX as u64),
        };
        assert_eq!(ids.next_id(), Id::Unsigned(i64::MAX as u64));
        assert_eq!(ids.next_id(), Id::Unsigned(i64::MAX as u64 + 1));
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
    fn decode_preserves_validation_before_result_type_errors() {
        let bytes = br#"{"jsonrpc":"2.0","result":"wrong type","id":8}"#;
        assert!(matches!(
            decode_result::<i64>(bytes, &Id::Number(7)),
            Err(ClientError::IdMismatch)
        ));

        let bytes =
            br#"{"jsonrpc":"2.0","result":"wrong type","error":{"code":0,"message":""},"id":7}"#;
        assert!(matches!(
            decode_result::<i64>(bytes, &Id::Number(7)),
            Err(ClientError::MalformedResponse)
        ));

        for bytes in [
            br#"{"jsonrpc":"2.0","result":"wrong type","id":7}"#.as_slice(),
            br#"{"jsonrpc":"1.0","result":7,"id":7}"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":7,"result":8,"id":7}"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":7,"id":7} trailing"#.as_slice(),
        ] {
            assert!(matches!(
                decode_result::<i64>(bytes, &Id::Number(7)),
                Err(ClientError::Protocol(_))
            ));
        }
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn cached_response_preserves_bytes_and_validation() {
        for bytes in [
            br#"{ "id":7, "result":42, "jsonrpc":"2.0" }"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":null,"id":7}"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":42,"id":8}"#.as_slice(),
            br#"{"jsonrpc":"2.0","error":{"code":0,"message":""},"id":7}"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":42,"error":{"code":0,"message":""},"id":7}"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":42,"error":null,"id":7}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":7}"#.as_slice(),
            br#"{"jsonrpc":"1.0","result":42,"id":7}"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":42,"result":43,"id":7}"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":42,"id":7} trailing"#.as_slice(),
        ] {
            assert_eq!(ReceivedResponse::parsed(bytes.to_vec()).into_bytes(), bytes);
            let expected = decode_result::<i64>(bytes, &Id::Number(7));
            let actual = ReceivedResponse::parsed(bytes.to_vec()).decode::<i64>(&Id::Number(7));
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => assert_eq!(expected, actual),
                (Err(expected), Err(actual)) => {
                    assert_eq!(
                        std::mem::discriminant(&expected),
                        std::mem::discriminant(&actual)
                    );
                }
                other => panic!("cached and direct decoding disagree: {other:?}"),
            }
        }

        let bytes = br#"{"jsonrpc":"2.0","result":"quote: \" and newline: \n","id":7}"#;
        let response = ReceivedResponse::parsed(bytes.to_vec());
        assert!(response.envelope.is_some());
        assert_eq!(
            response.decode::<String>(&Id::Number(7)).unwrap(),
            "quote: \" and newline: \n"
        );
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
        let result: i64 = parsed.param_as().unwrap();
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

        let reply = client
            .round_trip_raw_with_id(b"arbitrary bytes".to_vec(), Id::Number(7))
            .await
            .unwrap();
        assert_eq!(reply, b"arbitrary bytes");
    }

    #[tokio::test]
    async fn client_passes_known_id_to_transport() {
        struct IdAware;
        impl Transport for IdAware {
            type Error = std::convert::Infallible;

            async fn round_trip(&self, _request: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
                panic!("known-id calls must use the id-aware method");
            }

            async fn round_trip_with_id(
                &self,
                request: Vec<u8>,
                id: Id,
            ) -> Result<Vec<u8>, Self::Error> {
                let request: Request = json::from_slice(&request).unwrap();
                assert_eq!(request.id(), Some(&id));
                Ok(json::to_vec(&Response::result(id, 7)).unwrap())
            }
        }

        let client = Client::new(IdAware);
        assert_eq!(client.call::<_, i64>("m", ()).await.unwrap(), 7);

        let (request, id) = encode_call("m", &(), Id::String("known".into())).unwrap();
        let reply = client
            .round_trip_raw_with_id(request, id.clone())
            .await
            .unwrap();
        assert_eq!(decode_result::<i64>(&reply, &id).unwrap(), 7);
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

    #[cfg(feature = "tokio")]
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

    #[cfg(feature = "tokio")]
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

        let err = client
            .round_trip_raw_with_id(b"{}".to_vec(), Id::Number(1))
            .await
            .unwrap_err();
        assert!(matches!(err, ClientError::Timeout(_)), "got {err:?}");
    }

    #[cfg(feature = "tokio")]
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
