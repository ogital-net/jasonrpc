//! Core JSON-RPC 2.0 data model and (de)serialization.
//!
//! This module is the "core" layer: pure data types with no I/O and no async.
//! The types here conform to the JSON-RPC 2.0 specification. Higher layers
//! (`server`, `client`, `transport`, integrations) build on top of these types.

use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::json::{JsonError, JsonRawValue};

/// Predefined JSON-RPC 2.0 error codes.
///
/// See <https://www.jsonrpc.org/specification#error_object>. The range
/// `-32768..=-32000` is reserved; `-32000..=-32099` is reserved for
/// implementation-defined server errors.
pub mod codes {
    /// Invalid JSON was received by the server.
    pub const PARSE_ERROR: i64 = -32700;
    /// The JSON sent is not a valid Request object.
    pub const INVALID_REQUEST: i64 = -32600;
    /// The method does not exist / is not available.
    pub const METHOD_NOT_FOUND: i64 = -32601;
    /// Invalid method parameter(s).
    pub const INVALID_PARAMS: i64 = -32602;
    /// Internal JSON-RPC error.
    pub const INTERNAL_ERROR: i64 = -32603;

    /// Inclusive lower bound of the implementation-defined server error range.
    pub const SERVER_ERROR_MIN: i64 = -32099;
    /// Inclusive upper bound of the implementation-defined server error range.
    pub const SERVER_ERROR_MAX: i64 = -32000;
}

/// The `jsonrpc` version marker. Serializes to the string `"2.0"` and rejects
/// any other value on the wire.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Version;

impl Serialize for Version {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str("2.0")
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = Version;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(r#"the string "2.0""#)
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<Version, E> {
                if s == "2.0" {
                    Ok(Version)
                } else {
                    Err(E::custom(format!("unsupported jsonrpc version: {s:?}")))
                }
            }
        }
        d.deserialize_str(V)
    }
}

/// A JSON-RPC request/response identifier.
///
/// The original wire type is preserved: `"1"` (String) and `1` (Number) are
/// distinct and never coerced into one another. `Null` is permitted but
/// discouraged by the spec.
///
/// `Hash` is derived so an `Id` can key a correlation map (used by the
/// multiplexing client transport).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Id {
    /// A null id. Discouraged by the spec, but accepted.
    Null,
    /// A numeric id.
    Number(i64),
    /// A string id.
    String(String),
}

impl Serialize for Id {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Id::Null => s.serialize_none(),
            Id::Number(n) => s.serialize_i64(*n),
            Id::String(v) => s.serialize_str(v),
        }
    }
}

impl fmt::Display for Id {
    /// Human-readable rendering for logs and error messages. A [`Null`](Id::Null)
    /// id renders as `null`; this is *not* JSON serialization (a string id is
    /// shown unquoted).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Id::Null => f.write_str("null"),
            Id::Number(n) => write!(f, "{n}"),
            Id::String(s) => f.write_str(s),
        }
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = Id;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string, integer, or null")
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Id, E> {
                Ok(Id::Number(v))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Id, E> {
                Ok(i64::try_from(v).map_or_else(|_| Id::String(v.to_string()), Id::Number))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Id, E> {
                Ok(Id::String(v.to_owned()))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<Id, E> {
                Ok(Id::String(v))
            }

            fn visit_unit<E: de::Error>(self) -> Result<Id, E> {
                Ok(Id::Null)
            }

            fn visit_none<E: de::Error>(self) -> Result<Id, E> {
                Ok(Id::Null)
            }
        }
        d.deserialize_any(V)
    }
}

/// Deserialize the request `id` field, distinguishing "present but null"
/// (`Some(Id::Null)`) from "absent" (`None`, which marks a notification).
///
/// This is only invoked by serde when the field is present, so an absent field
/// falls back to the container's `#[serde(default)]` (i.e. `None`).
fn de_present_id<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Id>, D::Error> {
    Ok(Some(Id::deserialize(d)?))
}

/// The JSON-RPC 2.0 error object (a protocol value, distinct from a Rust error).
///
/// ```
/// use jasonrpc::Error;
///
/// let err = Error::new(-32000, "Something went wrong");
/// assert_eq!(err.code(), -32000);
/// assert_eq!(err.message(), "Something went wrong");
///
/// // Attach structured data.
/// let err = err.with_data("extra details");
/// assert!(err.data_raw().is_some());
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Error {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    data: Option<JsonRawValue>,
}

impl PartialEq for Error {
    /// Two errors are equal when their code, message, and data match. Data is
    /// compared by its serialized JSON bytes (the raw-value type isn't directly
    /// comparable across backends), so semantically equal but differently
    /// formatted data may compare unequal.
    fn eq(&self, other: &Self) -> bool {
        if self.code != other.code || self.message != other.message {
            return false;
        }
        match (&self.data, &other.data) {
            (None, None) => true,
            (Some(a), Some(b)) => crate::json::to_vec(a).ok() == crate::json::to_vec(b).ok(),
            _ => false,
        }
    }
}

impl Error {
    /// Construct an error with the given code and message.
    #[must_use]
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Attach structured `data` to the error. The data is serialized to JSON at
    /// construction time.
    ///
    /// If serialization fails the error is returned with no data attached (the
    /// `data` field is left `None`). For types built with `#[derive(Serialize)]`
    /// this never fails; use [`try_with_data`](Self::try_with_data) if you need
    /// to observe a serialization failure (e.g. a type containing `f64::NAN` or
    /// a non-string map key).
    ///
    /// ```
    /// use jasonrpc::Error;
    ///
    /// let err = Error::new(-32000, "boom").with_data(vec![1, 2, 3]);
    /// assert!(err.data_raw().is_some());
    /// ```
    #[must_use]
    pub fn with_data(mut self, data: impl Serialize) -> Self {
        self.data = crate::json::to_raw_value(&data).ok();
        self
    }

    /// Attach structured `data`, returning an error if serialization fails.
    ///
    /// The fallible sibling of [`with_data`](Self::with_data). Prefer this when
    /// the data type can genuinely fail to serialize and you want to handle
    /// that rather than silently drop the data.
    ///
    /// # Errors
    ///
    /// Returns a [`JsonError`] if `data` cannot be serialized to JSON.
    ///
    /// ```
    /// # fn main() -> Result<(), jasonrpc::json::JsonError> {
    /// use jasonrpc::Error;
    ///
    /// let err = Error::new(-32000, "boom").try_with_data(vec![1, 2, 3])?;
    /// assert!(err.data_raw().is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_with_data(mut self, data: impl Serialize) -> Result<Self, JsonError> {
        self.data = Some(crate::json::to_raw_value(&data)?);
        Ok(self)
    }

    /// The error code.
    #[must_use]
    pub fn code(&self) -> i64 {
        self.code
    }

    /// The error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Raw error data, if any. Use [`data_as`](Self::data_as) for typed
    /// deserialization.
    #[must_use]
    pub fn data_raw(&self) -> Option<&JsonRawValue> {
        self.data.as_ref()
    }

    /// Deserialize the error data into a concrete type.
    ///
    /// Returns `None` if the error has no data or deserialization fails.
    #[must_use]
    pub fn data_as<T: serde::de::DeserializeOwned>(&self) -> Option<T> {
        self.data
            .as_ref()
            .and_then(|raw| crate::json::from_raw_value(raw).ok())
    }

    /// `-32700` Parse error.
    #[must_use]
    pub fn parse_error() -> Self {
        Self::new(codes::PARSE_ERROR, "Parse error")
    }

    /// `-32600` Invalid Request.
    #[must_use]
    pub fn invalid_request() -> Self {
        Self::new(codes::INVALID_REQUEST, "Invalid Request")
    }

    /// `-32601` Method not found.
    #[must_use]
    pub fn method_not_found() -> Self {
        Self::new(codes::METHOD_NOT_FOUND, "Method not found")
    }

    /// `-32602` Invalid params.
    #[must_use]
    pub fn invalid_params() -> Self {
        Self::new(codes::INVALID_PARAMS, "Invalid params")
    }

    /// `-32603` Internal error.
    #[must_use]
    pub fn internal_error() -> Self {
        Self::new(codes::INTERNAL_ERROR, "Internal error")
    }

    /// An implementation-defined server error. `code` should fall within
    /// `-32099..=-32000`; this is not enforced but is debug-asserted.
    ///
    /// # Panics
    ///
    /// In debug builds, panics if `code` is outside the reserved server error
    /// range (`-32099..=-32000`).
    #[must_use]
    pub fn server_error(code: i64, message: impl Into<String>) -> Self {
        debug_assert!(
            (codes::SERVER_ERROR_MIN..=codes::SERVER_ERROR_MAX).contains(&code),
            "server error code {code} outside reserved range -32099..=-32000",
        );
        Self::new(code, message)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (code {})", self.message, self.code)
    }
}

impl std::error::Error for Error {}

/// A JSON-RPC 2.0 request or notification.
///
/// A request with `id == None` is a *notification*: the server must not reply.
///
/// ```
/// use jasonrpc::{Request, Id, json};
///
/// // Build a call (expects a response).
/// let req = Request::call(
///     "subtract",
///     &(42, 23),
///     Id::Number(1),
/// );
/// assert!(!req.is_notification());
/// assert_eq!(req.method(), "subtract");
///
/// // Build a notification (no response expected).
/// let notif = Request::notification("update", &[1, 2, 3]);
/// assert!(notif.is_notification());
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    jsonrpc: Version,
    method: String,
    /// Parameters as raw JSON. Use [`params_as`](Self::params_as) to
    /// deserialize into a concrete type.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    params: Option<JsonRawValue>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "de_present_id"
    )]
    id: Option<Id>,
}

impl Request {
    /// Build a request that expects a response.
    ///
    /// The `params` argument is serialized to JSON bytes. Pass `()` for a
    /// no-params call.
    ///
    /// If serialization of `params` fails the request is constructed with no
    /// params. For `#[derive(Serialize)]` types this never happens; use
    /// [`try_call`](Self::try_call) if `params` can genuinely fail to serialize
    /// and you want the error rather than a silently param-less request.
    pub fn call<P: Serialize>(method: impl Into<String>, params: &P, id: Id) -> Self {
        Self {
            jsonrpc: Version,
            method: method.into(),
            params: crate::json::to_raw_value(params).ok(),
            id: Some(id),
        }
    }

    /// Build a request that expects a response, returning an error if `params`
    /// cannot be serialized.
    ///
    /// The fallible sibling of [`call`](Self::call).
    ///
    /// # Errors
    ///
    /// Returns a [`JsonError`] if `params` cannot be serialized to JSON.
    pub fn try_call<P: Serialize>(
        method: impl Into<String>,
        params: &P,
        id: Id,
    ) -> Result<Self, JsonError> {
        Ok(Self {
            jsonrpc: Version,
            method: method.into(),
            params: Some(crate::json::to_raw_value(params)?),
            id: Some(id),
        })
    }

    /// Build a notification (a request with no id, expecting no response).
    ///
    /// See [`call`](Self::call) for notes on params serialization.
    pub fn notification<P: Serialize>(method: impl Into<String>, params: &P) -> Self {
        Self {
            jsonrpc: Version,
            method: method.into(),
            params: crate::json::to_raw_value(params).ok(),
            id: None,
        }
    }

    /// Build a notification, returning an error if `params` cannot be
    /// serialized.
    ///
    /// The fallible sibling of [`notification`](Self::notification).
    ///
    /// # Errors
    ///
    /// Returns a [`JsonError`] if `params` cannot be serialized to JSON.
    pub fn try_notification<P: Serialize>(
        method: impl Into<String>,
        params: &P,
    ) -> Result<Self, JsonError> {
        Ok(Self {
            jsonrpc: Version,
            method: method.into(),
            params: Some(crate::json::to_raw_value(params)?),
            id: None,
        })
    }

    /// The method name.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Whether this request is a notification (has no id).
    #[must_use]
    pub fn is_notification(&self) -> bool {
        self.id.is_none()
    }

    /// The request id, if present (absent for notifications).
    #[must_use]
    pub fn id(&self) -> Option<&Id> {
        self.id.as_ref()
    }

    /// Set the request id (used by the gateway for id rewriting).
    pub fn set_id(&mut self, id: Id) {
        self.id = Some(id);
    }

    /// Deserialize the params into a concrete type.
    ///
    /// Missing params are treated as JSON `null`, which succeeds for types
    /// like `Option<T>`. Returns `None` if deserialization fails.
    #[must_use]
    pub fn params_as<T: serde::de::DeserializeOwned>(&self) -> Option<T> {
        match &self.params {
            Some(raw) => crate::json::from_raw_value(raw).ok(),
            None => crate::json::from_slice::<T>(b"null").ok(),
        }
    }
}

/// A JSON-RPC 2.0 response object.
///
/// Exactly one of `result` / `error` is populated. Use [`Response::result`] and
/// [`Response::error`] to construct valid responses; the constructors guarantee
/// the invariant.
///
/// ```
/// # fn main() -> Result<(), jasonrpc::json::JsonError> {
/// use jasonrpc::{Response, Id, Error};
///
/// let ok = Response::result(Id::Number(1), 19_i64);
/// assert!(ok.is_valid());
/// assert!(!ok.is_error());
///
/// let err = Response::error(Id::Null, Error::method_not_found());
/// assert!(err.is_valid());
/// assert!(err.is_error());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    jsonrpc: Version,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    result: Option<JsonRawValue>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    error: Option<Error>,
    id: Id,
}

impl Response {
    /// A successful response. The `value` is serialized to JSON at
    /// construction time.
    ///
    /// If serialization fails the response is constructed with a `null` result.
    /// For `#[derive(Serialize)]` types this never happens; use
    /// [`try_result`](Self::try_result) if `value` can genuinely fail to
    /// serialize and you want the error rather than a silent `null`.
    #[must_use]
    pub fn result(id: Id, value: impl Serialize) -> Self {
        Self {
            jsonrpc: Version,
            result: crate::json::to_raw_value(&value).ok(),
            error: None,
            id,
        }
    }

    /// A successful response, returning an error if `value` cannot be
    /// serialized.
    ///
    /// The fallible sibling of [`result`](Self::result).
    ///
    /// # Errors
    ///
    /// Returns a [`JsonError`] if `value` cannot be serialized to JSON.
    pub fn try_result(id: Id, value: impl Serialize) -> Result<Self, JsonError> {
        Ok(Self {
            jsonrpc: Version,
            result: Some(crate::json::to_raw_value(&value)?),
            error: None,
            id,
        })
    }

    /// An error response.
    #[must_use]
    pub fn error(id: Id, error: Error) -> Self {
        Self {
            jsonrpc: Version,
            result: None,
            error: Some(error),
            id,
        }
    }

    /// A successful response from an already-serialized raw JSON result.
    ///
    /// This is the zero-DOM path used by the server: a handler's return value
    /// is serialized straight to a [`JsonRawValue`] once, and dropped in here
    /// without a round trip through a [`Value`](crate::json::Value).
    #[cfg(feature = "server")]
    pub(crate) fn from_raw_result(id: Id, result: JsonRawValue) -> Self {
        Self {
            jsonrpc: Version,
            result: Some(result),
            error: None,
            id,
        }
    }

    /// Whether this response carries an error.
    #[must_use]
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }

    /// Validate the spec invariant that exactly one of `result`/`error` is set.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.result.is_some() ^ self.error.is_some()
    }

    /// The response id.
    #[must_use]
    pub fn id(&self) -> &Id {
        &self.id
    }

    /// Set the response id (used in a proxy to restore caller's original id).
    pub fn set_id(&mut self, id: Id) {
        self.id = id;
    }

    /// The raw result value, if this is a success response. Use
    /// [`result_as`](Self::result_as) for typed deserialization.
    #[must_use]
    pub fn result_raw(&self) -> Option<&JsonRawValue> {
        self.result.as_ref()
    }

    /// The error, if this is an error response.
    #[must_use]
    pub fn error_obj(&self) -> Option<&Error> {
        self.error.as_ref()
    }

    /// Deserialize the result into a concrete type.
    ///
    /// Returns `None` if the response has no result or deserialization fails.
    #[must_use]
    pub fn result_as<T: serde::de::DeserializeOwned>(&self) -> Option<T> {
        self.result
            .as_ref()
            .and_then(|raw| crate::json::from_raw_value(raw).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_preserves_wire_type() {
        let n: Id = crate::json::from_slice(b"1").unwrap();
        assert_eq!(n, Id::Number(1));
        let s: Id = crate::json::from_slice(b"\"1\"").unwrap();
        assert_eq!(s, Id::String("1".into()));
        let z: Id = crate::json::from_slice(b"null").unwrap();
        assert_eq!(z, Id::Null);
    }

    #[test]
    fn version_must_be_2_0() {
        assert!(crate::json::from_slice::<Version>(b"\"2.0\"").is_ok());
        assert!(crate::json::from_slice::<Version>(b"\"1.0\"").is_err());
    }

    #[test]
    fn id_serialize_all_variants() {
        assert_eq!(crate::json::to_vec(&Id::Null).unwrap(), b"null");
        assert_eq!(crate::json::to_vec(&Id::Number(42)).unwrap(), b"42");
        assert_eq!(
            crate::json::to_vec(&Id::String("x".into())).unwrap(),
            b"\"x\""
        );
    }

    #[test]
    fn id_deserialize_edge_cases() {
        // u64 that fits in i64
        let id: Id = crate::json::from_slice(b"123").unwrap();
        assert_eq!(id, Id::Number(123));
        // u64 beyond i64 range becomes a string
        let id: Id = crate::json::from_slice(b"9999999999999999999").unwrap();
        assert!(matches!(id, Id::String(..)), "got {id:?}");
        // null vs absent: when the field is present with null, it's Some(Null)
        let req: crate::Request =
            crate::json::from_slice(br#"{"jsonrpc":"2.0","method":"m","id":null}"#).unwrap();
        assert!(!req.is_notification());
    }

    #[test]
    fn error_with_data_and_display() {
        let err = Error::new(-32000, "boom").with_data(42);
        assert_eq!(err.code(), -32000);
        assert_eq!(err.message(), "boom");
        assert!(err.data_raw().is_some());
        assert!(err.to_string().contains("boom"));
    }

    #[test]
    fn response_is_error_and_is_valid() {
        let ok = Response::result(Id::Number(1), 1);
        assert!(ok.is_valid());
        assert!(!ok.is_error());

        let err = Response::error(Id::Null, Error::method_not_found());
        assert!(err.is_valid());
        assert!(err.is_error());

        // Constructors guarantee the invariant — a response with both set can
        // only come from deserialization, which we trust the spec for.
    }

    #[test]
    fn notification_has_no_id_field() {
        let req = Request::notification("update", &());
        let bytes = crate::json::to_vec(&req).unwrap();
        let s = String::from_utf8(bytes).unwrap();
        assert!(
            !s.contains("\"id\""),
            "notification must not serialize an id: {s}"
        );
    }

    #[test]
    fn explicit_null_id_is_not_a_notification() {
        let req: Request =
            crate::json::from_slice(br#"{"jsonrpc":"2.0","method":"m","id":null}"#).unwrap();
        assert!(!req.is_notification());
        assert_eq!(req.id(), Some(&Id::Null));
    }

    #[test]
    fn response_serializes_exactly_one_member() {
        let ok = crate::json::to_vec(&Response::result(Id::Number(1), 19)).unwrap();
        let ok = String::from_utf8(ok).unwrap();
        assert!(ok.contains("\"result\"") && !ok.contains("\"error\""));

        let err =
            crate::json::to_vec(&Response::error(Id::Null, Error::method_not_found())).unwrap();
        let err = String::from_utf8(err).unwrap();
        assert!(err.contains("\"error\"") && !err.contains("\"result\""));
        assert!(err.contains("\"id\":null"));
    }

    #[test]
    fn id_display() {
        assert_eq!(Id::Null.to_string(), "null");
        assert_eq!(Id::Number(42).to_string(), "42");
        assert_eq!(Id::String("abc".into()).to_string(), "abc");
    }

    #[test]
    fn error_partial_eq() {
        assert_eq!(Error::new(-32000, "boom"), Error::new(-32000, "boom"));
        assert_ne!(Error::new(-32000, "boom"), Error::new(-32001, "boom"));
        assert_ne!(Error::new(-32000, "boom"), Error::new(-32000, "bang"));

        // Data participates in equality.
        let a = Error::new(-32000, "x").with_data(42);
        let b = Error::new(-32000, "x").with_data(42);
        let c = Error::new(-32000, "x").with_data(43);
        assert_eq!(a, b);
        assert_ne!(a, c);
        // Present vs absent data differ.
        assert_ne!(a, Error::new(-32000, "x"));
    }

    /// A type whose `Serialize` impl always fails, to exercise the fallible
    /// constructors.
    struct AlwaysFails;
    impl Serialize for AlwaysFails {
        fn serialize<S: Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("nope"))
        }
    }

    #[test]
    fn try_constructors_surface_serialization_errors() {
        assert!(Request::try_call("m", &AlwaysFails, Id::Number(1)).is_err());
        assert!(Request::try_notification("m", &AlwaysFails).is_err());
        assert!(Response::try_result(Id::Number(1), AlwaysFails).is_err());
        assert!(Error::new(-32000, "x").try_with_data(AlwaysFails).is_err());
    }

    #[test]
    fn try_constructors_succeed_on_good_types() {
        let req = Request::try_call("m", &(1, 2), Id::Number(1)).unwrap();
        assert_eq!(req.method(), "m");
        assert_eq!(req.params_as::<(i64, i64)>(), Some((1, 2)));

        let notif = Request::try_notification("m", &[1, 2, 3]).unwrap();
        assert!(notif.is_notification());

        let resp = Response::try_result(Id::Number(1), "ok").unwrap();
        assert_eq!(resp.result_as::<String>(), Some("ok".into()));

        let err = Error::new(-32000, "x").try_with_data(vec![1, 2]).unwrap();
        assert!(err.data_raw().is_some());
    }

    /// The infallible constructors drop params/data on serialization failure
    /// rather than panicking or propagating.
    #[test]
    fn infallible_constructors_drop_on_failure() {
        let req = Request::call("m", &AlwaysFails, Id::Number(1));
        assert!(req.params_as::<()>().is_some() || req.params_as::<i64>().is_none());
        let bytes = crate::json::to_vec(&req).unwrap();
        let s = String::from_utf8(bytes).unwrap();
        assert!(!s.contains("params"), "params should be dropped: {s}");

        let err = Error::new(-32000, "x").with_data(AlwaysFails);
        assert!(err.data_raw().is_none());
    }
}
