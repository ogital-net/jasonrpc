//! Core JSON-RPC 2.0 data model and (de)serialization.
//!
//! This module is the "core" layer: pure data types with no I/O and no async.
//! The types here conform to the JSON-RPC 2.0 specification. Higher layers
//! (`server`, `client`, `transport`, integrations) build on top of these types.

use std::fmt;
use std::hash::{Hash, Hasher};

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
/// distinct and never coerced into one another. Integer and finite fractional
/// numbers are accepted; fractional ids are discouraged by the JSON-RPC spec.
/// `Null` is permitted but also discouraged.
///
/// `Hash` and equality allow equivalent non-negative signed and unsigned ids
/// to key the same correlation-map entry.
#[derive(Clone, Debug)]
pub enum Id {
    /// A null id. Discouraged by the spec, but accepted.
    Null,
    /// A numeric id.
    Number(i64),
    /// A non-negative numeric id larger than [`i64::MAX`].
    Unsigned(u64),
    /// A finite fractional numeric id.
    Float(f64),
    /// A string id.
    String(String),
}

impl PartialEq for Id {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Id::Null, Id::Null) => true,
            (Id::Number(a), Id::Number(b)) => a == b,
            (Id::Unsigned(a), Id::Unsigned(b)) => a == b,
            (Id::Number(a), Id::Unsigned(b)) | (Id::Unsigned(b), Id::Number(a)) => {
                u64::try_from(*a).is_ok_and(|a| a == *b)
            }
            (Id::Float(a), Id::Float(b)) => a.to_bits() == b.to_bits(),
            (Id::String(a), Id::String(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Id {}

impl Hash for Id {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Id::Null => 0_u8.hash(state),
            Id::Number(value) if *value < 0 => {
                1_u8.hash(state);
                value.hash(state);
            }
            Id::Number(value) => {
                2_u8.hash(state);
                (*value as u64).hash(state);
            }
            Id::Unsigned(value) => {
                2_u8.hash(state);
                value.hash(state);
            }
            Id::Float(value) => {
                3_u8.hash(state);
                value.to_bits().hash(state);
            }
            Id::String(value) => {
                4_u8.hash(state);
                value.hash(state);
            }
        }
    }
}

impl Serialize for Id {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Id::Null => s.serialize_none(),
            Id::Number(n) => s.serialize_i64(*n),
            Id::Unsigned(n) => s.serialize_u64(*n),
            Id::Float(n) => s.serialize_f64(*n),
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
            Id::Unsigned(n) => write!(f, "{n}"),
            Id::Float(n) => write!(f, "{n}"),
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
                f.write_str("a string, finite number, or null")
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Id, E> {
                Ok(Id::Number(v))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Id, E> {
                Ok(i64::try_from(v).map_or(Id::Unsigned(v), Id::Number))
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Id, E> {
                if v.is_finite() {
                    Ok(Id::Float(v))
                } else {
                    Err(E::custom("id must be a finite JSON number"))
                }
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

fn de_result<'de, D: Deserializer<'de>>(d: D) -> Result<Option<JsonRawValue>, D::Error> {
    JsonRawValue::deserialize(d).map(Some)
}

fn de_error<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Error>, D::Error> {
    Error::deserialize(d).map(Some)
}

fn de_params<'de, D: Deserializer<'de>>(d: D) -> Result<Option<JsonRawValue>, D::Error> {
    let raw = JsonRawValue::deserialize(d)?;
    match crate::json::raw_value_kind(&raw) {
        crate::json::RawValueKind::Structured => Ok(Some(raw)),
        crate::json::RawValueKind::Null | crate::json::RawValueKind::Scalar => {
            Err(de::Error::custom("params must be an array or object"))
        }
    }
}

fn normalize_params<P: Serialize>(params: &P) -> Result<Option<JsonRawValue>, JsonError> {
    let raw = crate::json::to_raw_value(params)?;
    match crate::json::raw_value_kind(&raw) {
        crate::json::RawValueKind::Null => Ok(None),
        crate::json::RawValueKind::Structured => Ok(Some(raw)),
        crate::json::RawValueKind::Scalar => crate::json::to_raw_value(&(params,)).map(Some),
    }
}

#[derive(Clone, Debug)]
enum RequestParams {
    Owned(JsonRawValue),
    #[cfg(feature = "server")]
    Shared(bytes::Bytes),
}

impl Serialize for RequestParams {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Owned(raw) => raw.serialize(serializer),
            #[cfg(feature = "server")]
            Self::Shared(bytes) => {
                let text = std::str::from_utf8(bytes).map_err(serde::ser::Error::custom)?;
                let raw: crate::json::BorrowedRawValue<'_> =
                    crate::json::from_str_borrowed(text).map_err(serde::ser::Error::custom)?;
                raw.serialize(serializer)
            }
        }
    }
}

#[cfg(feature = "server")]
fn de_shared_params<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<crate::json::BorrowedRawValue<'de>>, D::Error> {
    let raw = crate::json::BorrowedRawValue::deserialize(deserializer)?;
    let text = crate::json::borrowed_raw_text(&raw);
    if crate::json::classify_raw_value(text.as_bytes()) != crate::json::RawValueKind::Structured {
        return Err(de::Error::custom("params must be an array or object"));
    }
    Ok(Some(raw))
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
/// Deserialization requires an object; arrays are not request representations.
///
/// ```
/// use jasonrpc::{Request, Id};
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
#[derive(Clone, Debug, Serialize)]
pub struct Request {
    jsonrpc: Version,
    method: String,
    /// Parameters as raw JSON. Use [`params_as`](Self::params_as) to
    /// deserialize into a concrete type.
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<RequestParams>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Id>,
    #[cfg(feature = "backend-sonic")]
    #[serde(skip)]
    params_text: std::sync::OnceLock<std::sync::Arc<String>>,
}

impl<'de> Deserialize<'de> for Request {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            jsonrpc: Version,
            method: String,
            #[serde(default, deserialize_with = "de_params")]
            params: Option<JsonRawValue>,
            #[serde(default, deserialize_with = "de_present_id")]
            id: Option<Id>,
        }

        struct RequestVisitor;

        impl<'de> Visitor<'de> for RequestVisitor {
            type Value = Request;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON-RPC request object")
            }

            fn visit_map<Map: de::MapAccess<'de>>(self, map: Map) -> Result<Request, Map::Error> {
                let fields = Fields::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(Request {
                    jsonrpc: fields.jsonrpc,
                    method: fields.method,
                    params: fields.params.map(RequestParams::Owned),
                    id: fields.id,
                    #[cfg(feature = "backend-sonic")]
                    params_text: std::sync::OnceLock::new(),
                })
            }
        }

        deserializer.deserialize_map(RequestVisitor)
    }
}

impl Request {
    #[cfg(feature = "server")]
    pub(crate) fn from_shared_bytes(bytes: bytes::Bytes) -> Result<Self, JsonError> {
        #[derive(Deserialize)]
        struct Fields<'a> {
            jsonrpc: Version,
            method: String,
            #[serde(default, borrow, deserialize_with = "de_shared_params")]
            params: Option<crate::json::BorrowedRawValue<'a>>,
            #[serde(default, deserialize_with = "de_present_id")]
            id: Option<Id>,
        }

        let text = std::str::from_utf8(&bytes).map_err(<JsonError as de::Error>::custom)?;
        if !text.trim_start().starts_with('{') {
            return Err(<JsonError as de::Error>::custom(
                "expected a JSON-RPC request object",
            ));
        }
        let fields: Fields<'_> = crate::json::from_str_borrowed(text)?;
        let params = fields.params.as_ref().map(|raw| {
            let text = crate::json::borrowed_raw_text(raw);
            RequestParams::Shared(match text {
                std::borrow::Cow::Borrowed(text) => bytes.slice_ref(text.as_bytes()),
                std::borrow::Cow::Owned(text) => bytes::Bytes::from(text),
            })
        });
        Ok(Self {
            jsonrpc: fields.jsonrpc,
            method: fields.method,
            params,
            id: fields.id,
            #[cfg(feature = "backend-sonic")]
            params_text: std::sync::OnceLock::new(),
        })
    }

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
            params: normalize_params(params)
                .ok()
                .flatten()
                .map(RequestParams::Owned),
            id: Some(id),
            #[cfg(feature = "backend-sonic")]
            params_text: std::sync::OnceLock::new(),
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
            params: normalize_params(params)?.map(RequestParams::Owned),
            id: Some(id),
            #[cfg(feature = "backend-sonic")]
            params_text: std::sync::OnceLock::new(),
        })
    }

    /// Build a notification (a request with no id, expecting no response).
    ///
    /// See [`call`](Self::call) for notes on params serialization.
    pub fn notification<P: Serialize>(method: impl Into<String>, params: &P) -> Self {
        Self {
            jsonrpc: Version,
            method: method.into(),
            params: normalize_params(params)
                .ok()
                .flatten()
                .map(RequestParams::Owned),
            id: None,
            #[cfg(feature = "backend-sonic")]
            params_text: std::sync::OnceLock::new(),
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
            params: normalize_params(params)?.map(RequestParams::Owned),
            id: None,
            #[cfg(feature = "backend-sonic")]
            params_text: std::sync::OnceLock::new(),
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
    /// like `Option<T>`.
    ///
    /// # Errors
    ///
    /// Returns the JSON backend's native [`JsonError`] if deserialization
    /// fails, preserving its message and location information. Locations are
    /// relative to the params JSON, not the full request body.
    pub fn params_as<T: serde::de::DeserializeOwned>(&self) -> Result<T, JsonError> {
        match &self.params {
            Some(RequestParams::Owned(raw)) => crate::json::from_raw_value(raw),
            #[cfg(feature = "server")]
            Some(RequestParams::Shared(bytes)) => {
                let text = std::str::from_utf8(bytes).map_err(<JsonError as de::Error>::custom)?;
                crate::json::from_str_borrowed(text)
            }
            None => crate::json::from_slice::<T>(b"null"),
        }
    }

    /// Deserialize params while allowing the result to borrow from this request.
    ///
    /// Unescaped strings can be borrowed with `&str` or `#[serde(borrow)]`
    /// `Cow<str>`. Escaped strings require an owned value; `Cow<str>` provides
    /// that fallback. Missing params are deserialized as JSON `null`.
    ///
    /// Requests dispatched from shared `Bytes` borrow directly on both backends.
    /// Other requests borrow stored raw JSON with serde-json; Sonic caches a
    /// serialized copy on the first call. Keep the request alive while using
    /// borrowed values, including across an await.
    ///
    /// # Errors
    ///
    /// Returns the backend's native JSON error if params cannot be decoded.
    /// Error locations are relative to the params, not the request envelope.
    ///
    /// ```
    /// # fn main() -> Result<(), jasonrpc::json::JsonError> {
    /// use jasonrpc::{Id, Request};
    /// use std::borrow::Cow;
    ///
    /// #[derive(serde::Deserialize)]
    /// struct Attachment<'a> {
    ///     #[serde(borrow)]
    ///     data: Cow<'a, str>,
    /// }
    ///
    /// #[derive(serde::Serialize)]
    /// struct Payload { data: &'static str }
    ///
    /// let request = Request::call("ingest", &Payload { data: "YWJj" }, Id::Number(1));
    /// let params: Attachment<'_> = request.params_as_borrowed()?;
    /// assert_eq!(params.data, "YWJj");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Borrowed values cannot outlive the request:
    ///
    /// ```compile_fail
    /// use jasonrpc::{Id, Request};
    /// let data;
    /// {
    ///     let request = Request::call("ingest", &("YWJj",), Id::Number(1));
    ///     let (value,): (&str,) = request.params_as_borrowed().unwrap();
    ///     data = value;
    /// }
    /// println!("{data}");
    /// ```
    pub fn params_as_borrowed<'a, T: serde::Deserialize<'a>>(&'a self) -> Result<T, JsonError> {
        #[cfg(feature = "server")]
        if let Some(RequestParams::Shared(bytes)) = &self.params {
            let text = std::str::from_utf8(bytes).map_err(<JsonError as de::Error>::custom)?;
            return crate::json::from_str_borrowed(text);
        }
        let Some(RequestParams::Owned(raw)) = &self.params else {
            return crate::json::from_str_borrowed("null");
        };
        #[cfg(feature = "backend-serde-json")]
        let text = raw.get();
        #[cfg(feature = "backend-sonic")]
        let text = {
            if self.params_text.get().is_none() {
                let bytes = crate::json::to_vec(raw)?;
                let text =
                    String::from_utf8(bytes).map_err(<JsonError as serde::de::Error>::custom)?;
                let _ = self.params_text.set(std::sync::Arc::new(text));
            }
            self.params_text
                .get()
                .expect("params text initialized")
                .as_str()
        };
        crate::json::from_str_borrowed(text)
    }

    /// Deserialize a single positional parameter.
    ///
    /// Convenience for the common single-argument method: it accepts a
    /// one-element positional array (`[value]`) and yields the inner `T`. This
    /// saves defining a newtype or matching a one-element tuple for methods
    /// like `device.get(id)`.
    ///
    /// # Errors
    ///
    /// Returns the JSON backend's native [`JsonError`] if there are no params,
    /// if the array does not hold exactly one element, or if the element fails
    /// to deserialize into `T`. Locations are relative to the params JSON,
    /// not the full request body.
    ///
    /// ```
    /// # fn main() -> Result<(), jasonrpc::json::JsonError> {
    /// use jasonrpc::{Request, Id};
    ///
    /// // `{"params": [7]}` — the usual positional form.
    /// let req: Request =
    ///     jasonrpc::json::from_slice(br#"{"jsonrpc":"2.0","method":"get","params":[7],"id":1}"#)?;
    /// assert_eq!(req.param_as::<i64>()?, 7);
    /// # Ok(())
    /// # }
    /// ```
    pub fn param_as<T: serde::de::DeserializeOwned>(&self) -> Result<T, JsonError> {
        self.params_as::<(T,)>().map(|(value,)| value)
    }
}

/// A JSON-RPC 2.0 response object.
///
/// Exactly one of `result` / `error` is populated. Use [`Response::result`] and
/// [`Response::error`] to construct valid responses; the constructors guarantee
/// the invariant. An explicit `error: null` is rejected during deserialization
/// rather than treated as an absent field.
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
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "de_result"
    )]
    result: Option<JsonRawValue>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "de_error"
    )]
    error: Option<Error>,
    id: Id,
}

impl Response {
    /// A successful response. The `value` is serialized to JSON at
    /// construction time.
    ///
    /// If serialization fails the response is constructed with a `null` result.
    /// Use [`try_result`](Self::try_result) to receive the serialization error
    /// instead of a silent `null`.
    #[must_use]
    pub fn result(id: Id, value: impl Serialize) -> Self {
        let result = crate::json::to_raw_value(&value)
            .unwrap_or_else(|_| crate::json::to_raw_value(&()).expect("null is valid JSON"));
        Self {
            jsonrpc: Version,
            result: Some(result),
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
    fn request_deserialization_requires_an_object() {
        for input in [
            br#"["2.0","tracked",[],1]"#.as_slice(),
            br#"["2.0","tracked",[]]"#.as_slice(),
            b"[]".as_slice(),
            b"null".as_slice(),
            b"true".as_slice(),
            b"7".as_slice(),
            br#""tracked""#.as_slice(),
        ] {
            assert!(
                crate::json::from_slice::<Request>(input).is_err(),
                "accepted non-object request: {}",
                String::from_utf8_lossy(input)
            );
        }
    }

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
        // u64 beyond i64 range remains a JSON number
        let id: Id = crate::json::from_slice(b"9999999999999999999").unwrap();
        assert_eq!(id, Id::Unsigned(9_999_999_999_999_999_999));
        assert_eq!(crate::json::to_vec(&id).unwrap(), b"9999999999999999999");
        let id: Id = crate::json::from_slice(b"1.5").unwrap();
        assert_eq!(id, Id::Float(1.5));
        assert_eq!(crate::json::to_vec(&id).unwrap(), b"1.5");
        assert_eq!(Id::Unsigned(1), Id::Number(1));
        let mut ids = std::collections::HashSet::new();
        ids.insert(Id::Unsigned(1));
        assert!(ids.contains(&Id::Number(1)));
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
    fn response_null_result_round_trips() {
        let bytes = br#"{"jsonrpc":"2.0","result":null,"id":1}"#;
        let response: Response = crate::json::from_slice(bytes).unwrap();
        assert!(response.is_valid());
        assert!(!response.is_error());
        assert!(response.result_raw().is_some());
        assert_eq!(response.result_as::<()>(), Some(()));
        assert_eq!(response.result_as::<Option<String>>(), Some(None));
        assert_eq!(crate::json::to_vec(&response).unwrap(), bytes);
    }

    #[test]
    fn response_rejects_explicit_null_error() {
        for input in [
            br#"{"jsonrpc":"2.0","result":42,"error":null,"id":1}"#.as_slice(),
            br#"{"jsonrpc":"2.0","error":null,"result":42,"id":1}"#.as_slice(),
            br#"{"jsonrpc":"2.0","result":null,"error":null,"id":1}"#.as_slice(),
            br#"{"jsonrpc":"2.0","error":null,"id":1}"#.as_slice(),
        ] {
            assert!(
                crate::json::from_slice::<Response>(input).is_err(),
                "accepted null error: {}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn response_missing_result_stays_absent() {
        let bytes = br#"{"jsonrpc":"2.0","id":1}"#;
        let response: Response = crate::json::from_slice(bytes).unwrap();
        assert!(!response.is_valid());
        assert!(response.result_raw().is_none());
        assert_eq!(crate::json::to_vec(&response).unwrap(), bytes);
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
    fn response_result_falls_back_to_null_on_serialization_failure() {
        #[derive(Serialize)]
        struct Payload {
            value: AlwaysFails,
        }

        for response in [
            Response::result(Id::Number(1), AlwaysFails),
            Response::result(Id::Number(1), Payload { value: AlwaysFails }),
        ] {
            assert!(response.is_valid());
            assert!(!response.is_error());
            assert_eq!(response.id(), &Id::Number(1));
            assert_eq!(response.result_as::<()>(), Some(()));
            assert_eq!(response.result_as::<Option<String>>(), Some(None));

            let bytes = crate::json::to_vec(&response).unwrap();
            assert_eq!(bytes, br#"{"jsonrpc":"2.0","result":null,"id":1}"#);
            let parsed: Response = crate::json::from_slice(&bytes).unwrap();
            assert!(parsed.is_valid());
            assert_eq!(parsed.result_as::<()>(), Some(()));
        }
    }

    #[test]
    fn try_constructors_succeed_on_good_types() {
        let req = Request::try_call("m", &(1, 2), Id::Number(1)).unwrap();
        assert_eq!(req.method(), "m");
        assert_eq!(req.params_as::<(i64, i64)>().unwrap(), (1, 2));

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
        assert!(req.params_as::<()>().is_ok());
        assert!(req.params_as::<i64>().is_err());
        let bytes = crate::json::to_vec(&req).unwrap();
        let s = String::from_utf8(bytes).unwrap();
        assert!(!s.contains("params"), "params should be dropped: {s}");

        let err = Error::new(-32000, "x").with_data(AlwaysFails);
        assert!(err.data_raw().is_none());
    }

    #[test]
    fn param_as_single_positional() {
        // `[value]` — the spec-conformant positional form.
        let req: Request =
            crate::json::from_slice(br#"{"jsonrpc":"2.0","method":"m","params":[7],"id":1}"#)
                .unwrap();
        assert_eq!(req.param_as::<i64>().unwrap(), 7);

        // A single-element string array.
        let req: Request =
            crate::json::from_slice(br#"{"jsonrpc":"2.0","method":"m","params":["hi"],"id":1}"#)
                .unwrap();
        assert_eq!(req.param_as::<String>().unwrap(), "hi");
    }

    #[test]
    fn request_params_are_normalized_and_validated() {
        let scalar = Request::call("m", &7, Id::Number(1));
        assert_eq!(scalar.param_as::<i64>().unwrap(), 7);
        let bytes = crate::json::to_vec(&scalar).unwrap();
        assert!(String::from_utf8(bytes).unwrap().contains("\"params\":[7]"));

        let no_params = Request::call("m", &(), Id::Number(1));
        let bytes = crate::json::to_vec(&no_params).unwrap();
        assert!(!String::from_utf8(bytes).unwrap().contains("params"));

        assert!(crate::json::from_slice::<Request>(
            br#"{"jsonrpc":"2.0","method":"m","params":7,"id":1}"#
        )
        .is_err());
        assert!(crate::json::from_slice::<Request>(
            br#"{"jsonrpc":"2.0","method":"m","params":null,"id":1}"#
        )
        .is_err());
    }

    #[test]
    fn param_as_rejects_wrong_arity_and_missing() {
        // Two elements: not a single param.
        let req: Request =
            crate::json::from_slice(br#"{"jsonrpc":"2.0","method":"m","params":[1,2],"id":1}"#)
                .unwrap();
        assert!(req.param_as::<i64>().is_err());

        // Empty array.
        let req: Request =
            crate::json::from_slice(br#"{"jsonrpc":"2.0","method":"m","params":[],"id":1}"#)
                .unwrap();
        assert!(req.param_as::<i64>().is_err());

        // No params at all.
        let req: Request =
            crate::json::from_slice(br#"{"jsonrpc":"2.0","method":"m","id":1}"#).unwrap();
        assert!(req.param_as::<i64>().is_err());

        // Wrong element type.
        let req: Request =
            crate::json::from_slice(br#"{"jsonrpc":"2.0","method":"m","params":["x"],"id":1}"#)
                .unwrap();
        assert!(req.param_as::<i64>().is_err());
    }

    #[test]
    #[cfg(feature = "server")]
    fn shared_params_preserve_raw_json_and_validate_envelopes() {
        let body = bytes::Bytes::from_static(
            br#"{"jsonrpc":"2.0","method":"ingest","params":{ "n": 123456789012345678901234567890, "s": "\u0061", "f": 1.2300 },"id":null}"#,
        );
        let request = Request::from_shared_bytes(body).unwrap();
        let bytes = crate::json::to_vec(&request).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(
            text.contains(r#"{ "n": 123456789012345678901234567890, "s": "\u0061", "f": 1.2300 }"#)
        );
        assert!(!request.is_notification());
        for input in [
            br#"["2.0","ingest",{},1]"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"ingest","params":null}"#,
            br#"{"jsonrpc":"2.0","method":"ingest","params":3}"#,
            br#"{"jsonrpc":"2.0","method":"ingest","method":"other"}"#,
            br#"{"jsonrpc":"2.0","method":"ingest","params":{},"params":{}}"#,
            br#"{"jsonrpc":"2.0","method":"ingest","params":{"data":"\q"}}"#,
            b"{\"jsonrpc\":\"2.0\",\"method\":\"ingest\",\"extra\":\"\xff\"}",
            b"{\"jsonrpc\":\"2.0\",\"method\":\"ingest\",\"params\":{\"data\":\"raw\nnewline\"}}",
            br#"{"jsonrpc":"2.0","method":"ingest"} true"#,
        ] {
            assert!(
                Request::from_shared_bytes(bytes::Bytes::copy_from_slice(input)).is_err(),
                "accepted {input:?}"
            );
        }
    }

    #[test]
    #[cfg(feature = "server")]
    fn shared_params_borrow_from_the_original_body() {
        #[derive(Deserialize)]
        struct Params<'a> {
            data: &'a str,
        }
        let body = bytes::Bytes::from_static(
            br#"{"jsonrpc":"2.0","method":"ingest","params":{"data":"YWJj"},"id":1}"#,
        );
        let offset = body.windows(4).position(|bytes| bytes == b"YWJj").unwrap();
        let request = Request::from_shared_bytes(body.clone()).unwrap();
        let params: Params<'_> = request.params_as_borrowed().unwrap();
        assert_eq!(params.data.as_ptr(), body[offset..].as_ptr());
        let clone = request.clone();
        let cloned: Params<'_> = clone.params_as_borrowed().unwrap();
        assert_eq!(cloned.data.as_ptr(), params.data.as_ptr());
        assert_eq!(crate::json::to_vec(&request).unwrap(), body.as_ref());
        drop(body);
        assert_eq!(params.data, "YWJj");
    }

    #[test]
    fn params_as_borrowed_borrows_and_preserves_requests() {
        #[derive(Deserialize)]
        struct Params<'a> {
            #[serde(borrow)]
            data: std::borrow::Cow<'a, str>,
        }

        let request: Request = crate::json::from_slice(
            br#"{"jsonrpc":"2.0","method":"ingest","params":{"data":"YWJj"},"id":1}"#,
        )
        .unwrap();
        let before = crate::json::to_vec(&request).unwrap();
        let first: Params<'_> = request.params_as_borrowed().unwrap();
        let second: Params<'_> = request.params_as_borrowed().unwrap();
        assert!(matches!(first.data, std::borrow::Cow::Borrowed("YWJj")));
        assert_eq!(first.data.as_ptr(), second.data.as_ptr());
        assert_eq!(crate::json::to_vec(&request).unwrap(), before);
    }

    #[test]
    fn params_as_borrowed_handles_escapes_missing_and_errors() {
        #[derive(Debug, Deserialize)]
        struct Params<'a> {
            #[serde(borrow)]
            data: std::borrow::Cow<'a, str>,
        }

        let request: Request = crate::json::from_slice(
            br#"{"jsonrpc":"2.0","method":"ingest","params":{"data":"YW\u004aj"},"id":1}"#,
        )
        .unwrap();
        let params: Params<'_> = request.params_as_borrowed().unwrap();
        assert!(matches!(params.data, std::borrow::Cow::Owned(_)));
        assert_eq!(params.data, "YWJj");

        let request = Request::call("ingest", &(), Id::Number(1));
        assert_eq!(request.params_as_borrowed::<Option<&str>>().unwrap(), None);
        assert!(request.params_as_borrowed::<Params<'_>>().is_err());
        let request: Request = crate::json::from_slice(
            br#"{"jsonrpc":"2.0","method":"ingest","params":{"data":123},"id":1}"#,
        )
        .unwrap();
        let error = request.params_as_borrowed::<Params<'_>>().unwrap_err();
        assert!(error.to_string().contains("expected a string"));
        assert_eq!(error.line(), 1);
        assert!(error.column() > 0);
    }

    #[test]
    fn params_as_preserves_missing_params_behavior() {
        let req = Request::call("m", &(), Id::Number(1));
        assert_eq!(req.params_as::<Option<String>>().unwrap(), None);
        assert!(req.params_as::<()>().is_ok());
        assert!(req.params_as::<String>().is_err());
        assert!(req.param_as::<Option<String>>().is_err());

        let req = Request::call("m", &(None::<String>,), Id::Number(1));
        assert_eq!(req.param_as::<Option<String>>().unwrap(), None);
    }

    #[test]
    fn params_as_preserves_native_error_details() {
        #[derive(Debug, Deserialize)]
        struct Device {
            external_id: String,
        }

        #[derive(Debug, Deserialize)]
        struct Params {
            devices: Vec<Device>,
        }

        let req: Request = crate::json::from_slice(
            br#"{"jsonrpc":"2.0","method":"m","params":{"devices":[{"external_id":"device-1"}]},"id":1}"#,
        )
        .unwrap();
        assert_eq!(
            req.params_as::<Params>().unwrap().devices[0].external_id,
            "device-1"
        );

        for params in [
            r#"{"devices":[{"external_id":123}]}"#,
            r#"[{"devices":[{"external_id":123}]}]"#,
        ] {
            let body = format!(r#"{{"jsonrpc":"2.0","method":"m","params":{params},"id":1}}"#);
            let req: Request = crate::json::from_slice(body.as_bytes()).unwrap();
            let error: JsonError = if params.starts_with('[') {
                req.param_as::<Params>().unwrap_err()
            } else {
                req.params_as::<Params>().unwrap_err()
            };
            let message = error.to_string();
            assert!(message.contains("123"), "{message}");
            assert!(message.contains("expected a string"), "{message}");
            assert_eq!(error.line(), 1);
            assert!(error.column() > 0);
            assert!(!error.is_syntax());
        }
    }
}
