//! Pluggable JSON backend.
//!
//! The public API of `jasonrpc` is backend-neutral: user code deals in the
//! [`Value`] type and the helper functions here, never in `serde_json` or
//! `sonic_rs` directly. Exactly one backend is selected at compile time via the
//! `backend-serde-json` (default) or `backend-sonic` feature.
//!
//! All fallible functions return [`JsonError`], leaving error-code assignment
//! to the caller. This keeps the JSON layer free of protocol semantics.

use std::fmt;

#[cfg(feature = "server")]
pub(crate) struct ArrayItems<'a> {
    pub(crate) items: Vec<std::borrow::Cow<'a, [u8]>>,
    pub(crate) total_len: usize,
}

/// A JSON-level error: parsing or serialization failed.
///
/// Wraps the backend's error message. Callers decide what JSON-RPC error code
/// (if any) to attach.
///
/// ```
/// use jasonrpc::json::JsonError;
///
/// let err = JsonError::new("expected ident at line 1 column 2");
/// assert!(!err.message().is_empty());
/// ```
#[derive(Debug, Clone)]
pub struct JsonError(pub(crate) String);

impl JsonError {
    /// Create a `JsonError` from a message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    /// The error message from the JSON backend.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for JsonError {}

#[cfg(all(feature = "backend-serde-json", feature = "backend-sonic"))]
compile_error!(
    "features `backend-serde-json` and `backend-sonic` are mutually exclusive; enable exactly one"
);

#[cfg(not(any(feature = "backend-serde-json", feature = "backend-sonic")))]
compile_error!(
    "a JSON backend must be selected: enable `backend-serde-json` (default) or `backend-sonic`"
);

#[cfg(feature = "backend-serde-json")]
mod imp {
    use super::JsonError;

    pub use serde_json::Value;

    pub fn from_slice<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, JsonError> {
        serde_json::from_slice(bytes).map_err(|e| JsonError(e.to_string()))
    }

    pub fn to_vec<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, JsonError> {
        serde_json::to_vec(v).map_err(|e| JsonError(e.to_string()))
    }

    pub fn null() -> Value {
        Value::Null
    }

    pub fn string(s: &str) -> Value {
        Value::String(s.to_owned())
    }

    /// Split a JSON array into the raw bytes of each element, preserving each
    /// element's exact serialization. Each element borrows from `bytes`.
    #[cfg(feature = "server")]
    pub fn split_array(
        bytes: &[u8],
        max_items: Option<usize>,
    ) -> Result<super::ArrayItems<'_>, JsonError> {
        use serde::de::{SeqAccess, Visitor};
        use serde::Deserializer;

        struct ArrayVisitor {
            max_items: Option<usize>,
        }

        impl<'de> Visitor<'de> for ArrayVisitor {
            type Value = super::ArrayItems<'de>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON array")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut items = Vec::new();
                let mut total_len = 0usize;
                while let Some(raw) = seq.next_element::<&'de serde_json::value::RawValue>()? {
                    total_len = total_len.saturating_add(1);
                    if self.max_items.is_none_or(|max| items.len() < max) {
                        items.push(std::borrow::Cow::Borrowed(raw.get().as_bytes()));
                    }
                }
                Ok(super::ArrayItems { items, total_len })
            }
        }

        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        let items = deserializer
            .deserialize_seq(ArrayVisitor { max_items })
            .map_err(|e| JsonError(e.to_string()))?;
        deserializer.end().map_err(|e| JsonError(e.to_string()))?;
        Ok(items)
    }

    pub fn to_raw_value<T: serde::Serialize>(v: &T) -> Result<super::JsonRawValue, JsonError> {
        let json = serde_json::to_string(v).map_err(|e| JsonError(e.to_string()))?;
        serde_json::value::RawValue::from_string(json).map_err(|e| JsonError(e.to_string()))
    }

    /// Deserialize a `JsonRawValue` into a typed structure. Zero-copy:
    /// `RawValue` holds the JSON text as a `&str` pointing into the original
    /// input buffer.
    pub fn from_raw_value<T: serde::de::DeserializeOwned>(
        raw: &super::JsonRawValue,
    ) -> Result<T, JsonError> {
        serde_json::from_str(raw.get()).map_err(|e| JsonError(e.to_string()))
    }

    pub fn raw_value_kind(raw: &super::JsonRawValue) -> super::RawValueKind {
        super::classify_raw_value(raw.get().as_bytes())
    }
}

#[cfg(feature = "backend-sonic")]
mod imp {
    use super::JsonError;

    pub use sonic_rs::Value;

    pub fn from_slice<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, JsonError> {
        sonic_rs::from_slice(bytes).map_err(|e| JsonError(e.to_string()))
    }

    pub fn to_vec<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, JsonError> {
        sonic_rs::to_vec(v).map_err(|e| JsonError(e.to_string()))
    }

    pub fn null() -> Value {
        // `sonic_rs::Value`'s default is a JSON null.
        Value::default()
    }

    pub fn string(s: &str) -> Value {
        // sonic's `Value` implements `From<&str>`.
        Value::from(s)
    }

    /// Split a JSON array into the raw bytes of each element, preserving each
    /// element's exact serialization. Each element borrows from `bytes` when
    /// the element's raw text is unescaped (the common case), otherwise it is
    /// owned.
    #[cfg(feature = "server")]
    pub fn split_array(
        bytes: &[u8],
        max_items: Option<usize>,
    ) -> Result<super::ArrayItems<'_>, JsonError> {
        use std::borrow::Cow;
        // `to_array_iter` lazily yields each element; `as_raw_cow` returns a
        // slice borrowing from `bytes` (the `'de` lifetime) for raw input,
        // owning only if the element required unescaping.
        let mut out = Vec::new();
        let mut total_len = 0usize;
        for item in sonic_rs::to_array_iter(bytes) {
            let raw = item.map_err(|e| JsonError(e.to_string()))?;
            total_len = total_len.saturating_add(1);
            if max_items.is_none_or(|max| out.len() < max) {
                out.push(match raw.as_raw_cow() {
                    Cow::Borrowed(s) => Cow::Borrowed(s.as_bytes()),
                    Cow::Owned(s) => Cow::Owned(s.into_bytes()),
                });
            }
        }
        Ok(super::ArrayItems {
            items: out,
            total_len,
        })
    }

    pub fn to_raw_value<T: serde::Serialize>(v: &T) -> Result<super::JsonRawValue, JsonError> {
        let json = sonic_rs::to_vec(v).map_err(|e| JsonError(e.to_string()))?;
        sonic_rs::from_slice::<super::JsonRawValue>(&json).map_err(|e| JsonError(e.to_string()))
    }

    /// Deserialize a `JsonRawValue` into a typed structure.
    ///
    /// TODO: when <https://github.com/cloudwego/sonic-rs/pull/230> is merged
    /// and released, replace this with a zero-copy
    /// `sonic_rs::from_str(raw.as_raw_str().unwrap_or("null"))`.
    pub fn from_raw_value<T: serde::de::DeserializeOwned>(
        raw: &super::JsonRawValue,
    ) -> Result<T, JsonError> {
        let bytes = sonic_rs::to_vec(raw).map_err(|e| JsonError(e.to_string()))?;
        sonic_rs::from_slice(&bytes).map_err(|e| JsonError(e.to_string()))
    }

    pub fn raw_value_kind(raw: &super::JsonRawValue) -> super::RawValueKind {
        use sonic_rs::{JsonType, JsonValueTrait};

        match raw.get_type() {
            JsonType::Null => super::RawValueKind::Null,
            JsonType::Array | JsonType::Object => super::RawValueKind::Structured,
            _ => super::RawValueKind::Scalar,
        }
    }
}

/// The backend-neutral JSON value type.
pub use imp::Value;

/// The raw JSON value type used by [`protocol`](crate::protocol) types
/// for lazy deserialization.
///
/// Depending on the selected backend, this is either
/// `Box<serde_json::value::RawValue>` or `sonic_rs::OwnedLazyValue`.
#[cfg(feature = "backend-serde-json")]
pub type JsonRawValue = Box<serde_json::value::RawValue>;
/// The raw JSON value type used by [`protocol`](crate::protocol) types
/// for lazy deserialization.
///
/// Depending on the selected backend, this is either
/// `Box<serde_json::value::RawValue>` or `sonic_rs::OwnedLazyValue`.
#[cfg(feature = "backend-sonic")]
pub type JsonRawValue = sonic_rs::OwnedLazyValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RawValueKind {
    Null,
    Structured,
    Scalar,
}

#[cfg(feature = "backend-serde-json")]
fn classify_raw_value(bytes: &[u8]) -> RawValueKind {
    match bytes.iter().find(|byte| !byte.is_ascii_whitespace()) {
        Some(b'n') => RawValueKind::Null,
        Some(b'[' | b'{') => RawValueKind::Structured,
        _ => RawValueKind::Scalar,
    }
}

pub(crate) fn raw_value_kind(raw: &JsonRawValue) -> RawValueKind {
    imp::raw_value_kind(raw)
}

/// Serialize a value into a `JsonRawValue`.
pub(crate) fn to_raw_value<T: serde::Serialize>(v: &T) -> Result<JsonRawValue, JsonError> {
    imp::to_raw_value(v)
}

/// Deserialize a `JsonRawValue` into a typed structure.
pub(crate) fn from_raw_value<T: serde::de::DeserializeOwned>(
    raw: &JsonRawValue,
) -> Result<T, JsonError> {
    imp::from_raw_value(raw)
}

/// Parse raw bytes into a value or typed structure.
///
/// # Errors
///
/// Returns a [`JsonError`] if parsing fails.
///
/// ```
/// # fn main() -> Result<(), jasonrpc::json::JsonError> {
/// use jasonrpc::json;
///
/// let v: i64 = json::from_slice(b"42")?;
/// assert_eq!(v, 42);
///
/// let err = json::from_slice::<i64>(b"not json").unwrap_err();
/// assert!(!err.message().is_empty());
/// # Ok(())
/// # }
/// ```
pub fn from_slice<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, JsonError> {
    imp::from_slice(bytes)
}

/// Serialize a value to raw bytes.
///
/// # Errors
///
/// Returns a [`JsonError`] if serialization fails.
///
/// ```
/// # fn main() -> Result<(), jasonrpc::json::JsonError> {
/// use jasonrpc::json;
///
/// let bytes = json::to_vec(&42_i64)?;
/// assert_eq!(bytes, b"42");
/// # Ok(())
/// # }
/// ```
pub fn to_vec<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, JsonError> {
    imp::to_vec(v)
}

/// Construct a JSON `null` [`Value`].
#[must_use]
pub fn null() -> Value {
    imp::null()
}

/// Construct a JSON string [`Value`] from a string slice.
#[allow(dead_code)]
#[must_use]
pub(crate) fn string(s: &str) -> Value {
    imp::string(s)
}

/// Split a JSON array into the raw bytes of each element.
///
/// Each returned element is the exact serialization of one array entry,
/// borrowing from `bytes` where the backend allows, so a batch entry can be
/// re-parsed from its own bytes without a copy in the common case.
#[cfg(feature = "server")]
pub(crate) fn split_array(
    bytes: &[u8],
    max_items: Option<usize>,
) -> Result<ArrayItems<'_>, JsonError> {
    imp::split_array(bytes, max_items)
}

#[cfg(all(test, feature = "server"))]
mod tests {
    use super::*;

    #[test]
    fn split_array_retains_only_configured_items() {
        let split = split_array(b"[1,2,3]", Some(2)).unwrap();
        assert_eq!(split.total_len, 3);
        assert_eq!(split.items.len(), 2);
        assert_eq!(split.items[0], b"1".as_slice());
        assert_eq!(split.items[1], b"2".as_slice());
    }

    #[test]
    fn split_array_validates_elements_after_limit() {
        assert!(split_array(b"[1,2,invalid]", Some(1)).is_err());
    }
}
