//! Pluggable JSON backend.
//!
//! The public API of `jasonrpc` is backend-neutral: user code deals in the
//! [`Value`] type and the helper functions here, never in `serde_json` or
//! `sonic_rs` directly. Exactly one backend is selected at compile time via the
//! `backend-serde-json` (default) or `backend-sonic` feature.
//!
//! All fallible functions return [`JsonError`], leaving error-code assignment
//! to the caller. This keeps the JSON layer free of protocol semantics.
//! [`JsonError`] is the selected backend's native error type; backend-specific
//! error APIs may differ.

#[cfg(feature = "server")]
pub(crate) struct ArrayItems<'a> {
    pub(crate) items: Vec<std::borrow::Cow<'a, [u8]>>,
    pub(crate) total_len: usize,
}

/// The selected JSON backend's native parsing and serialization error.
#[cfg(feature = "backend-serde-json")]
pub use serde_json::Error as JsonError;
/// The selected JSON backend's native parsing and serialization error.
#[cfg(feature = "backend-sonic")]
pub use sonic_rs::Error as JsonError;

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
        serde_json::from_slice(bytes)
    }

    pub fn to_vec<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, JsonError> {
        serde_json::to_vec(v)
    }

    #[cfg(feature = "client")]
    pub fn to_writer<T: serde::Serialize>(writer: &mut Vec<u8>, v: &T) -> Result<(), JsonError> {
        serde_json::to_writer(writer, v)
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
        let items = deserializer.deserialize_seq(ArrayVisitor { max_items })?;
        deserializer.end()?;
        Ok(items)
    }

    pub fn to_raw_value<T: serde::Serialize>(v: &T) -> Result<super::JsonRawValue, JsonError> {
        serde_json::value::to_raw_value(v)
    }

    /// Deserialize directly from the raw JSON text without an intermediate buffer.
    pub fn from_raw_value<T: serde::de::DeserializeOwned>(
        raw: &super::JsonRawValue,
    ) -> Result<T, JsonError> {
        serde_json::from_str(raw.get())
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
        sonic_rs::from_slice(bytes)
    }

    pub fn to_vec<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, JsonError> {
        sonic_rs::to_vec(v)
    }

    #[cfg(feature = "client")]
    pub fn to_writer<T: serde::Serialize>(writer: &mut Vec<u8>, v: &T) -> Result<(), JsonError> {
        sonic_rs::to_writer(writer, v)
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
        use serde::de::{SeqAccess, Visitor};
        use serde::Deserializer;
        use std::borrow::Cow;

        struct ArrayVisitor {
            max_items: Option<usize>,
        }

        impl<'de> Visitor<'de> for ArrayVisitor {
            type Value = super::ArrayItems<'de>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON array")
            }

            fn visit_seq<Access>(self, mut seq: Access) -> Result<Self::Value, Access::Error>
            where
                Access: SeqAccess<'de>,
            {
                let mut items = Vec::new();
                let mut total_len = 0usize;
                while let Some(raw) = seq.next_element::<sonic_rs::LazyValue<'de>>()? {
                    total_len = total_len.saturating_add(1);
                    if self.max_items.is_none_or(|max| items.len() < max) {
                        items.push(match raw.as_raw_cow() {
                            Cow::Borrowed(text) => Cow::Borrowed(text.as_bytes()),
                            Cow::Owned(text) => Cow::Owned(text.into_bytes()),
                        });
                    }
                }
                Ok(super::ArrayItems { items, total_len })
            }
        }

        let text = std::str::from_utf8(bytes).map_err(<JsonError as serde::de::Error>::custom)?;
        let mut deserializer = sonic_rs::Deserializer::from_str(text);
        let items = deserializer.deserialize_seq(ArrayVisitor { max_items })?;
        deserializer.end()?;
        Ok(items)
    }

    pub fn to_raw_value<T: serde::Serialize>(v: &T) -> Result<super::JsonRawValue, JsonError> {
        let text = sonic_rs::to_string(v)?;
        sonic_rs::from_str::<super::JsonRawValue>(&text)
    }

    /// Deserialize reserialized raw JSON without rechecking the serializer's UTF-8.
    /// Sonic 0.5's owned lazy values do not expose their raw text directly.
    pub fn from_raw_value<T: serde::de::DeserializeOwned>(
        raw: &super::JsonRawValue,
    ) -> Result<T, JsonError> {
        let text = sonic_rs::to_string(raw)?;
        sonic_rs::from_str(&text)
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

#[cfg(all(
    any(feature = "server", all(feature = "client", feature = "tokio")),
    feature = "backend-serde-json"
))]
pub(crate) type BorrowedRawValue<'a> = &'a serde_json::value::RawValue;
#[cfg(all(
    any(feature = "server", all(feature = "client", feature = "tokio")),
    feature = "backend-sonic"
))]
pub(crate) type BorrowedRawValue<'a> = sonic_rs::LazyValue<'a>;

#[cfg(all(feature = "client", feature = "tokio"))]
pub(crate) fn from_slice_borrowed<'de, T: serde::Deserialize<'de>>(
    bytes: &'de [u8],
) -> Result<T, JsonError> {
    #[cfg(feature = "backend-serde-json")]
    {
        serde_json::from_slice(bytes)
    }
    #[cfg(feature = "backend-sonic")]
    {
        sonic_rs::from_slice(bytes)
    }
}

#[cfg(any(feature = "server", all(feature = "client", feature = "tokio")))]
pub(crate) fn borrowed_raw_text<'a>(raw: &'a BorrowedRawValue<'_>) -> std::borrow::Cow<'a, str> {
    #[cfg(feature = "backend-serde-json")]
    {
        std::borrow::Cow::Borrowed(raw.get())
    }
    #[cfg(feature = "backend-sonic")]
    {
        raw.as_raw_cow()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RawValueKind {
    Null,
    Structured,
    Scalar,
}

#[cfg(any(feature = "backend-serde-json", feature = "client", feature = "server"))]
pub(crate) fn classify_raw_value(bytes: &[u8]) -> RawValueKind {
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

pub(crate) fn from_str_borrowed<'de, T: serde::Deserialize<'de>>(
    text: &'de str,
) -> Result<T, JsonError> {
    #[cfg(feature = "backend-serde-json")]
    {
        serde_json::from_str(text)
    }
    #[cfg(feature = "backend-sonic")]
    {
        sonic_rs::from_str(text)
    }
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
/// assert!(!err.to_string().is_empty());
/// assert!(err.line() > 0);
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

#[cfg(feature = "client")]
pub(crate) fn to_writer<T: serde::Serialize>(writer: &mut Vec<u8>, v: &T) -> Result<(), JsonError> {
    imp::to_writer(writer, v)
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
    fn raw_value_roundtrips_escaped_utf8_and_nested_values() {
        #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        struct Payload {
            text: String,
            items: Vec<Option<i64>>,
        }

        let payload = Payload {
            text: "\u{00e9}\u{1f680}\n\"\\\t".into(),
            items: vec![Some(i64::MIN), None, Some(i64::MAX)],
        };
        let raw = to_raw_value(&payload).unwrap();
        assert_eq!(to_vec(&raw).unwrap(), to_vec(&payload).unwrap());
        for _ in 0..2 {
            assert_eq!(from_raw_value::<Payload>(&raw).unwrap(), payload);
        }
        assert_eq!(to_vec(&raw).unwrap(), to_vec(&payload).unwrap());
    }

    #[test]
    fn raw_value_preserves_serialization_errors() {
        let invalid_keys = std::collections::BTreeMap::from([(vec![1, 2], true)]);
        assert!(to_raw_value(&invalid_keys).is_err());
    }

    #[test]
    fn raw_value_classifies_scalar_roots() {
        for input in [b"null".as_slice(), b"true", b"false", b"1", br#""text""#] {
            let value: Value = from_slice(input).unwrap();
            let raw = to_raw_value(&value).unwrap();
            if input == b"null" {
                assert!(matches!(raw_value_kind(&raw), RawValueKind::Null));
            } else {
                assert!(matches!(raw_value_kind(&raw), RawValueKind::Scalar));
            }
            assert_eq!(to_vec(&raw).unwrap(), to_vec(&value).unwrap());
        }
    }

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
        for input in [
            b"[1,2,invalid]".as_slice(),
            b"[1,\"\xff\"]".as_slice(),
            br#"[1,"\q"]"#.as_slice(),
            b"[1,\"raw\nnewline\"]".as_slice(),
        ] {
            for limit in [None, Some(1), Some(0)] {
                assert!(
                    split_array(input, limit).is_err(),
                    "accepted invalid element with limit {limit:?}: {input:?}"
                );
            }
        }
    }

    #[test]
    fn split_array_rejects_trailing_input() {
        for input in [
            b"[1] garbage".as_slice(),
            b"[1][]".as_slice(),
            b"[1] {}".as_slice(),
            b"[1] true".as_slice(),
            b"[1]\0".as_slice(),
            b"[1]\xff".as_slice(),
            b"[] garbage".as_slice(),
        ] {
            for limit in [None, Some(1), Some(0)] {
                assert!(
                    split_array(input, limit).is_err(),
                    "accepted trailing input with limit {limit:?}: {input:?}"
                );
            }
        }
    }

    #[test]
    fn split_array_accepts_trailing_whitespace() {
        for input in [b"[1,2,3] \r\n\t".as_slice(), b"[] \r\n\t".as_slice()] {
            let split = split_array(input, None).unwrap();
            let values: Vec<Value> = from_slice(input).unwrap();
            assert_eq!(split.total_len, values.len());
            assert_eq!(split.items.len(), values.len());
        }
    }

    #[test]
    fn split_array_preserves_raw_elements_and_borrowing() {
        let input = br#"[{"text":"a\\b","items":[1, 2]},"escaped\nstring","\u0061",3]"#;
        let split = split_array(input, None).unwrap();
        let expected: [&[u8]; 4] = [
            br#"{"text":"a\\b","items":[1, 2]}"#,
            br#""escaped\nstring""#,
            br#""\u0061""#,
            b"3",
        ];
        assert_eq!(split.total_len, expected.len());
        assert_eq!(split.items.len(), expected.len());
        assert!(matches!(&split.items[0], std::borrow::Cow::Borrowed(_)));
        for (item, expected) in split.items.iter().zip(expected) {
            assert_eq!(item.as_ref(), expected);
        }
    }
}
