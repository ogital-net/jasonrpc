//! JSON-RPC 2.0 spec conformance tests, driven through the [`Router`].
//!
//! These encode the Section 7 examples from
//! <https://www.jsonrpc.org/specification> plus the key edge cases from the
//! conformance checklist. They are backend-agnostic: the same assertions run
//! under whichever JSON backend is selected, so they double as backend-parity
//! coverage. Run under each backend:
//!
//! ```sh
//! cargo test --test spec_conformance --features "server,tokio"
//! cargo test --test spec_conformance --no-default-features \
//!     --features "backend-sonic,server,tokio"
//! ```

#![cfg(feature = "server")]
#![allow(clippy::ignored_unit_patterns)]

use jasonrpc::json::{self, Value};
use jasonrpc::server::{Output, Router};
use jasonrpc::{Error, Request};

/// Build a router mirroring the methods used in the spec's Section 7 examples.
fn spec_router() -> Router<()> {
    Router::new()
        .register("subtract", |_, req: Request| async move {
            #[derive(serde::Deserialize)]
            struct ByName {
                minuend: i64,
                subtrahend: i64,
            }
            // Support both positional `[minuend, subtrahend]` and by-name.
            if let Some((minuend, subtrahend)) = req.params_as::<(i64, i64)>() {
                return Ok(minuend - subtrahend);
            }
            let p: ByName = req.params_as().ok_or_else(Error::invalid_params)?;
            Ok(p.minuend - p.subtrahend)
        })
        .register("sum", |_, req: Request| async move {
            let nums: Vec<i64> = req.params_as().ok_or_else(Error::invalid_params)?;
            Ok(nums.into_iter().sum::<i64>())
        })
        .register("get_data", |_, _req: Request| async move {
            Ok(json::from_slice::<Value>(br#"["hello", 5]"#).unwrap())
        })
        .register("update", |_, _req: Request| async move { Ok(json::null()) })
        .register("notify_hello", |_, _req: Request| async move {
            Ok(json::null())
        })
        .register(
            "notify_sum",
            |_, _req: Request| async move { Ok(json::null()) },
        )
}

/// Dispatch bytes and return the response as a UTF-8 string (panics if empty).
async fn call(router: &Router<()>, input: &[u8]) -> String {
    let out = router.handle_bytes(input).await;
    let bytes = out.to_bytes().unwrap().expect("expected a response body");
    String::from_utf8(bytes).unwrap()
}

/// Dispatch bytes and assert there is no response at all.
async fn call_empty(router: &Router<()>, input: &[u8]) {
    let out = router.handle_bytes(input).await;
    assert!(matches!(out, Output::Empty), "expected no response");
    assert!(out.to_bytes().unwrap().is_none());
}

#[tokio::test]
async fn positional_parameters() {
    let r = spec_router();
    let out = call(
        &r,
        br#"{"jsonrpc": "2.0", "method": "subtract", "params": [42, 23], "id": 1}"#,
    )
    .await;
    assert!(out.contains("\"result\":19"), "{out}");
    assert!(out.contains("\"id\":1"), "{out}");

    let out = call(
        &r,
        br#"{"jsonrpc": "2.0", "method": "subtract", "params": [23, 42], "id": 2}"#,
    )
    .await;
    assert!(out.contains("\"result\":-19"), "{out}");
}

#[tokio::test]
async fn named_parameters() {
    let r = spec_router();
    let out = call(
        &r,
        br#"{"jsonrpc": "2.0", "method": "subtract", "params": {"subtrahend": 23, "minuend": 42}, "id": 3}"#,
    )
    .await;
    assert!(out.contains("\"result\":19"), "{out}");
    assert!(out.contains("\"id\":3"), "{out}");
}

#[tokio::test]
async fn notification_produces_no_response() {
    let r = spec_router();
    call_empty(
        &r,
        br#"{"jsonrpc": "2.0", "method": "update", "params": [1,2,3,4,5]}"#,
    )
    .await;
    call_empty(&r, br#"{"jsonrpc": "2.0", "method": "foobar"}"#).await;
}

#[tokio::test]
async fn non_existent_method() {
    let r = spec_router();
    let out = call(&r, br#"{"jsonrpc": "2.0", "method": "foobar", "id": "1"}"#).await;
    assert!(out.contains("-32601"), "{out}");
    assert!(out.contains("\"id\":\"1\""), "{out}");
}

#[tokio::test]
async fn invalid_json_is_parse_error() {
    let r = spec_router();
    let out = call(
        &r,
        br#"{"jsonrpc": "2.0", "method": "foobar, "params": "bar", "baz]"#,
    )
    .await;
    assert!(out.contains("-32700"), "{out}");
    assert!(out.contains("\"id\":null"), "{out}");
}

#[tokio::test]
async fn invalid_request_object() {
    let r = spec_router();
    // `method` is a Number, not a String.
    let out = call(&r, br#"{"jsonrpc": "2.0", "method": 1, "params": "bar"}"#).await;
    assert!(out.contains("-32600"), "{out}");
    assert!(out.contains("\"id\":null"), "{out}");
}

#[tokio::test]
async fn invalid_batch_bad_json() {
    let r = spec_router();
    let out = call(
        &r,
        br#"[ {"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"}, {"jsonrpc": "2.0", "method" ]"#,
    )
    .await;
    assert!(out.starts_with('{'), "must be a single object: {out}");
    assert!(out.contains("-32700"), "{out}");
}

#[tokio::test]
async fn empty_array_is_single_invalid_request() {
    let r = spec_router();
    let out = call(&r, b"[]").await;
    assert!(out.starts_with('{'), "{out}");
    assert!(out.contains("-32600"), "{out}");
    assert!(out.contains("\"id\":null"), "{out}");
}

#[tokio::test]
async fn invalid_batch_but_not_empty() {
    let r = spec_router();
    let out = call(&r, b"[1]").await;
    assert!(out.starts_with('['), "must be an array: {out}");
    assert_eq!(out.matches("-32600").count(), 1, "{out}");
}

#[tokio::test]
async fn batch_of_invalids() {
    let r = spec_router();
    let out = call(&r, b"[1,2,3]").await;
    assert!(out.starts_with('['), "{out}");
    assert_eq!(out.matches("-32600").count(), 3, "{out}");
}

#[tokio::test]
async fn mixed_batch() {
    let r = spec_router();
    let out = call(
        &r,
        br#"[
            {"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"},
            {"jsonrpc": "2.0", "method": "notify_hello", "params": [7]},
            {"jsonrpc": "2.0", "method": "subtract", "params": [42,23], "id": "2"},
            {"foo": "boo"},
            {"jsonrpc": "2.0", "method": "foo.get", "params": {"name": "myself"}, "id": "5"},
            {"jsonrpc": "2.0", "method": "get_data", "id": "9"}
        ]"#,
    )
    .await;

    // Five responses: sum, subtract, invalid request, method-not-found, get_data.
    // The notification (notify_hello) yields nothing.
    assert!(out.starts_with('['), "{out}");
    assert!(out.contains("\"result\":7"), "sum result missing: {out}");
    assert!(
        out.contains("\"result\":19"),
        "subtract result missing: {out}"
    );
    assert!(out.contains("-32600"), "invalid request missing: {out}");
    assert!(out.contains("-32601"), "method-not-found missing: {out}");
    assert!(out.contains("hello"), "get_data result missing: {out}");
    assert!(
        !out.contains("notify_hello"),
        "notification leaked into output: {out}"
    );
}

#[tokio::test]
async fn all_notification_batch_is_silent() {
    let r = spec_router();
    call_empty(
        &r,
        br#"[
            {"jsonrpc": "2.0", "method": "notify_sum", "params": [1,2,4]},
            {"jsonrpc": "2.0", "method": "notify_hello", "params": [7]}
        ]"#,
    )
    .await;
}

#[tokio::test]
async fn string_id_is_not_coerced_to_number() {
    let r = spec_router();
    let out = call(
        &r,
        br#"{"jsonrpc": "2.0", "method": "subtract", "params": [42, 23], "id": "1"}"#,
    )
    .await;
    // Must echo the *string* id, not a number.
    assert!(out.contains("\"id\":\"1\""), "{out}");
}

#[tokio::test]
async fn wrong_version_is_invalid_request() {
    let r = spec_router();
    let out = call(
        &r,
        br#"{"jsonrpc": "1.0", "method": "subtract", "params": [42, 23], "id": 1}"#,
    )
    .await;
    assert!(out.contains("-32600"), "{out}");
}

#[tokio::test]
async fn invalid_params_type() {
    let r = spec_router();
    // `subtract` expects numbers; strings should surface invalid params.
    let out = call(
        &r,
        br#"{"jsonrpc": "2.0", "method": "subtract", "params": ["a", "b"], "id": 1}"#,
    )
    .await;
    assert!(out.contains("-32602"), "{out}");
}
