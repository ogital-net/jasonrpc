# jasonrpc

A transport-agnostic, spec-conforming [JSON-RPC 2.0](https://www.jsonrpc.org/specification) library for Rust.

```rust
# #[cfg(feature = "server")]
# #[tokio::main]
# async fn main() {
# use jasonrpc::server::Router;
# use jasonrpc::{Error, Request};
let router = Router::new()
    .register("add", |_, req: Request| async move {
        let (a, b): (i64, i64) = req
            .params_as()
            .map_err(|error| Error::invalid_params().with_data(error.to_string()))?;
        Ok(a + b)
    });

// Dispatch raw bytes from any transport:
let _output = router.handle_bytes(
    br#"{"jsonrpc":"2.0","method":"add","params":[1,2],"id":1}"#
).await;
# }
# #[cfg(not(feature = "server"))]
# fn main() {}
```

---

## Design

Params, results, and error data are held as *raw* JSON (`RawValue`) and only
deserialized when you ask for a concrete type. Many JSON-RPC libraries fully
parse every message into a value DOM and re-serialize it on the way out;
`jasonrpc` avoids that round trip, so bytes you don't inspect are never
re-encoded. This keeps single servers fast and makes byte-forwarding workloads
(proxies, gateways, fan-out) cheap — but the crate is a general-purpose
JSON-RPC toolkit, not a gateway framework.

## Layers

The crate is organized so each layer is usable independently:

| Layer | Feature | Purpose |
|-------|---------|---------|
| `protocol` | always-on | `Request`, `Response`, `Error`, `Id`, `Version` types plus (de)serialization |
| `json` | always-on | Pluggable JSON backend (`serde_json` or `sonic-rs`) |
| `server` | `server` | `Router` for method registration and dispatch (single + batch) |
| `client` | `client` | `Client` with typed call/notify, id correlation, and raw passthrough |
| `transport` | `transport` | Framing codecs (`netstring`, `newline`) and async I/O |
| `integration` | `hyper`, `tower`, `websocket` | Adapters: `HyperService`, `RpcService` (axum-ready), WebSocket `serve` |

## Quick start

Add to `Cargo.toml`:

```toml
[dependencies]
jasonrpc = { version = "0.7", features = ["server"] }
```

### Server

```rust
# #[cfg(feature = "server")]
# #[tokio::main]
# async fn main() {
# use jasonrpc::server::Router;
# use jasonrpc::{Error, Request};
let router = Router::new()
    .register("ping", |_, _req: Request| async move {
        Ok("pong")
    })
    .register("greet", |_, req: Request| async move {
        #[derive(serde::Deserialize)]
        struct Params { name: String }
        let p: Params = req
            .params_as()
            .map_err(|error| Error::invalid_params().with_data(error.to_string()))?;
        Ok(format!("hello, {}!", p.name))
    });

// Feed it bytes from any transport:
let output = router.handle_bytes(
    br#"{"jsonrpc":"2.0","method":"greet","params":{"name":"Alice"},"id":1}"#
).await;
// `to_bytes()` produces wire-ready output (None for all-notification batches)
let _ = output.to_bytes();
# }
# #[cfg(not(feature = "server"))]
# fn main() {}
```

### Parameter errors

`Request::params_as` and `Request::param_as` return `Result<T, json::JsonError>`,
preserving the selected backend's error message and location information.
Locations refer to the params JSON, not the full request body; field paths are
not added. Missing params are still treated as JSON `null` by `params_as`, while
`param_as` requires exactly one positional argument.

The examples attach deserialization details to the JSON-RPC error's `data` field.
Those details may include submitted values. To return only a generic error, use
`.map_err(|_| Error::invalid_params())?` instead. Callers must inspect `error.data`
to display the attached details; the protocol error's `Display` shows only its
message and code.

### Large Requests And Borrowed Params

`Request::params_as_borrowed` allows decoded fields to borrow from the request.
Use `#[serde(borrow)] Cow<'_, str>` for an attachment's base64 text: ordinary
unescaped strings are borrowed, while JSON escapes fall back to owned strings.
Keep the request alive while using the decoded fields, including across awaits.
The existing `params_as` and `param_as` APIs are unchanged.

```rust
# #[cfg(feature = "server")]
# #[tokio::main]
# async fn main() {
use jasonrpc::{Error, Request};
use jasonrpc::server::Router;
use std::borrow::Cow;

#[derive(serde::Deserialize)]
struct Attachment<'a> {
    #[serde(borrow)]
    data: Cow<'a, str>,
}

let router = Router::new().register("ingest", |_, request: Request| async move {
    let params: Attachment<'_> = request.params_as_borrowed()
        .map_err(|_| Error::invalid_params())?;
    Ok(params.data.len())
});

let body = bytes::Bytes::from_static(
    br#"{"jsonrpc":"2.0","method":"ingest","params":{"data":"YWJj"},"id":1}"#,
);
let _output = router.handle_shared_bytes(body).await;
# }
# #[cfg(not(feature = "server"))]
# fn main() {}
```

Hyper and Tower adapters automatically dispatch collected `Bytes` through this
shared path. Params are slices of the body, and cloned requests share its
allocation on both JSON backends. `server` now enables the small `bytes`
dependency; protocol-only users still do not need it. `handle_bytes(&[u8])`
remains available, but must allocate owned raw params.

Sonic requests built directly or deserialized outside shared dispatch cache a
serialized params string on the first borrowed decode. The shared HTTP path
does not need that cache. Sharing is not streaming: HTTP chunk coalescing may
still copy, and retaining a request can retain the whole body or batch buffer.
The default HTTP request limit remains 16 MiB; set `with_max_request_body_size`
on the adapter to a bounded limit covering the encoded body and metadata.
Base64 expands binary data by approximately one third. Bound concurrent large
requests and decode attachments into storage incrementally to avoid an additional
full decoded allocation. See the [attachment ownership measurements](benches/comparison/README.md#large-attachment-ownership).

### Client

```rust,ignore
use jasonrpc::client::{Client, HttpTransport};

let transport = HttpTransport::new("http://127.0.0.1:8080/")?;
let client = Client::new(transport);

let sum: i64 = client.call("add", (1, 2)).await?;
assert_eq!(sum, 3);

client.notify("shutdown", ()).await?;
```

### Unix-socket client

`UdsClient::connect` establishes the connection and installs the framing in one
call, over a multiplexed connection (`uds` feature):

```rust,ignore
use jasonrpc::client::UdsClient;
use jasonrpc::transport::Netstring;

let client = UdsClient::connect("/run/app.sock", Netstring).await?;
let sum: i64 = client.call("add", (1, 2)).await?;
```

### Byte-forwarding (e.g. an HTTP -> UDS proxy)

```rust,ignore
use jasonrpc::client::UdsClient;
use jasonrpc::transport::Netstring;

// One long-lived multiplexed connection to the upstream.
let upstream = UdsClient::connect("/run/upstream.sock", Netstring).await?;

// Pass the rewritten id with the request bytes to avoid re-parsing them.
let reply = upstream.round_trip_raw_with_id(request_bytes, request_id).await?;
```

The supplied id must match the id encoded in the request bytes. Typed
`Client::call` requests use this path automatically. The byte-only
`round_trip_raw(request_bytes)` method remains available when the id is not
already known; multiplexed transports extract it from the bytes. Transports
that do not need correlation metadata, such as HTTP, use the default
`Transport::round_trip_with_id` implementation, which delegates to `round_trip`.

Typed calls also use `Transport::round_trip_response`. Its default wraps the
raw reply in a `ReceivedResponse`; multiplexed transports reuse the envelope
already parsed for routing. The result is decoded in the caller's task, not
the shared reader. Raw forwarding retains the original bytes, including
formatting and recoverable malformed envelopes.

`FramedReader::recv_bytes` and `FramedConn::recv_bytes` return shared `Bytes`
payloads. All built-in codecs extract these without copying the payload.
Custom codecs inherit a copying `Framing::decode_bytes` fallback. The existing
`recv` methods still return `Vec<u8>` and use a direct owned-payload path.
Keeping a shared payload alive also retains its underlying read allocation.

For a stream you already hold (not a socket path), wrap it yourself with
`Client::new(MultiplexTransport::new(stream, framing))`; `UdsClient` is the
shortcut for the common Unix-socket case. See the `gateway_demo` example for a
full HTTP-to-UDS proxy with id rewriting.

## JSON backends

Exactly one backend must be selected:

```toml
# Default
jasonrpc = "0.7"

# Or sonic-rs:
jasonrpc = { version = "0.7", default-features = false, features = ["backend-sonic"] }
```

Use `json::Value` and `json::JsonError` to refer to the selected backend's native
types. Backend-specific methods may differ.

## Examples

```sh
# HTTP client + server
cargo run --example hyper_demo --features "hyper,http-client,tokio"

# Raw UDS, netstring-framed
cargo run --example uds_demo --features "server,client,uds,netstring,tokio"

# HTTP front door -> multiplexed UDS upstream, with id rewriting
cargo run --example gateway_demo --features "server,http-client,hyper,uds,netstring,tokio"

# JSON-RPC over WebSocket: a server dispatching messages from WS clients
cargo run --example ws_controller --features "server,websocket,tokio,hyper"
```

## Benchmarks

`benches/dispatch.rs` covers single/batch dispatch, notification results,
request parse/build, framing, and chunked reads of large frames. Enable `newline`
to compare newline and netstring read performance. The JSON backends are mutually
exclusive, so the same bench is run once per backend and compared through
criterion baselines — the baseline name carries
the backend identity, and the benchmark ids match, so the second run prints a
per-benchmark delta:

```sh
# serde_json backend — save its numbers under the `serde_json` baseline
cargo bench --bench dispatch \
    --features "server,tokio,netstring,newline" -- --save-baseline serde_json

# sonic-rs backend, diffed against the serde_json baseline
cargo bench --bench dispatch --no-default-features \
    --features "backend-sonic,server,tokio,netstring,newline" -- --baseline serde_json
```

A plain `cargo bench` (no features) prints the command to re-run with the
features the benchmark needs, rather than skipping silently.

Enable `client` to benchmark multiplexed typed calls and byte-only raw calls
over a persistent in-memory netstring connection. The `multiplex` group uses
arrays containing 0, 512, or 32768 integers (approximately 0, 1 KiB, or 64 KiB
of parameters), a current-thread runtime, and a server that extracts the id
and returns a null result. Timings include request encoding for typed calls,
framing, scheduling, response routing, and decoding; these are not socket or
network throughput measurements. Raw calls clone pre-encoded request bytes
and provide a control for typed-path optimizations.

```sh
# Before changing the implementation, save a baseline.
cargo bench --bench dispatch \
    --features "server,client,tokio,netstring" -- multiplex --save-baseline multiplex-before-serde-json

# After changing the implementation, compare against the saved baseline.
cargo bench --bench dispatch \
    --features "server,client,tokio,netstring" -- multiplex --baseline multiplex-before-serde-json
```

For sonic-rs, add `--no-default-features`, include `backend-sonic` in the
features, and use a separate baseline name such as `multiplex-before-sonic`.

The `receive` group separately measures standalone `decode_result` and
multiplexed calls with small requests returning arrays of the same three
sizes. The server prebuilds the result, updates the response id, and serializes
the envelope for each reply. Multiplexed timings include that server work,
framing, scheduling, routing, and typed decoding. Standalone timings isolate
the decoder. Save receive baselines before changing the implementation:

```sh
cargo bench --bench dispatch \
    --features "server,client,tokio,netstring" -- receive --save-baseline receive-before-serde-json

cargo bench --bench dispatch \
    --features "server,client,tokio,netstring" -- receive --baseline receive-before-serde-json
```

Use `receive-before-sonic` with the Sonic feature configuration for its
independent baseline. Shared buffers remove payload copies, but do not
guarantee lower latency for small replies; compare the complete workload as
well as standalone decoding.

On x86-64, add `RUSTFLAGS="-C target-cpu=native"` for a fair `sonic-rs`
comparison — the default `target-cpu` there is a conservative baseline (SSE2)
that handicaps SIMD parsing. On aarch64, NEON is already enabled by default.

### Cross-crate comparison

The isolated [comparison benchmark](benches/comparison/README.md) measures
matched raw JSON single-call workloads against `jsonrpsee-core` and
`jsonrpc-core`, with output-equivalence checks and separate serde-json/Sonic
runs. Its dependencies do not affect the library's normal build or MSRV.
The comparison documents helper-specific overhead and local results; it is
not an end-to-end network benchmark.

### Allocation profiling

The benchmark also has a fixed-count mode that runs 1,000 operations without
Criterion setup, warm-up, or reporting. Build with symbols and set `BENCH` to
the executable path printed by Cargo:

```sh
CARGO_PROFILE_BENCH_DEBUG=1 cargo bench --bench dispatch --no-run \
    --features "server,client,tokio,netstring"
BENCH=target/release/deps/dispatch-HASH

JASONRPC_ALLOC_PROFILE=encode_call valgrind --tool=dhat --num-callers=20 \
    --dhat-out-file=target/dhat-encode-call.json "$BENCH"

JASONRPC_ALLOC_PROFILE=multiplex JASONRPC_ALLOC_SIZE=0 \
    valgrind --tool=dhat --num-callers=20 \
    --dhat-out-file=target/dhat-multiplex-small.json "$BENCH"
```

Load the JSON files in Valgrind's `dh_view.html` viewer. Capture separate files
before and after a change, with the same backend, feature set, and payload size.
For Sonic, rebuild with `--no-default-features` and `backend-sonic` in the
feature list, and use the new executable path.

`JASONRPC_ALLOC_PROFILE` selects `encode_call`, `encode_notification`,
`decode_result`, `decode_unit`, `multiplex`, `multiplex_raw`, `dispatch`,
`notification`, `notification_unknown`, `batch`, `batch_notification`, `framed_read`,
`framed_read_shared`, or `framed_write`. `JASONRPC_ALLOC_SIZE` sets the number
of integers in the payload (default 32,768), or entries in a batch (default
100). Multiplexed profiles include the in-memory peer's allocations; raw
calls clone a pre-encoded request. Frame-read profiles create a reader for
each operation; the writer profile reuses one writer. All totals include
fixture and runtime setup, so use call stacks to distinguish setup from
per-operation allocations.

Expected allocations include owned request fields and raw params on dispatch,
boxed handler futures, output buffers and their growth, owned typed results,
and multiplexing's oneshot channels. Call and notification encoding writes
params directly into the final buffer, omits null params, and wraps scalars
without serializing them twice. Numeric-id null-result decoding needs no heap
allocation. Writers reuse their framing buffer; shared readers use spare
capacity before growing. Shared payloads can retain backing storage after
delivery, and raw `Vec<u8>` replies can require a copy when that storage is
still shared. Unknown-method notifications do not construct a discarded error.
Batch response capacity is reserved once when the first response
is produced, using the batch length as an upper bound; mixed batches may keep
spare capacity, while all-notification batches allocate no response vector.

For a separate leak and invalid-access check:

```sh
JASONRPC_ALLOC_PROFILE=multiplex JASONRPC_ALLOC_SIZE=0 \
    valgrind --leak-check=full --show-leak-kinds=definite,indirect \
    --errors-for-leak-kinds=definite,indirect --error-exitcode=1 "$BENCH"
```

## Spec conformance

Implements [JSON-RPC 2.0](https://www.jsonrpc.org/specification) to the letter:

- `jsonrpc` field must be `"2.0"`; anything else is rejected
- `id` preserves original wire type (String, finite Number, Null)
- `params`, when present, must be an Array or Object
- Notifications (no `id`) produce no response
- Batch processing with correct error semantics (sequential dispatch; defaults to 1,024 entries and can be tuned with `Router::with_max_batch_len`)
- Empty batches and malformed JSON produce spec-mandated error responses

Run the spec's Section 7 examples as tests:

```sh
cargo test --test spec_conformance --features "server,tokio"
```

## Feature flags

| Feature | Deps | Description |
|---------|------|-------------|
| `backend-serde-json` | `serde_json` | Default JSON backend |
| `backend-sonic` | `sonic-rs` | Alternative JSON backend |
| `server` | -- | `Router` and handler infrastructure |
| `client` | `bytes` | `Client`, `Transport` trait, id correlation |
| `transport` | -- | `Framing` trait |
| `netstring` | -- | Netstring framing (`transport`) |
| `newline` | -- | Newline / byte-delimited framing (`transport`) |
| `content-length` | -- | LSP/DAP `Content-Length` header framing (`transport`) |
| `tokio` | `tokio`, `bytes` | Async I/O helpers (`transport`) |
| `uds` | `tokio` | `UdsClient`: Unix-socket client (`client`) |
| `hyper` | `hyper` | `HyperService` adapter (`server`) |
| `tower` | `tower-service`, `http`, `http-body` | `RpcService` adapter (`server`) |
| `websocket` | `fastwebsockets` | WebSocket `serve` adapter, reuses hyper (`server`) |
| `http-client` | `hyper`, `hyper-util` | `HttpTransport` (`client`) |

HTTP request bodies and WebSocket messages default to a 16 MiB limit. WebSocket
limits include all fragments of a logical message. Configure smaller limits at
the integration boundary for deployments that accept only small RPC payloads.

## License

[BSD-2-Clause](https://github.com/ogital-net/jasonrpc/blob/main/LICENSE)
