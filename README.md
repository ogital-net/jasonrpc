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
        let (a, b): (i64, i64) = req.params_as().ok_or_else(Error::invalid_params)?;
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
| `integration` | `hyper`, `tower` | Adapters: `HyperService`, `RpcService` (axum-ready) |

## Quick start

Add to `Cargo.toml`:

```toml
[dependencies]
jasonrpc = { version = "0.1", features = ["server"] }
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
        let p: Params = req.params_as().ok_or_else(Error::invalid_params)?;
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

### Client

```rust,ignore
use jasonrpc::client::{Client, HttpTransport};

let transport = HttpTransport::new("http://127.0.0.1:8080/")?;
let client = Client::new(transport);

let sum: i64 = client.call("add", (1, 2)).await?;
assert_eq!(sum, 3);

client.notify("shutdown", ()).await?;
```

### Byte-forwarding (e.g. an HTTP -> UDS proxy)

```rust,ignore
use jasonrpc::client::{Client, MultiplexTransport};
use jasonrpc::transport::Netstring;

// One long-lived multiplexed connection to the upstream
let mux = MultiplexTransport::new(upstream_socket, Netstring);
let upstream = Client::new(mux);

// Proxy raw bytes: rewrite ids, forward, restore ids on reply
let reply = upstream.round_trip_raw(request_bytes).await?;
```

## JSON backends

Exactly one backend must be selected:

```toml
# Default
jasonrpc = "0.1"

# Or sonic-rs for faster parsing:
jasonrpc = { version = "0.1", default-features = false, features = ["backend-sonic"] }
```

The public API is backend-neutral -- user code never sees `serde_json::Value` or `sonic_rs::Value` directly.

## Examples

```sh
# HTTP client + server
cargo run --example hyper_demo --features "hyper,http-client"

# Raw UDS, netstring-framed
cargo run --example uds_demo --features "server,client,netstring,tokio"

# HTTP front door -> multiplexed UDS upstream, with id rewriting
cargo run --example gateway_demo --features "server,http-client,hyper,netstring,tokio"
```

## Spec conformance

Implements [JSON-RPC 2.0](https://www.jsonrpc.org/specification) to the letter:

- `jsonrpc` field must be `"2.0"`; anything else is rejected
- `id` preserves original wire type (String, Number, Null)
- Notifications (no `id`) produce no response
- Batch processing with correct error semantics (sequential dispatch; cap batch
  size with `Router::with_max_batch_len`)
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
| `client` | -- | `Client`, `Transport` trait, id correlation |
| `transport` | -- | `Framing` trait |
| `netstring` | -- | Netstring framing (`transport`) |
| `newline` | -- | Newline-delimited framing (`transport`) |
| `tokio` | `tokio` | Async I/O helpers (`transport`) |
| `hyper` | `hyper` | `HyperService` adapter (`server`) |
| `tower` | `tower-service`, `http`, `http-body` | `RpcService` adapter (`server`) |
| `http-client` | `hyper`, `hyper-util` | `HttpTransport` (`client`) |

## License

[BSD-2-Clause](https://github.com/ogital-net/jasonrpc/blob/main/LICENSE)
