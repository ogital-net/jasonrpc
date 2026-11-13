# Cross-crate dispatch comparison

This standalone Criterion package compares transport-independent, single-call
JSON-RPC dispatch. It has its own workspace and lockfile so comparison dependencies
do not change jasonrpc's dependencies or its Rust 1.82 requirement. The comparison
package requires Rust 1.87 or newer because of jsonrpsee.

| Implementation | Version | Timed public API |
| --- | --- | --- |
| jasonrpc | Local checkout (0.7.0) | `Router::handle_bytes` followed by `to_bytes` |
| jsonrpsee-core | 0.26.1 | `RpcModule::raw_json_request` |
| jsonrpc-core | 18.0.0 | `IoHandler::handle_request` |

jsonrpsee is the current async comparator; jsonrpc-core is an older,
transport-independent baseline. Both competitors always use serde-json. The
default run also uses serde-json for jasonrpc; Sonic is a separate comparison.

## Methodology

Each implementation receives the same prebuilt JSON request, parses its envelope
and typed parameters, invokes an async handler, and produces an owned serialized
JSON-RPC response. No HTTP, WebSocket, framing, client, or network work is timed.
Every call runs through the same current-thread Tokio runtime. Handlers do not
perform I/O, yield, or spawn tasks. Setup and semantic response validation occur
outside the timed loops; response destruction is inside them.

The harness retains each API's native output (`Vec<u8>`, `String`, or boxed raw
JSON) rather than copying it to a common buffer. `jsonrpc-core` requires a JSON
Value result, so its struct handler uses `serde_json::to_value`; the other handlers
return typed results. This is part of each library's normal API cost, not bypassed
with pre-serialized results. Before timing, every response is compared against
the same expected JSON value, including its ID and error fields.

Workloads cover no-op, addition with numeric and 36-byte string IDs,
method-not-found, a small struct echo, and the same struct with a 65,536-byte
unescaped ASCII label. These payloads do not represent every JSON shape.

**Important limitation:** jsonrpsee's `raw_json_request` is a local-call helper,
not its production transport dispatcher. It creates a subscription channel,
mock subscription permit, and connection extensions for each call, even without
subscriptions. These costs remain timed; production servers manage connection
state differently. The benchmark therefore matches the raw input/output boundary
and method semantics, not every internal operation. Do not use its results to
claim an equivalent speedup over a running jsonrpsee server.

Batches, notifications, and subscriptions are excluded because this jsonrpsee
helper only accepts single calls/subscription requests. A matched network
comparison would need the same transport, client, connection reuse, concurrency,
payloads, limits, and runtime configuration on both servers.

## Run

From the repository root:

```sh
# Compile and validate output parity without measurements.
cargo bench --manifest-path benches/comparison/Cargo.toml --locked \
    --bench comparison -- --test

# Common serde-json backend.
cargo bench --manifest-path benches/comparison/Cargo.toml --locked \
    --bench comparison -- --save-baseline serde-json

# Repeat in the opposite implementation order.
JSONRPC_COMPARE_REVERSE=1 cargo bench \
    --manifest-path benches/comparison/Cargo.toml --locked \
    --bench comparison -- --save-baseline serde-json-reverse

# Separate Sonic run; competitors still use serde-json.
cargo bench --manifest-path benches/comparison/Cargo.toml --locked \
    --bench comparison --no-default-features --features sonic \
    -- --save-baseline sonic
```

Each benchmark uses 50 samples, a one-second warm-up, and three seconds of
measurement. Filter a workload by putting its name after `--`, for example
`-- add_string_id`. Reports are written under `benches/comparison/target/criterion`.
Use consistent compiler flags across runs; on x86-64, consider
`RUSTFLAGS="-C target-cpu=native"` for all implementations when comparing Sonic.

For DHAT, build with `CARGO_PROFILE_BENCH_DEBUG=1` and `--no-run`, then use the
printed executable path with `JSONRPC_COMPARE_ALLOC=jsonrpc_core` (or
`jasonrpc_serde_json`, `jasonrpc_sonic`, `jsonrpsee`) and optionally
`JSONRPC_COMPARE_CASE=echo_point_64k`. This runs 1,000 selected dispatches without
Criterion warm-up or measurement. Allocation totals still include all fixtures,
engines, and parity checks; distinguish their stacks from per-call allocations.
Allocation profiles were not collected for the results below.

## Local Results

Measured on the Apple/AArch64 Linux dev container (16 visible CPUs), Rust 1.99.1,
LLVM 23.1.1, default optimized bench profile. Backend versions were serde-json
1.0.154 and sonic-rs 0.5.11; Tokio was 1.54.1 and Criterion 0.5.1. jasonrpc was
commit `a29abbd`. All runs were sequential, not competing with other benchmarks.

Times below are Criterion point estimates in microseconds per call; lower is
better. Ranges span two serde-json runs with opposite implementation orders,
not confidence intervals. The string-ID case was added before the second run
and has one serde-json measurement. Sonic has one separate run.

| Workload | jasonrpc serde-json | jsonrpc-core | jsonrpsee helper | jasonrpc Sonic |
| --- | ---: | ---: | ---: | ---: |
| No-op | 0.328-0.358 | 0.641-0.661 | 0.759-0.789 | 0.373 |
| Add, numeric ID | 0.414-0.440 | 0.961-0.971 | 0.819-0.882 | 0.584 |
| Add, string ID | 0.454 | 1.107 | 1.033 | 0.648 |
| Method not found | 0.294-0.319 | 0.623-0.662 | 0.572-0.605 | 0.340 |
| Small struct echo | 0.604-0.626 | 1.352-1.436 | 1.111-1.130 | 0.749 |
| 64 KiB struct echo | 45.64-47.04 | 30.63-30.75 | 42.20-45.31 | 28.42 |

jasonrpc leads on the small local dispatch workloads. For the large echo,
jsonrpc-core is faster than serde-json jasonrpc in both repeats. Sonic improves
jasonrpc's large echo, but is slower than its serde-json backend on these small
calls. Absolute timings varied between runs; these are machine-specific dispatch
measurements, not network latency, concurrent throughput, or a universal crate
ranking.

## Large-Echo Diagnostics

An opt-in, serde-json-only group isolates stages of the same 64 KiB echo:

```sh
JSONRPC_COMPARE_STAGES=1 cargo bench \
    --manifest-path benches/comparison/Cargo.toml --locked \
    --bench comparison -- --save-baseline echo-stages
```

Before the parse-reduction changes, a diagnostic run on the same machine
measured these point estimates:

| Stage | Microseconds |
| --- | ---: |
| Initial jasonrpc result construction (`Response::try_result`) | 27.42 |
| Direct `serde_json::value::to_raw_value` | 20.78 |
| jasonrpc request envelope parsing | 9.92 |
| Typed params decoding from an already-parsed jasonrpc request | 7.73 |
| Combined jasonrpc request-to-Point | 17.81 |
| Combined jsonrpc-core request-to-Point | 8.54 |
| Prebuilt raw response to final bytes | 1.92 |

jasonrpc initially serialized typed results to a String, then called
`RawValue::from_string`, which reparses that freshly serialized JSON. Serde's
direct raw-value serializer avoids this validation pass. The initial-result
measurement also includes constructing the small response wrapper; neither
result-construction measurement includes final response serialization.

On input, jasonrpc validates and owns the raw params, then `params_as` parses
those bytes again into Point, copying its label into a second owned String.
jsonrpc-core parses params into its Value tree once; `Params::parse` consumes
that tree and transfers its owned String into Point without parsing the JSON
text again. This is a tradeoff of raw parameter storage, not a different JSON
backend. jasonrpc's raw result is also copied into the final response buffer.

The stage measurements support these explanations but are not an end-to-end
measurement of a proposed optimization. The initial diagnostics did not change
production code; the following section records the subsequent implementation.

## Implemented Parse Reductions

The serde-json backend now uses `serde_json::value::to_raw_value`, removing
the result-validation parse. Its isolated result-construction stage improved
from 27.42 to 20.56 microseconds (approximately 25%). This also applies to
serialized request params and error data constructed through the same helper.

Sonic now deserializes its own serializer's String output with `from_str`,
rather than rechecking UTF-8 through `from_slice`. This applies to raw-value
construction and typed decoding from raw values. Incoming byte validation is
unchanged. Sonic's native `to_lazyvalue` was not retained: root null/boolean
values triggered `get_type` panics in the 0.5.10 protocol tests, so the JSON
parse needed for its compatible scalar representation remains.

Subsequent end-to-end dispatch measurements against the saved pre-change
baselines gave these point estimates, in microseconds:

| Workload | Before | After |
| --- | ---: | ---: |
| serde-json 64 KiB echo | 47.04 | 43.64 |
| Sonic 64 KiB echo | 28.42 | 26.87 |
| serde-json small struct echo | 0.604 | 0.548 |
| Sonic small struct echo | 0.749 | 0.719 |

Criterion reported approximately 7-8% and 5% improvements for the large
serde-json and Sonic echoes respectively. These are individual runs against
saved baselines, not paired interleaved measurements. The unchanged
method-not-found path also moved approximately 3-4% slower, so small deltas
should not be treated as guaranteed improvements or regressions.

For reproduction, run the serde-json command with
`-- 'crate_dispatch/.*/jasonrpc_serde_json' --baseline serde-json-reverse`,
or the Sonic command with
`-- 'crate_dispatch/.*/jasonrpc_sonic' --baseline sonic`.
Save the baselines before applying the implementation changes.

The raw request API still validates params during envelope parsing and parses
them into the handler's requested type later. Replacing that with a Value tree
would change raw forwarding and number/text preservation, while eagerly typed
dispatch would need a different handler contract. Likewise, `Output::to_bytes`
still builds an owned final buffer from borrowed raw results. Neither contract
was changed for these optimizations. Regressions cover escaped Unicode, repeated
raw decoding, root scalars, and serialization failures; full-stack tests and
Clippy passed under both backends.

## Large Attachment Ownership

An opt-in workload dispatches a request with a 50 MiB unescaped base64-shaped
attachment and returns only its length. It compares slice/shared dispatch with
owned/borrowed field decoding within jasonrpc, not against the other crates.

```sh
JSONRPC_COMPARE_OWNERSHIP=1 cargo bench \
    --manifest-path benches/comparison/Cargo.toml --locked \
    --bench comparison -- --save-baseline ownership-serde-json

JSONRPC_COMPARE_OWNERSHIP=1 cargo bench \
    --manifest-path benches/comparison/Cargo.toml --locked \
    --bench comparison --no-default-features --features sonic \
    -- --save-baseline ownership-sonic
```

`JSONRPC_COMPARE_OWNERSHIP_SIZE` overrides the field length in bytes. The group
uses 20 samples, a one-second warm-up, and a three-second measurement target;
Criterion may extend collection to obtain all samples. Fixtures and output
parity checks are outside timing. Shared dispatch clones the Bytes handle, not
the payload. Base64 decoding, HTTP collection, network I/O, and storage are
excluded.

Local point estimates on the same AArch64/Rust environment, in milliseconds:

| Dispatch / field | serde-json | Sonic |
| --- | ---: | ---: |
| Slice / owned String | 17.50 | 20.95 |
| Slice / borrowed Cow | 13.43 | 18.25 |
| Shared Bytes / owned String | 15.13 | 15.95 |
| Shared Bytes / borrowed Cow | 12.53 | 13.02 |

Shared dispatch stores params as a slice of the original body on both backends.
Unescaped borrowed strings refer to that same allocation, including across an
await and after cloning the request. These guarantees are checked with pointer
identity tests through the router, batches, and Hyper. Shared and slice dispatch
also run through the same spec-conformance fixtures.

For allocation profiling, build with `CARGO_PROFILE_BENCH_DEBUG=1` and
`--no-run`, then set `BENCH` to the printed executable path:

```sh
JSONRPC_COMPARE_OWNERSHIP=1 JSONRPC_COMPARE_OWNERSHIP_ALLOC=slice_owned \
    valgrind --tool=dhat --dhat-out-file=target/dhat-ownership-owned.json "$BENCH"
JSONRPC_COMPARE_OWNERSHIP=1 JSONRPC_COMPARE_OWNERSHIP_ALLOC=shared_borrowed \
    valgrind --tool=dhat --dhat-out-file=target/dhat-ownership-shared.json "$BENCH"
```

The profile mode executes ten dispatches, without Criterion measurement or
parity-check dispatches. It still includes the reusable 50 MiB fixture, runtime,
and harness setup. Rebuild separately for Sonic. DHAT 3.24.0 measured:

| Backend / path | Total allocated, decimal MB | Peak live heap, MiB |
| --- | ---: | ---: |
| serde-json slice / owned | 1101.26 | 150.04 |
| serde-json shared / borrowed | 52.68 | 50.04 |
| Sonic slice / owned | 1625.55 | 200.04 |
| Sonic shared / borrowed | 52.68 | 50.04 |

These are allocation-volume and live-heap measurements, not process RSS. HTTP
chunk coalescing and binary attachment decoding can add allocations in a real
service. Holding a request also retains the body allocation, including other
elements of a batch. No body limit was increased; configure the adapter's
encoded-body cap and concurrent-request limits for the service.