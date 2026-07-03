//! Dispatch, serialization, and framing microbenchmarks.
//!
//! The JSON backends (`backend-serde-json` and `backend-sonic`) are mutually
//! exclusive, so a single bench binary cannot link both. Benchmark ids are the
//! same across backends; the criterion *baseline name* carries the backend
//! identity. Save one baseline per backend, then compare against it:
//!
//! ```sh
//! # Default backend (serde_json) — save its numbers under the `serde_json` name
//! cargo bench --bench dispatch \
//!     --features "server,tokio,netstring" -- --save-baseline serde_json
//!
//! # sonic-rs backend, diffed against the serde_json baseline. Because ids
//! # match, criterion prints a per-benchmark percentage delta.
//! cargo bench --bench dispatch --no-default-features \
//!     --features "backend-sonic,server,tokio,netstring" -- --baseline serde_json
//! ```
//!
//! On x86-64 the default `target-cpu` is a conservative baseline (SSE2 only),
//! which handicaps SIMD-heavy parsing such as `sonic-rs`. For a fair comparison
//! there, build with `RUSTFLAGS="-C target-cpu=native"`. On aarch64 (e.g. Apple
//! silicon) NEON is already in the default feature set, so no flag is needed.
//!
//! The benchmark needs the `server`, `tokio`, and `netstring` features. Rather
//! than declare them as `required-features` (which makes a plain `cargo bench`
//! silently skip this target), the bench compiles without them and prints how
//! to re-run.

// When the needed features are missing, compile a tiny `main` that explains how
// to run the benchmark, so `cargo bench` is never a silent no-op.
#[cfg(not(all(feature = "server", feature = "tokio", feature = "netstring")))]
fn main() {
    eprintln!(
        "the `dispatch` benchmark needs the `server`, `tokio`, and `netstring` \
         features; re-run for example with:\n\n    \
         cargo bench --bench dispatch --features \"server,tokio,netstring\"\n"
    );
}

#[cfg(all(feature = "server", feature = "tokio", feature = "netstring"))]
use bench_impl::main;

#[cfg(all(feature = "server", feature = "tokio", feature = "netstring"))]
mod bench_impl {
    use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};

    use jasonrpc::server::Router;
    use jasonrpc::{Error, Request};

    /// A small, representative parameter/result payload.
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Point {
        x: i64,
        y: i64,
        label: String,
    }

    /// Build a router with a mix of handler shapes used by the benchmarks.
    fn bench_router() -> Router<()> {
        Router::new()
            .register("add", |_, req: Request| async move {
                let (a, b): (i64, i64) = req.params_as().ok_or_else(Error::invalid_params)?;
                Ok(a + b)
            })
            .register("echo_point", |_, req: Request| async move {
                let p: Point = req.params_as().ok_or_else(Error::invalid_params)?;
                Ok(p)
            })
            .register("noop", |_, _req: Request| async move { Ok(()) })
    }

    /// A current-thread runtime is enough: handlers here are CPU-bound and never
    /// yield, so we measure dispatch cost, not scheduling.
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("build runtime")
    }

    fn bench_single_dispatch(c: &mut Criterion) {
        let rt = runtime();
        let router = bench_router();

        let mut group = c.benchmark_group("dispatch");

        let add = br#"{"jsonrpc":"2.0","method":"add","params":[42,23],"id":1}"#;
        group.throughput(Throughput::Bytes(add.len() as u64));
        group.bench_function("single_call_add", |b| {
            b.iter(|| {
                let out = rt.block_on(router.handle_bytes(std::hint::black_box(add)));
                std::hint::black_box(out.to_bytes().unwrap());
            });
        });

        let point = br#"{"jsonrpc":"2.0","method":"echo_point","params":{"x":1,"y":2,"label":"origin"},"id":1}"#;
        group.throughput(Throughput::Bytes(point.len() as u64));
        group.bench_function("single_call_struct", |b| {
            b.iter(|| {
                let out = rt.block_on(router.handle_bytes(std::hint::black_box(point)));
                std::hint::black_box(out.to_bytes().unwrap());
            });
        });

        let notif = br#"{"jsonrpc":"2.0","method":"noop","params":[]}"#;
        group.bench_function("single_notification", |b| {
            b.iter(|| {
                let out = rt.block_on(router.handle_bytes(std::hint::black_box(notif)));
                std::hint::black_box(out.to_bytes().unwrap());
            });
        });

        group.finish();
    }

    fn bench_batch_dispatch(c: &mut Criterion) {
        let rt = runtime();
        let router = bench_router();

        let mut group = c.benchmark_group("batch");

        for &n in &[1usize, 10, 100] {
            // Build an n-element batch of `add` calls.
            let mut batch = String::from("[");
            for i in 0..n {
                if i > 0 {
                    batch.push(',');
                }
                batch.push_str(&format!(
                    r#"{{"jsonrpc":"2.0","method":"add","params":[{i},1],"id":{i}}}"#
                ));
            }
            batch.push(']');
            let batch = batch.into_bytes();

            group.throughput(Throughput::Elements(n as u64));
            group.bench_with_input(BenchmarkId::from_parameter(n), &batch, |b, batch| {
                b.iter(|| {
                    let out = rt.block_on(router.handle_bytes(std::hint::black_box(batch)));
                    std::hint::black_box(out.to_bytes().unwrap());
                });
            });
        }

        group.finish();
    }

    fn bench_protocol_roundtrip(c: &mut Criterion) {
        use jasonrpc::json;

        let mut group = c.benchmark_group("protocol");

        // Parse a request from bytes.
        let bytes = br#"{"jsonrpc":"2.0","method":"echo_point","params":{"x":1,"y":2,"label":"origin"},"id":1}"#;
        group.bench_function("parse_request", |b| {
            b.iter(|| {
                let req: Request = json::from_slice(std::hint::black_box(bytes)).unwrap();
                std::hint::black_box(req);
            });
        });

        // Build + serialize a request (params serialized once into a raw value).
        group.bench_function("build_request", |b| {
            b.iter(|| {
                let req = Request::call(
                    "echo_point",
                    std::hint::black_box(&Point {
                        x: 1,
                        y: 2,
                        label: "origin".into(),
                    }),
                    jasonrpc::Id::Number(1),
                );
                std::hint::black_box(json::to_vec(&req).unwrap());
            });
        });

        group.finish();
    }

    fn bench_framing(c: &mut Criterion) {
        use jasonrpc::transport::{Framing, Netstring};

        let mut group = c.benchmark_group("framing");
        let payload = br#"{"jsonrpc":"2.0","method":"add","params":[42,23],"id":1}"#;

        let mut framed = Vec::new();
        Netstring.encode(payload, &mut framed);

        group.throughput(Throughput::Bytes(payload.len() as u64));
        group.bench_function("netstring_encode", |b| {
            b.iter(|| {
                let mut dst = Vec::with_capacity(framed.len());
                Netstring.encode(std::hint::black_box(payload), &mut dst);
                std::hint::black_box(dst);
            });
        });
        group.bench_function("netstring_decode", |b| {
            b.iter(|| {
                let decoded = Netstring.decode(std::hint::black_box(&framed)).unwrap();
                std::hint::black_box(decoded);
            });
        });

        group.finish();
    }

    criterion_group!(
        benches,
        bench_single_dispatch,
        bench_batch_dispatch,
        bench_protocol_roundtrip,
        bench_framing,
    );

    /// Entry point re-exported as the binary `main` when the required features
    /// are enabled. `criterion_main!` would define a private `main`; wrap it so
    /// it can be pulled up to the crate root.
    pub fn main() {
        benches();
        criterion::Criterion::default()
            .configure_from_args()
            .final_summary();
    }
}
