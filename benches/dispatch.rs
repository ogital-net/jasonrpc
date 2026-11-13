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
//!     --features "server,tokio,netstring,newline" -- --save-baseline serde_json
//!
//! # sonic-rs backend, diffed against the serde_json baseline. Because ids
//! # match, criterion prints a per-benchmark percentage delta.
//! cargo bench --bench dispatch --no-default-features \
//!     --features "backend-sonic,server,tokio,netstring,newline" -- --baseline serde_json
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
//! Enable `newline` as well to compare large newline and netstring framed reads.
//! Enable `client` to include typed and byte-only multiplexed round trips.

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
                let (a, b): (i64, i64) = req
                    .params_as()
                    .map_err(|error| Error::invalid_params().with_data(error.to_string()))?;
                Ok(a + b)
            })
            .register("echo_point", |_, req: Request| async move {
                let p: Point = req
                    .params_as()
                    .map_err(|error| Error::invalid_params().with_data(error.to_string()))?;
                Ok(p)
            })
            .register("noop", |_, _req: Request| async move { Ok(()) })
            .register("large_result", |_, _req: Request| async move {
                Ok(vec![0_u8; 64 * 1024])
            })
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

        let large_notif = br#"{"jsonrpc":"2.0","method":"large_result"}"#;
        group.throughput(Throughput::Bytes(large_notif.len() as u64));
        group.bench_function("single_notification_large_result", |b| {
            b.iter(|| {
                let out = rt.block_on(router.handle_bytes(std::hint::black_box(large_notif)));
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

    fn bench_framed_reads(c: &mut Criterion) {
        use jasonrpc::transport::io::FramedReader;
        use jasonrpc::transport::{Framing, Netstring};

        let rt = runtime();
        let mut group = c.benchmark_group("framed_read");

        for size in [1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024] {
            let payload = vec![b'x'; size];
            let mut netstring = Vec::new();
            Netstring.encode(&payload, &mut netstring);
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(
                BenchmarkId::new("netstring", size),
                &netstring,
                |bench, framed| {
                    bench.iter(|| {
                        let mut reader =
                            FramedReader::new(std::hint::black_box(framed.as_slice()), Netstring);
                        std::hint::black_box(rt.block_on(reader.recv()).unwrap().unwrap());
                    });
                },
            );

            #[cfg(feature = "newline")]
            {
                use jasonrpc::transport::Newline;

                let mut newline = Vec::new();
                Newline.encode(&payload, &mut newline);
                group.bench_with_input(
                    BenchmarkId::new("newline", size),
                    &newline,
                    |bench, framed| {
                        bench.iter(|| {
                            let mut reader =
                                FramedReader::new(std::hint::black_box(framed.as_slice()), Newline);
                            std::hint::black_box(rt.block_on(reader.recv()).unwrap().unwrap());
                        });
                    },
                );
            }
        }

        group.finish();
    }

    #[cfg(feature = "client")]
    fn bench_multiplex(c: &mut Criterion) {
        use jasonrpc::client::{encode_call, Client, MultiplexTransport};
        use jasonrpc::transport::io::FramedConn;
        use jasonrpc::transport::Netstring;
        use jasonrpc::{json, Id, Response};

        #[derive(serde::Deserialize)]
        struct IdOnly {
            id: Id,
        }

        let rt = runtime();
        let client = rt.block_on(async {
            let (client_io, server_io) = tokio::io::duplex(256 * 1024);
            tokio::spawn(async move {
                let mut conn = FramedConn::new(server_io, Netstring);
                while let Some(frame) = conn.recv().await.unwrap() {
                    let request: IdOnly = json::from_slice(&frame).unwrap();
                    let response = Response::result(request.id, json::null());
                    conn.send(&json::to_vec(&response).unwrap()).await.unwrap();
                }
            });
            Client::new(MultiplexTransport::new(client_io, Netstring))
        });

        let mut group = c.benchmark_group("multiplex");
        group.sample_size(50);
        group.warm_up_time(std::time::Duration::from_secs(1));
        group.measurement_time(std::time::Duration::from_secs(3));

        for count in [0, 512, 32 * 1024] {
            let params = vec![7_u64; count];
            let (request, _) = encode_call("noop", &params, Id::Number(1)).unwrap();
            group.throughput(Throughput::Bytes(request.len() as u64));
            group.bench_with_input(
                BenchmarkId::new("typed_call", count),
                &params,
                |bench, params| {
                    bench.iter(|| {
                        rt.block_on(client.call::<_, ()>("noop", std::hint::black_box(params)))
                            .unwrap();
                    });
                },
            );
            group.bench_with_input(
                BenchmarkId::new("raw_call", count),
                &request,
                |bench, request| {
                    bench.iter(|| {
                        std::hint::black_box(
                            rt.block_on(
                                client.round_trip_raw(std::hint::black_box(request.clone())),
                            )
                            .unwrap(),
                        );
                    });
                },
            );
        }

        group.finish();
    }

    #[cfg(not(feature = "client"))]
    fn bench_multiplex(_c: &mut Criterion) {}

    #[cfg(feature = "client")]
    fn bench_receive(c: &mut Criterion) {
        use jasonrpc::client::{decode_result, Client, MultiplexTransport};
        use jasonrpc::transport::io::FramedConn;
        use jasonrpc::transport::Netstring;
        use jasonrpc::{json, Id, Response};

        #[derive(serde::Deserialize)]
        struct IdOnly {
            id: Id,
        }

        let rt = runtime();
        let mut group = c.benchmark_group("receive");
        group.sample_size(50);
        group.warm_up_time(std::time::Duration::from_secs(1));
        group.measurement_time(std::time::Duration::from_secs(3));

        for count in [0, 512, 32 * 1024] {
            let result = vec![7_u64; count];
            let response = Response::result(Id::Number(1), &result);
            let bytes = json::to_vec(&response).unwrap();
            group.throughput(Throughput::Bytes(bytes.len() as u64));
            group.bench_with_input(
                BenchmarkId::new("decode_result", count),
                &bytes,
                |bench, bytes| {
                    bench.iter(|| {
                        std::hint::black_box(
                            decode_result::<Vec<u64>>(std::hint::black_box(bytes), &Id::Number(1))
                                .unwrap(),
                        );
                    });
                },
            );

            let client = rt.block_on(async {
                let (client_io, server_io) = tokio::io::duplex(256 * 1024);
                tokio::spawn(async move {
                    let mut conn = FramedConn::new(server_io, Netstring);
                    let mut response = response;
                    while let Some(frame) = conn.recv().await.unwrap() {
                        let request: IdOnly = json::from_slice(&frame).unwrap();
                        response.set_id(request.id);
                        conn.send(&json::to_vec(&response).unwrap()).await.unwrap();
                    }
                });
                Client::new(MultiplexTransport::new(client_io, Netstring))
            });
            group.bench_function(BenchmarkId::new("multiplex", count), |bench| {
                bench.iter(|| {
                    std::hint::black_box(
                        rt.block_on(client.call::<_, Vec<u64>>("result", ()))
                            .unwrap(),
                    );
                });
            });
        }

        group.finish();
    }

    #[cfg(not(feature = "client"))]
    fn bench_receive(_c: &mut Criterion) {}

    #[cfg(feature = "client")]
    fn allocation_profile(case: &str) {
        use jasonrpc::client::{decode_result, encode_call, encode_notification};
        use jasonrpc::{json, Id, Response};

        let size = std::env::var("JASONRPC_ALLOC_SIZE")
            .map(|size| {
                size.parse::<usize>()
                    .expect("invalid allocation payload size")
            })
            .unwrap_or(match case {
                "batch" | "batch_notification" => 100,
                _ => 32 * 1024,
            });
        let params = vec![7_u64; size];
        let response = json::to_vec(&Response::result(Id::Number(1), &params)).unwrap();

        if case == "multiplex" || case == "multiplex_raw" {
            use jasonrpc::client::{Client, MultiplexTransport};
            use jasonrpc::transport::io::FramedConn;
            use jasonrpc::transport::Netstring;

            #[derive(serde::Deserialize)]
            struct IdOnly {
                id: Id,
            }

            let rt = runtime();
            let (request, _) = encode_call("noop", &params, Id::Number(1)).unwrap();
            rt.block_on(async {
                let (client_io, server_io) = tokio::io::duplex(256 * 1024);
                let server = tokio::spawn(async move {
                    let mut conn = FramedConn::new(server_io, Netstring);
                    while let Some(frame) = conn.recv().await.unwrap() {
                        let request: IdOnly = json::from_slice(&frame).unwrap();
                        let response = Response::result(request.id, ());
                        conn.send(&json::to_vec(&response).unwrap()).await.unwrap();
                    }
                });
                let client = Client::new(MultiplexTransport::new(client_io, Netstring));
                for _ in 0..1000 {
                    if case == "multiplex_raw" {
                        std::hint::black_box(client.round_trip_raw(request.clone()).await.unwrap());
                    } else {
                        client.call::<_, ()>("noop", &params).await.unwrap();
                    }
                }
                drop(client);
                server.abort();
                let _ = server.await;
            });
            return;
        }

        if matches!(
            case,
            "dispatch" | "batch" | "notification" | "notification_unknown" | "batch_notification"
        ) {
            let rt = runtime();
            let router = bench_router();
            let request = match case {
                "dispatch" => {
                    br#"{"jsonrpc":"2.0","method":"add","params":[42,23],"id":1}"#.to_vec()
                }
                "notification" => br#"{"jsonrpc":"2.0","method":"noop","params":[]}"#.to_vec(),
                "notification_unknown" => {
                    br#"{"jsonrpc":"2.0","method":"unknown","params":[]}"#.to_vec()
                }
                "batch_notification" => {
                    let requests = vec![Request::notification("noop", &()); size];
                    json::to_vec(&requests).unwrap()
                }
                _ => {
                    let requests = vec![Request::call("add", &(42, 23), Id::Number(1)); size];
                    json::to_vec(&requests).unwrap()
                }
            };
            for _ in 0..1000 {
                let output = rt.block_on(router.handle_bytes(&request));
                std::hint::black_box(output.to_bytes().unwrap());
            }
            return;
        }

        if case == "framed_write" {
            use jasonrpc::transport::io::FramedWriter;
            use jasonrpc::transport::Netstring;

            let rt = runtime();
            let mut writer = FramedWriter::new(tokio::io::sink(), Netstring);
            for _ in 0..1000 {
                rt.block_on(writer.send(&response)).unwrap();
            }
            return;
        }

        if case == "framed_read" || case == "framed_read_shared" {
            use jasonrpc::transport::io::FramedReader;
            use jasonrpc::transport::{Framing, Netstring};

            let rt = runtime();
            let mut framed = Vec::new();
            Netstring.encode(&response, &mut framed);
            for _ in 0..1000 {
                let mut reader = FramedReader::new(framed.as_slice(), Netstring);
                if case == "framed_read_shared" {
                    std::hint::black_box(rt.block_on(reader.recv_bytes()).unwrap().unwrap());
                } else {
                    std::hint::black_box(rt.block_on(reader.recv()).unwrap().unwrap());
                }
            }
            return;
        }

        for _ in 0..1000 {
            match case {
                "encode_call" => {
                    std::hint::black_box(encode_call("noop", &params, Id::Number(1)).unwrap());
                }
                "encode_notification" => {
                    std::hint::black_box(encode_notification("noop", &params).unwrap());
                }
                "decode_result" => {
                    std::hint::black_box(
                        decode_result::<Vec<u64>>(&response, &Id::Number(1)).unwrap(),
                    );
                }
                "decode_unit" => {
                    decode_result::<()>(
                        br#"{"jsonrpc":"2.0","result":null,"id":1}"#,
                        &Id::Number(1),
                    )
                    .unwrap();
                }
                _ => panic!("unknown allocation profile: {case}"),
            }
        }
    }

    criterion_group!(
        benches,
        bench_single_dispatch,
        bench_batch_dispatch,
        bench_protocol_roundtrip,
        bench_framing,
        bench_framed_reads,
        bench_multiplex,
        bench_receive,
    );

    /// Entry point re-exported as the binary `main` when the required features
    /// are enabled. `criterion_main!` would define a private `main`; wrap it so
    /// it can be pulled up to the crate root.
    pub fn main() {
        #[cfg(feature = "client")]
        if let Ok(case) = std::env::var("JASONRPC_ALLOC_PROFILE") {
            allocation_profile(&case);
            return;
        }
        benches();
        criterion::Criterion::default()
            .configure_from_args()
            .final_summary();
    }
}
