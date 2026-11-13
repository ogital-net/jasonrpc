use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use jasonrpc::server::Router;
use jasonrpc::{Error, Request};
use jsonrpsee_core::server::RpcModule;
use jsonrpsee_types::ErrorObjectOwned;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Point {
    x: i64,
    y: i64,
    label: String,
}

enum Engine {
    Jason(Router<()>),
    Jsonrpc(jsonrpc_core::IoHandler),
    Jsonrpsee(RpcModule<()>),
}

enum Reply {
    Jason(Vec<u8>),
    Jsonrpc(String),
    Jsonrpsee(Box<serde_json::value::RawValue>),
}

impl Reply {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Jason(bytes) => bytes,
            Self::Jsonrpc(text) => text.as_bytes(),
            Self::Jsonrpsee(raw) => raw.get().as_bytes(),
        }
    }
}

impl Engine {
    fn jason() -> Self {
        Self::Jason(
            Router::new()
                .register("noop", |_, _request: Request| async { Ok(()) })
                .register("add", |_, request: Request| async move {
                    let (first, second): (i64, i64) = request
                        .params_as()
                        .map_err(|error| Error::invalid_params().with_data(error.to_string()))?;
                    Ok(first + second)
                })
                .register("echo_point", |_, request: Request| async move {
                    request
                        .params_as::<Point>()
                        .map_err(|error| Error::invalid_params().with_data(error.to_string()))
                }),
        )
    }

    fn jsonrpc() -> Self {
        let mut handler =
            jsonrpc_core::IoHandler::with_compatibility(jsonrpc_core::Compatibility::V2);
        handler.add_method("noop", |_| async { Ok(jsonrpc_core::Value::Null) });
        handler.add_method("add", |params: jsonrpc_core::Params| async move {
            let (first, second): (i64, i64) = params.parse()?;
            Ok(jsonrpc_core::Value::from(first + second))
        });
        handler.add_method("echo_point", |params: jsonrpc_core::Params| async move {
            let point: Point = params.parse()?;
            serde_json::to_value(point).map_err(|_| jsonrpc_core::Error::internal_error())
        });
        Self::Jsonrpc(handler)
    }

    fn jsonrpsee() -> Self {
        let mut module = RpcModule::new(());
        module
            .register_async_method("noop", |_, _, _| async { Ok::<_, ErrorObjectOwned>(()) })
            .unwrap();
        module
            .register_async_method("add", |params, _, _| async move {
                let (first, second): (i64, i64) = params.parse()?;
                Ok::<_, ErrorObjectOwned>(first + second)
            })
            .unwrap();
        module
            .register_async_method("echo_point", |params, _, _| async move {
                params.parse::<Point>()
            })
            .unwrap();
        Self::Jsonrpsee(module)
    }

    async fn dispatch(&self, request: &str) -> Reply {
        match self {
            Self::Jason(router) => Reply::Jason(
                router
                    .handle_bytes(request.as_bytes())
                    .await
                    .to_bytes()
                    .unwrap()
                    .unwrap(),
            ),
            Self::Jsonrpc(handler) => {
                Reply::Jsonrpc(handler.handle_request(request).await.unwrap())
            }
            Self::Jsonrpsee(module) => {
                let (reply, _) = module.raw_json_request(request, 1).await.unwrap();
                Reply::Jsonrpsee(reply)
            }
        }
    }
}

struct Case {
    name: &'static str,
    request: String,
    expected: serde_json::Value,
}

fn cases() -> Vec<Case> {
    let mut cases = vec![
        Case {
            name: "noop",
            request: r#"{"jsonrpc":"2.0","method":"noop","id":1}"#.into(),
            expected: serde_json::json!({"jsonrpc": "2.0", "result": null, "id": 1}),
        },
        Case {
            name: "add",
            request: r#"{"jsonrpc":"2.0","method":"add","params":[42,23],"id":1}"#.into(),
            expected: serde_json::json!({"jsonrpc": "2.0", "result": 65, "id": 1}),
        },
        Case {
            name: "add_string_id",
            request: r#"{"jsonrpc":"2.0","method":"add","params":[42,23],"id":"550e8400-e29b-41d4-a716-446655440000"}"#.into(),
            expected: serde_json::json!({
                "jsonrpc": "2.0", "result": 65, "id": "550e8400-e29b-41d4-a716-446655440000"
            }),
        },
        Case {
            name: "method_not_found",
            request: r#"{"jsonrpc":"2.0","method":"missing","id":1}"#.into(),
            expected: serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": {"code": -32601, "message": "Method not found"}
            }),
        },
    ];
    for (name, label) in [
        ("echo_point", "origin".into()),
        ("echo_point_64k", "x".repeat(64 * 1024)),
    ] {
        let point = Point { x: 1, y: 2, label };
        cases.push(Case {
            name,
            request: serde_json::json!({
                "jsonrpc": "2.0", "method": "echo_point", "params": &point, "id": 1
            })
            .to_string(),
            expected: serde_json::json!({"jsonrpc": "2.0", "result": point, "id": 1}),
        });
    }
    cases
}

fn engines() -> [(&'static str, Engine); 3] {
    #[cfg(feature = "serde-json")]
    let jason_name = "jasonrpc_serde_json";
    #[cfg(feature = "sonic")]
    let jason_name = "jasonrpc_sonic";
    [
        (jason_name, Engine::jason()),
        ("jsonrpc_core", Engine::jsonrpc()),
        ("jsonrpsee", Engine::jsonrpsee()),
    ]
}

#[cfg(feature = "serde-json")]
fn echo_stages(criterion: &mut Criterion, case: &Case) {
    let point: Point = serde_json::from_value(case.expected["result"].clone()).unwrap();
    let request: Request = jasonrpc::json::from_slice(case.request.as_bytes()).unwrap();
    let current = jasonrpc::Response::try_result(jasonrpc::Id::Number(1), &point).unwrap();
    let direct_raw = serde_json::value::to_raw_value(&point).unwrap();
    assert_eq!(current.result_raw().unwrap().get(), direct_raw.get());

    let mut group = criterion.benchmark_group("echo_64k_stages");
    group.sample_size(50);
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.bench_function("result_current", |bench| {
        bench.iter(|| {
            jasonrpc::Response::try_result(jasonrpc::Id::Number(1), std::hint::black_box(&point))
                .unwrap()
        });
    });
    group.bench_function("result_serde_direct", |bench| {
        bench.iter(|| serde_json::value::to_raw_value(std::hint::black_box(&point)).unwrap());
    });
    group.bench_function("jasonrpc_envelope_parse", |bench| {
        bench.iter(|| {
            jasonrpc::json::from_slice::<Request>(std::hint::black_box(case.request.as_bytes()))
                .unwrap()
        });
    });
    group.bench_function("jasonrpc_params_decode", |bench| {
        bench.iter(|| std::hint::black_box(&request).params_as::<Point>().unwrap());
    });
    group.bench_function("jasonrpc_request_to_point", |bench| {
        bench.iter(|| {
            let request: Request =
                jasonrpc::json::from_slice(std::hint::black_box(case.request.as_bytes())).unwrap();
            request.params_as::<Point>().unwrap()
        });
    });
    group.bench_function("jsonrpc_core_request_to_point", |bench| {
        bench.iter(|| {
            let request: jsonrpc_core::Request =
                serde_json::from_str(std::hint::black_box(&case.request)).unwrap();
            match request {
                jsonrpc_core::Request::Single(jsonrpc_core::Call::MethodCall(call)) => {
                    call.params.parse::<Point>().unwrap()
                }
                _ => unreachable!(),
            }
        });
    });
    group.bench_function("response_raw_to_bytes", |bench| {
        bench.iter(|| jasonrpc::json::to_vec(std::hint::black_box(&current)).unwrap());
    });
    group.finish();
}

fn request_ownership(criterion: &mut Criterion, runtime: &tokio::runtime::Runtime) {
    #[derive(serde::Deserialize)]
    struct OwnedAttachment {
        data: String,
    }
    #[derive(serde::Deserialize)]
    struct BorrowedAttachment<'a> {
        #[serde(borrow)]
        data: std::borrow::Cow<'a, str>,
    }

    let size = std::env::var("JSONRPC_COMPARE_OWNERSHIP_SIZE")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(50 * 1024 * 1024);
    let prefix = br#"{"jsonrpc":"2.0","method":"ingest","params":{"data":""#;
    let suffix = br#""},"id":1}"#;
    let mut body = Vec::with_capacity(prefix.len() + size + suffix.len());
    body.extend_from_slice(prefix);
    body.resize(body.len() + size, b'A');
    body.extend_from_slice(suffix);
    let body = bytes::Bytes::from(body);

    let owned = Router::new().register("ingest", |_, request: Request| async move {
        let params: OwnedAttachment = request
            .params_as()
            .map_err(|error| Error::invalid_params().with_data(error.to_string()))?;
        Ok(params.data.len())
    });
    let borrowed = Router::new().register("ingest", |_, request: Request| async move {
        let params: BorrowedAttachment<'_> = request
            .params_as_borrowed()
            .map_err(|error| Error::invalid_params().with_data(error.to_string()))?;
        assert!(matches!(params.data, std::borrow::Cow::Borrowed(_)));
        Ok(params.data.len())
    });
    let cases = [
        ("slice_owned", &owned, false),
        ("slice_borrowed", &borrowed, false),
        ("shared_owned", &owned, true),
        ("shared_borrowed", &borrowed, true),
    ];
    let dispatch = |router: &Router<()>, shared: bool| {
        let output = if shared {
            runtime.block_on(router.handle_shared_bytes(std::hint::black_box(body.clone())))
        } else {
            runtime.block_on(router.handle_bytes(std::hint::black_box(&body)))
        };
        output.to_bytes().unwrap().unwrap()
    };
    if let Ok(name) = std::env::var("JSONRPC_COMPARE_OWNERSHIP_ALLOC") {
        let (_, router, shared) = cases.iter().find(|(case, _, _)| *case == name).unwrap();
        for _ in 0..10 {
            std::hint::black_box(dispatch(router, *shared));
        }
        return;
    }
    for (_, router, shared) in &cases {
        let response = dispatch(router, *shared);
        let response: jasonrpc::Response = jasonrpc::json::from_slice(&response).unwrap();
        assert_eq!(response.result_as::<usize>().unwrap(), size);
    }
    let mut group = criterion.benchmark_group("request_ownership");
    group.sample_size(20);
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.throughput(Throughput::Bytes(body.len() as u64));
    for (name, router, shared) in cases {
        group.bench_function(BenchmarkId::new(name, size), |bench| {
            bench.iter(|| std::hint::black_box(dispatch(router, shared)));
        });
    }
    group.finish();
}

fn comparison(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if std::env::var_os("JSONRPC_COMPARE_OWNERSHIP").is_some() {
        request_ownership(criterion, &runtime);
        return;
    }
    let mut engines = engines();
    if std::env::var_os("JSONRPC_COMPARE_REVERSE").is_some() {
        engines.reverse();
    }
    let cases = cases();

    for (name, engine) in &engines {
        for case in &cases {
            let response = runtime.block_on(engine.dispatch(&case.request));
            let value: serde_json::Value = serde_json::from_slice(response.as_bytes()).unwrap();
            assert_eq!(
                value, case.expected,
                "{name}/{} returned different output",
                case.name
            );
        }
    }

    #[cfg(feature = "serde-json")]
    if std::env::var_os("JSONRPC_COMPARE_STAGES").is_some() {
        let case = cases
            .iter()
            .find(|case| case.name == "echo_point_64k")
            .unwrap();
        echo_stages(criterion, case);
        return;
    }

    if let Ok(name) = std::env::var("JSONRPC_COMPARE_ALLOC") {
        let case_name = std::env::var("JSONRPC_COMPARE_CASE").unwrap_or_else(|_| "add".into());
        let (_, engine) = engines
            .iter()
            .find(|(engine_name, _)| *engine_name == name)
            .unwrap();
        let case = cases.iter().find(|case| case.name == case_name).unwrap();
        for _ in 0..1000 {
            let reply = runtime.block_on(engine.dispatch(&case.request));
            std::hint::black_box(reply.as_bytes());
        }
        return;
    }

    let mut group = criterion.benchmark_group("crate_dispatch");
    group.sample_size(50);
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(3));
    for case in &cases {
        group.throughput(Throughput::Bytes(case.request.len() as u64));
        for (name, engine) in &engines {
            group.bench_function(BenchmarkId::new(case.name, name), |bench| {
                bench.iter(|| {
                    let response =
                        runtime.block_on(engine.dispatch(std::hint::black_box(&case.request)));
                    std::hint::black_box(response.as_bytes());
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, comparison);
criterion_main!(benches);
