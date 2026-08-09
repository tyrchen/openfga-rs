//! Criterion coverage for multi-limit authorization-model compilation.

use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use openfga_model::{AuthorizationModelSource, ModelCompiler};
use tokio::{runtime::Runtime, task::JoinSet};

mod support;

const LATENCY_GATE_SAMPLES: usize = 2_000;
const MODEL_COMPILE_P95_LIMIT: Duration = Duration::from_millis(250);
const COMPILATION_BURST: usize = 100;

fn benchmark_model_compile(criterion: &mut Criterion) {
    let source = support::maximum_supported_model();
    assert!(source.is_ok(), "benchmark model fixture must be valid");
    let Ok(source) = source else {
        return;
    };
    let compiler = ModelCompiler::default();
    gate_p95(&compiler, &source);
    let mut group = criterion.benchmark_group("authorization_model_compile");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("maximum_supported_limits", |bencher| {
        bencher.iter(|| {
            let result = compiler.compile(black_box(&source));
            assert!(result.is_ok(), "benchmark compilation must remain valid");
            black_box(result.ok())
        });
    });
    group.finish();

    let runtime = Runtime::new();
    assert!(runtime.is_ok(), "benchmark runtime must start");
    let Ok(runtime) = runtime else {
        return;
    };
    let source = Arc::new(source);
    let compiler = Arc::new(compiler);
    let mut group = criterion.benchmark_group("authorization_model_compile_concurrent");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements(COMPILATION_BURST as u64));
    group.bench_function("maximum_supported_burst_100", |bencher| {
        bencher.iter(|| compile_burst(&runtime, Arc::clone(&compiler), Arc::clone(&source)));
    });
    group.finish();
}

fn compile_burst(
    runtime: &Runtime,
    compiler: Arc<ModelCompiler>,
    source: Arc<AuthorizationModelSource>,
) {
    let result = runtime.block_on(async move {
        let mut compilations = JoinSet::new();
        for _ in 0..COMPILATION_BURST {
            let compiler = Arc::clone(&compiler);
            let source = Arc::clone(&source);
            compilations.spawn(async move {
                compiler
                    .compile(&source)
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            });
        }
        while let Some(compilation) = compilations.join_next().await {
            compilation.map_err(|error| error.to_string())??;
        }
        Ok::<(), String>(())
    });
    assert!(result.is_ok(), "concurrent model compilation must succeed");
}

fn gate_p95(compiler: &ModelCompiler, source: &openfga_model::AuthorizationModelSource) {
    let mut samples = Vec::with_capacity(LATENCY_GATE_SAMPLES);
    for _ in 0..LATENCY_GATE_SAMPLES {
        let started_at = Instant::now();
        let result = compiler.compile(black_box(source));
        samples.push(started_at.elapsed());
        assert!(result.is_ok(), "benchmark compilation must remain valid");
        black_box(result.ok());
    }
    samples.sort_unstable();
    let rank = samples
        .len()
        .saturating_mul(95)
        .div_ceil(100)
        .saturating_sub(1);
    let Some(p95) = samples.get(rank).copied() else {
        return;
    };
    assert!(
        p95 <= MODEL_COMPILE_P95_LIMIT,
        "maximum-model compilation p95 {p95:?} exceeded {MODEL_COMPILE_P95_LIMIT:?}",
    );
    println!("maximum-model compilation individual-operation p95: {p95:?}");
}

criterion_group!(benches, benchmark_model_compile);
criterion_main!(benches);
