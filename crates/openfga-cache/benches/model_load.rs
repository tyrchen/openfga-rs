//! Criterion coverage for cold and warm explicit authorization-model loads.

use std::{
    error::Error,
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use openfga_cache::{CachedModelStorage, ModelCacheConfig};
use openfga_domain::{
    AuthorizationModelId, ConsistencyPreference, Deadline, RelationName, RequestTimeout, StoreId,
    TypeName,
};
use openfga_model::{
    AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_storage::{
    ModelReader, ModelWriter, OperationContext, StorageCancellationToken, StoredAuthorizationModel,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
use tokio::runtime::Runtime;

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
const LATENCY_SAMPLES: usize = 2_000;

fn benchmark_model_load(criterion: &mut Criterion) {
    let fixture = Fixture::build();
    assert!(
        fixture.is_ok(),
        "model-load benchmark fixture must be valid: {:?}",
        fixture.as_ref().err(),
    );
    let Ok(fixture) = fixture else {
        return;
    };
    sample_p95(&fixture, true, "cold explicit model load");
    sample_p95(&fixture, false, "warm explicit model-cache hit");

    let mut group = criterion.benchmark_group("authorization_model_load");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("cold_explicit", |bencher| {
        bencher.iter_batched(
            || fixture.fresh_cache(),
            |cache| fixture.read(&cache),
            BatchSize::SmallInput,
        );
    });
    let warm = fixture.fresh_cache();
    fixture.read(&warm);
    group.bench_function("warm_explicit", |bencher| {
        bencher.iter(|| fixture.read(black_box(&warm)));
    });
    group.finish();
    drop(warm);
    fixture.shutdown();
}

fn sample_p95(fixture: &Fixture, cold: bool, workload: &str) {
    let warm = (!cold).then(|| {
        let cache = fixture.fresh_cache();
        fixture.read(&cache);
        cache
    });
    let mut samples = Vec::with_capacity(LATENCY_SAMPLES);
    for _ in 0..LATENCY_SAMPLES {
        let cache = warm.clone().unwrap_or_else(|| fixture.fresh_cache());
        let started_at = Instant::now();
        fixture.read(&cache);
        samples.push(started_at.elapsed());
    }
    samples.sort_unstable();
    let rank = samples
        .len()
        .saturating_mul(95)
        .div_ceil(100)
        .saturating_sub(1);
    if let Some(p95) = samples.get(rank) {
        println!("{workload} individual-operation p95: {p95:?}");
    }
}

struct Fixture {
    runtime: Runtime,
    storage: Arc<MemoryStorage>,
    store_id: StoreId,
    model_id: AuthorizationModelId,
}

impl Fixture {
    fn build() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let runtime = Runtime::new()?;
        let storage = {
            let _runtime_guard = runtime.enter();
            Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?)
        };
        let store_id = STORE_ID.parse()?;
        let model_id = MODEL_ID.parse()?;
        let model = stored_model(store_id, model_id)?;
        runtime.block_on(storage.write_model(&operation_context()?, model))?;
        Ok(Self {
            runtime,
            storage,
            store_id,
            model_id,
        })
    }

    fn fresh_cache(&self) -> CachedModelStorage {
        let reader: Arc<dyn ModelReader> = self.storage.clone();
        let writer: Arc<dyn ModelWriter> = self.storage.clone();
        CachedModelStorage::new(
            reader,
            writer,
            ModelCompiler::default(),
            ModelCacheConfig::default(),
        )
    }

    fn read(&self, cache: &CachedModelStorage) {
        let context = operation_context();
        assert!(context.is_ok(), "benchmark operation context must be valid");
        let Ok(context) = context else {
            return;
        };
        let result = self.runtime.block_on(cache.read_model(
            black_box(&context),
            self.store_id,
            self.model_id,
        ));
        assert!(result.is_ok(), "benchmark model load must succeed");
        black_box(result.ok());
    }

    fn shutdown(self) {
        let Self {
            runtime,
            storage,
            store_id: _,
            model_id: _,
        } = self;
        let storage = Arc::try_unwrap(storage);
        assert!(storage.is_ok(), "benchmark storage references must drain");
        let Ok(mut storage) = storage else {
            return;
        };
        let result = runtime.block_on(storage.stop());
        assert!(result.is_ok(), "benchmark storage must stop cleanly");
    }
}

fn operation_context() -> Result<OperationContext, Box<dyn Error + Send + Sync>> {
    let timeout = RequestTimeout::new(Duration::from_secs(30))?;
    let deadline = Deadline::from_timeout(Instant::now(), timeout)?;
    Ok(OperationContext::new(
        ConsistencyPreference::MinimizeLatency,
        deadline,
        StorageCancellationToken::new(),
    ))
}

fn stored_model(
    store_id: StoreId,
    model_id: AuthorizationModelId,
) -> Result<Arc<StoredAuthorizationModel>, Box<dyn Error + Send + Sync>> {
    let source = Arc::new(AuthorizationModelSource::new(
        store_id,
        model_id,
        "1.1".to_owned(),
        vec![
            TypeDefinitionSource::new("user".parse::<TypeName>()?, Vec::new()),
            TypeDefinitionSource::new(
                "document".parse::<TypeName>()?,
                vec![RelationSource::new(
                    "viewer".parse::<RelationName>()?,
                    RewriteSource::Direct,
                    vec![DirectRestrictionSource::new(
                        "user".parse::<TypeName>()?,
                        RestrictionKindSource::Object,
                        None,
                    )],
                )],
            ),
        ],
        Vec::new(),
    ));
    let compiled = ModelCompiler::default().compile(&source)?;
    Ok(Arc::new(StoredAuthorizationModel::new(
        source,
        compiled,
        SystemTime::now(),
    )?))
}

criterion_group!(benches, benchmark_model_load);
criterion_main!(benches);
