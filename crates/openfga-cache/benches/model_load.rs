//! Criterion coverage for cold and warm explicit authorization-model loads.

use std::{
    cell::Cell,
    error::Error,
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
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
use tokio::{runtime::Runtime, task::JoinSet};
use ulid::Ulid;

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
const LATENCY_SAMPLES: usize = 2_000;
const PUBLICATION_BURST: usize = 100;

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

fn benchmark_model_publication(criterion: &mut Criterion) {
    let fixture = Fixture::build();
    assert!(
        fixture.is_ok(),
        "model-publication benchmark fixture must be valid: {:?}",
        fixture.as_ref().err(),
    );
    let Ok(fixture) = fixture else {
        return;
    };
    let storage_writer: Arc<dyn ModelWriter> = fixture.storage.clone();
    let cached_writer: Arc<dyn ModelWriter> = Arc::new(fixture.fresh_cache());
    let next_model = Cell::new(1_u128 << 80);
    let mut group = criterion.benchmark_group("authorization_model_publication");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements(PUBLICATION_BURST as u64));
    group.bench_function("memory_storage_burst_100", |bencher| {
        bencher.iter_batched(
            || fixture.publication_models(&next_model),
            |models| fixture.publish(Arc::clone(&storage_writer), models),
            BatchSize::LargeInput,
        );
    });
    group.bench_function("cached_storage_burst_100", |bencher| {
        bencher.iter_batched(
            || fixture.publication_models(&next_model),
            |models| fixture.publish(Arc::clone(&cached_writer), models),
            BatchSize::LargeInput,
        );
    });
    group.finish();
    drop(cached_writer);
    drop(storage_writer);
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
    compiler: ModelCompiler,
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
            compiler: ModelCompiler::default(),
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
            self.compiler.clone(),
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

    fn publication_models(&self, next_model: &Cell<u128>) -> Vec<Arc<StoredAuthorizationModel>> {
        let models = (0..PUBLICATION_BURST)
            .map(|_| {
                let sequence = next_model.get();
                next_model.set(sequence.saturating_add(1));
                stored_model(
                    self.store_id,
                    AuthorizationModelId::from_ulid(Ulid::from(sequence)),
                )
            })
            .collect::<Result<Vec<_>, _>>();
        assert!(models.is_ok(), "publication model fixtures must be valid");
        models.unwrap_or_default()
    }

    fn publish(&self, writer: Arc<dyn ModelWriter>, models: Vec<Arc<StoredAuthorizationModel>>) {
        let result = self.runtime.block_on(async move {
            let mut tasks = JoinSet::new();
            for model in models {
                let writer = Arc::clone(&writer);
                tasks.spawn(async move {
                    let context = operation_context().map_err(|error| error.to_string())?;
                    writer
                        .write_model(&context, model)
                        .await
                        .map_err(|error| error.to_string())
                });
            }
            while let Some(write) = tasks.join_next().await {
                write.map_err(|error| error.to_string())??;
            }
            Ok::<(), String>(())
        });
        assert!(result.is_ok(), "model-publication benchmark must succeed");
    }

    fn shutdown(self) {
        let Self {
            runtime,
            storage,
            compiler: _,
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

criterion_group!(benches, benchmark_model_load, benchmark_model_publication);
criterion_main!(benches);
