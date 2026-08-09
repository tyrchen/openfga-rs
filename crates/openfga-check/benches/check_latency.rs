//! Criterion coverage for direct in-memory evaluation and decision-cache hits.

use std::{
    error::Error,
    hint::black_box,
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{Criterion, criterion_group, criterion_main};
use openfga_cache::{DecisionCache, DecisionCacheConfig, DecisionKeyHasher, InvalidationWatermark};
use openfga_check::{
    CachedCheckEvaluator, CheckBudget, CheckCoalescingConfig, CheckCoalescingMode, CheckEvaluator,
    CoalescingCheckEvaluator, DirectCheckEvaluator,
};
use openfga_domain::{
    AuthorizationModelId, CheckCommand, ConditionContext, ConsistencyPreference, ContextualTuples,
    Deadline, InputLimits, ModelSelection, Principal, PrincipalKind, QueryContext,
    RelationshipTuple, RequestTimeout, StoreId,
};
use openfga_model::{
    AuthorizationModelSource, DirectRestrictionSource, ModelCompiler, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};
use openfga_storage::{
    OperationContext, StorageCancellationToken, StoreName, StoreWriter, TupleWriteOptions,
    TupleWriter,
};
use openfga_storage_memory::{MemoryStorage, MemoryStorageConfig};
use tokio::runtime::Runtime;

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
const LATENCY_GATE_SAMPLES: usize = 10_000;
const DIRECT_CHECK_P95_LIMIT: Duration = Duration::from_millis(1);
const DECISION_CACHE_P95_LIMIT: Duration = Duration::from_micros(250);

fn benchmark_check_latency(criterion: &mut Criterion) {
    let fixture = Fixture::build();
    assert!(
        fixture.is_ok(),
        "Check benchmark fixture must be valid: {:?}",
        fixture.as_ref().err(),
    );
    let Ok(fixture) = fixture else {
        return;
    };
    fixture.prime_cache();
    gate_p95(
        &fixture,
        &fixture.direct,
        "direct in-memory Check",
        DIRECT_CHECK_P95_LIMIT,
    );
    gate_p95(
        &fixture,
        &fixture.cached,
        "warm decision-cache hit",
        DECISION_CACHE_P95_LIMIT,
    );

    let mut group = criterion.benchmark_group("check_latency");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("direct_in_memory", |bencher| {
        bencher.iter(|| fixture.run(&fixture.direct));
    });
    group.bench_function("warm_decision_cache_hit", |bencher| {
        bencher.iter(|| fixture.run(&fixture.cached));
    });
    group.bench_function("identical_burst_direct_32", |bencher| {
        bencher.iter(|| fixture.run_burst(&fixture.direct, 32));
    });
    group.bench_function("identical_burst_coalesced_32", |bencher| {
        bencher.iter(|| fixture.run_burst(&fixture.coalesced, 32));
    });
    group.finish();
    fixture.shutdown();
}

fn gate_p95(
    fixture: &Fixture,
    evaluator: &Arc<dyn CheckEvaluator>,
    workload: &str,
    limit: Duration,
) {
    let mut samples = Vec::with_capacity(LATENCY_GATE_SAMPLES);
    for _ in 0..LATENCY_GATE_SAMPLES {
        let started_at = Instant::now();
        fixture.run(evaluator);
        samples.push(started_at.elapsed());
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
    assert!(p95 <= limit, "{workload} p95 {p95:?} exceeded {limit:?}");
    println!("{workload} individual-operation p95: {p95:?}");
}

struct Fixture {
    runtime: Runtime,
    storage: Arc<MemoryStorage>,
    model: Arc<openfga_model::CompiledModel>,
    command: CheckCommand,
    direct: Arc<dyn CheckEvaluator>,
    cached: Arc<dyn CheckEvaluator>,
    coalesced: Arc<dyn CheckEvaluator>,
}

impl Fixture {
    fn build() -> Result<Self, Box<dyn Error>> {
        let runtime = Runtime::new()?;
        let storage = {
            let _runtime_guard = runtime.enter();
            Arc::new(MemoryStorage::start(MemoryStorageConfig::default())?)
        };
        let operation = operation_context()?;
        runtime.block_on(async {
            storage
                .create_store(
                    &operation,
                    STORE_ID.parse()?,
                    StoreName::new("criterion-check".to_owned())?,
                )
                .await?;
            storage
                .write_tuples(
                    &operation,
                    STORE_ID.parse()?,
                    Vec::new(),
                    vec![RelationshipTuple::unconditional(
                        "document:criterion#viewer@user:anne".parse()?,
                    )],
                    TupleWriteOptions::default(),
                )
                .await?;
            Ok::<(), Box<dyn Error>>(())
        })?;
        let model = ModelCompiler::default().compile(&direct_model()?)?;
        let command = CheckCommand::new(
            query_context()?,
            "document:criterion#viewer@user:anne".parse()?,
        );
        let direct: Arc<dyn CheckEvaluator> = Arc::new(DirectCheckEvaluator::default());
        let cached: Arc<dyn CheckEvaluator> = Arc::new(CachedCheckEvaluator::new(
            Arc::clone(&direct),
            DecisionCache::new(
                DecisionCacheConfig::new(
                    NonZeroU64::new(16 * 1_024 * 1_024).ok_or("invalid cache capacity")?,
                    Duration::from_mins(1),
                )?,
                InvalidationWatermark::new(),
            ),
            DecisionKeyHasher::random()?,
            InputLimits::default(),
        ));
        let coalesced: Arc<dyn CheckEvaluator> = Arc::new(CoalescingCheckEvaluator::new(
            Arc::clone(&direct),
            CheckCoalescingConfig::new(
                CheckCoalescingMode::Enabled,
                NonZeroU64::new(4_096).ok_or("invalid coalescing capacity")?,
            )?,
            DecisionKeyHasher::random()?,
        ));
        Ok(Self {
            runtime,
            storage,
            model,
            command,
            direct,
            cached,
            coalesced,
        })
    }

    fn prime_cache(&self) {
        self.run(&self.cached);
    }

    fn run(&self, evaluator: &Arc<dyn CheckEvaluator>) {
        let outcome = self.runtime.block_on(evaluator.check(
            black_box(&self.command),
            Arc::clone(&self.model),
            self.storage.clone(),
            CheckBudget::default(),
            None,
            StorageCancellationToken::new(),
        ));
        assert!(
            outcome.as_ref().is_ok_and(|outcome| outcome.allowed()),
            "benchmark Check must remain an allow",
        );
        black_box(outcome.ok());
    }

    fn run_burst(&self, evaluator: &Arc<dyn CheckEvaluator>, callers: usize) {
        let outcomes = self.runtime.block_on(async {
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..callers {
                let evaluator = Arc::clone(evaluator);
                let command = self.command.clone();
                let model = Arc::clone(&self.model);
                let tuples = self.storage.clone();
                tasks.spawn(async move {
                    evaluator
                        .check(
                            &command,
                            model,
                            tuples,
                            CheckBudget::default(),
                            None,
                            StorageCancellationToken::new(),
                        )
                        .await
                });
            }
            let mut outcomes = Vec::with_capacity(callers);
            while let Some(outcome) = tasks.join_next().await {
                outcomes.push(outcome);
            }
            outcomes
        });
        assert_eq!(outcomes.len(), callers);
        assert!(outcomes.iter().all(|outcome| {
            outcome
                .as_ref()
                .is_ok_and(|outcome| outcome.as_ref().is_ok_and(|result| result.allowed()))
        }));
        black_box(outcomes);
    }

    fn shutdown(self) {
        drop(self.cached);
        drop(self.coalesced);
        drop(self.direct);
        let storage = Arc::try_unwrap(self.storage);
        assert!(storage.is_ok(), "benchmark storage references must drain");
        if let Ok(mut storage) = storage {
            let stopped = self.runtime.block_on(storage.stop());
            assert!(stopped.is_ok(), "benchmark storage must stop cleanly");
        }
    }
}

fn operation_context() -> Result<OperationContext, Box<dyn Error>> {
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        Deadline::from_timeout(Instant::now(), RequestTimeout::new(Duration::from_mins(5))?)?,
        StorageCancellationToken::new(),
    ))
}

fn query_context() -> Result<QueryContext, Box<dyn Error>> {
    Ok(QueryContext::builder()
        .store_id(STORE_ID.parse::<StoreId>()?)
        .model_selection(ModelSelection::Explicit(
            MODEL_ID.parse::<AuthorizationModelId>()?,
        ))
        .consistency(ConsistencyPreference::MinimizeLatency)
        .contextual_tuples(ContextualTuples::empty())
        .condition_context(ConditionContext::empty())
        .deadline(Deadline::from_timeout(
            Instant::now(),
            RequestTimeout::new(Duration::from_mins(5))?,
        )?)
        .principal(Principal::new(
            PrincipalKind::Internal,
            "criterion-check".parse()?,
        ))
        .build())
}

fn direct_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    Ok(AuthorizationModelSource::new(
        STORE_ID.parse()?,
        MODEL_ID.parse()?,
        "1.1".to_owned(),
        vec![
            TypeDefinitionSource::new("user".parse()?, Vec::new()),
            TypeDefinitionSource::new(
                "document".parse()?,
                vec![RelationSource::new(
                    "viewer".parse()?,
                    RewriteSource::Direct,
                    vec![DirectRestrictionSource::new(
                        "user".parse()?,
                        RestrictionKindSource::Object,
                        None,
                    )],
                )],
            ),
        ],
        Vec::new(),
    ))
}

criterion_group!(benches, benchmark_check_latency);
criterion_main!(benches);
