//! Criterion coverage for concurrent monotonic identifier allocation.

use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use openfga_domain::{ConsistencyPreference, Deadline, RequestTimeout};
use openfga_service::{IdentifierSource, SystemIdentifierSource, SystemIdentifierSourceConfig};
use openfga_storage::{OperationContext, StorageCancellationToken};
use tokio::{runtime::Runtime, task::JoinSet};

const ALLOCATION_BURST: usize = 100;

fn benchmark_identifier_allocation(criterion: &mut Criterion) {
    let fixture = Fixture::build();
    assert!(
        fixture.is_ok(),
        "identifier benchmark fixture must be valid: {:?}",
        fixture.as_ref().err(),
    );
    let Ok(mut fixture) = fixture else {
        return;
    };
    let mut group = criterion.benchmark_group("identifier_allocation");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements(ALLOCATION_BURST as u64));
    group.bench_function("model_id_burst_100", |bencher| {
        bencher.iter(|| fixture.allocate_model_ids());
    });
    group.finish();
    fixture.shutdown();
}

struct Fixture {
    runtime: Runtime,
    identifiers: Option<Arc<SystemIdentifierSource>>,
}

impl Fixture {
    fn build() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let runtime = Runtime::new()?;
        let identifiers = {
            let _runtime_guard = runtime.enter();
            Arc::new(SystemIdentifierSource::start(
                SystemIdentifierSourceConfig::default(),
            )?)
        };
        Ok(Self {
            runtime,
            identifiers: Some(identifiers),
        })
    }

    fn allocate_model_ids(&self) {
        let Some(identifiers) = self.identifiers.as_ref().map(Arc::clone) else {
            return;
        };
        let result = self.runtime.block_on(async move {
            let mut allocations = JoinSet::new();
            for _ in 0..ALLOCATION_BURST {
                let identifiers = Arc::clone(&identifiers);
                allocations.spawn(async move {
                    identifiers
                        .next_model_id(&operation_context().map_err(|error| error.to_string())?)
                        .await
                        .map_err(|error| error.to_string())
                });
            }
            while let Some(allocation) = allocations.join_next().await {
                allocation.map_err(|error| error.to_string())??;
            }
            Ok::<(), String>(())
        });
        assert!(
            result.is_ok(),
            "identifier benchmark allocation must succeed"
        );
    }

    fn shutdown(&mut self) {
        let Some(identifiers) = self.identifiers.take() else {
            return;
        };
        let identifiers = Arc::try_unwrap(identifiers);
        assert!(
            identifiers.is_ok(),
            "identifier benchmark references must drain"
        );
        let Ok(mut identifiers) = identifiers else {
            return;
        };
        let result = self.runtime.block_on(identifiers.stop());
        assert!(result.is_ok(), "identifier benchmark must stop cleanly");
    }
}

fn operation_context() -> Result<OperationContext, Box<dyn Error + Send + Sync>> {
    let timeout = RequestTimeout::new(Duration::from_secs(30))?;
    let deadline = Deadline::from_timeout(Instant::now(), timeout)?;
    Ok(OperationContext::new(
        ConsistencyPreference::HigherConsistency,
        deadline,
        StorageCancellationToken::new(),
    ))
}

criterion_group!(benches, benchmark_identifier_allocation);
criterion_main!(benches);
