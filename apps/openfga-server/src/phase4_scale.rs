//! Reproducible Phase 4 consistency, load, soak, and reference measurements.

use std::{
    str::FromStr,
    sync::Arc,
    thread::available_parallelism,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use openfga_domain::AuthorizationModelId;
use openfga_proto::openfga::v1 as pb;
use reqwest::{Client, StatusCode, Url, redirect::Policy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{process::Command, task::JoinSet, time::Instant as TokioInstant};

use crate::{
    check_probe::{
        GO_BASELINE_COMMIT, configure_differential_server, go_model_document, read_bounded,
        require_success,
    },
    validated_loopback_url, write_value,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAXIMUM_ITERATIONS: usize = 10_000;
const MAXIMUM_CLIENTS: usize = 1_000;
const MAXIMUM_REQUESTS: usize = 1_000_000;
const MAXIMUM_SOAK_SECONDS: u64 = 86_400;
const WARM_REQUESTS: usize = 32;
const WARM_RECOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const WARM_RECOVERY_INTERVAL: Duration = Duration::from_millis(10);
const ENUMERATION_REQUESTS_PER_CLIENT: usize = 5;
const SOAK_CLIENT_INTERVAL: Duration = Duration::from_millis(10);
const REFERENCE_CLIENTS: [usize; 3] = [1, 10, 100];
const POST_DRAIN_SETTLE: Duration = Duration::from_secs(2);
const POST_DRAIN_TASK_TOLERANCE: usize = 8;
const READINESS_RECOVERY_TIMEOUT: Duration = Duration::from_secs(20);
const READINESS_STABILITY_WINDOW: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
struct Target {
    name: &'static str,
    base_url: Url,
    store_id: String,
    model_id: String,
    process_id: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckResult {
    Allowed,
    Denied,
    Overloaded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReferenceSemantic {
    CheckAllowed,
    BatchCheck(Vec<(String, bool)>),
    ListObjects(Vec<String>),
    ListUsers(Vec<String>),
    ModelLoaded,
    ModelPublished,
    TupleWritten,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReferenceResult {
    Success(ReferenceSemantic),
    Overloaded,
}

#[derive(Debug)]
struct MeasuredReference {
    measurement: BenchmarkMeasurement,
    semantic: Option<ReferenceSemantic>,
}

#[derive(Clone, Copy, Debug)]
struct ProcessSnapshot {
    cpu_time_microseconds: u64,
    rss_kib: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkReport {
    baseline_commit: &'static str,
    maximum_requests_per_client: usize,
    measurements: Vec<BenchmarkMeasurement>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkMeasurement {
    workload: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    residual_check_ratio_percent: Option<u8>,
    cache_state: &'static str,
    store_state: &'static str,
    implementation: &'static str,
    clients: usize,
    total_requests: usize,
    allowed: usize,
    overloaded: usize,
    elapsed_milliseconds: u64,
    requests_per_second: u64,
    allowed_p50_microseconds: Option<u64>,
    allowed_p95_microseconds: Option<u64>,
    allowed_p99_microseconds: Option<u64>,
    process_cpu_percent: Option<f64>,
    process_rss_kib: u64,
}

#[derive(Clone, Copy, Debug)]
enum ReferenceOperation {
    Check {
        object: &'static str,
        relation: &'static str,
        user: &'static str,
        contextual: bool,
    },
    BatchCheck,
    ListObjects {
        relation: &'static str,
    },
    ListUsers,
    ModelLoad,
    ModelCompileAndPublish,
    TupleWriteAndChangelog,
}

#[derive(Clone, Copy, Debug)]
struct ReferenceWorkload {
    name: &'static str,
    operation: ReferenceOperation,
    residual_check_ratio_percent: Option<u8>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConsistencyReport {
    baseline_commit: &'static str,
    mutation_path: &'static str,
    concurrent_sequences: usize,
    higher_consistency_checks: usize,
    minimize_latency_checks: usize,
    stale_results: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SoakReport {
    baseline_commit: &'static str,
    consistency: &'static str,
    clients: usize,
    maximum_attempts_per_second: usize,
    requested_seconds: u64,
    elapsed_milliseconds: u64,
    readiness_probes: u64,
    allowed: u64,
    overloaded: u64,
    requests_per_second: u64,
    maximum_request_microseconds: u64,
    resources: ResourceReport,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CapacitySnapshot {
    runtime_tasks: usize,
    endpoint_permits_available: usize,
    endpoint_permits_capacity: usize,
    storage_work_permits_available: Option<usize>,
    storage_work_permits_capacity: Option<usize>,
    primary_pool_open: Option<u32>,
    primary_pool_idle: Option<usize>,
    primary_pool_capacity: Option<u32>,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct CapacityHighWater {
    runtime_tasks: usize,
    endpoint_permits_in_flight: usize,
    storage_work_permits_in_flight: usize,
    primary_pool_open: u32,
    primary_pool_in_use: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ResourceReport {
    samples: u64,
    baseline: CapacitySnapshot,
    high_water: CapacityHighWater,
    post_drain: CapacitySnapshot,
    post_drain_task_tolerance: usize,
}

#[derive(Debug)]
struct ResourceObservations {
    readiness_probes: u64,
    samples: u64,
    baseline: CapacitySnapshot,
    high_water: CapacityHighWater,
}

#[derive(Clone, Copy, Debug, Default)]
struct SoakCounts {
    allowed: u64,
    overloaded: u64,
    maximum_request_microseconds: u64,
}

/// Runs concurrent write/check/delete sequences against the complete Rust server.
pub(crate) async fn run_consistency_faults(
    rust_url: &str,
    writer_url: Option<&str>,
    iterations: usize,
) -> Result<()> {
    validate_count("consistency iterations", iterations, MAXIMUM_ITERATIONS)?;
    let client = build_client()?;
    let reader = configure_target(&client, "rust", rust_url, 0).await?;
    let writer = match writer_url {
        Some(writer_url) => Target {
            name: "rust-writer",
            base_url: validated_loopback_url(writer_url)?,
            store_id: reader.store_id.clone(),
            model_id: reader.model_id.clone(),
            process_id: 0,
        },
        None => reader.clone(),
    };
    warm(&client, &reader).await?;
    let mut tasks = JoinSet::new();
    for iteration in 0..iterations {
        let client = client.clone();
        let reader = reader.clone();
        let writer = writer.clone();
        tasks
            .spawn(async move { consistency_sequence(&client, &reader, &writer, iteration).await });
    }
    let mut higher_consistency_checks = 0_usize;
    let mut minimize_latency_checks = 0_usize;
    while let Some(joined) = tasks.join_next().await {
        let (higher_checks, minimize_checks) =
            joined.context("a consistency fault worker panicked or was cancelled")??;
        higher_consistency_checks = higher_consistency_checks
            .checked_add(higher_checks)
            .context("consistency check count overflowed")?;
        minimize_latency_checks = minimize_latency_checks
            .checked_add(minimize_checks)
            .context("minimize-latency consistency check count overflowed")?;
    }
    write_value(&ConsistencyReport {
        baseline_commit: GO_BASELINE_COMMIT,
        mutation_path: if writer_url.is_some() {
            "independent writer process through shared PostgreSQL changelog"
        } else {
            "reader process local mutation"
        },
        concurrent_sequences: iterations,
        higher_consistency_checks,
        minimize_latency_checks,
        stale_results: 0,
    })
}

/// Runs the bounded semantic workload matrix against Go and Rust.
pub(crate) async fn run_reference_benchmark(
    go_url: &str,
    rust_url: &str,
    go_pid: u32,
    rust_pid: u32,
    requests_per_client: usize,
) -> Result<()> {
    validate_count(
        "requests per client",
        requests_per_client,
        MAXIMUM_ITERATIONS,
    )?;
    let client = build_client()?;
    let go = configure_target(&client, "go", go_url, go_pid).await?;
    let rust = configure_target(&client, "rust", rust_url, rust_pid).await?;
    seed_scale_dataset(&client, &go).await?;
    seed_scale_dataset(&client, &rust).await?;
    let go_cold_store = configure_target(&client, "go", go_url, go_pid).await?;
    let rust_cold_store = configure_target(&client, "rust", rust_url, rust_pid).await?;
    seed_scale_dataset(&client, &go_cold_store).await?;
    seed_scale_dataset(&client, &rust_cold_store).await?;
    let workloads = reference_workloads();
    let mut measurements = Vec::with_capacity(
        workloads
            .len()
            .saturating_mul(REFERENCE_CLIENTS.len().saturating_add(1))
            .saturating_mul(2),
    );
    measurements.push(measure_invalidated_check(&client, &go).await?);
    measurements.push(measure_invalidated_check(&client, &rust).await?);
    measurements.extend(measure_cold_stores(&client, &go_cold_store, &rust_cold_store).await?);
    for workload in workloads {
        let initial_cache_state = if matches!(workload.operation, ReferenceOperation::ModelLoad) {
            "post-publish"
        } else {
            "cold"
        };
        let go_cold = measure(&client, &go, workload, initial_cache_state, "hot", 1, 1).await?;
        let rust_cold = measure(&client, &rust, workload, initial_cache_state, "hot", 1, 1).await?;
        compare_reference_semantics(
            workload.name,
            go_cold.semantic.as_ref(),
            rust_cold.semantic.as_ref(),
        )?;
        measurements.push(go_cold.measurement);
        measurements.push(rust_cold.measurement);
        let go_warm = warm_operation(&client, &go, workload).await?;
        let rust_warm = warm_operation(&client, &rust, workload).await?;
        compare_reference_semantics(workload.name, Some(&go_warm), Some(&rust_warm))?;
        let workload_requests = match workload.operation {
            ReferenceOperation::ModelCompileAndPublish
            | ReferenceOperation::TupleWriteAndChangelog => 1,
            ReferenceOperation::ListObjects { .. } | ReferenceOperation::ListUsers => {
                requests_per_client.min(ENUMERATION_REQUESTS_PER_CLIENT)
            }
            ReferenceOperation::Check { .. }
            | ReferenceOperation::BatchCheck
            | ReferenceOperation::ModelLoad => requests_per_client,
        };
        for clients in REFERENCE_CLIENTS {
            let go_measured = measure(
                &client,
                &go,
                workload,
                "warm",
                "hot",
                clients,
                workload_requests,
            )
            .await?;
            let rust_measured = measure(
                &client,
                &rust,
                workload,
                "warm",
                "hot",
                clients,
                workload_requests,
            )
            .await?;
            validate_measured_semantics(
                workload.name,
                go_measured.semantic.as_ref(),
                rust_measured.semantic.as_ref(),
                &go_warm,
            )?;
            measurements.push(go_measured.measurement);
            measurements.push(rust_measured.measurement);
        }
    }
    write_value(&BenchmarkReport {
        baseline_commit: GO_BASELINE_COMMIT,
        maximum_requests_per_client: requests_per_client,
        measurements,
    })
}

async fn measure_cold_stores(
    client: &Client,
    go: &Target,
    rust: &Target,
) -> Result<[BenchmarkMeasurement; 2]> {
    let workload = check_workload(
        "check_direct_cold_store",
        "document:direct",
        "viewer",
        "user:anne",
    );
    let go_measurement = measure(client, go, workload, "cold", "cold", 1, 1).await?;
    let rust_measurement = measure(client, rust, workload, "cold", "cold", 1, 1).await?;
    compare_reference_semantics(
        workload.name,
        go_measurement.semantic.as_ref(),
        rust_measurement.semantic.as_ref(),
    )?;
    Ok([go_measurement.measurement, rust_measurement.measurement])
}

async fn measure_invalidated_check(
    client: &Client,
    target: &Target,
) -> Result<BenchmarkMeasurement> {
    const OBJECT: &str = "document:phase4-invalidated";
    require_check(
        check(client, target, OBJECT, "user:anne", "MINIMIZE_LATENCY").await?,
        CheckResult::Denied,
        "pre-invalidation benchmark prime",
    )?;
    mutate_tuple(client, target, OBJECT, "user:anne", true).await?;
    Ok(measure(
        client,
        target,
        check_workload(
            "check_after_tuple_cache_invalidation",
            OBJECT,
            "viewer",
            "user:anne",
        ),
        "invalidated",
        "hot",
        1,
        1,
    )
    .await?
    .measurement)
}

fn reference_workloads() -> [ReferenceWorkload; 16] {
    [
        check_workload(
            "check_direct_exact",
            "document:direct",
            "viewer",
            "user:anne",
        ),
        check_workload(
            "check_recursive_userset",
            "document:cycle-userset-allow",
            "viewer",
            "user:anne",
        ),
        check_workload(
            "check_deep_recursive_userset",
            "document:deep",
            "viewer",
            "user:anne",
        ),
        check_workload("check_ttu", "document:ttu", "viewer", "user:anne"),
        check_workload(
            "check_wide_union",
            "document:wide",
            "wide_union",
            "user:anne",
        ),
        check_workload("check_intersection", "document:both", "both", "user:anne"),
        check_workload(
            "check_difference",
            "document:included",
            "allowed",
            "user:anne",
        ),
        check_workload(
            "check_conditioned_tuple",
            "document:condition",
            "conditional",
            "user:anne",
        ),
        ReferenceWorkload {
            name: "check_contextual_tuple",
            operation: ReferenceOperation::Check {
                object: "document:phase4-contextual",
                relation: "viewer",
                user: "user:carol",
                contextual: true,
            },
            residual_check_ratio_percent: None,
        },
        ReferenceWorkload {
            name: "batch_check_repeated_subproblems",
            operation: ReferenceOperation::BatchCheck,
            residual_check_ratio_percent: None,
        },
        ReferenceWorkload {
            name: "list_users_set_algebra",
            operation: ReferenceOperation::ListUsers,
            residual_check_ratio_percent: None,
        },
        ReferenceWorkload {
            name: "list_objects_reverse_only",
            operation: ReferenceOperation::ListObjects {
                relation: "reverse_only",
            },
            residual_check_ratio_percent: Some(0),
        },
        ReferenceWorkload {
            name: "list_objects_residual_heavy",
            operation: ReferenceOperation::ListObjects {
                relation: "residual_all",
            },
            residual_check_ratio_percent: Some(100),
        },
        ReferenceWorkload {
            name: "model_load_explicit_post_publish",
            operation: ReferenceOperation::ModelLoad,
            residual_check_ratio_percent: None,
        },
        ReferenceWorkload {
            name: "model_compile_and_publish",
            operation: ReferenceOperation::ModelCompileAndPublish,
            residual_check_ratio_percent: None,
        },
        ReferenceWorkload {
            name: "tuple_write_and_changelog",
            operation: ReferenceOperation::TupleWriteAndChangelog,
            residual_check_ratio_percent: None,
        },
    ]
}

async fn seed_scale_dataset(client: &Client, target: &Target) -> Result<()> {
    let mut tuples = Vec::with_capacity(100);
    for index in 0..70 {
        tuples.push(json!({
            "object": format!("document:dense-{index}"),
            "relation": "viewer",
            "user": "user:anne"
        }));
    }
    for index in 0..5 {
        tuples.push(json!({
            "object": format!("document:reverse-only-{index}"),
            "relation": "reverse_only",
            "user": "user:anne"
        }));
    }
    for index in 0..5 {
        tuples.push(json!({
            "object": format!("document:residual-all-{index}"),
            "relation": "residual_base",
            "user": "user:anne"
        }));
    }
    tuples.push(json!({
        "object": "document:deep",
        "relation": "viewer",
        "user": "group:deep-0#member"
    }));
    for index in 0_u8..16 {
        let user = if index == 15 {
            "user:anne".to_owned()
        } else {
            format!("group:deep-{}#member", index.saturating_add(1))
        };
        tuples.push(json!({
            "object": format!("group:deep-{index}"),
            "relation": "member",
            "user": user
        }));
    }
    tuples.extend([
        json!({
            "object": "document:phase4-list-users",
            "relation": "viewer",
            "user": "user:anne"
        }),
        json!({
            "object": "document:phase4-list-users",
            "relation": "viewer",
            "user": "user:bob"
        }),
        json!({
            "object": "document:phase4-list-users",
            "relation": "banned",
            "user": "user:anne"
        }),
    ]);
    let url = target
        .base_url
        .join(&format!("stores/{}/write", target.store_id))
        .context("failed to construct scale dataset URL")?;
    let response = client
        .post(url)
        .json(&json!({
            "writes": {"tuple_keys": tuples},
            "authorization_model_id": target.model_id
        }))
        .send()
        .await
        .context("scale dataset write failed")?;
    require_success(response, "scale dataset write").await
}

const fn check_workload(
    name: &'static str,
    object: &'static str,
    relation: &'static str,
    user: &'static str,
) -> ReferenceWorkload {
    ReferenceWorkload {
        name,
        operation: ReferenceOperation::Check {
            object,
            relation,
            user,
            contextual: false,
        },
        residual_check_ratio_percent: None,
    }
}

/// Runs a fixed-concurrency, bounded-duration Rust Check soak.
pub(crate) async fn run_soak(
    rust_url: &str,
    seconds: u64,
    clients: usize,
    higher_consistency: bool,
) -> Result<()> {
    if seconds == 0 || seconds > MAXIMUM_SOAK_SECONDS {
        bail!("soak seconds must be between 1 and {MAXIMUM_SOAK_SECONDS}");
    }
    validate_count("soak clients", clients, MAXIMUM_CLIENTS)?;
    let client = build_client()?;
    let target = configure_target(&client, "rust", rust_url, 0).await?;
    wait_for_readiness(&client, &target.base_url).await?;
    warm(&client, &target).await?;
    let duration = Duration::from_secs(seconds);
    let consistency = if higher_consistency {
        "HIGHER_CONSISTENCY"
    } else {
        "MINIMIZE_LATENCY"
    };
    let baseline = capture_idle_baseline(&client, &target.base_url).await?;
    let deadline = TokioInstant::now() + duration;
    let started_at = Instant::now();
    let readiness = tokio::spawn(monitor_readiness(
        target.base_url.clone(),
        deadline,
        baseline,
    ));
    let mut tasks = JoinSet::new();
    for _ in 0..clients {
        let client = client.clone();
        let target = target.clone();
        tasks.spawn(async move { soak_worker(&client, &target, deadline, consistency).await });
    }
    let mut counts = SoakCounts::default();
    while let Some(joined) = tasks.join_next().await {
        let worker = joined.context("a soak worker panicked or was cancelled")??;
        counts.allowed = counts
            .allowed
            .checked_add(worker.allowed)
            .context("soak allowed count overflowed")?;
        counts.overloaded = counts
            .overloaded
            .checked_add(worker.overloaded)
            .context("soak overload count overflowed")?;
        counts.maximum_request_microseconds = counts
            .maximum_request_microseconds
            .max(worker.maximum_request_microseconds);
    }
    let elapsed = started_at.elapsed();
    drop(client);
    let observations = readiness
        .await
        .context("the soak readiness monitor panicked or was cancelled")??;
    tokio::time::sleep(POST_DRAIN_SETTLE).await;
    let post_drain = capacity_snapshot(&build_client()?, &target.base_url).await?;
    validate_post_drain(&observations.baseline, &post_drain)?;
    let total = counts
        .allowed
        .checked_add(counts.overloaded)
        .context("soak request count overflowed")?;
    if counts.allowed == 0 {
        bail!("soak completed without a successful authorization result");
    }
    let maximum_attempts_per_second = clients
        .checked_mul(100)
        .context("soak attempt-rate bound overflowed")?;
    write_value(&SoakReport {
        baseline_commit: GO_BASELINE_COMMIT,
        consistency,
        clients,
        maximum_attempts_per_second,
        requested_seconds: seconds,
        elapsed_milliseconds: millis(elapsed),
        readiness_probes: observations.readiness_probes,
        allowed: counts.allowed,
        overloaded: counts.overloaded,
        requests_per_second: requests_per_second(total, elapsed),
        maximum_request_microseconds: counts.maximum_request_microseconds,
        resources: ResourceReport {
            samples: observations.samples,
            baseline: observations.baseline,
            high_water: observations.high_water,
            post_drain,
            post_drain_task_tolerance: POST_DRAIN_TASK_TOLERANCE,
        },
    })
}

async fn configure_target(
    client: &Client,
    name: &'static str,
    input: &str,
    process_id: u32,
) -> Result<Target> {
    let base_url = validated_loopback_url(input)?;
    let (store_id, model_id) = configure_differential_server(client, &base_url)
        .await
        .with_context(|| format!("failed to configure {name} benchmark target"))?;
    Ok(Target {
        name,
        base_url,
        store_id,
        model_id,
        process_id,
    })
}

async fn consistency_sequence(
    client: &Client,
    reader: &Target,
    writer: &Target,
    iteration: usize,
) -> Result<(usize, usize)> {
    let object = format!("document:phase4-consistency-{iteration}");
    require_check(
        check(client, reader, &object, "user:phase4", "MINIMIZE_LATENCY").await?,
        CheckResult::Denied,
        "initial cached deny",
    )?;
    mutate(client, writer, &object, true).await?;
    require_check(
        check(client, reader, &object, "user:phase4", "HIGHER_CONSISTENCY").await?,
        CheckResult::Allowed,
        "higher-consistency read after write",
    )?;
    let after_write = wait_for_check(
        client,
        reader,
        &object,
        CheckResult::Allowed,
        "minimize-latency convergence after write",
    )
    .await?;
    mutate(client, writer, &object, false).await?;
    require_check(
        check(client, reader, &object, "user:phase4", "HIGHER_CONSISTENCY").await?,
        CheckResult::Denied,
        "higher-consistency read after delete",
    )?;
    let after_delete = wait_for_check(
        client,
        reader,
        &object,
        CheckResult::Denied,
        "minimize-latency convergence after delete",
    )
    .await?;
    Ok((
        2,
        after_write
            .checked_add(after_delete)
            .context("minimize-latency consistency check count overflowed")?,
    ))
}

async fn wait_for_check(
    client: &Client,
    target: &Target,
    object: &str,
    expected: CheckResult,
    operation: &str,
) -> Result<usize> {
    let deadline = TokioInstant::now() + Duration::from_secs(15);
    let mut checks = 0_usize;
    loop {
        checks = checks
            .checked_add(1)
            .context("consistency convergence check count overflowed")?;
        let actual = check(client, target, object, "user:phase4", "MINIMIZE_LATENCY").await?;
        if actual == expected {
            return Ok(checks);
        }
        if TokioInstant::now() >= deadline {
            bail!("{operation} returned {actual:?}, expected {expected:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn mutate(client: &Client, target: &Target, object: &str, write: bool) -> Result<()> {
    mutate_tuple(client, target, object, "user:phase4", write).await
}

async fn mutate_tuple(
    client: &Client,
    target: &Target,
    object: &str,
    user: &str,
    write: bool,
) -> Result<()> {
    let tuple = json!({"object": object, "relation": "viewer", "user": user});
    let body = if write {
        json!({
            "writes": {"tuple_keys": [tuple]},
            "authorization_model_id": target.model_id,
        })
    } else {
        json!({
            "deletes": {"tuple_keys": [tuple]},
            "authorization_model_id": target.model_id,
        })
    };
    let url = target
        .base_url
        .join(&format!("stores/{}/write", target.store_id))
        .context("failed to construct consistency mutation URL")?;
    let response = client
        .post(url)
        .json(&body)
        .send()
        .await
        .context("consistency mutation request failed")?;
    require_success(response, "consistency mutation").await
}

async fn warm(client: &Client, target: &Target) -> Result<()> {
    for _ in 0..WARM_REQUESTS {
        require_check(
            check(
                client,
                target,
                "document:direct",
                "user:anne",
                "MINIMIZE_LATENCY",
            )
            .await?,
            CheckResult::Allowed,
            "benchmark warmup",
        )?;
    }
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    require_check(
        check(
            client,
            target,
            "document:direct",
            "user:anne",
            "MINIMIZE_LATENCY",
        )
        .await?,
        CheckResult::Allowed,
        "post-controller warmup",
    )
}

async fn measure(
    client: &Client,
    target: &Target,
    workload: ReferenceWorkload,
    cache_state: &'static str,
    store_state: &'static str,
    clients: usize,
    requests_per_client: usize,
) -> Result<MeasuredReference> {
    let total_requests = clients
        .checked_mul(requests_per_client)
        .filter(|total| *total <= MAXIMUM_REQUESTS)
        .context("benchmark request count exceeds its finite limit")?;
    let process_before = process_snapshot(target.process_id).await?;
    let started_at = Instant::now();
    let mut tasks = JoinSet::new();
    let mutation_namespace: Arc<str> = format!("{cache_state}-{clients}").into();
    for worker_index in 0..clients {
        let client = client.clone();
        let target = target.clone();
        let mutation_namespace = Arc::clone(&mutation_namespace);
        tasks.spawn(async move {
            let mut samples = Vec::with_capacity(requests_per_client);
            for request_index in 0..requests_per_client {
                let sequence = worker_index
                    .checked_mul(requests_per_client)
                    .and_then(|value| value.checked_add(request_index))
                    .context("benchmark request sequence overflowed")?;
                let request_started_at = Instant::now();
                let result = execute_reference(
                    &client,
                    &target,
                    workload.operation,
                    &mutation_namespace,
                    sequence,
                )
                .await
                .with_context(|| {
                    format!(
                        "benchmark workload {} request {sequence} failed",
                        workload.name,
                    )
                })?;
                samples.push((micros(request_started_at.elapsed()), result));
            }
            Ok::<Vec<(u64, ReferenceResult)>, anyhow::Error>(samples)
        });
    }
    let mut latencies = Vec::with_capacity(total_requests);
    let mut allowed = 0_usize;
    let mut overloaded = 0_usize;
    let mut semantic = None;
    while let Some(joined) = tasks.join_next().await {
        for (latency, result) in
            joined.context("a benchmark worker panicked or was cancelled")??
        {
            match result {
                ReferenceResult::Success(result_semantic) => {
                    if let Some(expected) = &semantic
                        && expected != &result_semantic
                    {
                        bail!(
                            "benchmark workload {} returned inconsistent semantic results",
                            workload.name,
                        );
                    }
                    semantic.get_or_insert(result_semantic);
                    allowed = allowed.checked_add(1).context("allowed count overflowed")?;
                    latencies.push(latency);
                }
                ReferenceResult::Overloaded => {
                    overloaded = overloaded
                        .checked_add(1)
                        .context("overload count overflowed")?;
                }
            }
        }
    }
    let elapsed = started_at.elapsed();
    let process_after = process_snapshot(target.process_id).await?;
    let process_cpu_percent = process_interval_cpu_percent(process_before, process_after, elapsed);
    latencies.sort_unstable();
    Ok(MeasuredReference {
        measurement: BenchmarkMeasurement {
            workload: workload.name,
            residual_check_ratio_percent: workload.residual_check_ratio_percent,
            cache_state,
            store_state,
            implementation: target.name,
            clients,
            total_requests,
            allowed,
            overloaded,
            elapsed_milliseconds: millis(elapsed),
            requests_per_second: requests_per_second(
                u64::try_from(total_requests).context("benchmark request count is out of range")?,
                elapsed,
            ),
            allowed_p50_microseconds: percentile(&latencies, 50),
            allowed_p95_microseconds: percentile(&latencies, 95),
            allowed_p99_microseconds: percentile(&latencies, 99),
            process_cpu_percent,
            process_rss_kib: process_after.rss_kib,
        },
        semantic,
    })
}

async fn process_snapshot(process_id: u32) -> Result<ProcessSnapshot> {
    if process_id == 0 {
        bail!("benchmark process ID must be nonzero");
    }
    let output = Command::new("ps")
        .env("LC_ALL", "C")
        .args(["-o", "time=", "-o", "rss=", "-p"])
        .arg(process_id.to_string())
        .output()
        .await
        .context("failed to execute process resource sampler")?;
    if !output.status.success() {
        bail!("process resource sampler failed");
    }
    let text = std::str::from_utf8(&output.stdout)
        .context("process resource sampler returned non-UTF-8 output")?;
    let mut fields = text.split_ascii_whitespace();
    let cpu_time_microseconds = fields
        .next()
        .context("process resource sampler omitted CPU time")?
        .parse::<ProcessCpuTime>()?
        .microseconds;
    let rss_kib = fields
        .next()
        .context("process resource sampler omitted RSS")?
        .parse::<u64>()
        .context("process resource sampler returned invalid RSS")?;
    if fields.next().is_some() {
        bail!("process resource sampler returned an invalid field set");
    }
    Ok(ProcessSnapshot {
        cpu_time_microseconds,
        rss_kib,
    })
}

#[derive(Clone, Copy, Debug)]
struct ProcessCpuTime {
    microseconds: u64,
}

impl FromStr for ProcessCpuTime {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let (days, clock) = if let Some((days, clock)) = value.split_once('-') {
            (
                days.parse::<u64>()
                    .context("process CPU time returned invalid days")?,
                clock,
            )
        } else {
            (0, value)
        };
        let fields = clock.split(':').collect::<Vec<_>>();
        let (hours, minutes, seconds) = match fields.as_slice() {
            [minutes, seconds] => (0, minutes.parse::<u64>()?, seconds.parse::<f64>()?),
            [hours, minutes, seconds] => (
                hours.parse::<u64>()?,
                minutes.parse::<u64>()?,
                seconds.parse::<f64>()?,
            ),
            _ => bail!("process CPU time returned an invalid clock"),
        };
        if minutes >= 60 || !seconds.is_finite() || !(0.0..60.0).contains(&seconds) {
            bail!("process CPU time returned an out-of-range clock");
        }
        let whole_seconds = days
            .checked_mul(24)
            .and_then(|value| value.checked_add(hours))
            .and_then(|value| value.checked_mul(60))
            .and_then(|value| value.checked_add(minutes))
            .and_then(|value| value.checked_mul(60))
            .context("process CPU time overflowed")?;
        let subminute = Duration::try_from_secs_f64(seconds)
            .context("process CPU time seconds were out of range")?;
        let subminute_microseconds =
            u64::try_from(subminute.as_micros()).context("process CPU time seconds overflowed")?;
        let microseconds = whole_seconds
            .checked_mul(1_000_000)
            .and_then(|value| value.checked_add(subminute_microseconds))
            .context("process CPU time overflowed")?;
        Ok(Self { microseconds })
    }
}

fn process_cpu_percent(before: u64, after: u64, elapsed: Duration) -> f64 {
    let cpu_time = Duration::from_micros(after.saturating_sub(before));
    cpu_time.as_secs_f64() / elapsed.as_secs_f64().max(0.000_001) * 100.0
}

fn process_interval_cpu_percent(
    before: ProcessSnapshot,
    after: ProcessSnapshot,
    elapsed: Duration,
) -> Option<f64> {
    const MINIMUM_ATTRIBUTABLE_INTERVAL: Duration = Duration::from_millis(100);
    if elapsed < MINIMUM_ATTRIBUTABLE_INTERVAL {
        return None;
    }
    let percent = process_cpu_percent(
        before.cpu_time_microseconds,
        after.cpu_time_microseconds,
        elapsed,
    );
    let maximum = available_parallelism()
        .ok()
        .and_then(|parallelism| u32::try_from(parallelism.get()).ok())
        .map(f64::from)?
        * 100.0;
    (percent <= maximum).then_some(percent)
}

async fn warm_operation(
    client: &Client,
    target: &Target,
    workload: ReferenceWorkload,
) -> Result<ReferenceSemantic> {
    let requests = if matches!(
        workload.operation,
        ReferenceOperation::ModelCompileAndPublish | ReferenceOperation::TupleWriteAndChangelog
    ) {
        1
    } else {
        WARM_REQUESTS
    };
    let mut semantic = None;
    for sequence in 0..requests {
        let deadline = TokioInstant::now() + WARM_RECOVERY_TIMEOUT;
        loop {
            let result = execute_reference(client, target, workload.operation, "warmup", sequence)
                .await
                .with_context(|| format!("benchmark workload {} warmup failed", workload.name))?;
            match result {
                ReferenceResult::Success(result_semantic) => {
                    if let Some(expected) = &semantic
                        && expected != &result_semantic
                    {
                        bail!(
                            "benchmark workload {} warmup returned inconsistent semantic results",
                            workload.name,
                        );
                    }
                    semantic.get_or_insert(result_semantic);
                    break;
                }
                ReferenceResult::Overloaded if TokioInstant::now() < deadline => {
                    tokio::time::sleep(WARM_RECOVERY_INTERVAL).await;
                }
                ReferenceResult::Overloaded => {
                    bail!(
                        "Phase 4 {} benchmark workload {} warmup remained overloaded for {} \
                         seconds",
                        target.name,
                        workload.name,
                        WARM_RECOVERY_TIMEOUT.as_secs(),
                    );
                }
            }
        }
    }
    semantic.context("benchmark warmup completed without a semantic result")
}

async fn execute_reference(
    client: &Client,
    target: &Target,
    operation: ReferenceOperation,
    mutation_namespace: &str,
    sequence: usize,
) -> Result<ReferenceResult> {
    match operation {
        ReferenceOperation::Check {
            object,
            relation,
            user,
            contextual,
        } => {
            return match check_request(
                client,
                target,
                object,
                relation,
                user,
                "MINIMIZE_LATENCY",
                contextual,
            )
            .await?
            {
                CheckResult::Allowed => {
                    Ok(ReferenceResult::Success(ReferenceSemantic::CheckAllowed))
                }
                CheckResult::Overloaded => Ok(ReferenceResult::Overloaded),
                CheckResult::Denied => bail!("reference Check returned deny for an allow fixture"),
            };
        }
        ReferenceOperation::ModelLoad => {
            let url = target
                .base_url
                .join(&format!(
                    "stores/{}/authorization-models/{}",
                    target.store_id, target.model_id,
                ))
                .context("failed to construct model-load benchmark URL")?;
            let response = client
                .get(url)
                .send()
                .await
                .context("model-load benchmark request failed")?;
            return classify_reference_success(target, operation, response).await;
        }
        _ => {}
    }
    let (path, body) = reference_post_request(operation, target, mutation_namespace, sequence)?;
    let url = target
        .base_url
        .join(&path)
        .context("failed to construct reference workload URL")?;
    let response = client
        .post(url)
        .json(&body)
        .send()
        .await
        .context("reference workload request failed")?;
    classify_reference_success(target, operation, response).await
}

fn reference_post_request(
    operation: ReferenceOperation,
    target: &Target,
    mutation_namespace: &str,
    sequence: usize,
) -> Result<(String, Value)> {
    match operation {
        ReferenceOperation::BatchCheck => Ok((
            format!("stores/{}/batch-check", target.store_id),
            json!({
                "checks": [
                    {
                        "tuple_key": {
                            "object": "document:direct",
                            "relation": "viewer",
                            "user": "user:anne"
                        },
                        "correlation_id": "direct-a"
                    },
                    {
                        "tuple_key": {
                            "object": "document:direct",
                            "relation": "viewer",
                            "user": "user:anne"
                        },
                        "correlation_id": "direct-b"
                    }
                ],
                "authorization_model_id": target.model_id,
                "consistency": "MINIMIZE_LATENCY"
            }),
        )),
        ReferenceOperation::ListObjects { relation } => Ok((
            format!("stores/{}/list-objects", target.store_id),
            json!({
                "authorization_model_id": target.model_id,
                "type": "document",
                "relation": relation,
                "user": "user:anne",
                "consistency": "MINIMIZE_LATENCY"
            }),
        )),
        ReferenceOperation::ListUsers => Ok((
            format!("stores/{}/list-users", target.store_id),
            json!({
                "authorization_model_id": target.model_id,
                "object": {"type": "document", "id": "phase4-list-users"},
                "relation": "allowed",
                "user_filters": [{"type": "user"}],
                "consistency": "MINIMIZE_LATENCY"
            }),
        )),
        ReferenceOperation::ModelCompileAndPublish => Ok((
            format!("stores/{}/authorization-models", target.store_id),
            go_model_document(),
        )),
        ReferenceOperation::TupleWriteAndChangelog => Ok((
            format!("stores/{}/write", target.store_id),
            json!({
                "writes": {"tuple_keys": [{
                    "object": format!(
                        "document:phase4-benchmark-{mutation_namespace}-{sequence}"
                    ),
                    "relation": "viewer",
                    "user": "user:anne"
                }], "on_duplicate": "ignore"},
                "authorization_model_id": target.model_id
            }),
        )),
        ReferenceOperation::Check { .. } | ReferenceOperation::ModelLoad => {
            bail!("internal benchmark error: non-POST operation reached POST request builder")
        }
    }
}

async fn classify_reference_success(
    target: &Target,
    operation: ReferenceOperation,
    response: reqwest::Response,
) -> Result<ReferenceResult> {
    let status = response.status();
    let body = read_bounded(response).await?;
    let value: Value =
        serde_json::from_slice(&body).context("reference workload response is not JSON")?;
    let error_code = value.get("code").and_then(Value::as_str);
    if status == StatusCode::TOO_MANY_REQUESTS || error_code == Some("throttled_timeout_error") {
        return Ok(ReferenceResult::Overloaded);
    }
    if !status.is_success() {
        bail!(
            "Phase 4 {} workload failed with HTTP status {status} ({})",
            target.name,
            error_code.unwrap_or("unclassified"),
        );
    }
    reference_semantic(operation, value, &target.model_id).map(ReferenceResult::Success)
}

fn compare_reference_semantics(
    workload: &str,
    go: Option<&ReferenceSemantic>,
    rust: Option<&ReferenceSemantic>,
) -> Result<()> {
    match (go, rust) {
        (Some(go), Some(rust)) if go == rust => Ok(()),
        (Some(ReferenceSemantic::ListObjects(go)), Some(ReferenceSemantic::ListObjects(rust))) => {
            let only_go = sorted_difference(go, rust);
            let only_rust = sorted_difference(rust, go);
            bail!(
                "Phase 4 semantic mismatch for workload {workload}: only Go count {}, sample \
                 {:?}; only Rust count {}, sample {:?}",
                only_go.len(),
                only_go.iter().take(5).collect::<Vec<_>>(),
                only_rust.len(),
                only_rust.iter().take(5).collect::<Vec<_>>(),
            )
        }
        (None, _) => bail!("Phase 4 Go workload {workload} produced no semantic result"),
        (_, None) => bail!("Phase 4 Rust workload {workload} produced no semantic result"),
        _ => bail!("Phase 4 semantic mismatch for workload {workload}"),
    }
}

fn validate_measured_semantics(
    workload: &str,
    go: Option<&ReferenceSemantic>,
    rust: Option<&ReferenceSemantic>,
    warm_oracle: &ReferenceSemantic,
) -> Result<()> {
    if let Some(go) = go {
        compare_reference_semantics(workload, Some(warm_oracle), Some(go))?;
    }
    if let Some(rust) = rust {
        compare_reference_semantics(workload, Some(warm_oracle), Some(rust))?;
    }
    Ok(())
}

fn sorted_difference<'a>(left: &'a [String], right: &[String]) -> Vec<&'a String> {
    left.iter()
        .filter(|value| right.binary_search(value).is_err())
        .collect()
}

fn reference_semantic(
    operation: ReferenceOperation,
    value: Value,
    expected_model_id: &str,
) -> Result<ReferenceSemantic> {
    match operation {
        ReferenceOperation::Check { .. } => {
            bail!("internal benchmark error: Check bypassed typed result validation")
        }
        ReferenceOperation::BatchCheck => batch_check_semantic(value),
        ReferenceOperation::ListObjects { relation } => {
            let mut objects = serde_json::from_value::<pb::ListObjectsResponse>(value)
                .context("ListObjects benchmark response did not match its schema")?
                .objects;
            objects.sort_unstable();
            if objects.is_empty() {
                bail!("ListObjects benchmark returned an empty allow set");
            }
            if objects.windows(2).any(|window| window[0] == window[1]) {
                bail!("ListObjects benchmark returned duplicate objects");
            }
            let expected_fixture = match relation {
                "reverse_only" => "document:reverse-only-0",
                "residual_all" => "document:residual-all-0",
                _ => "document:direct",
            };
            if objects.binary_search(&expected_fixture.to_owned()).is_err() {
                bail!(
                    "ListObjects benchmark omitted its {relation} allow fixture {expected_fixture}"
                );
            }
            Ok(ReferenceSemantic::ListObjects(objects))
        }
        ReferenceOperation::ListUsers => {
            let response = serde_json::from_value::<pb::ListUsersResponse>(value)
                .context("ListUsers benchmark response did not match its schema")?;
            let mut users = response
                .users
                .into_iter()
                .map(normalize_list_user)
                .collect::<Result<Vec<_>>>()?;
            users.sort_unstable();
            if users != ["user:bob".to_owned()] {
                bail!("ListUsers benchmark returned an unexpected result set");
            }
            Ok(ReferenceSemantic::ListUsers(users))
        }
        ReferenceOperation::ModelLoad => {
            let response = serde_json::from_value::<pb::ReadAuthorizationModelResponse>(value)
                .context("model-load benchmark response did not match its schema")?;
            let model = response
                .authorization_model
                .context("model-load benchmark omitted its model")?;
            let model_id = model
                .id
                .parse::<AuthorizationModelId>()
                .context("model-load benchmark returned an invalid model ID")?;
            let expected = expected_model_id
                .parse::<AuthorizationModelId>()
                .context("model-load benchmark expected model ID was invalid")?;
            if model_id != expected {
                bail!("model-load benchmark returned the wrong model ID");
            }
            if model.schema_version != "1.1" {
                bail!("model-load benchmark returned an unexpected schema version");
            }
            Ok(ReferenceSemantic::ModelLoaded)
        }
        ReferenceOperation::ModelCompileAndPublish => {
            let response = serde_json::from_value::<pb::WriteAuthorizationModelResponse>(value)
                .context("model-publish benchmark response did not match its schema")?;
            response
                .authorization_model_id
                .parse::<AuthorizationModelId>()
                .context("model-publish benchmark returned an invalid model ID")?;
            Ok(ReferenceSemantic::ModelPublished)
        }
        ReferenceOperation::TupleWriteAndChangelog => {
            serde_json::from_value::<pb::WriteResponse>(value)
                .context("tuple-write benchmark response did not match its schema")?;
            Ok(ReferenceSemantic::TupleWritten)
        }
    }
}

fn batch_check_semantic(value: Value) -> Result<ReferenceSemantic> {
    let response = serde_json::from_value::<pb::BatchCheckResponse>(value)
        .context("BatchCheck benchmark response did not match its schema")?;
    let mut decisions = response
        .result
        .into_iter()
        .map(|(correlation_id, result)| {
            let allowed = match result.check_result {
                Some(pb::batch_check_single_result::CheckResult::Allowed(allowed)) => allowed,
                Some(pb::batch_check_single_result::CheckResult::Error(_)) => {
                    bail!("BatchCheck benchmark item returned an error")
                }
                None => bail!("BatchCheck benchmark item omitted its result"),
            };
            if !allowed {
                bail!("BatchCheck benchmark allow fixture returned deny");
            }
            Ok((correlation_id, allowed))
        })
        .collect::<Result<Vec<_>>>()?;
    decisions.sort_unstable();
    if decisions != [("direct-a".to_owned(), true), ("direct-b".to_owned(), true)] {
        bail!("BatchCheck benchmark returned an unexpected correlation set");
    }
    Ok(ReferenceSemantic::BatchCheck(decisions))
}

fn normalize_list_user(user: pb::User) -> Result<String> {
    match user.user {
        Some(pb::user::User::Object(object)) => Ok(format!("{}:{}", object.r#type, object.id)),
        Some(pb::user::User::Userset(userset)) => Ok(format!(
            "{}:{}#{}",
            userset.r#type, userset.id, userset.relation,
        )),
        Some(pb::user::User::Wildcard(wildcard)) => Ok(format!("{}:*", wildcard.r#type)),
        None => bail!("ListUsers benchmark returned an empty user variant"),
    }
}

async fn soak_worker(
    client: &Client,
    target: &Target,
    deadline: TokioInstant,
    consistency: &'static str,
) -> Result<SoakCounts> {
    let mut counts = SoakCounts::default();
    while TokioInstant::now() < deadline {
        let cycle_started_at = TokioInstant::now();
        let started_at = Instant::now();
        let result = check(client, target, "document:direct", "user:anne", consistency).await?;
        let latency = micros(started_at.elapsed());
        counts.maximum_request_microseconds = counts.maximum_request_microseconds.max(latency);
        match result {
            CheckResult::Allowed => {
                counts.allowed = counts
                    .allowed
                    .checked_add(1)
                    .context("worker allowed count overflowed")?;
            }
            CheckResult::Overloaded => {
                counts.overloaded = counts
                    .overloaded
                    .checked_add(1)
                    .context("worker overload count overflowed")?;
            }
            CheckResult::Denied => bail!("soak direct-allow request was denied"),
        }
        let next_cycle = cycle_started_at + SOAK_CLIENT_INTERVAL;
        if next_cycle < deadline {
            tokio::time::sleep_until(next_cycle).await;
        }
    }
    Ok(counts)
}

async fn monitor_readiness(
    base_url: Url,
    deadline: TokioInstant,
    baseline: CapacitySnapshot,
) -> Result<ResourceObservations> {
    let client = build_client()?;
    let ready_url = base_url
        .join("readyz")
        .context("failed to construct soak readiness URL")?;
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut high_water = CapacityHighWater::default();
    high_water.observe(baseline)?;
    let mut probes = 0_u64;
    let mut samples = 1_u64;
    while TokioInstant::now() < deadline {
        ticker.tick().await;
        let response = client
            .get(ready_url.clone())
            .send()
            .await
            .context("soak readiness request failed")?;
        let status = response.status();
        let _body = read_bounded(response).await?;
        if !status.is_success() {
            bail!("soak readiness failed with HTTP status {status}");
        }
        probes = probes
            .checked_add(1)
            .context("soak readiness probe count overflowed")?;
        high_water.observe(capacity_snapshot(&client, &base_url).await?)?;
        samples = samples
            .checked_add(1)
            .context("soak resource sample count overflowed")?;
    }
    Ok(ResourceObservations {
        readiness_probes: probes,
        samples,
        baseline,
        high_water,
    })
}

fn validate_idle_baseline(baseline: &CapacitySnapshot) -> Result<()> {
    if baseline.endpoint_permits_available != baseline.endpoint_permits_capacity {
        bail!(
            "endpoint permits were not idle before soak: {} of {} available",
            baseline.endpoint_permits_available,
            baseline.endpoint_permits_capacity,
        );
    }
    if let Some(capacity) = baseline.storage_work_permits_capacity
        && baseline.storage_work_permits_available != Some(capacity)
    {
        bail!(
            "storage permits were not idle before soak: {:?} of {capacity} available",
            baseline.storage_work_permits_available,
        );
    }
    if let Some(open) = baseline.primary_pool_open
        && baseline.primary_pool_idle != usize::try_from(open).ok()
    {
        bail!(
            "primary pool was not idle before soak: {:?} of {open} connections idle",
            baseline.primary_pool_idle,
        );
    }
    Ok(())
}

async fn capture_idle_baseline(client: &Client, base_url: &Url) -> Result<CapacitySnapshot> {
    let deadline = TokioInstant::now() + READINESS_RECOVERY_TIMEOUT;
    loop {
        let snapshot = capacity_snapshot(client, base_url).await?;
        if validate_idle_baseline(&snapshot).is_ok() {
            return Ok(snapshot);
        }
        if TokioInstant::now() >= deadline {
            validate_idle_baseline(&snapshot)?;
        }
        tokio::time::sleep(WARM_RECOVERY_INTERVAL).await;
    }
}

async fn wait_for_readiness(client: &Client, base_url: &Url) -> Result<()> {
    let url = base_url
        .join("readyz")
        .context("failed to construct readiness recovery URL")?;
    let deadline = TokioInstant::now() + READINESS_RECOVERY_TIMEOUT;
    let mut ready_since = None;
    loop {
        let response = client
            .get(url.clone())
            .send()
            .await
            .context("readiness recovery request failed")?;
        let ready = response.status().is_success();
        let _body = read_bounded(response).await?;
        if ready {
            let since = ready_since.get_or_insert_with(TokioInstant::now);
            if since.elapsed() >= READINESS_STABILITY_WINDOW {
                return Ok(());
            }
        } else {
            ready_since = None;
        }
        if TokioInstant::now() >= deadline {
            bail!("server readiness did not recover before the Phase 4 soak");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

impl CapacityHighWater {
    fn observe(&mut self, snapshot: CapacitySnapshot) -> Result<()> {
        let endpoint_in_flight = snapshot
            .endpoint_permits_capacity
            .checked_sub(snapshot.endpoint_permits_available)
            .context("endpoint available permits exceeded configured capacity")?;
        let storage_in_flight = match (
            snapshot.storage_work_permits_capacity,
            snapshot.storage_work_permits_available,
        ) {
            (Some(capacity), Some(available)) => capacity
                .checked_sub(available)
                .context("storage available permits exceeded configured capacity")?,
            (None, None) => 0,
            _ => bail!("storage permit diagnostics were incomplete"),
        };
        let pool_in_use = match (snapshot.primary_pool_open, snapshot.primary_pool_idle) {
            (Some(open), Some(idle)) => usize::try_from(open)
                .context("primary pool size is out of range")?
                .checked_sub(idle)
                .context("primary pool idle count exceeded open connections")?,
            (None, None) => 0,
            _ => bail!("primary pool diagnostics were incomplete"),
        };
        self.runtime_tasks = self.runtime_tasks.max(snapshot.runtime_tasks);
        self.endpoint_permits_in_flight = self.endpoint_permits_in_flight.max(endpoint_in_flight);
        self.storage_work_permits_in_flight =
            self.storage_work_permits_in_flight.max(storage_in_flight);
        self.primary_pool_open = self
            .primary_pool_open
            .max(snapshot.primary_pool_open.unwrap_or_default());
        self.primary_pool_in_use = self.primary_pool_in_use.max(pool_in_use);
        Ok(())
    }
}

async fn capacity_snapshot(client: &Client, base_url: &Url) -> Result<CapacitySnapshot> {
    let url = base_url
        .join("capacityz")
        .context("failed to construct capacity diagnostics URL")?;
    let response = client
        .get(url)
        .send()
        .await
        .context("capacity diagnostics request failed")?;
    let status = response.status();
    let body = read_bounded(response).await?;
    if !status.is_success() {
        bail!("capacity diagnostics failed with HTTP status {status}");
    }
    serde_json::from_slice(&body).context("capacity diagnostics response is not valid JSON")
}

fn validate_post_drain(baseline: &CapacitySnapshot, post_drain: &CapacitySnapshot) -> Result<()> {
    if post_drain.endpoint_permits_available != post_drain.endpoint_permits_capacity {
        bail!("endpoint permits did not return to capacity after drain");
    }
    match (
        post_drain.storage_work_permits_available,
        post_drain.storage_work_permits_capacity,
    ) {
        (Some(available), Some(capacity)) if available == capacity => {}
        (None, None) => {}
        _ => bail!("storage work permits did not return to capacity after drain"),
    }
    match (post_drain.primary_pool_open, post_drain.primary_pool_idle) {
        (Some(open), Some(idle))
            if usize::try_from(open).context("primary pool size is out of range")? == idle => {}
        (None, None) => {}
        _ => bail!("PostgreSQL pool retained checked-out connections after drain"),
    }
    let maximum_post_drain_tasks = baseline
        .runtime_tasks
        .checked_add(POST_DRAIN_TASK_TOLERANCE)
        .context("post-drain task ceiling overflowed")?;
    if post_drain.runtime_tasks > maximum_post_drain_tasks {
        bail!(
            "runtime tasks did not return near baseline after drain: {} > {}",
            post_drain.runtime_tasks,
            maximum_post_drain_tasks,
        );
    }
    Ok(())
}

async fn check(
    client: &Client,
    target: &Target,
    object: &str,
    user: &str,
    consistency: &str,
) -> Result<CheckResult> {
    check_request(client, target, object, "viewer", user, consistency, false).await
}

async fn check_request(
    client: &Client,
    target: &Target,
    object: &str,
    relation: &str,
    user: &str,
    consistency: &str,
    contextual: bool,
) -> Result<CheckResult> {
    let url = target
        .base_url
        .join(&format!("stores/{}/check", target.store_id))
        .context("failed to construct Phase 4 Check URL")?;
    let contextual_tuples = contextual.then(|| {
        json!({"tuple_keys": [{
            "object": object,
            "relation": relation,
            "user": user
        }]})
    });
    let body = json!({
        "tuple_key": {
            "object": object,
            "relation": relation,
            "user": user
        },
        "authorization_model_id": target.model_id,
        "consistency": consistency,
        "contextual_tuples": contextual_tuples,
    });
    let response = client
        .post(url)
        .json(&body)
        .send()
        .await
        .context("Phase 4 Check request failed")?;
    let status = response.status();
    let body = read_bounded(response).await?;
    let value: Value =
        serde_json::from_slice(&body).context("Phase 4 Check response is not JSON")?;
    classify_check(target.name, status, &value)
}

fn classify_check(target: &str, status: StatusCode, value: &Value) -> Result<CheckResult> {
    let error_code = value.get("code").and_then(Value::as_str);
    if status == StatusCode::TOO_MANY_REQUESTS || error_code == Some("throttled_timeout_error") {
        return Ok(CheckResult::Overloaded);
    }
    if !status.is_success() {
        let error_code = error_code.unwrap_or("unclassified");
        bail!("Phase 4 {target} Check failed with HTTP status {status} ({error_code})");
    }
    match value.get("allowed") {
        Some(value) if value.as_bool() == Some(true) => Ok(CheckResult::Allowed),
        Some(value) if value.as_bool() == Some(false) => Ok(CheckResult::Denied),
        Some(_) => bail!("Phase 4 Check response allowed field is not boolean"),
        None => Ok(CheckResult::Denied),
    }
}

fn require_check(actual: CheckResult, expected: CheckResult, operation: &str) -> Result<()> {
    if actual != expected {
        bail!("{operation} returned {actual:?}, expected {expected:?}");
    }
    Ok(())
}

fn validate_count(name: &str, value: usize, maximum: usize) -> Result<()> {
    if value == 0 || value > maximum {
        bail!("{name} must be between 1 and {maximum}");
    }
    Ok(())
}

fn percentile(sorted: &[u64], percentage: usize) -> Option<u64> {
    let rank = sorted
        .len()
        .saturating_mul(percentage)
        .div_ceil(100)
        .saturating_sub(1);
    sorted.get(rank).copied()
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn requests_per_second(requests: u64, duration: Duration) -> u64 {
    let scaled = u128::from(requests).saturating_mul(1_000_000_000);
    let rate = scaled / duration.as_nanos().max(1);
    u64::try_from(rate).unwrap_or(u64::MAX)
}

fn build_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(REQUEST_TIMEOUT)
        .redirect(Policy::none())
        .pool_max_idle_per_host(MAXIMUM_CLIENTS)
        .build()
        .context("failed to build the Phase 4 client")
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use reqwest::StatusCode;
    use serde_json::json;

    use super::{
        CapacityHighWater, CapacitySnapshot, CheckResult, ProcessCpuTime, ProcessSnapshot,
        classify_check, percentile, process_cpu_percent, process_interval_cpu_percent,
        validate_count, validate_idle_baseline, validate_post_drain,
    };

    #[test]
    fn test_should_classify_protobuf_default_and_overload_responses() -> anyhow::Result<()> {
        assert_eq!(
            classify_check("rust", StatusCode::OK, &json!({}))?,
            CheckResult::Denied,
        );
        assert_eq!(
            classify_check("rust", StatusCode::OK, &json!({"allowed": true}))?,
            CheckResult::Allowed,
        );
        assert_eq!(
            classify_check(
                "rust",
                StatusCode::UNPROCESSABLE_ENTITY,
                &json!({"code": "throttled_timeout_error"}),
            )?,
            CheckResult::Overloaded,
        );
        assert!(
            classify_check(
                "rust",
                StatusCode::UNPROCESSABLE_ENTITY,
                &json!({"code": "validation_error"}),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn test_should_calculate_nearest_rank_percentiles() {
        let values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(percentile(&values, 50), Some(5));
        assert_eq!(percentile(&values, 95), Some(10));
        assert_eq!(percentile(&[], 50), None);
    }

    #[test]
    fn test_should_parse_process_cpu_time_and_attribute_interval_usage() -> anyhow::Result<()> {
        assert_eq!(
            ProcessCpuTime::from_str("1-02:03:04.50")?.microseconds,
            93_784_500_000,
        );
        assert_eq!(
            ProcessCpuTime::from_str("3:04.25")?.microseconds,
            184_250_000,
        );
        assert!(ProcessCpuTime::from_str("invalid").is_err());
        let cpu_percent = process_cpu_percent(10_000, 260_000, Duration::from_secs(1));
        assert!((cpu_percent - 25.0).abs() < f64::EPSILON);
        let before = ProcessSnapshot {
            cpu_time_microseconds: 10_000,
            rss_kib: 1,
        };
        let after = ProcessSnapshot {
            cpu_time_microseconds: 260_000,
            rss_kib: 1,
        };
        assert_eq!(
            process_interval_cpu_percent(before, after, Duration::from_millis(50)),
            None,
        );
        assert!(
            process_interval_cpu_percent(before, after, Duration::from_secs(1))
                .is_some_and(|percent| (percent - 25.0).abs() < f64::EPSILON)
        );
        Ok(())
    }

    #[test]
    fn test_should_reject_unbounded_phase4_counts() {
        assert!(validate_count("clients", 0, 100).is_err());
        assert!(validate_count("clients", 101, 100).is_err());
        assert!(validate_count("clients", 100, 100).is_ok());
    }

    #[test]
    fn test_should_measure_capacity_high_water_and_validate_drain() -> anyhow::Result<()> {
        let baseline = snapshot(12, 64, 64, 16, 16, 8, 8);
        let loaded = snapshot(40, 17, 64, 3, 16, 16, 2);
        let drained = snapshot(10, 64, 64, 16, 16, 16, 16);
        let mut high_water = CapacityHighWater::default();

        high_water.observe(baseline)?;
        high_water.observe(loaded)?;

        assert_eq!(high_water.runtime_tasks, 40);
        assert_eq!(high_water.endpoint_permits_in_flight, 47);
        assert_eq!(high_water.storage_work_permits_in_flight, 13);
        assert_eq!(high_water.primary_pool_open, 16);
        assert_eq!(high_water.primary_pool_in_use, 14);
        validate_post_drain(&baseline, &drained)?;
        assert!(validate_post_drain(&baseline, &loaded).is_err());
        Ok(())
    }

    #[test]
    fn test_should_require_an_idle_resource_baseline_before_soak() {
        assert!(validate_idle_baseline(&snapshot(12, 64, 64, 16, 16, 8, 8)).is_ok());
        assert!(validate_idle_baseline(&snapshot(12, 63, 64, 16, 16, 8, 8)).is_err());
        assert!(validate_idle_baseline(&snapshot(12, 64, 64, 15, 16, 8, 8)).is_err());
        assert!(validate_idle_baseline(&snapshot(12, 64, 64, 16, 16, 8, 7)).is_err());
    }

    const fn snapshot(
        runtime_tasks: usize,
        endpoint_available: usize,
        endpoint_capacity: usize,
        storage_available: usize,
        storage_capacity: usize,
        pool_open: u32,
        pool_idle: usize,
    ) -> CapacitySnapshot {
        CapacitySnapshot {
            runtime_tasks,
            endpoint_permits_available: endpoint_available,
            endpoint_permits_capacity: endpoint_capacity,
            storage_work_permits_available: Some(storage_available),
            storage_work_permits_capacity: Some(storage_capacity),
            primary_pool_open: Some(pool_open),
            primary_pool_idle: Some(pool_idle),
            primary_pool_capacity: Some(pool_open),
        }
    }
}
