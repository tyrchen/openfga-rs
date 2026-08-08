//! Reproducible Phase 4 consistency, load, soak, and reference measurements.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use reqwest::{Client, StatusCode, Url, redirect::Policy};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{task::JoinSet, time::Instant as TokioInstant};

use crate::{
    check_probe::{
        GO_BASELINE_COMMIT, configure_differential_server, read_bounded, require_success,
    },
    validated_loopback_url, write_value,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAXIMUM_ITERATIONS: usize = 10_000;
const MAXIMUM_CLIENTS: usize = 1_000;
const MAXIMUM_REQUESTS: usize = 1_000_000;
const MAXIMUM_SOAK_SECONDS: u64 = 86_400;
const WARM_REQUESTS: usize = 32;
const SOAK_CLIENT_INTERVAL: Duration = Duration::from_millis(10);
const REFERENCE_CLIENTS: [usize; 3] = [1, 10, 100];

#[derive(Clone, Debug)]
struct Target {
    name: &'static str,
    base_url: Url,
    store_id: String,
    model_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckResult {
    Allowed,
    Denied,
    Overloaded,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkReport {
    baseline_commit: &'static str,
    workload: &'static str,
    requests_per_client: usize,
    measurements: Vec<BenchmarkMeasurement>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BenchmarkMeasurement {
    implementation: &'static str,
    clients: usize,
    total_requests: usize,
    allowed: usize,
    overloaded: usize,
    elapsed_milliseconds: u64,
    requests_per_second: u64,
    allowed_p50_microseconds: u64,
    allowed_p95_microseconds: u64,
    allowed_p99_microseconds: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConsistencyReport {
    baseline_commit: &'static str,
    concurrent_sequences: usize,
    higher_consistency_checks: usize,
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
}

#[derive(Clone, Copy, Debug, Default)]
struct SoakCounts {
    allowed: u64,
    overloaded: u64,
    maximum_request_microseconds: u64,
}

/// Runs concurrent write/check/delete sequences against the complete Rust server.
pub(crate) async fn run_consistency_faults(rust_url: &str, iterations: usize) -> Result<()> {
    validate_count("consistency iterations", iterations, MAXIMUM_ITERATIONS)?;
    let client = client()?;
    let target = configure_target(&client, "rust", rust_url).await?;
    warm(&client, &target).await?;
    let mut tasks = JoinSet::new();
    for iteration in 0..iterations {
        let client = client.clone();
        let target = target.clone();
        tasks.spawn(async move { consistency_sequence(&client, &target, iteration).await });
    }
    let mut higher_consistency_checks = 0_usize;
    while let Some(joined) = tasks.join_next().await {
        let checks = joined.context("a consistency fault worker panicked or was cancelled")??;
        higher_consistency_checks = higher_consistency_checks
            .checked_add(checks)
            .context("consistency check count overflowed")?;
    }
    write_value(&ConsistencyReport {
        baseline_commit: GO_BASELINE_COMMIT,
        concurrent_sequences: iterations,
        higher_consistency_checks,
        stale_results: 0,
    })
}

/// Runs the same bounded direct-allow workload against Go and Rust.
pub(crate) async fn run_reference_benchmark(
    go_url: &str,
    rust_url: &str,
    requests_per_client: usize,
) -> Result<()> {
    validate_count(
        "requests per client",
        requests_per_client,
        MAXIMUM_ITERATIONS,
    )?;
    let client = client()?;
    let go = configure_target(&client, "go", go_url).await?;
    let rust = configure_target(&client, "rust", rust_url).await?;
    warm(&client, &go).await?;
    warm(&client, &rust).await?;
    let mut measurements = Vec::with_capacity(REFERENCE_CLIENTS.len().saturating_mul(2));
    for clients in REFERENCE_CLIENTS {
        measurements.push(measure(&client, &go, clients, requests_per_client).await?);
        measurements.push(measure(&client, &rust, clients, requests_per_client).await?);
    }
    write_value(&BenchmarkReport {
        baseline_commit: GO_BASELINE_COMMIT,
        workload: "HTTP Check direct allow, warm model and tuple state",
        requests_per_client,
        measurements,
    })
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
    let client = client()?;
    let target = configure_target(&client, "rust", rust_url).await?;
    warm(&client, &target).await?;
    let duration = Duration::from_secs(seconds);
    let consistency = if higher_consistency {
        "HIGHER_CONSISTENCY"
    } else {
        "MINIMIZE_LATENCY"
    };
    let deadline = TokioInstant::now() + duration;
    let started_at = Instant::now();
    let readiness = tokio::spawn(monitor_readiness(
        client.clone(),
        target.base_url.clone(),
        deadline,
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
    let readiness_probes = readiness
        .await
        .context("the soak readiness monitor panicked or was cancelled")??;
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
        readiness_probes,
        allowed: counts.allowed,
        overloaded: counts.overloaded,
        requests_per_second: requests_per_second(total, elapsed),
        maximum_request_microseconds: counts.maximum_request_microseconds,
    })
}

async fn configure_target(client: &Client, name: &'static str, input: &str) -> Result<Target> {
    let base_url = validated_loopback_url(input)?;
    let (store_id, model_id) = configure_differential_server(client, &base_url)
        .await
        .with_context(|| format!("failed to configure {name} benchmark target"))?;
    Ok(Target {
        name,
        base_url,
        store_id,
        model_id,
    })
}

async fn consistency_sequence(client: &Client, target: &Target, iteration: usize) -> Result<usize> {
    let object = format!("document:phase4-consistency-{iteration}");
    require_check(
        check(client, target, &object, "user:phase4", "MINIMIZE_LATENCY").await?,
        CheckResult::Denied,
        "initial cached deny",
    )?;
    mutate(client, target, &object, true).await?;
    require_check(
        check(client, target, &object, "user:phase4", "HIGHER_CONSISTENCY").await?,
        CheckResult::Allowed,
        "higher-consistency read after write",
    )?;
    require_check(
        check(client, target, &object, "user:phase4", "MINIMIZE_LATENCY").await?,
        CheckResult::Allowed,
        "cacheable read after write",
    )?;
    mutate(client, target, &object, false).await?;
    require_check(
        check(client, target, &object, "user:phase4", "HIGHER_CONSISTENCY").await?,
        CheckResult::Denied,
        "higher-consistency read after delete",
    )?;
    Ok(2)
}

async fn mutate(client: &Client, target: &Target, object: &str, write: bool) -> Result<()> {
    let tuple = json!({"object": object, "relation": "viewer", "user": "user:phase4"});
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
    clients: usize,
    requests_per_client: usize,
) -> Result<BenchmarkMeasurement> {
    let total_requests = clients
        .checked_mul(requests_per_client)
        .filter(|total| *total <= MAXIMUM_REQUESTS)
        .context("benchmark request count exceeds its finite limit")?;
    let started_at = Instant::now();
    let mut tasks = JoinSet::new();
    for _ in 0..clients {
        let client = client.clone();
        let target = target.clone();
        tasks.spawn(async move {
            let mut samples = Vec::with_capacity(requests_per_client);
            for _ in 0..requests_per_client {
                let request_started_at = Instant::now();
                let result = check(
                    &client,
                    &target,
                    "document:direct",
                    "user:anne",
                    "MINIMIZE_LATENCY",
                )
                .await?;
                samples.push((micros(request_started_at.elapsed()), result));
            }
            Ok::<Vec<(u64, CheckResult)>, anyhow::Error>(samples)
        });
    }
    let mut latencies = Vec::with_capacity(total_requests);
    let mut allowed = 0_usize;
    let mut overloaded = 0_usize;
    while let Some(joined) = tasks.join_next().await {
        for (latency, result) in
            joined.context("a benchmark worker panicked or was cancelled")??
        {
            match result {
                CheckResult::Allowed => {
                    allowed = allowed.checked_add(1).context("allowed count overflowed")?;
                    latencies.push(latency);
                }
                CheckResult::Overloaded => {
                    overloaded = overloaded
                        .checked_add(1)
                        .context("overload count overflowed")?;
                }
                CheckResult::Denied => bail!("benchmark direct-allow request was denied"),
            }
        }
    }
    let elapsed = started_at.elapsed();
    latencies.sort_unstable();
    Ok(BenchmarkMeasurement {
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
        allowed_p50_microseconds: percentile(&latencies, 50)?,
        allowed_p95_microseconds: percentile(&latencies, 95)?,
        allowed_p99_microseconds: percentile(&latencies, 99)?,
    })
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

async fn monitor_readiness(client: Client, base_url: Url, deadline: TokioInstant) -> Result<u64> {
    let ready_url = base_url
        .join("readyz")
        .context("failed to construct soak readiness URL")?;
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut probes = 0_u64;
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
    }
    Ok(probes)
}

async fn check(
    client: &Client,
    target: &Target,
    object: &str,
    user: &str,
    consistency: &str,
) -> Result<CheckResult> {
    let url = target
        .base_url
        .join(&format!("stores/{}/check", target.store_id))
        .context("failed to construct Phase 4 Check URL")?;
    let response = client
        .post(url)
        .json(&json!({
            "tuple_key": {
                "object": object,
                "relation": "viewer",
                "user": user
            },
            "authorization_model_id": target.model_id,
            "consistency": consistency,
        }))
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

fn percentile(sorted: &[u64], percentage: usize) -> Result<u64> {
    let rank = sorted
        .len()
        .checked_mul(percentage)
        .context("percentile rank overflowed")?
        .div_ceil(100)
        .saturating_sub(1);
    sorted
        .get(rank)
        .copied()
        .context("cannot calculate a percentile from no samples")
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

fn client() -> Result<Client> {
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
    use reqwest::StatusCode;
    use serde_json::json;

    use super::{CheckResult, classify_check, percentile, validate_count};

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
    fn test_should_calculate_nearest_rank_percentiles() -> anyhow::Result<()> {
        let values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(percentile(&values, 50)?, 5);
        assert_eq!(percentile(&values, 95)?, 10);
        assert!(percentile(&[], 50).is_err());
        Ok(())
    }

    #[test]
    fn test_should_reject_unbounded_phase4_counts() {
        assert!(validate_count("clients", 0, 100).is_err());
        assert!(validate_count("clients", 101, 100).is_err());
        assert!(validate_count("clients", 100, 100).is_ok());
    }
}
