//! `OpenFGA` server composition, configuration, command line, and lifecycle.

#![forbid(unsafe_code)]

use std::{
    io::{self, Write},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::{Json, Router, http::StatusCode, routing::get};
use clap::{Parser, Subcommand};
use reqwest::{Client, Url, redirect::Policy};
use serde::Serialize;
use serde_json::Value;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer};

mod check_corpus;
mod check_probe;

const MAX_PROBE_URL_BYTES: usize = 1_024;
const MAX_HEALTH_BODY_BYTES: u16 = 4_096;
const MAX_PROBE_REQUEST_BODY_BYTES: usize = 1_024;
const MAX_PROBE_CONCURRENCY: usize = 16;
const PROBE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Parser)]
#[command(name = "openfga-server", about = "OpenFGA-compatible Rust server")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Serve the Phase 0 compatibility probe.
    ProbeServer {
        /// Loopback socket used by the probe server.
        #[arg(long, default_value = "127.0.0.1:18081")]
        address: SocketAddr,
    },
    /// Serve the Phase 1 local Check compatibility probe.
    CheckProbeServer {
        /// Loopback socket used by the Check probe server.
        #[arg(long, default_value = "127.0.0.1:18081")]
        address: SocketAddr,
    },
    /// Compare normalized health observations from the pinned Go server and Rust probe.
    DifferentialSmoke {
        /// Base URL of the vendored Go baseline.
        #[arg(long)]
        go_url: String,
        /// Base URL of the Rust probe.
        #[arg(long)]
        rust_url: String,
    },
    /// Compare a bounded Check corpus against the pinned Go and Rust probes.
    DifferentialCheck {
        /// Base URL of the vendored Go baseline.
        #[arg(long)]
        go_url: String,
        /// Base URL of the Rust Check probe.
        #[arg(long)]
        rust_url: String,
    },
    /// Replay the complete pinned upstream Check fixture corpus against Rust.
    DifferentialCheckCorpus {
        /// Recorded corpus JSON produced by the pinned Go fixture exporter.
        #[arg(long)]
        corpus: PathBuf,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct NormalizedHealth {
    http_status: u16,
    serving: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Mismatch {
    field: &'static str,
    go: String,
    rust: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DifferentialReport {
    go: NormalizedHealth,
    rust: NormalizedHealth,
    mismatches: Vec<Mismatch>,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Arguments::parse().command {
        Command::ProbeServer { address } => serve_probe(address).await,
        Command::CheckProbeServer { address } => check_probe::serve(address).await,
        Command::DifferentialSmoke { go_url, rust_url } => {
            run_differential_smoke(&go_url, &rust_url).await
        }
        Command::DifferentialCheck { go_url, rust_url } => {
            check_probe::run_differential(&go_url, &rust_url).await
        }
        Command::DifferentialCheckCorpus { corpus } => check_corpus::run(corpus).await,
    }
}

async fn serve_probe(address: SocketAddr) -> Result<()> {
    if !address.ip().is_loopback() {
        bail!("the Phase 0 probe may bind only to a loopback address");
    }
    let application = Router::new()
        .route("/healthz", get(health))
        .layer(RequestBodyLimitLayer::new(MAX_PROBE_REQUEST_BODY_BYTES))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            PROBE_REQUEST_TIMEOUT,
        ))
        .layer(ConcurrencyLimitLayer::new(MAX_PROBE_CONCURRENCY));
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind the Phase 0 probe to {address}"))?;
    axum::serve(listener, application)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Phase 0 probe server failed")
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "SERVING" })
}

async fn shutdown_signal() {
    let _signal_result = tokio::signal::ctrl_c().await;
}

async fn run_differential_smoke(go_url: &str, rust_url: &str) -> Result<()> {
    let go_url = validated_loopback_url(go_url)?;
    let rust_url = validated_loopback_url(rust_url)?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(5))
        .redirect(Policy::none())
        .build()
        .context("failed to build the differential HTTP client")?;

    let go = observe_health(&client, &go_url)
        .await
        .context("failed to probe the vendored Go baseline")?;
    let rust = observe_health(&client, &rust_url)
        .await
        .context("failed to probe the Rust baseline")?;
    let mismatches = compare_health(&go, &rust);
    let report = DifferentialReport {
        go,
        rust,
        mismatches,
    };
    write_report(&report)?;
    if !report.mismatches.is_empty() {
        bail!("differential smoke found normalized mismatches");
    }
    Ok(())
}

fn validated_loopback_url(input: &str) -> Result<Url> {
    if input.len() > MAX_PROBE_URL_BYTES {
        bail!("probe URL exceeds the {MAX_PROBE_URL_BYTES}-byte limit");
    }
    let url = Url::parse(input).context("probe URL is invalid")?;
    if url.scheme() != "http" {
        bail!("Phase 0 differential probes require the http scheme");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("probe URL must not contain credentials");
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        bail!("probe URL must be an origin without a path, query, or fragment");
    }
    let host = url.host_str().context("probe URL has no host")?;
    let ip_literal = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    let address = ip_literal
        .parse::<IpAddr>()
        .context("Phase 0 differential probes require an IP-literal host")?;
    if !address.is_loopback() {
        bail!("Phase 0 differential probes may target only loopback addresses");
    }
    Ok(url)
}

async fn observe_health(client: &Client, base_url: &Url) -> Result<NormalizedHealth> {
    let health_url = base_url
        .join("healthz")
        .context("failed to construct health endpoint URL")?;
    let mut response = client
        .get(health_url)
        .send()
        .await
        .context("health request failed")?;
    let http_status = response.status().as_u16();
    if response
        .content_length()
        .is_some_and(|length| length > u64::from(MAX_HEALTH_BODY_BYTES))
    {
        bail!("health response exceeds the {MAX_HEALTH_BODY_BYTES}-byte limit");
    }
    let mut body = Vec::with_capacity(256);
    while let Some(chunk) = response
        .chunk()
        .await
        .context("failed to read health body")?
    {
        let next_length = body
            .len()
            .checked_add(chunk.len())
            .context("health response length overflowed")?;
        if next_length > usize::from(MAX_HEALTH_BODY_BYTES) {
            bail!("health response exceeds the {MAX_HEALTH_BODY_BYTES}-byte limit");
        }
        body.extend_from_slice(&chunk);
    }
    let document: Value = serde_json::from_slice(&body).context("health body is not JSON")?;
    let serving = document
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("SERVING"));
    Ok(NormalizedHealth {
        http_status,
        serving,
    })
}

fn compare_health(go: &NormalizedHealth, rust: &NormalizedHealth) -> Vec<Mismatch> {
    let mut mismatches = Vec::new();
    if go.http_status != rust.http_status {
        mismatches.push(Mismatch {
            field: "health.httpStatus",
            go: go.http_status.to_string(),
            rust: rust.http_status.to_string(),
        });
    }
    if go.serving != rust.serving {
        mismatches.push(Mismatch {
            field: "health.serving",
            go: go.serving.to_string(),
            rust: rust.serving.to_string(),
        });
    }
    mismatches
}

fn write_report(report: &DifferentialReport) -> Result<()> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, report)
        .context("failed to serialize the differential report")?;
    writeln!(output).context("failed to terminate the differential report")
}

#[cfg(test)]
mod tests {
    use super::{NormalizedHealth, compare_health, validated_loopback_url};

    #[test]
    fn test_should_reject_non_loopback_probe_urls() {
        assert!(validated_loopback_url("https://example.com").is_err());
        assert!(validated_loopback_url("http://localhost:8080/").is_err());
        assert!(validated_loopback_url("http://192.0.2.1:8080").is_err());
        assert!(validated_loopback_url("http://127.0.0.1:8080/admin").is_err());
        assert!(validated_loopback_url("http://127.0.0.1:8080/?token=secret").is_err());
        assert!(validated_loopback_url("http://[::1]:8080/").is_ok());
    }

    #[test]
    fn test_should_report_field_level_health_mismatches() {
        let go = NormalizedHealth {
            http_status: 200,
            serving: true,
        };
        let rust = NormalizedHealth {
            http_status: 503,
            serving: false,
        };
        let mismatches = compare_health(&go, &rust);
        assert_eq!(mismatches.len(), 2);
        assert_eq!(
            mismatches.first().map(|mismatch| mismatch.field),
            Some("health.httpStatus")
        );
    }
}
