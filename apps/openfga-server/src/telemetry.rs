//! Structured local logging and optional OTLP trace export.

use std::{fmt, time::Duration};

use anyhow::{Context, Result};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use tracing_subscriber::{
    EnvFilter,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

use crate::config::{LogFormat, TelemetryConfig};

/// Owns the optional exporter so shutdown can flush it deterministically.
pub(crate) struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
    shutdown_timeout: Duration,
}

impl TelemetryGuard {
    pub(crate) fn install(config: &TelemetryConfig) -> Result<Self> {
        let shutdown_timeout = Duration::from_millis(config.export_timeout_ms);
        let provider = config
            .otlp_endpoint
            .as_deref()
            .map(|endpoint| build_provider(endpoint, shutdown_timeout))
            .transpose()?;
        install_subscriber(config, provider.as_ref())?;
        if let Some(provider) = &provider {
            opentelemetry::global::set_tracer_provider(provider.clone());
        }
        Ok(Self {
            provider,
            shutdown_timeout,
        })
    }

    pub(crate) fn shutdown(&self) -> Result<()> {
        if let Some(provider) = &self.provider {
            provider
                .shutdown_with_timeout(self.shutdown_timeout)
                .context("failed to flush OpenTelemetry traces")?;
        }
        Ok(())
    }
}

impl fmt::Debug for TelemetryGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryGuard")
            .field("otlp_enabled", &self.provider.is_some())
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish()
    }
}

fn build_provider(endpoint: &str, timeout: Duration) -> Result<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(timeout)
        .build()
        .context("failed to build OTLP trace exporter")?;
    Ok(SdkTracerProvider::builder()
        .with_resource(
            Resource::builder()
                .with_service_name("openfga-server")
                .build(),
        )
        .with_batch_exporter(exporter)
        .build())
}

fn install_subscriber(
    config: &TelemetryConfig,
    provider: Option<&SdkTracerProvider>,
) -> Result<()> {
    let filter = EnvFilter::try_new(&config.log_filter).context("telemetry filter is invalid")?;
    let result = match (config.log_format, provider) {
        (LogFormat::Pretty, None) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .try_init(),
        (LogFormat::Json, None) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .try_init(),
        (LogFormat::Pretty, Some(provider)) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("openfga-server")))
            .try_init(),
        (LogFormat::Json, Some(provider)) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("openfga-server")))
            .try_init(),
    };
    result.map_err(map_init_error)
}

fn map_init_error(error: TryInitError) -> anyhow::Error {
    anyhow::Error::new(error).context("failed to install tracing subscriber")
}
