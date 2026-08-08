//! Structured local logging and optional OTLP trace export.

use std::{fmt, time::Duration};

use anyhow::{Context, Result};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use tracing_subscriber::{
    EnvFilter,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

use crate::config::{LogFormat, TelemetryConfig};

/// Owns the optional exporter so shutdown can flush it deterministically.
pub(crate) struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    shutdown_timeout: Duration,
}

impl TelemetryGuard {
    pub(crate) fn install(config: &TelemetryConfig) -> Result<Self> {
        let shutdown_timeout = Duration::from_millis(config.export_timeout_ms);
        let providers = config
            .otlp_endpoint
            .as_deref()
            .map(|endpoint| build_providers(endpoint, shutdown_timeout))
            .transpose()?;
        let (tracer_provider, meter_provider) = match providers {
            Some((tracer, meter)) => (Some(tracer), Some(meter)),
            None => (None, None),
        };
        install_subscriber(config, tracer_provider.as_ref())?;
        if let Some(provider) = &tracer_provider {
            opentelemetry::global::set_tracer_provider(provider.clone());
        }
        if let Some(provider) = &meter_provider {
            opentelemetry::global::set_meter_provider(provider.clone());
        }
        Ok(Self {
            tracer_provider,
            meter_provider,
            shutdown_timeout,
        })
    }

    pub(crate) fn shutdown(&self) -> Result<()> {
        let metrics = self.meter_provider.as_ref().map(|provider| {
            provider
                .shutdown_with_timeout(self.shutdown_timeout)
                .context("failed to flush OpenTelemetry metrics")
        });
        let traces = self.tracer_provider.as_ref().map(|provider| {
            provider
                .shutdown_with_timeout(self.shutdown_timeout)
                .context("failed to flush OpenTelemetry traces")
        });
        if let Some(result) = metrics {
            result?;
        }
        if let Some(result) = traces {
            result?;
        }
        Ok(())
    }
}

impl fmt::Debug for TelemetryGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryGuard")
            .field("otlp_traces_enabled", &self.tracer_provider.is_some())
            .field("otlp_metrics_enabled", &self.meter_provider.is_some())
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish()
    }
}

fn build_providers(
    endpoint: &str,
    timeout: Duration,
) -> Result<(SdkTracerProvider, SdkMeterProvider)> {
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(timeout)
        .build()
        .context("failed to build OTLP trace exporter")?;
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(timeout)
        .build()
        .context("failed to build OTLP metric exporter")?;
    let resource = Resource::builder()
        .with_service_name("openfga-server")
        .build();
    let tracer = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();
    let meter = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(metric_exporter)
        .build();
    Ok((tracer, meter))
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
