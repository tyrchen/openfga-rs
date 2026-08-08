//! Low-cardinality cache metrics shared by cache implementations.

use std::fmt;

use opentelemetry::{KeyValue, metrics::Counter};

#[derive(Clone)]
pub(crate) struct CacheMetrics {
    requests: Counter<u64>,
}

impl CacheMetrics {
    pub(crate) fn new() -> Self {
        Self {
            requests: opentelemetry::global::meter("openfga-cache")
                .u64_counter("openfga.cache.requests")
                .with_description("Cache lookups by bounded cache and result class")
                .build(),
        }
    }

    pub(crate) fn record(&self, cache: &'static str, result: &'static str) {
        self.requests.add(
            1,
            &[
                KeyValue::new("cache", cache),
                KeyValue::new("result", result),
            ],
        );
    }
}

impl fmt::Debug for CacheMetrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CacheMetrics")
    }
}
