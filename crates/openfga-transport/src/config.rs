//! Bounded transport policy and service assembly.

use std::{fmt, num::NonZeroU32, sync::Arc, time::Duration};

use openfga_domain::{InputLimits, Principal, RequestTimeout, TokenCodec};
use openfga_service::{
    AssertionService, ChangeService, CheckService, ModelService, StoreService, TupleService,
};
use typed_builder::TypedBuilder;

const MAXIMUM_MESSAGE_BYTES: usize = 16 * 1_024 * 1_024;
const MAXIMUM_CONCURRENCY: usize = 65_536;
const MAXIMUM_TOKEN_TTL: Duration = Duration::from_hours(720);

/// Complete service set consumed by the `OpenFGA` transport adapters.
#[derive(Clone, Debug, TypedBuilder)]
pub struct OpenFgaServices {
    /// Store lifecycle use cases.
    pub(crate) stores: StoreService,
    /// Authorization-model use cases.
    pub(crate) models: ModelService,
    /// Assertion use cases.
    pub(crate) assertions: AssertionService,
    /// Relationship-tuple use cases.
    pub(crate) tuples: TupleService,
    /// Tuple-changelog use cases.
    pub(crate) changes: ChangeService,
    /// Check and `BatchCheck` use cases.
    pub(crate) checks: CheckService,
}

/// Validated finite policy shared by HTTP and gRPC adapters.
#[derive(Clone, TypedBuilder)]
pub struct TransportConfig {
    /// Domain input ceilings.
    pub(crate) limits: InputLimits,
    /// Authenticated principal supplied until request authentication lands in task 2.5.
    pub(crate) principal: Principal,
    /// Rotating continuation-token codec.
    pub(crate) token_codec: Arc<TokenCodec>,
    /// Default number of records returned by list methods.
    #[builder(default = trusted_page_size(50))]
    pub(crate) default_page_size: NonZeroU32,
    /// Maximum request duration.
    pub(crate) request_timeout: RequestTimeout,
    /// Continuation-token lifetime.
    #[builder(default = Duration::from_hours(24))]
    pub(crate) token_ttl: Duration,
    /// Maximum accepted HTTP body or gRPC message size.
    #[builder(default = 1_048_576)]
    pub(crate) maximum_message_bytes: usize,
    /// Maximum concurrent requests admitted by the HTTP adapter.
    #[builder(default = 1_024)]
    pub(crate) maximum_concurrency: usize,
}

const fn trusted_page_size(value: u32) -> NonZeroU32 {
    match NonZeroU32::new(value) {
        Some(value) => value,
        None => NonZeroU32::MIN,
    }
}

impl TransportConfig {
    /// Validates relationships among individually bounded transport settings.
    ///
    /// # Errors
    ///
    /// Returns a static diagnostic code when a transport ceiling is invalid.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.default_page_size.get() > self.limits.results() {
            return Err("default_page_size_exceeds_result_limit");
        }
        if self.token_ttl.as_secs() == 0 || self.token_ttl > MAXIMUM_TOKEN_TTL {
            return Err("token_ttl_out_of_range");
        }
        if !(1..=MAXIMUM_MESSAGE_BYTES).contains(&self.maximum_message_bytes) {
            return Err("maximum_message_bytes_out_of_range");
        }
        if !(1..=MAXIMUM_CONCURRENCY).contains(&self.maximum_concurrency) {
            return Err("maximum_concurrency_out_of_range");
        }
        Ok(())
    }
}

impl fmt::Debug for TransportConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportConfig")
            .field("limits", &self.limits)
            .field("principal", &self.principal)
            .field("token_codec", &"[REDACTED]")
            .field("default_page_size", &self.default_page_size)
            .field("request_timeout", &self.request_timeout)
            .field("token_ttl", &self.token_ttl)
            .field("maximum_message_bytes", &self.maximum_message_bytes)
            .field("maximum_concurrency", &self.maximum_concurrency)
            .finish_non_exhaustive()
    }
}
