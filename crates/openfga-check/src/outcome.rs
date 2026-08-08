//! Redacted decisions and bounded execution metadata.

use std::{fmt, time::Duration};

use openfga_domain::CorrelationId;

use crate::CheckError;

/// Internal evidence class that resolved a successful check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CheckResolution {
    /// A complete semantic decision key resolved from the mutable cache.
    Cached,
    /// No authorization path matched.
    Denied,
    /// Conservative reachability proved that no path can match.
    Unreachable,
    /// A branch-local semantic cycle was denied.
    Cycle,
    /// Direct tuple membership resolved the query.
    Direct,
    /// A same-object computed relation resolved the query.
    Computed,
    /// A tuple-to-userset edge resolved the query.
    TupleToUserset,
    /// A union reducer resolved the query.
    Union,
    /// An intersection reducer resolved the query.
    Intersection,
    /// A difference reducer resolved the query.
    Difference,
}

/// Low-cardinality counters for one completed root evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CheckMetadata {
    dispatches: u32,
    datastore_queries: u32,
    tuple_items: u32,
    condition_cost: u32,
    cycles: u32,
    maximum_depth: u32,
    duration: Duration,
}

impl CheckMetadata {
    pub(crate) const fn new(
        dispatches: u32,
        datastore_queries: u32,
        tuple_items: u32,
        condition_cost: u32,
        cycles: u32,
        maximum_depth: u32,
        duration: Duration,
    ) -> Self {
        Self {
            dispatches,
            datastore_queries,
            tuple_items,
            condition_cost,
            cycles,
            maximum_depth,
            duration,
        }
    }

    /// Returns the semantic and rewrite work dispatched.
    #[must_use]
    pub const fn dispatches(self) -> u32 {
        self.dispatches
    }

    /// Returns datastore calls made by the root.
    #[must_use]
    pub const fn datastore_queries(self) -> u32 {
        self.datastore_queries
    }

    /// Returns stored and contextual tuple rows inspected.
    #[must_use]
    pub const fn tuple_items(self) -> u32 {
        self.tuple_items
    }

    /// Returns aggregate deterministic condition cost.
    #[must_use]
    pub const fn condition_cost(self) -> u32 {
        self.condition_cost
    }

    /// Returns branch-local cycles denied during traversal.
    #[must_use]
    pub const fn cycles(self) -> u32 {
        self.cycles
    }

    /// Returns the deepest semantic branch reached.
    #[must_use]
    pub const fn maximum_depth(self) -> u32 {
        self.maximum_depth
    }

    /// Returns elapsed root evaluation time.
    #[must_use]
    pub const fn duration(self) -> Duration {
        self.duration
    }
}

/// One successful Boolean authorization decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CheckOutcome {
    allowed: bool,
    resolution: CheckResolution,
    metadata: CheckMetadata,
}

impl CheckOutcome {
    pub(crate) const fn new(
        allowed: bool,
        resolution: CheckResolution,
        metadata: CheckMetadata,
    ) -> Self {
        Self {
            allowed,
            resolution,
            metadata,
        }
    }

    /// Returns whether the relationship is allowed.
    #[must_use]
    pub const fn allowed(self) -> bool {
        self.allowed
    }

    /// Returns the redacted internal evidence class.
    #[must_use]
    pub const fn resolution(self) -> CheckResolution {
        self.resolution
    }

    /// Returns bounded execution metadata.
    #[must_use]
    pub const fn metadata(self) -> CheckMetadata {
        self.metadata
    }
}

/// One request-ordered `BatchCheck` item result.
pub struct BatchCheckResult {
    correlation_id: CorrelationId,
    outcome: Result<CheckOutcome, CheckError>,
}

impl BatchCheckResult {
    pub(crate) const fn new(
        correlation_id: CorrelationId,
        outcome: Result<CheckOutcome, CheckError>,
    ) -> Self {
        Self {
            correlation_id,
            outcome,
        }
    }

    /// Returns the caller-provided stable result key.
    #[must_use]
    pub const fn correlation_id(&self) -> &CorrelationId {
        &self.correlation_id
    }

    /// Returns the independent item decision or typed item failure.
    pub const fn outcome(&self) -> &Result<CheckOutcome, CheckError> {
        &self.outcome
    }
}

impl fmt::Debug for BatchCheckResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BatchCheckResult")
            .field("correlation_id_bytes", &self.correlation_id.as_str().len())
            .field("succeeded", &self.outcome.is_ok())
            .finish()
    }
}

/// Request-ordered results for a bounded batch.
#[derive(Debug)]
#[non_exhaustive]
pub struct BatchCheckOutcome(Box<[BatchCheckResult]>);

impl BatchCheckOutcome {
    pub(crate) fn new(results: Vec<BatchCheckResult>) -> Self {
        Self(results.into_boxed_slice())
    }

    /// Returns every item result in request order.
    #[must_use]
    pub const fn results(&self) -> &[BatchCheckResult] {
        &self.0
    }
}
