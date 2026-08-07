//! Independent finite ceilings for reverse candidate discovery.

use openfga_check::CheckBudget;
use openfga_domain::Limit;
use typed_builder::TypedBuilder;

const fn trusted_limit<const MAX: u32>(value: u32) -> Limit<MAX> {
    match Limit::new(value) {
        Ok(limit) => limit,
        Err(_) => Limit::MIN,
    }
}

/// Independent resource ceilings for one reverse candidate traversal.
#[derive(Clone, Copy, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct CandidateBudget {
    #[builder(default = trusted_limit::<1_000>(25))]
    depth: Limit<1_000>,
    #[builder(default = trusted_limit::<1_000_000>(10_000))]
    dispatches: Limit<1_000_000>,
    #[builder(default = trusted_limit::<100_000>(1_000))]
    datastore_queries: Limit<100_000>,
    #[builder(default = trusted_limit::<1_000_000>(10_000))]
    tuple_items: Limit<1_000_000>,
    #[builder(default = trusted_limit::<100_000>(10_000))]
    candidates: Limit<100_000>,
}

impl CandidateBudget {
    /// Maximum semantic edges traversed by one derived candidate.
    #[must_use]
    pub const fn maximum_depth(self) -> u32 {
        self.depth.get()
    }

    /// Maximum queued graph and propagation work items.
    #[must_use]
    pub const fn maximum_dispatches(self) -> u32 {
        self.dispatches.get()
    }

    /// Maximum reverse datastore queries.
    #[must_use]
    pub const fn maximum_datastore_queries(self) -> u32 {
        self.datastore_queries.get()
    }

    /// Maximum stored and contextual tuples inspected.
    #[must_use]
    pub const fn maximum_tuple_items(self) -> u32 {
        self.tuple_items.get()
    }

    /// Maximum distinct intermediate and final candidates.
    #[must_use]
    pub const fn maximum_candidates(self) -> u32 {
        self.candidates.get()
    }
}

impl Default for CandidateBudget {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Independent candidate, residual-Check, concurrency, and stream ceilings.
#[derive(Clone, Copy, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct ListObjectsBudget {
    #[builder(default = CandidateBudget::default())]
    candidate: CandidateBudget,
    #[builder(default = CheckBudget::default())]
    check: CheckBudget,
    #[builder(default = trusted_limit::<1_024>(16))]
    residual_concurrency: Limit<1_024>,
    #[builder(default = trusted_limit::<1_024>(16))]
    stream_buffer: Limit<1_024>,
}

impl ListObjectsBudget {
    /// Returns reverse-candidate ceilings.
    #[must_use]
    pub const fn candidate(self) -> CandidateBudget {
        self.candidate
    }

    /// Returns the independent budget applied to every residual Check.
    #[must_use]
    pub const fn check(self) -> CheckBudget {
        self.check
    }

    /// Returns maximum concurrent residual Checks.
    #[must_use]
    pub const fn maximum_residual_concurrency(self) -> usize {
        self.residual_concurrency.as_usize()
    }

    /// Returns bounded stream channel capacity.
    #[must_use]
    pub const fn stream_buffer(self) -> usize {
        self.stream_buffer.as_usize()
    }
}

impl Default for ListObjectsBudget {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Independent finite ceilings for one `ListUsers` expansion.
#[derive(Clone, Copy, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct ListUsersBudget {
    #[builder(default = trusted_limit::<1_000>(25))]
    depth: Limit<1_000>,
    #[builder(default = trusted_limit::<1_000_000>(10_000))]
    dispatches: Limit<1_000_000>,
    #[builder(default = trusted_limit::<100_000>(1_000))]
    datastore_queries: Limit<100_000>,
    #[builder(default = trusted_limit::<1_000_000>(10_000))]
    tuple_items: Limit<1_000_000>,
    #[builder(default = trusted_limit::<100_000>(10_000))]
    subjects: Limit<100_000>,
    #[builder(default = trusted_limit::<1_000_000>(100_000))]
    condition_cost: Limit<1_000_000>,
}

impl ListUsersBudget {
    /// Returns the maximum recursive userset depth.
    #[must_use]
    pub const fn maximum_depth(self) -> u32 {
        self.depth.get()
    }

    /// Returns the maximum rewrite and userset dispatch count.
    #[must_use]
    pub const fn maximum_dispatches(self) -> u32 {
        self.dispatches.get()
    }

    /// Returns the maximum number of forward datastore reads.
    #[must_use]
    pub const fn maximum_datastore_queries(self) -> u32 {
        self.datastore_queries.get()
    }

    /// Returns the maximum number of stored and contextual tuples inspected.
    #[must_use]
    pub const fn maximum_tuple_items(self) -> u32 {
        self.tuple_items.get()
    }

    /// Returns the maximum symbolic members or exclusions retained by one set.
    #[must_use]
    pub const fn maximum_subjects(self) -> u32 {
        self.subjects.get()
    }

    /// Returns the maximum cumulative CEL condition evaluation cost.
    #[must_use]
    pub const fn maximum_condition_cost(self) -> u32 {
        self.condition_cost.get()
    }
}

impl Default for ListUsersBudget {
    fn default() -> Self {
        Self::builder().build()
    }
}
