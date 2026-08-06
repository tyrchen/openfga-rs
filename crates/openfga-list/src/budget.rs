//! Independent finite ceilings for reverse candidate discovery.

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
