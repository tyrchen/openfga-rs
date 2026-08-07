//! Finite independent evaluator resource ceilings.

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use openfga_domain::Limit;
use typed_builder::TypedBuilder;

const fn trusted_limit<const MAX: u32>(value: u32) -> Limit<MAX> {
    match Limit::new(value) {
        Ok(limit) => limit,
        Err(_) => Limit::MIN,
    }
}

/// One validated set of independent limits for an authorization query root.
///
/// The generated builder accepts only ceiling-checked [`Limit`] values. This
/// keeps runtime configuration conversion fallible while making an invalid
/// evaluator budget unrepresentable once constructed.
#[derive(Clone, Copy, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct CheckBudget {
    #[builder(default = trusted_limit::<1_000>(25))]
    depth: Limit<1_000>,
    #[builder(default = trusted_limit::<1_000_000>(10_000))]
    dispatches: Limit<1_000_000>,
    #[builder(default = trusted_limit::<100_000>(100))]
    datastore_queries: Limit<100_000>,
    #[builder(default = trusted_limit::<1_000_000>(10_000))]
    tuple_items: Limit<1_000_000>,
    #[builder(default = trusted_limit::<1_000_000>(100_000))]
    condition_cost: Limit<1_000_000>,
    #[builder(default = trusted_limit::<1_024>(16))]
    concurrent_reads: Limit<1_024>,
    #[builder(default = trusted_limit::<1_000>(16))]
    batch_concurrency: Limit<1_000>,
}

impl CheckBudget {
    /// Maximum number of semantic edges on one branch.
    #[must_use]
    pub const fn maximum_depth(self) -> u32 {
        self.depth.get()
    }

    /// Maximum semantic and rewrite work items created by one root.
    #[must_use]
    pub const fn maximum_dispatches(self) -> u32 {
        self.dispatches.get()
    }

    /// Maximum datastore calls made by one root.
    #[must_use]
    pub const fn maximum_datastore_queries(self) -> u32 {
        self.datastore_queries.get()
    }

    /// Maximum stored and contextual tuple rows inspected by one root.
    #[must_use]
    pub const fn maximum_tuple_items(self) -> u32 {
        self.tuple_items.get()
    }

    /// Maximum aggregate deterministic condition cost charged by one root.
    #[must_use]
    pub const fn maximum_condition_cost(self) -> u32 {
        self.condition_cost.get()
    }

    /// Maximum concurrent datastore reads within one root.
    #[must_use]
    pub const fn maximum_concurrent_reads(self) -> usize {
        self.concurrent_reads.as_usize()
    }

    /// Maximum concurrently evaluated `BatchCheck` items.
    #[must_use]
    pub const fn maximum_batch_concurrency(self) -> usize {
        self.batch_concurrency.as_usize()
    }
}

impl Default for CheckBudget {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Request-scoped work meter shared by multiple independent Check roots.
///
/// Each charge is admitted atomically before the evaluator creates the
/// corresponding dispatch, datastore read, or tuple-processing work. This is
/// used by enumeration to apply one aggregate residual-Check budget even when
/// several roots execute concurrently.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CheckWorkMeter {
    dispatches: Arc<SharedCounter>,
    datastore_queries: Arc<SharedCounter>,
    tuple_items: Arc<SharedCounter>,
}

impl CheckWorkMeter {
    /// Creates an empty meter with independent validated ceilings.
    #[must_use]
    pub fn new(
        dispatches: Limit<1_000_000>,
        datastore_queries: Limit<100_000>,
        tuple_items: Limit<1_000_000>,
    ) -> Self {
        Self {
            dispatches: Arc::new(SharedCounter::new(dispatches.get())),
            datastore_queries: Arc::new(SharedCounter::new(datastore_queries.get())),
            tuple_items: Arc::new(SharedCounter::new(tuple_items.get())),
        }
    }

    pub(crate) fn charge_dispatches(&self, amount: u32) -> bool {
        self.dispatches.charge(amount)
    }

    pub(crate) fn charge_datastore_queries(&self, amount: u32) -> bool {
        self.datastore_queries.charge(amount)
    }

    pub(crate) fn charge_tuple_items(&self, amount: u32) -> bool {
        self.tuple_items.charge(amount)
    }
}

#[derive(Debug)]
struct SharedCounter {
    used: AtomicU32,
    maximum: u32,
}

impl SharedCounter {
    const fn new(maximum: u32) -> Self {
        Self {
            used: AtomicU32::new(0),
            maximum,
        }
    }

    fn charge(&self, amount: u32) -> bool {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(amount)
                    .filter(|next| *next <= self.maximum)
            })
            .is_ok()
    }
}
