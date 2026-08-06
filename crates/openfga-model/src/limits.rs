//! Bounded authorization-model compiler policy.

use openfga_condition::ConditionLimits;
use openfga_domain::{InputLimits, Limit};
use typed_builder::TypedBuilder;

const fn trusted_limit<const MAX: u32>(value: u32) -> Limit<MAX> {
    match Limit::new(value) {
        Ok(limit) => limit,
        Err(_) => Limit::MIN,
    }
}

/// Runtime-configurable model ceilings bounded by compiled safety maxima.
#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct ModelLimits {
    #[builder(default)]
    input: InputLimits,
    #[builder(default)]
    conditions: ConditionLimits,
    #[builder(default = trusted_limit::<1_000>(100))]
    condition_definitions: Limit<1_000>,
    #[builder(default = trusted_limit::<100_000>(10_000))]
    rewrite_nodes: Limit<100_000>,
    #[builder(default = trusted_limit::<256>(64))]
    rewrite_depth: Limit<256>,
    #[builder(default = trusted_limit::<1_000_000>(100_000))]
    graph_edges: Limit<1_000_000>,
    #[builder(default = trusted_limit::<1_000>(100))]
    model_errors: Limit<1_000>,
}

impl Default for ModelLimits {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ModelLimits {
    /// Returns the shared boundary-input policy.
    #[must_use]
    pub const fn input(&self) -> &InputLimits {
        &self.input
    }

    /// Returns the condition compiler policy.
    #[must_use]
    pub const fn conditions(&self) -> &ConditionLimits {
        &self.conditions
    }

    pub(crate) const fn condition_definitions(&self) -> usize {
        self.condition_definitions.as_usize()
    }

    pub(crate) const fn rewrite_nodes(&self) -> usize {
        self.rewrite_nodes.as_usize()
    }

    pub(crate) const fn rewrite_depth(&self) -> usize {
        self.rewrite_depth.as_usize()
    }

    pub(crate) const fn graph_edges(&self) -> usize {
        self.graph_edges.as_usize()
    }

    pub(crate) const fn model_errors(&self) -> usize {
        self.model_errors.as_usize()
    }
}
