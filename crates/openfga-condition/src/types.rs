//! Validated condition definitions, parameter types, and runtime controls.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use openfga_domain::{ConditionContext, ConditionName, Fingerprint, Limit, ParameterName};
use typed_builder::TypedBuilder;

use crate::error::{CompileError, CompileErrorKind};

const MAX_GENERIC_DEPTH: u8 = 16;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ParameterTypeKind {
    Any,
    Bool,
    String,
    Int,
    Uint,
    Double,
    Bytes,
    Duration,
    Timestamp,
    IpAddress,
    List(Box<ParameterType>),
    Map(Box<ParameterType>),
}

/// One statically declared `OpenFGA` condition parameter type.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct ParameterType {
    pub(crate) kind: ParameterTypeKind,
    depth: u8,
}

/// Borrowed structural view of one validated condition parameter type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParameterTypeRef<'a> {
    /// Dynamic CEL `any`.
    Any,
    /// Boolean.
    Bool,
    /// UTF-8 string.
    String,
    /// Signed 64-bit integer.
    Int,
    /// Unsigned 64-bit integer.
    Uint,
    /// Finite double.
    Double,
    /// Byte string.
    Bytes,
    /// Signed duration.
    Duration,
    /// Absolute timestamp.
    Timestamp,
    /// IP address.
    IpAddress,
    /// Homogeneous list.
    List(&'a ParameterType),
    /// String-keyed homogeneous map.
    Map(&'a ParameterType),
}

macro_rules! scalar_parameter_type {
    ($name:ident, $kind:ident, $docs:literal) => {
        #[doc = $docs]
        #[must_use]
        pub const fn $name() -> Self {
            Self {
                kind: ParameterTypeKind::$kind,
                depth: 1,
            }
        }
    };
}

impl ParameterType {
    scalar_parameter_type!(any, Any, "Returns the dynamic CEL `any` type.");
    scalar_parameter_type!(bool, Bool, "Returns the Boolean type.");
    scalar_parameter_type!(string, String, "Returns the UTF-8 string type.");
    scalar_parameter_type!(int, Int, "Returns the signed 64-bit integer type.");
    scalar_parameter_type!(uint, Uint, "Returns the unsigned 64-bit integer type.");
    scalar_parameter_type!(double, Double, "Returns the finite IEEE-754 double type.");
    scalar_parameter_type!(bytes, Bytes, "Returns the byte-string type.");
    scalar_parameter_type!(duration, Duration, "Returns the signed duration type.");
    scalar_parameter_type!(timestamp, Timestamp, "Returns the absolute timestamp type.");
    scalar_parameter_type!(ip_address, IpAddress, "Returns the parsed IP-address type.");

    /// Creates a bounded homogeneous list type.
    ///
    /// # Errors
    ///
    /// Returns [`CompileError`] when generic nesting exceeds the compiled ceiling.
    pub fn list(element: Self) -> Result<Self, CompileError> {
        let depth = element.depth;
        Self::collection(ParameterTypeKind::List(Box::new(element)), depth)
    }

    /// Creates a bounded string-keyed homogeneous map type.
    ///
    /// # Errors
    ///
    /// Returns [`CompileError`] when generic nesting exceeds the compiled ceiling.
    pub fn map(value: Self) -> Result<Self, CompileError> {
        let depth = value.depth;
        Self::collection(ParameterTypeKind::Map(Box::new(value)), depth)
    }

    fn collection(kind: ParameterTypeKind, child_depth: u8) -> Result<Self, CompileError> {
        let depth = child_depth
            .checked_add(1)
            .ok_or_else(|| CompileError::new(CompileErrorKind::LimitExceeded, 0))?;
        if depth > MAX_GENERIC_DEPTH {
            return Err(CompileError::new(CompileErrorKind::LimitExceeded, 0));
        }
        Ok(Self { kind, depth })
    }

    /// Returns a structural view suitable for validated wire or persistence conversion.
    #[must_use]
    pub const fn as_ref(&self) -> ParameterTypeRef<'_> {
        match &self.kind {
            ParameterTypeKind::Any => ParameterTypeRef::Any,
            ParameterTypeKind::Bool => ParameterTypeRef::Bool,
            ParameterTypeKind::String => ParameterTypeRef::String,
            ParameterTypeKind::Int => ParameterTypeRef::Int,
            ParameterTypeKind::Uint => ParameterTypeRef::Uint,
            ParameterTypeKind::Double => ParameterTypeRef::Double,
            ParameterTypeKind::Bytes => ParameterTypeRef::Bytes,
            ParameterTypeKind::Duration => ParameterTypeRef::Duration,
            ParameterTypeKind::Timestamp => ParameterTypeRef::Timestamp,
            ParameterTypeKind::IpAddress => ParameterTypeRef::IpAddress,
            ParameterTypeKind::List(child) => ParameterTypeRef::List(child),
            ParameterTypeKind::Map(child) => ParameterTypeRef::Map(child),
        }
    }
}

/// An authorization-model condition before compilation.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConditionDefinition {
    name: ConditionName,
    expression: String,
    parameters: BTreeMap<ParameterName, ParameterType>,
}

impl ConditionDefinition {
    /// Creates a condition definition. Structural expression limits are enforced by compilation.
    #[must_use]
    pub fn new(
        name: ConditionName,
        expression: String,
        parameters: BTreeMap<ParameterName, ParameterType>,
    ) -> Self {
        Self {
            name,
            expression,
            parameters,
        }
    }

    /// Returns the condition name.
    #[must_use]
    pub const fn name(&self) -> &ConditionName {
        &self.name
    }

    /// Returns source text. Callers must not include it in logs or errors.
    #[must_use]
    pub fn expression(&self) -> &str {
        &self.expression
    }

    /// Returns declared parameters in canonical name order.
    #[must_use]
    pub const fn parameters(&self) -> &BTreeMap<ParameterName, ParameterType> {
        &self.parameters
    }

    /// Returns a deterministic proof of this exact source definition.
    #[must_use]
    pub fn fingerprint(&self) -> Fingerprint {
        crate::compiler::fingerprint_definition(self)
    }
}

impl fmt::Debug for ConditionDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConditionDefinition")
            .field("name", &self.name)
            .field("expression_bytes", &self.expression.len())
            .field("parameters", &self.parameters.len())
            .finish_non_exhaustive()
    }
}

const fn trusted_limit<const MAX: u32>(value: u32) -> Limit<MAX> {
    match Limit::new(value) {
        Ok(limit) => limit,
        Err(_) => Limit::MIN,
    }
}

/// Runtime-configurable condition ceilings, each bounded by a compiled maximum.
#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct ConditionLimits {
    #[builder(default = trusted_limit::<16_384>(4_096))]
    expression_bytes: Limit<16_384>,
    #[builder(default = trusted_limit::<16_384>(4_096))]
    ast_nodes: Limit<16_384>,
    #[builder(default = trusted_limit::<128>(64))]
    ast_depth: Limit<128>,
    #[builder(default = trusted_limit::<4_096>(256))]
    identifiers: Limit<4_096>,
    #[builder(default = trusted_limit::<4_096>(1_024))]
    literals: Limit<4_096>,
    #[builder(default = trusted_limit::<256>(32))]
    comprehensions: Limit<256>,
    #[builder(default = trusted_limit::<256>(100))]
    parameters: Limit<256>,
    #[builder(default = trusted_limit::<1_024>(100))]
    literal_collection_items: Limit<1_024>,
    #[builder(default = trusted_limit::<4_194_304>(1_048_576))]
    runtime_value_bytes: Limit<4_194_304>,
    #[builder(default = trusted_limit::<100_000>(10_000))]
    runtime_collection_items: Limit<100_000>,
    #[builder(default = trusted_limit::<1_000_000>(100))]
    default_evaluation_cost: Limit<1_000_000>,
}

impl Default for ConditionLimits {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ConditionLimits {
    pub(crate) const fn expression_bytes(&self) -> usize {
        self.expression_bytes.as_usize()
    }
    pub(crate) const fn ast_nodes(&self) -> usize {
        self.ast_nodes.as_usize()
    }
    pub(crate) const fn ast_depth(&self) -> usize {
        self.ast_depth.as_usize()
    }
    pub(crate) const fn identifiers(&self) -> usize {
        self.identifiers.as_usize()
    }
    pub(crate) const fn literals(&self) -> usize {
        self.literals.as_usize()
    }
    pub(crate) const fn comprehensions(&self) -> usize {
        self.comprehensions.as_usize()
    }
    pub(crate) const fn parameters(&self) -> usize {
        self.parameters.as_usize()
    }
    pub(crate) const fn literal_collection_items(&self) -> usize {
        self.literal_collection_items.as_usize()
    }
    pub(crate) const fn runtime_value_bytes(&self) -> usize {
        self.runtime_value_bytes.as_usize()
    }
    pub(crate) const fn runtime_collection_items(&self) -> usize {
        self.runtime_collection_items.as_usize()
    }

    /// Returns the configured default operation budget.
    #[must_use]
    pub const fn default_evaluation_budget(&self) -> EvaluationBudget {
        EvaluationBudget {
            maximum_cost: self.default_evaluation_cost.get() as u64,
        }
    }
}

/// A positive deterministic evaluation operation budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct EvaluationBudget {
    maximum_cost: u64,
}

impl EvaluationBudget {
    /// Creates a budget at or below the compiled safety ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`CompileError`] for zero or more than one million operations.
    pub const fn new(maximum_cost: u64) -> Result<Self, CompileError> {
        if maximum_cost == 0 || maximum_cost > 1_000_000 {
            Err(CompileError::new(CompileErrorKind::LimitExceeded, 0))
        } else {
            Ok(Self { maximum_cost })
        }
    }

    pub(crate) const fn maximum_cost(self) -> u64 {
        self.maximum_cost
    }
}

/// A cheap cloneable cancellation signal checked by the evaluator.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct CancellationToken(Arc<AtomicBool>);

/// Read-only cancellation source polled throughout synchronous evaluation.
///
/// This trait lets an orchestration layer link condition work directly to its
/// own cancellation and deadline state without a bridging task or second token.
pub trait CancellationCheck: Send + Sync {
    /// Returns whether evaluation should stop promptly.
    fn is_cancelled(&self) -> bool;
}

impl CancellationToken {
    /// Creates a non-cancelled signal.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests prompt cancellation of every evaluator sharing this signal.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

impl CancellationCheck for CancellationToken {
    fn is_cancelled(&self) -> bool {
        self.is_cancelled()
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// Successful Boolean condition evaluation metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConditionOutcome {
    condition_met: bool,
    cost: u64,
}

impl ConditionOutcome {
    pub(crate) const fn new(condition_met: bool, cost: u64) -> Self {
        Self {
            condition_met,
            cost,
        }
    }

    /// Returns whether the relationship condition is satisfied.
    #[must_use]
    pub const fn condition_met(self) -> bool {
        self.condition_met
    }

    /// Returns deterministic operations charged by this evaluation.
    #[must_use]
    pub const fn cost(self) -> u64 {
        self.cost
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct EvaluationContexts<'a> {
    pub(crate) request: &'a ConditionContext,
    pub(crate) tuple: &'a ConditionContext,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CompiledMetadata {
    pub(crate) fingerprint: Fingerprint,
}
