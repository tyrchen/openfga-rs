//! Bounded OpenFGA-compatible CEL condition compilation and evaluation.
//!
//! Expressions compile once into immutable project-owned state. Evaluation
//! overlays tuple context over request context, enforces declared types without
//! lossy conversion, applies the pinned CEL actual-cost model, and checks
//! cancellation throughout bounded evaluation.
//!
//! # Examples
//!
//! ```
//! use std::collections::BTreeMap;
//!
//! use openfga_condition::{
//!     CancellationToken, ConditionCompiler, ConditionDefinition, ConditionLimits,
//!     EvaluationBudget,
//! };
//! use openfga_domain::{ConditionContext, ConditionName};
//!
//! let definition = ConditionDefinition::new(
//!     ConditionName::try_from("business_hours")?,
//!     "timestamp('2024-01-01T00:00:00Z') < timestamp('2025-01-01T00:00:00Z')".to_owned(),
//!     BTreeMap::new(),
//! );
//! let compiled = ConditionCompiler::default().compile(&definition, &ConditionLimits::default())?;
//! let outcome = compiled.evaluate(
//!     &ConditionContext::empty(),
//!     &ConditionContext::empty(),
//!     EvaluationBudget::new(100)?,
//!     &CancellationToken::new(),
//! )?;
//!
//! assert!(outcome.condition_met());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![forbid(unsafe_code)]

mod compiler;
mod error;
mod evaluator;
mod ir;
mod types;
mod value;

pub use compiler::{CompiledCondition, ConditionCompiler};
pub use error::{CompileError, CompileErrorKind, EvaluationError, EvaluationErrorKind};
pub use types::{
    CancellationToken, ConditionDefinition, ConditionLimits, ConditionLimitsBuilder,
    ConditionOutcome, EvaluationBudget, ParameterType,
};
