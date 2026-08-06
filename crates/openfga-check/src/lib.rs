//! Correctness-first `Check` oracle with bounded iterative traversal.
//!
//! The direct evaluator owns an explicit work graph instead of recursively
//! polling Rust futures. Rewrite operands retain deterministic source ordering,
//! branch-local cycle state is copied for each semantic child, and every spawned
//! datastore read is joined before a root returns.
//!
//! # Examples
//!
//! Evaluator limits are independently bounded and validated before use:
//!
//! ```
//! use openfga_check::CheckBudget;
//! use openfga_domain::Limit;
//!
//! let budget = CheckBudget::builder()
//!     .depth(Limit::<1_000>::new(32)?)
//!     .datastore_queries(Limit::<100_000>::new(128)?)
//!     .build();
//!
//! assert_eq!(budget.maximum_depth(), 32);
//! assert_eq!(budget.maximum_datastore_queries(), 128);
//! # Ok::<(), openfga_domain::LimitError>(())
//! ```

#![forbid(unsafe_code)]

mod budget;
mod error;
mod evaluator;
mod outcome;

pub use budget::{CheckBudget, CheckBudgetBuilder};
pub use error::{CheckError, CheckErrorKind};
pub use evaluator::{CheckEvaluator, DirectCheckEvaluator};
pub use outcome::{
    BatchCheckOutcome, BatchCheckResult, CheckMetadata, CheckOutcome, CheckResolution,
};
