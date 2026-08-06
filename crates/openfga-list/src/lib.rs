//! Bounded enumeration and expansion engines with conservative candidate traversal.
//!
//! Reverse candidate discovery is a monotone fixpoint over compiled rewrite
//! dependencies. Every datastore access uses the exact reverse tuple capability;
//! no operation can fall back to scanning a store-wide object universe.

#![forbid(unsafe_code)]

mod budget;
mod candidate;
mod error;

pub use budget::{CandidateBudget, CandidateBudgetBuilder};
pub use candidate::{Candidate, CandidateMetadata, CandidateSet, ReverseCandidateTraversal};
pub use error::{ListError, ListErrorKind};
