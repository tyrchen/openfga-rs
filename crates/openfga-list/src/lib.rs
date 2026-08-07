//! Bounded enumeration and expansion engines with conservative candidate traversal.
//!
//! Reverse candidate discovery is a monotone fixpoint over compiled rewrite
//! dependencies. Every datastore access uses the exact reverse tuple capability;
//! no operation can fall back to scanning a store-wide object universe.

#![forbid(unsafe_code)]

mod budget;
mod candidate;
mod common;
mod error;
mod expand;
mod list_objects;
mod list_users;

pub use budget::{
    CandidateBudget, CandidateBudgetBuilder, ExpandBudget, ExpandBudgetBuilder, ListObjectsBudget,
    ListObjectsBudgetBuilder, ListUsersBudget, ListUsersBudgetBuilder,
};
pub use candidate::{Candidate, CandidateMetadata, CandidateSet, ReverseCandidateTraversal};
pub use error::{ListError, ListErrorKind};
pub use expand::{
    DirectExpandEngine, ExpandEngine, ExpandMetadata, ExpandNode, ExpandNodeValue, ExpandOutcome,
};
pub use list_objects::{
    DirectListObjectsEngine, ListObjectsEngine, ListObjectsMetadata, ListObjectsOutcome,
    ListObjectsStream,
};
pub use list_users::{DirectListUsersEngine, ListUsersEngine, ListUsersMetadata, ListUsersOutcome};
