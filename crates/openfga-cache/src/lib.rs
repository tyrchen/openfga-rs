//! Bounded consistency-aware caches and supervised invalidation controllers.
//!
//! Immutable authorization-model caches are safe under every consistency
//! preference because model identifiers never change their meaning. Mutable
//! tuple and decision caches are exposed separately so their callers must make
//! an explicit consistency and invalidation choice.

#![forbid(unsafe_code)]

mod controller;
mod decision;
mod invalidation;
mod model;
mod tuple;

pub use controller::{
    InvalidationController, InvalidationControllerConfig, InvalidationControllerConfigError,
    InvalidationControllerDiagnostics, InvalidationControllerError, InvalidationControllerHandle,
};
pub use decision::{
    DecisionCache, DecisionCacheConfig, DecisionCacheConfigError, DecisionKey, DecisionKeyHasher,
    DecisionKeyHasherError,
};
pub use invalidation::InvalidationWatermark;
pub use model::{CachedModelStorage, ModelCacheConfig, ModelCacheConfigError};
pub use tuple::{CachedTupleStorage, TupleCacheConfig, TupleCacheConfigError};
