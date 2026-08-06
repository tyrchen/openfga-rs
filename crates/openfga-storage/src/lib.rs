//! Narrow, object-safe storage capabilities and backend-independent records.
//!
//! The async capability traits deliberately use `async-trait`: server assembly
//! stores them behind `Arc<dyn Trait>`, while native async trait methods are not
//! dyn-compatible. Every input is a validated domain value or a bounded filter;
//! no backend receives generated protobufs or free-form query predicates.
//!
//! # Examples
//!
//! ```
//! use openfga_domain::{RelationshipTuple, TupleKey};
//! use openfga_storage::TupleStream;
//!
//! let tuple = RelationshipTuple::unconditional(
//!     "document:roadmap#viewer@user:anne".parse::<TupleKey>()?,
//! );
//! let mut stream = TupleStream::from_tuples(vec![tuple]);
//! assert!(stream.next_item().transpose()?.is_some());
//! assert!(stream.is_closed());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![forbid(unsafe_code)]

mod context;
mod error;
mod stream;
mod traits;
mod types;

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod contract;

pub use context::{OperationContext, StorageCancellationToken};
pub use error::{StorageError, StorageErrorKind};
pub use stream::TupleStream;
pub use traits::{
    AssertionReader, AssertionWriter, ChangeReader, HealthCheck, ModelReader, ModelWriter,
    StoreReader, StoreWriter, TupleReader, TupleWriter,
};
pub use types::{
    Assertion, ChangeFilter, ChangeOperation, ConditionFilter, HealthStatus, MutationOutcome,
    ObjectRelationFilter, Page, PageOptions, ReadOptions, ReverseTupleFilter, StorageCursor,
    StoreName, StoreRecord, StoredAuthorizationModel, StoredTuple, TupleChange, TupleReadFilter,
    TupleWriteOptions, UsersetRestrictionFilter, UsersetTupleFilter, WriteConflictPolicy,
};
