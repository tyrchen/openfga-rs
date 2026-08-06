//! Validated domain values, commands, limits, and shared semantic errors.
//!
//! Wire and textual values enter the engine only after conversion into the
//! private-field types exported by this crate. Constructors enforce grammar,
//! byte, count, depth, and cross-field invariants once at the trust boundary.
//!
//! # Examples
//!
//! Canonical tuple parsing and bounded condition-context conversion use the
//! same policy regardless of whether a caller arrived through HTTP, gRPC, or
//! an in-process API:
//!
//! ```
//! use openfga_domain::{ConditionContext, InputLimits, TupleKey};
//! use serde_json::json;
//!
//! let limits = InputLimits::default();
//! let tuple = TupleKey::parse_with_limits(
//!     "document:roadmap#viewer@group:engineering#member",
//!     &limits,
//! )?;
//! let context = ConditionContext::try_from_json(json!({"country": "US"}), &limits)?;
//!
//! assert_eq!(tuple.to_string(), "document:roadmap#viewer@group:engineering#member");
//! assert_eq!(context.iter().len(), 1);
//! # Ok::<(), openfga_domain::DomainError>(())
//! ```

#![forbid(unsafe_code)]

mod command;
mod context;
mod error;
mod fingerprint;
mod identifier;
mod limits;
mod reference;
mod token;

pub use command::{
    BatchCheckCommand, BatchCheckItem, BatchCheckItems, CheckCommand, ConsistencyPreference,
    Deadline, ExpandCommand, ListControl, ListObjectsCommand, ListUsersCommand, ModelSelection,
    Principal, PrincipalKind, QueryContext, QueryContextBuilder, RequestTimeout, UserTypeFilter,
    UserTypeFilters,
};
pub use context::{
    ConditionContext, ContextBytes, ContextKey, ContextList, ContextMap, ContextString,
    ContextValue, FiniteFloat,
};
pub use error::{
    DomainError, ParseError, ParseKind, ResourceKind, SubsystemError, ValidationError,
    ValidationReason,
};
pub use fingerprint::{Fingerprint, FingerprintBuilder};
pub use identifier::{
    AuthorizationModelId, ChangeId, ConditionName, CorrelationId, ObjectId, ParameterName,
    PrincipalId, RelationName, StoreId, TokenKeyId, TypeName,
};
pub use limits::{InputLimits, InputLimitsBuilder, Limit, LimitError};
pub use reference::{
    ConditionBinding, ConditionReference, ContextualTuples, ObjectRef, RelationshipTuple,
    SubjectRef, TupleKey, UsersetRef,
};
pub use token::{ContinuationCursor, ContinuationScope, TokenCodec, TokenKey, TokenOperation};
