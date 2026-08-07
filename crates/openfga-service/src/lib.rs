//! Transport-neutral use cases and orchestration across semantic capabilities.
//!
//! The service layer resolves the immutable model selected by a validated
//! command, then delegates authorization semantics to bounded semantic engines.
//! Wire conversion, authentication, and protocol error mapping remain transport
//! responsibilities.

#![forbid(unsafe_code)]

mod assertion;
mod change;
mod check;
mod clock;
mod common;
mod error;
mod expand;
mod identifier;
mod list_objects;
mod list_users;
mod model;
mod store;
mod tuple;

pub use assertion::{AssertionService, AssertionSet, ResolvedAssertionModel};
pub use change::ChangeService;
pub use check::{CheckService, ResolvedCheckModel};
pub use clock::{ServiceClock, SystemServiceClock};
pub use error::{
    ModelRelationType, ModelSemanticContext, ModelSetOperator, ServiceError, ServiceErrorKind,
};
pub use expand::{ExpandService, ResolvedExpandModel};
pub use identifier::{
    IdentifierSource, IdentifierSourceError, IdentifierSourceErrorKind, SystemIdentifierSource,
    SystemIdentifierSourceConfig,
};
pub use list_objects::{ListObjectsService, ResolvedListObjectsModel};
pub use list_users::{ListUsersService, ResolvedListUsersModel};
pub use model::{ModelPublication, ModelService};
pub use store::StoreService;
pub use tuple::{ResolvedTupleWriteModel, TupleContextSizePolicy, TupleService};
