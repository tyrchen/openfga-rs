//! Transport-neutral use cases and orchestration across semantic capabilities.
//!
//! The service layer resolves the immutable model selected by a validated
//! command, then delegates authorization semantics to [`openfga_check`]. Wire
//! conversion, authentication, and protocol error mapping remain transport
//! responsibilities.

#![forbid(unsafe_code)]

mod assertion;
mod change;
mod check;
mod clock;
mod common;
mod error;
mod identifier;
mod model;
mod store;
mod tuple;

pub use assertion::{AssertionService, AssertionSet};
pub use change::ChangeService;
pub use check::CheckService;
pub use clock::{ServiceClock, SystemServiceClock};
pub use error::{ServiceError, ServiceErrorKind};
pub use identifier::{
    IdentifierSource, IdentifierSourceError, IdentifierSourceErrorKind, SystemIdentifierSource,
    SystemIdentifierSourceConfig,
};
pub use model::{ModelPublication, ModelService};
pub use store::StoreService;
pub use tuple::TupleService;
