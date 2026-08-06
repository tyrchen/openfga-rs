//! Transport-neutral use cases and orchestration across semantic capabilities.
//!
//! The service layer resolves the immutable model selected by a validated
//! command, then delegates authorization semantics to [`openfga_check`]. Wire
//! conversion, authentication, and protocol error mapping remain transport
//! responsibilities.

#![forbid(unsafe_code)]

mod check;
mod error;

pub use check::CheckService;
pub use error::{ServiceError, ServiceErrorKind};
