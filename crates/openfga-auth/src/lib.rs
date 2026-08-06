//! Principals, authentication mechanisms, and store/action authorization policy.
//!
//! Request credentials are converted into redacted domain principals before service
//! authorization. OIDC keys are fetched by one supervised actor and published through a watch
//! channel; request paths perform no network I/O. Policy evaluation is explicit and default-deny.

#![forbid(unsafe_code)]

mod authenticate;
mod oidc;
mod policy;

pub use authenticate::{
    AuthenticationConfigurationError, AuthenticationError, AuthenticationService, PresharedKey,
};
pub use oidc::{JwksActor, OidcAlgorithm, OidcConfig, OidcConfigBuilder, OidcError};
pub use policy::{Action, AuthorizationError, AuthorizationPolicy, PolicyBinding, StoreScope};
