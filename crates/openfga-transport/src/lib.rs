//! gRPC and HTTP adapters, middleware, validation, and wire error mapping.

#![forbid(unsafe_code)]

mod admission;
mod api;
mod config;
mod convert;
mod error;
mod grpc;
mod http;
mod pagination;
mod validation;

pub use admission::{AdmissionPolicy, AdmissionPolicyBuilder, EndpointClass};
pub use api::OpenFgaApi;
pub use config::{
    OpenFgaServices, OpenFgaServicesBuilder, TransportConfig, TransportConfigBuilder,
};
pub use convert::{assertion_from_wire, model_definition_from_wire, relationship_tuple_from_wire};
pub use error::ApiError;
pub use grpc::{
    AuthenticatedGrpcService, GrpcAuthenticationInterceptor, GrpcListObjectsStream, grpc_service,
};
pub use http::http_router;

#[cfg(test)]
mod tests;
