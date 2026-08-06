//! Pinned `OpenFGA` v1 protobuf messages, gRPC services, and HTTP route metadata.
//!
//! Generated files are checked in and reproduced with `make proto`. The source and tool pins are
//! recorded in `proto.lock.json`; generated files must never be edited by hand.

#![forbid(unsafe_code)]

/// `OpenFGA` protocol packages.
pub mod openfga {
    /// Version 1 of the OpenFGA API.
    // The upstream protocol intentionally omits rustdoc on many generated fields. Keeping the
    // generated output byte-for-byte reproducible is more important than synthesizing comments.
    // Upstream-generated Tonic/Prost code is kept byte-reproducible and is not project-owned style.
    #[allow(clippy::all, clippy::pedantic, missing_docs)]
    pub mod v1 {
        include!("generated/openfga.v1.rs");
        include!("generated/openfga.v1.serde.rs");
    }
}

/// An HTTP method attached to an `OpenFGA` RPC by the pinned protocol source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpMethod {
    /// HTTP DELETE.
    Delete,
    /// HTTP GET.
    Get,
    /// HTTP POST.
    Post,
    /// HTTP PUT.
    Put,
}

/// HTTP route metadata derived from the pinned upstream `OpenAPI` artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRoute {
    /// HTTP method used by the route.
    pub method: HttpMethod,
    /// URI template declared by the protocol.
    pub path: &'static str,
    /// Protocol operation identifier.
    pub operation_id: &'static str,
}

include!("generated/route_metadata.rs");

/// Deterministic descriptor set emitted by the pinned protobuf compiler.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/openfga_descriptor.bin");

#[cfg(test)]
mod tests {
    use super::{HttpMethod, OPENFGA_HTTP_ROUTES};

    #[test]
    fn test_should_generate_every_openfga_v1_http_route() {
        assert_eq!(OPENFGA_HTTP_ROUTES.len(), 18);
        assert!(OPENFGA_HTTP_ROUTES.iter().any(|route| {
            route.method == HttpMethod::Post
                && route.path == "/stores/{store_id}/check"
                && route.operation_id == "Check"
        }));
        assert!(OPENFGA_HTTP_ROUTES.iter().all(|route| {
            !route.path.contains("/access/v1/") && !route.path.contains("authzen")
        }));
    }

    #[test]
    fn test_should_emit_a_nonempty_descriptor_set() {
        assert!(super::FILE_DESCRIPTOR_SET.len() > 1_024);
    }
}
