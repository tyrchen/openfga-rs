//! Pinned `OpenFGA` v1 protobuf messages, gRPC services, and HTTP route metadata.
//!
//! Generated files are checked in and reproduced with `make proto`. The source and tool pins are
//! recorded in `proto.lock.json`; generated files must never be edited by hand.

#![forbid(unsafe_code)]

use std::{
    collections::HashMap,
    fmt::{self, Display},
    hash::Hash,
    marker::PhantomData,
};

use serde::de::{Deserialize, Deserializer, Error as _, MapAccess, Visitor};

#[doc(hidden)]
#[derive(Debug)]
pub struct DuplicateRejectingMap<K, V>(HashMap<K, V>);

impl<K, V> DuplicateRejectingMap<K, V> {
    #[doc(hidden)]
    #[must_use]
    pub fn into_inner(self) -> HashMap<K, V> {
        self.0
    }
}

impl<'de, K, V> Deserialize<'de> for DuplicateRejectingMap<K, V>
where
    K: Deserialize<'de> + Display + Eq + Hash,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MapVisitor<K, V>(PhantomData<(K, V)>);

        impl<'de, K, V> Visitor<'de> for MapVisitor<K, V>
        where
            K: Deserialize<'de> + Display + Eq + Hash,
            V: Deserialize<'de>,
        {
            type Value = DuplicateRejectingMap<K, V>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a protobuf JSON map with unique keys")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = HashMap::with_capacity(map.size_hint().unwrap_or_default());
                while let Some(key) = map.next_key()? {
                    if values.contains_key(&key) {
                        return Err(A::Error::custom(format_args!(
                            "duplicate map key \"{}\"",
                            BoundedDisplay(&key),
                        )));
                    }
                    let value = map.next_value()?;
                    values.insert(key, value);
                }
                Ok(DuplicateRejectingMap(values))
            }
        }

        deserializer.deserialize_map(MapVisitor(PhantomData))
    }
}

struct BoundedDisplay<'a, T>(&'a T);

impl<T: Display> Display for BoundedDisplay<'_, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct LimitWriter<'a, 'b> {
            formatter: &'a mut fmt::Formatter<'b>,
            remaining: usize,
        }

        impl fmt::Write for LimitWriter<'_, '_> {
            fn write_str(&mut self, value: &str) -> fmt::Result {
                if self.remaining == 0 {
                    return Ok(());
                }
                let end = value
                    .char_indices()
                    .map(|(index, _)| index)
                    .find(|index| *index >= self.remaining)
                    .unwrap_or(value.len());
                self.formatter.write_str(&value[..end])?;
                self.remaining = self.remaining.saturating_sub(end);
                Ok(())
            }
        }

        let mut writer = LimitWriter {
            formatter,
            remaining: 128,
        };
        fmt::write(&mut writer, format_args!("{}", self.0))
    }
}

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
    use std::error::Error;

    use super::{HttpMethod, OPENFGA_HTTP_ROUTES, openfga::v1 as pb};

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

    #[test]
    fn test_should_reject_duplicate_protobuf_json_fields_and_map_keys() -> Result<(), Box<dyn Error>>
    {
        let duplicate_field = r#"{"schema_version":"1.1","schema_version":"1.1","type_definitions":[{"type":"user"}]}"#;
        let field_error =
            serde_json::from_str::<pb::WriteAuthorizationModelRequest>(duplicate_field)
                .err()
                .ok_or("duplicate protobuf field unexpectedly decoded")?;
        assert_eq!(
            field_error.to_string(),
            "duplicate field `schema_version` at line 1 column 40"
        );

        let duplicate_map = r#"{"schema_version":"1.1","type_definitions":[{"type":"user"}],"conditions":{"c":{"name":"c","expression":"true"},"c":{"name":"c","expression":"true"}}}"#;
        let map_error = serde_json::from_str::<pb::WriteAuthorizationModelRequest>(duplicate_map)
            .err()
            .ok_or("duplicate protobuf map key unexpectedly decoded")?;
        assert!(
            map_error.to_string().starts_with("duplicate map key \"c\""),
            "{map_error}",
        );
        Ok(())
    }
}
