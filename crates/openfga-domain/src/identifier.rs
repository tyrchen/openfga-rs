//! Canonical identifiers and OpenFGA-compatible name grammars.

use std::{fmt, str::FromStr};

use ulid::Ulid;
use winnow::{Parser, combinator::eof, error::EmptyError, token::take_while};

use crate::{
    error::{ParseError, ParseKind},
    limits::InputLimits,
};

fn parse_complete_component(
    value: &str,
    field: &'static str,
    maximum_bytes: usize,
    allowed: fn(char) -> bool,
) -> Result<String, ParseError> {
    if value.is_empty() {
        return Err(ParseError::new(field, 0, ParseKind::Empty));
    }
    if value.len() > maximum_bytes {
        return Err(ParseError::new(field, maximum_bytes, ParseKind::TooLong));
    }
    let mut parser = (
        take_while::<_, _, EmptyError>(1.., allowed),
        eof::<_, EmptyError>,
    );
    if parser.parse(value).is_err() {
        let offset = value
            .char_indices()
            .find_map(|(offset, character)| (!allowed(character)).then_some(offset))
            .unwrap_or(value.len());
        return Err(ParseError::new(field, offset, ParseKind::InvalidCharacter));
    }
    Ok(value.to_owned())
}

const fn is_ascii_name_character(character: char) -> bool {
    character.is_ascii_graphic() && !matches!(character, ':' | '#' | '@')
}

fn is_object_id_character(character: char) -> bool {
    !character.is_control() && !character.is_whitespace() && !matches!(character, ':' | '#')
}

const fn is_ascii_graphic(character: char) -> bool {
    character.is_ascii_graphic()
}

const fn is_token_key_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
}

macro_rules! define_ulid_identifier {
    ($name:ident, $field:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[non_exhaustive]
        pub struct $name(Ulid);

        impl $name {
            #[doc = concat!("Returns the parsed ULID backing this `", stringify!($name), "`.")]
            #[must_use]
            pub const fn as_ulid(&self) -> &Ulid {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = ParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                if value.len() != 26 {
                    let kind = if value.is_empty() {
                        ParseKind::Empty
                    } else {
                        ParseKind::InvalidLength
                    };
                    return Err(ParseError::new($field, value.len().min(26), kind));
                }
                let parsed = value
                    .parse::<Ulid>()
                    .map_err(|_| ParseError::new($field, 0, ParseKind::InvalidCharacter))?;
                if parsed.to_string() != value {
                    return Err(ParseError::new($field, 0, ParseKind::NonCanonical));
                }
                Ok(Self(parsed))
            }
        }

        impl TryFrom<&str> for $name {
            type Error = ParseError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                value.parse()
            }
        }

        impl TryFrom<String> for $name {
            type Error = ParseError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                value.parse()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, formatter)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0.to_string())
                    .finish()
            }
        }
    };
}

define_ulid_identifier!(
    StoreId,
    "store_id",
    "A canonical ULID identifying an `OpenFGA` store."
);
define_ulid_identifier!(
    AuthorizationModelId,
    "authorization_model_id",
    "A canonical ULID identifying an immutable authorization model."
);
define_ulid_identifier!(
    ChangeId,
    "change_id",
    "A canonical monotonic ULID identifying a changelog record."
);

macro_rules! define_ascii_name {
    ($name:ident, $field:literal, $maximum:expr, $limit:ident, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[non_exhaustive]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Parses a `", stringify!($name), "` under the configured input policy.")]
            ///
            /// # Errors
            ///
            /// Returns [`ParseError`] for empty, oversized, non-ASCII, whitespace, or reserved-separator input.
            pub fn parse_with_limits(
                value: &str,
                limits: &InputLimits,
            ) -> Result<Self, ParseError> {
                parse_complete_component(
                    value,
                    $field,
                    limits.$limit(),
                    is_ascii_name_character,
                )
                .map(Self)
            }

            #[doc = concat!("Returns this `", stringify!($name), "` as a borrowed string.")]
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = ParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                parse_complete_component(value, $field, $maximum, is_ascii_name_character).map(Self)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = ParseError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                value.parse()
            }
        }

        impl TryFrom<String> for $name {
            type Error = ParseError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                value.parse()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }
    };
}

define_ascii_name!(
    TypeName,
    "type_name",
    254,
    type_name_bytes,
    "A validated `OpenFGA` object type name."
);
define_ascii_name!(
    RelationName,
    "relation_name",
    50,
    relation_name_bytes,
    "A validated `OpenFGA` relation name."
);
define_ascii_name!(
    ConditionName,
    "condition_name",
    50,
    condition_name_bytes,
    "A validated authorization-model condition name."
);
define_ascii_name!(
    ParameterName,
    "parameter_name",
    50,
    parameter_name_bytes,
    "A validated CEL condition parameter name."
);

/// A validated `OpenFGA` object identifier without its type prefix.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ObjectId(String);

impl ObjectId {
    /// Parses an object ID under the configured byte limit.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError`] for empty, oversized, whitespace/control, `:`, or `#` input.
    pub fn parse_with_limits(value: &str, limits: &InputLimits) -> Result<Self, ParseError> {
        parse_complete_component(
            value,
            "object_id",
            limits.object_id_bytes(),
            is_object_id_character,
        )
        .map(Self)
    }

    /// Returns the object ID as a borrowed string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this identifier is the wildcard sentinel.
    #[must_use]
    pub fn is_wildcard(&self) -> bool {
        self.0 == "*"
    }
}

impl FromStr for ObjectId {
    type Err = ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_complete_component(value, "object_id", 510, is_object_id_character).map(Self)
    }
}

impl TryFrom<&str> for ObjectId {
    type Error = ParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<String> for ObjectId {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectId")
            .field("bytes", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// A bounded `BatchCheck` item correlation identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct CorrelationId(String);

impl CorrelationId {
    /// Returns the correlation identifier as a borrowed string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for CorrelationId {
    type Err = ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_complete_component(value, "correlation_id", 36, is_ascii_graphic).map(Self)
    }
}

impl TryFrom<&str> for CorrelationId {
    type Error = ParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A short public identifier selecting one continuation-token MAC key.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct TokenKeyId(String);

impl TokenKeyId {
    /// Returns the key identifier as a borrowed string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for TokenKeyId {
    type Err = ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_complete_component(value, "token_key_id", 64, is_token_key_character).map(Self)
    }
}

impl TryFrom<&str> for TokenKeyId {
    type Error = ParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// A validated caller identity whose value is always redacted from `Debug`.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct PrincipalId(String);

impl PrincipalId {
    /// Returns the validated principal identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for PrincipalId {
    type Err = ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_complete_component(value, "principal_id", 256, is_ascii_graphic).map(Self)
    }
}

impl TryFrom<&str> for PrincipalId {
    type Error = ParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Debug for PrincipalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PrincipalId([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::{Just, any, proptest};

    use super::{
        AuthorizationModelId, ConditionName, CorrelationId, ObjectId, ParameterName, PrincipalId,
        RelationName, StoreId, TokenKeyId, TypeName,
    };
    use crate::ParseKind;

    #[test]
    fn test_should_accept_canonical_ulids_only() {
        let canonical = "01G5JAVJ41T49E9TT3SKVS7X1J";
        assert!(canonical.parse::<StoreId>().is_ok());
        assert!(canonical.parse::<AuthorizationModelId>().is_ok());
        assert!(canonical.to_ascii_lowercase().parse::<StoreId>().is_err());
        assert!("01G5JAVJ41T49E9TT3SKVS7X1I".parse::<StoreId>().is_err());
        assert_eq!(
            "01G5JAVJ41T49E9TT3SKVS7X1"
                .parse::<StoreId>()
                .map_err(|error| error.kind()),
            Err(ParseKind::InvalidLength)
        );
    }

    #[test]
    fn test_should_apply_name_and_auxiliary_grammars() {
        assert!("document".parse::<TypeName>().is_ok());
        assert!("parent_folder".parse::<RelationName>().is_ok());
        assert!("in-region".parse::<ConditionName>().is_ok());
        assert!("request_time".parse::<ParameterName>().is_ok());
        assert!("item-1".parse::<CorrelationId>().is_ok());
        assert!("client/request:1".parse::<CorrelationId>().is_ok());
        assert!("primary_2026".parse::<TokenKeyId>().is_ok());
        assert!("issuer|subject".parse::<PrincipalId>().is_ok());
        assert!("bad:name".parse::<TypeName>().is_err());
        assert!("bad relation".parse::<RelationName>().is_err());
    }

    proptest! {
        #[test]
        fn test_should_never_panic_parsing_arbitrary_identifiers(value in any::<String>()) {
            let _ = value.parse::<StoreId>();
            let _ = value.parse::<TypeName>();
            let _ = value.parse::<RelationName>();
            let _ = value.parse::<ConditionName>();
            let _ = value.parse::<ParameterName>();
            let _ = value.parse::<ObjectId>();
            let _ = value.parse::<CorrelationId>();
            let _ = value.parse::<TokenKeyId>();
            let _ = value.parse::<PrincipalId>();
        }

        #[test]
        fn test_should_round_trip_valid_object_ids(
            value in "[A-Za-z0-9_./@|+!$%&'(),;<=>?\\[\\]^`{}~-]{1,128}"
        ) {
            let parsed = value.parse::<ObjectId>();
            assert!(parsed.is_ok());
            if let Ok(parsed) = parsed {
                assert_eq!(parsed.to_string(), value);
            }
        }

        #[test]
        fn test_should_reject_noncanonical_ulid_aliases(
            canonical in Just("01G5JAVJ41T49E9TT3SKVS7X1J".to_owned())
        ) {
            assert!(canonical.to_ascii_lowercase().parse::<StoreId>().is_err());
        }
    }
}
