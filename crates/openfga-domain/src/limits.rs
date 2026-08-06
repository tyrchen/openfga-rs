//! Positive, ceiling-constrained input and collection limits.

use std::num::NonZeroU32;

use thiserror::Error;
use typed_builder::TypedBuilder;

/// Error returned when a configured limit is zero or exceeds its safety ceiling.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("configured limit must be between 1 and {maximum}, found {requested}")]
#[non_exhaustive]
pub struct LimitError {
    requested: u32,
    maximum: u32,
}

impl LimitError {
    /// Returns the rejected value.
    #[must_use]
    pub const fn requested(self) -> u32 {
        self.requested
    }

    /// Returns the compiled safety ceiling.
    #[must_use]
    pub const fn maximum(self) -> u32 {
        self.maximum
    }
}

/// A positive runtime limit that cannot exceed the compile-time ceiling `MAX`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct Limit<const MAX: u32>(NonZeroU32);

impl<const MAX: u32> Limit<MAX> {
    /// Creates a positive limit at or below `MAX`.
    ///
    /// # Errors
    ///
    /// Returns [`LimitError`] when `value` is zero or greater than `MAX`.
    pub const fn new(value: u32) -> Result<Self, LimitError> {
        match NonZeroU32::new(value) {
            Some(value) if value.get() <= MAX => Ok(Self(value)),
            _ => Err(LimitError {
                requested: value,
                maximum: MAX,
            }),
        }
    }

    /// Returns the configured non-zero value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }

    /// Returns the configured value as `usize` without a fallible cast.
    #[must_use]
    pub const fn as_usize(self) -> usize {
        self.0.get() as usize
    }
}

fn trusted_limit<const MAX: u32>(value: u32) -> Limit<MAX> {
    match Limit::new(value) {
        Ok(limit) => limit,
        Err(_) => Limit(NonZeroU32::MIN),
    }
}

/// One validated policy for all externally controlled Phase 1 domain inputs.
///
/// Each field is itself a [`Limit`] with a compiled ceiling, so the generated
/// type-state builder cannot create an unsafe configuration.
#[derive(Clone, Debug, TypedBuilder)]
#[non_exhaustive]
pub struct InputLimits {
    #[builder(default = trusted_limit::<254>(254))]
    type_name_bytes: Limit<254>,
    #[builder(default = trusted_limit::<50>(50))]
    relation_name_bytes: Limit<50>,
    #[builder(default = trusted_limit::<50>(50))]
    condition_name_bytes: Limit<50>,
    #[builder(default = trusted_limit::<50>(50))]
    parameter_name_bytes: Limit<50>,
    #[builder(default = trusted_limit::<510>(510))]
    object_id_bytes: Limit<510>,
    #[builder(default = trusted_limit::<256>(256))]
    object_ref_bytes: Limit<256>,
    #[builder(default = trusted_limit::<512>(512))]
    subject_ref_bytes: Limit<512>,
    #[builder(default = trusted_limit::<256>(256))]
    context_key_bytes: Limit<256>,
    #[builder(default = trusted_limit::<32_768>(4_096))]
    context_string_bytes: Limit<32_768>,
    #[builder(default = trusted_limit::<32_768>(32_768))]
    context_bytes: Limit<32_768>,
    #[builder(default = trusted_limit::<64>(16))]
    context_depth: Limit<64>,
    #[builder(default = trusted_limit::<4_096>(1_024))]
    context_values: Limit<4_096>,
    #[builder(default = trusted_limit::<1_024>(100))]
    context_collection_items: Limit<1_024>,
    #[builder(default = trusted_limit::<1_000>(100))]
    contextual_tuples: Limit<1_000>,
    #[builder(default = trusted_limit::<5_000>(100))]
    write_tuples: Limit<5_000>,
    #[builder(default = trusted_limit::<1_000>(50))]
    batch_items: Limit<1_000>,
    #[builder(default = trusted_limit::<1_000>(100))]
    user_filters: Limit<1_000>,
    #[builder(default = trusted_limit::<1_000>(100))]
    type_definitions: Limit<1_000>,
    #[builder(default = trusted_limit::<2_000>(100))]
    relations: Limit<2_000>,
    #[builder(default = trusted_limit::<4_096>(100))]
    operands: Limit<4_096>,
    #[builder(default = trusted_limit::<1_000>(100))]
    assertions: Limit<1_000>,
    #[builder(default = trusted_limit::<5_120>(5_120))]
    token_bytes: Limit<5_120>,
    #[builder(default = trusted_limit::<4_096>(1_024))]
    token_cursor_bytes: Limit<4_096>,
    #[builder(default = trusted_limit::<256>(8))]
    token_keys: Limit<256>,
    #[builder(default = trusted_limit::<100_000>(1_000))]
    results: Limit<100_000>,
}

impl InputLimits {
    /// Maximum type-name bytes.
    #[must_use]
    pub const fn type_name_bytes(&self) -> usize {
        self.type_name_bytes.as_usize()
    }

    /// Maximum relation-name bytes.
    #[must_use]
    pub const fn relation_name_bytes(&self) -> usize {
        self.relation_name_bytes.as_usize()
    }

    /// Maximum condition-name bytes.
    #[must_use]
    pub const fn condition_name_bytes(&self) -> usize {
        self.condition_name_bytes.as_usize()
    }

    /// Maximum condition parameter-name bytes.
    #[must_use]
    pub const fn parameter_name_bytes(&self) -> usize {
        self.parameter_name_bytes.as_usize()
    }

    /// Maximum object-ID bytes before adding its type prefix.
    #[must_use]
    pub const fn object_id_bytes(&self) -> usize {
        self.object_id_bytes.as_usize()
    }

    /// Maximum canonical target-object bytes.
    #[must_use]
    pub const fn object_ref_bytes(&self) -> usize {
        self.object_ref_bytes.as_usize()
    }

    /// Maximum canonical subject bytes.
    #[must_use]
    pub const fn subject_ref_bytes(&self) -> usize {
        self.subject_ref_bytes.as_usize()
    }

    /// Maximum bytes in one nested context-map key.
    #[must_use]
    pub const fn context_key_bytes(&self) -> usize {
        self.context_key_bytes.as_usize()
    }

    /// Maximum bytes in one context string or byte value.
    #[must_use]
    pub const fn context_string_bytes(&self) -> usize {
        self.context_string_bytes.as_usize()
    }

    /// Maximum aggregate bytes in one condition context.
    #[must_use]
    pub const fn context_bytes(&self) -> usize {
        self.context_bytes.as_usize()
    }

    /// Maximum nested condition-context depth.
    #[must_use]
    pub const fn context_depth(&self) -> usize {
        self.context_depth.as_usize()
    }

    /// Maximum scalar and collection nodes in one condition context.
    #[must_use]
    pub const fn context_values(&self) -> usize {
        self.context_values.as_usize()
    }

    /// Maximum items in one nested context list or map.
    #[must_use]
    pub const fn context_collection_items(&self) -> usize {
        self.context_collection_items.as_usize()
    }

    /// Maximum contextual tuples in one query item.
    #[must_use]
    pub const fn contextual_tuples(&self) -> usize {
        self.contextual_tuples.as_usize()
    }

    /// Maximum writes and deletes in one tuple mutation.
    #[must_use]
    pub const fn write_tuples(&self) -> usize {
        self.write_tuples.as_usize()
    }

    /// Maximum checks in one `BatchCheck` command.
    #[must_use]
    pub const fn batch_items(&self) -> usize {
        self.batch_items.as_usize()
    }

    /// Maximum user-type filters in one `ListUsers` command.
    #[must_use]
    pub const fn user_filters(&self) -> usize {
        self.user_filters.as_usize()
    }

    /// Maximum type definitions in one model.
    #[must_use]
    pub const fn type_definitions(&self) -> usize {
        self.type_definitions.as_usize()
    }

    /// Maximum relations in one model.
    #[must_use]
    pub const fn relations(&self) -> usize {
        self.relations.as_usize()
    }

    /// Maximum operands in one rewrite operator.
    #[must_use]
    pub const fn operands(&self) -> usize {
        self.operands.as_usize()
    }

    /// Maximum assertions in one write.
    #[must_use]
    pub const fn assertions(&self) -> usize {
        self.assertions.as_usize()
    }

    /// Maximum bytes in an encoded continuation token.
    #[must_use]
    pub const fn token_bytes(&self) -> usize {
        self.token_bytes.as_usize()
    }

    /// Maximum bytes in a backend-independent continuation cursor.
    #[must_use]
    pub const fn token_cursor_bytes(&self) -> usize {
        self.token_cursor_bytes.as_usize()
    }

    /// Maximum active and retired continuation-token keys.
    #[must_use]
    pub const fn token_keys(&self) -> usize {
        self.token_keys.as_usize()
    }

    /// Maximum results requested by a bounded list command.
    #[must_use]
    pub const fn results(&self) -> u32 {
        self.results.get()
    }
}

impl Default for InputLimits {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[cfg(test)]
mod tests {
    use super::{InputLimits, Limit};

    #[test]
    fn test_should_reject_zero_and_over_ceiling_limits() {
        assert!(Limit::<100>::new(0).is_err());
        assert!(Limit::<100>::new(101).is_err());
        assert_eq!(Limit::<100>::new(100).map(Limit::get), Ok(100));
    }

    #[test]
    fn test_should_match_pinned_api_default_limits() {
        let limits = InputLimits::default();
        assert_eq!(limits.type_name_bytes(), 254);
        assert_eq!(limits.relation_name_bytes(), 50);
        assert_eq!(limits.contextual_tuples(), 100);
        assert_eq!(limits.write_tuples(), 100);
        assert_eq!(limits.batch_items(), 50);
        assert_eq!(limits.token_bytes(), 5_120);
    }
}
