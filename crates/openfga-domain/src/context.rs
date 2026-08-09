//! Bounded, typed condition-context trees with deterministic fingerprints.

use std::{collections::BTreeMap, fmt, mem::size_of};

use serde_json::Value;

use crate::{
    error::{ValidationError, ValidationReason},
    fingerprint::{Fingerprint, FingerprintBuilder},
    identifier::ParameterName,
    limits::InputLimits,
};

/// A finite IEEE-754 value with canonical zero representation.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct FiniteFloat(u64);

impl FiniteFloat {
    /// Creates a finite double value.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for NaN or infinity.
    pub fn new(value: f64) -> Result<Self, ValidationError> {
        if !value.is_finite() {
            return Err(ValidationError::new(
                "context_number",
                ValidationReason::OutOfRange,
            ));
        }
        let normalized = if value == 0.0 { 0.0 } else { value };
        Ok(Self(normalized.to_bits()))
    }

    /// Returns the validated finite value.
    #[must_use]
    pub fn get(self) -> f64 {
        f64::from_bits(self.0)
    }

    fn bits(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for FiniteFloat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FiniteFloat([REDACTED])")
    }
}

/// A bounded context string.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ContextString(String);

impl ContextString {
    /// Validates a condition-context string under the configured byte limit.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] when the UTF-8 value exceeds the per-value limit.
    pub fn new(value: String, limits: &InputLimits) -> Result<Self, ValidationError> {
        Self::new_with_limit(value, limits.context_string_bytes())
    }

    fn new_with_limit(value: String, limit: usize) -> Result<Self, ValidationError> {
        if value.len() > limit {
            return Err(ValidationError::new(
                "context_string",
                ValidationReason::TooLarge,
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ContextString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextString")
            .field("bytes", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// A bounded context byte string.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ContextBytes(Vec<u8>);

impl ContextBytes {
    /// Validates condition-context bytes under the configured per-value limit.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] when the value exceeds the per-value limit.
    pub fn new(value: Vec<u8>, limits: &InputLimits) -> Result<Self, ValidationError> {
        Self::new_with_limit(value, limits.context_string_bytes())
    }

    fn new_with_limit(value: Vec<u8>, limit: usize) -> Result<Self, ValidationError> {
        if value.len() > limit {
            return Err(ValidationError::new(
                "context_bytes",
                ValidationReason::TooLarge,
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for ContextBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextBytes")
            .field("bytes", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// A bounded nested context-map key.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ContextKey(String);

impl ContextKey {
    /// Validates a nested map key under the configured byte limit.
    ///
    /// Empty and arbitrary UTF-8 keys are supported because CEL maps treat them
    /// as data rather than identifiers.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] when the key exceeds its byte limit.
    pub fn new(value: String, limits: &InputLimits) -> Result<Self, ValidationError> {
        if value.len() > limits.context_key_bytes() {
            return Err(ValidationError::new(
                "context_key",
                ValidationReason::TooLarge,
            ));
        }
        Ok(Self(value))
    }

    /// Returns the nested map key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ContextKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContextKey([REDACTED])")
    }
}

/// A validated bounded context list.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct ContextList(Vec<ContextValue>);

impl ContextList {
    /// Creates a list whose item count is within the configured limit.
    ///
    /// Aggregate depth/size validation occurs when the value enters a
    /// [`ConditionContext`].
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] when the list contains too many items.
    pub fn new(values: Vec<ContextValue>, limits: &InputLimits) -> Result<Self, ValidationError> {
        if values.len() > limits.context_collection_items() {
            return Err(ValidationError::new(
                "context_list",
                ValidationReason::TooManyItems,
            ));
        }
        Ok(Self(values))
    }

    /// Returns the validated list values.
    #[must_use]
    pub fn as_slice(&self) -> &[ContextValue] {
        &self.0
    }
}

impl fmt::Debug for ContextList {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextList")
            .field("items", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// A validated bounded nested context map.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct ContextMap(BTreeMap<ContextKey, ContextValue>);

impl ContextMap {
    /// Creates a nested map whose entry count is within the configured limit.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] when the map contains too many entries.
    pub fn new(
        values: BTreeMap<ContextKey, ContextValue>,
        limits: &InputLimits,
    ) -> Result<Self, ValidationError> {
        if values.len() > limits.context_collection_items() {
            return Err(ValidationError::new(
                "context_map",
                ValidationReason::TooManyItems,
            ));
        }
        Ok(Self(values))
    }

    /// Iterates over nested map entries in canonical key order.
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, ContextKey, ContextValue> {
        self.0.iter()
    }
}

impl<'a> IntoIterator for &'a ContextMap {
    type IntoIter = std::collections::btree_map::Iter<'a, ContextKey, ContextValue>;
    type Item = (&'a ContextKey, &'a ContextValue);

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl fmt::Debug for ContextMap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextMap")
            .field("entries", &self.0.len())
            .finish_non_exhaustive()
    }
}

/// One value in a validated condition-context tree.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ContextValue {
    /// CEL null.
    Null,
    /// A Boolean scalar.
    Bool(bool),
    /// A signed integer scalar.
    Int(i64),
    /// An unsigned integer scalar.
    Uint(u64),
    /// A finite double scalar.
    Double(FiniteFloat),
    /// A bounded UTF-8 string.
    String(ContextString),
    /// A bounded byte string.
    Bytes(ContextBytes),
    /// A bounded list.
    List(ContextList),
    /// A bounded map.
    Map(ContextMap),
}

impl ContextValue {
    /// Converts an untrusted JSON value into a typed, locally bounded value.
    ///
    /// Aggregate size/depth validation is repeated when the value enters a
    /// [`ConditionContext`]. JSON numbers retain wire semantics as finite doubles.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for oversized strings/collections, excessive
    /// nesting, invalid parameter-map keys, or non-finite numbers.
    pub fn try_from_json(value: Value, limits: &InputLimits) -> Result<Self, ValidationError> {
        convert_json_value(value, limits, 1, limits.context_string_bytes())
    }

    fn update_fingerprint(&self, builder: &mut FingerprintBuilder) {
        match self {
            Self::Null => builder.write_tag(0),
            Self::Bool(value) => {
                builder.write_tag(1);
                builder.write_tag(u8::from(*value));
            }
            Self::Int(value) => {
                builder.write_tag(2);
                builder.write_i64(*value);
            }
            Self::Uint(value) => {
                builder.write_tag(3);
                builder.write_u64(*value);
            }
            Self::Double(value) => {
                builder.write_tag(4);
                builder.write_u64(value.bits());
            }
            Self::String(value) => {
                builder.write_tag(5);
                builder.write_str(value.as_str());
            }
            Self::Bytes(value) => {
                builder.write_tag(6);
                builder.write_bytes(value.as_slice());
            }
            Self::List(values) => {
                builder.write_tag(7);
                builder.write_u64(u64::try_from(values.0.len()).unwrap_or(u64::MAX));
                for value in &values.0 {
                    value.update_fingerprint(builder);
                }
            }
            Self::Map(values) => {
                builder.write_tag(8);
                builder.write_u64(u64::try_from(values.0.len()).unwrap_or(u64::MAX));
                for (key, value) in &values.0 {
                    builder.write_str(key.as_str());
                    value.update_fingerprint(builder);
                }
            }
        }
    }
}

impl fmt::Debug for ContextValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Uint(_) => "uint",
            Self::Double(_) => "double",
            Self::String(_) => "string",
            Self::Bytes(_) => "bytes",
            Self::List(_) => "list",
            Self::Map(_) => "map",
        };
        formatter
            .debug_struct("ContextValue")
            .field("kind", &kind)
            .finish_non_exhaustive()
    }
}

fn convert_json_value(
    value: Value,
    limits: &InputLimits,
    depth: usize,
    value_byte_limit: usize,
) -> Result<ContextValue, ValidationError> {
    if depth > limits.context_depth() {
        return Err(ValidationError::new(
            "condition_context",
            ValidationReason::TooDeep,
        ));
    }
    match value {
        Value::Null => Ok(ContextValue::Null),
        Value::Bool(value) => Ok(ContextValue::Bool(value)),
        Value::Number(value) => value
            .as_f64()
            .ok_or_else(|| ValidationError::new("context_number", ValidationReason::OutOfRange))
            .and_then(FiniteFloat::new)
            .map(ContextValue::Double),
        Value::String(value) => {
            ContextString::new_with_limit(value, value_byte_limit).map(ContextValue::String)
        }
        Value::Array(values) => {
            if values.len() > limits.context_collection_items() {
                return Err(ValidationError::new(
                    "context_list",
                    ValidationReason::TooManyItems,
                ));
            }
            let child_depth = depth.checked_add(1).ok_or_else(|| {
                ValidationError::new("condition_context", ValidationReason::TooDeep)
            })?;
            values
                .into_iter()
                .map(|value| convert_json_value(value, limits, child_depth, value_byte_limit))
                .collect::<Result<Vec<_>, _>>()
                .and_then(|values| ContextList::new(values, limits))
                .map(ContextValue::List)
        }
        Value::Object(values) => {
            if values.len() > limits.context_collection_items() {
                return Err(ValidationError::new(
                    "context_map",
                    ValidationReason::TooManyItems,
                ));
            }
            let child_depth = depth.checked_add(1).ok_or_else(|| {
                ValidationError::new("condition_context", ValidationReason::TooDeep)
            })?;
            let mut converted = BTreeMap::new();
            for (key, value) in values {
                let key = ContextKey::new(key, limits)?;
                let value = convert_json_value(value, limits, child_depth, value_byte_limit)?;
                converted.insert(key, value);
            }
            ContextMap::new(converted, limits).map(ContextValue::Map)
        }
    }
}

#[derive(Debug, Default)]
struct ContextStats {
    bytes: usize,
    values: usize,
    byte_limit: Option<usize>,
}

impl ContextStats {
    const fn bounded(byte_limit: usize) -> Self {
        Self {
            bytes: 0,
            values: 0,
            byte_limit: Some(byte_limit),
        }
    }

    fn add_bytes(&mut self, bytes: usize) -> Result<(), ValidationError> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| ValidationError::new("condition_context", ValidationReason::TooLarge))?;
        if self.byte_limit.is_some_and(|limit| self.bytes > limit) {
            return Err(ValidationError::new(
                "condition_context",
                ValidationReason::TooLarge,
            ));
        }
        Ok(())
    }

    fn add_value(&mut self, limits: &InputLimits) -> Result<(), ValidationError> {
        self.values = self.values.checked_add(1).ok_or_else(|| {
            ValidationError::new("condition_context", ValidationReason::TooManyItems)
        })?;
        if self.values > limits.context_values() {
            return Err(ValidationError::new(
                "condition_context",
                ValidationReason::TooManyItems,
            ));
        }
        Ok(())
    }
}

fn validate_context_value(
    value: &ContextValue,
    depth: usize,
    limits: &InputLimits,
    stats: &mut ContextStats,
    enforce_value_byte_limit: bool,
) -> Result<(), ValidationError> {
    if depth > limits.context_depth() {
        return Err(ValidationError::new(
            "condition_context",
            ValidationReason::TooDeep,
        ));
    }
    stats.add_value(limits)?;
    match value {
        ContextValue::Null => Ok(()),
        ContextValue::Bool(_) => stats.add_bytes(1),
        ContextValue::Int(_) | ContextValue::Uint(_) | ContextValue::Double(_) => {
            stats.add_bytes(8)
        }
        ContextValue::String(value) => {
            if enforce_value_byte_limit && value.0.len() > limits.context_string_bytes() {
                return Err(ValidationError::new(
                    "context_string",
                    ValidationReason::TooLarge,
                ));
            }
            stats.add_bytes(value.0.len())
        }
        ContextValue::Bytes(value) => {
            if enforce_value_byte_limit && value.0.len() > limits.context_string_bytes() {
                return Err(ValidationError::new(
                    "context_bytes",
                    ValidationReason::TooLarge,
                ));
            }
            stats.add_bytes(value.0.len())
        }
        ContextValue::List(values) => {
            if values.0.len() > limits.context_collection_items() {
                return Err(ValidationError::new(
                    "context_list",
                    ValidationReason::TooManyItems,
                ));
            }
            let child_depth = depth.checked_add(1).ok_or_else(|| {
                ValidationError::new("condition_context", ValidationReason::TooDeep)
            })?;
            for value in &values.0 {
                validate_context_value(
                    value,
                    child_depth,
                    limits,
                    stats,
                    enforce_value_byte_limit,
                )?;
            }
            Ok(())
        }
        ContextValue::Map(values) => {
            if values.0.len() > limits.context_collection_items() {
                return Err(ValidationError::new(
                    "context_map",
                    ValidationReason::TooManyItems,
                ));
            }
            let child_depth = depth.checked_add(1).ok_or_else(|| {
                ValidationError::new("condition_context", ValidationReason::TooDeep)
            })?;
            for (key, value) in &values.0 {
                if key.0.len() > limits.context_key_bytes() {
                    return Err(ValidationError::new(
                        "context_key",
                        ValidationReason::TooLarge,
                    ));
                }
                stats.add_bytes(key.0.len())?;
                validate_context_value(
                    value,
                    child_depth,
                    limits,
                    stats,
                    enforce_value_byte_limit,
                )?;
            }
            Ok(())
        }
    }
}

/// A validated root condition context keyed by declared parameter names.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConditionContext {
    values: BTreeMap<ParameterName, ContextValue>,
    fingerprint: Fingerprint,
    estimated_owned_bytes: usize,
}

impl ConditionContext {
    /// Creates a context and enforces aggregate bytes, values, depth, and collection limits.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] when any nested or aggregate limit is exceeded.
    pub fn new(
        values: BTreeMap<ParameterName, ContextValue>,
        limits: &InputLimits,
    ) -> Result<Self, ValidationError> {
        Self::new_with_policy(values, limits, Some(limits.context_bytes()), true)
    }

    fn new_with_policy(
        values: BTreeMap<ParameterName, ContextValue>,
        limits: &InputLimits,
        byte_limit: Option<usize>,
        enforce_value_byte_limit: bool,
    ) -> Result<Self, ValidationError> {
        if values.len() > limits.context_collection_items() {
            return Err(ValidationError::new(
                "condition_context",
                ValidationReason::TooManyItems,
            ));
        }
        let mut stats = byte_limit.map_or_else(ContextStats::default, ContextStats::bounded);
        for (name, value) in &values {
            stats.add_bytes(name.as_str().len())?;
            validate_context_value(value, 1, limits, &mut stats, enforce_value_byte_limit)?;
        }
        let fingerprint = fingerprint_context(&values);
        let estimated_owned_bytes =
            size_of::<Self>()
                .saturating_add(stats.bytes)
                .saturating_add(stats.values.saturating_mul(
                    size_of::<ContextValue>().saturating_add(4 * size_of::<usize>()),
                ));
        Ok(Self {
            values,
            fingerprint,
            estimated_owned_bytes,
        })
    }

    /// Converts an untrusted JSON object into a bounded context.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] unless the root is an object with valid
    /// parameter names and the complete tree is within configured limits.
    pub fn try_from_json(value: Value, limits: &InputLimits) -> Result<Self, ValidationError> {
        Self::try_from_json_with_policy(
            value,
            limits,
            limits.context_string_bytes(),
            Some(limits.context_bytes()),
            true,
        )
    }

    /// Converts a protobuf context for model-semantic validation before its encoded-size check.
    ///
    /// The caller must have already measured and bounded the containing wire message. Aggregate
    /// and per-value byte enforcement is deferred so condition parameter errors retain protocol
    /// precedence; depth, key, name, collection, and value-count ceilings remain enforced.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] unless the root and non-byte structural limits are valid.
    pub fn try_from_json_for_wire_semantics(
        value: Value,
        limits: &InputLimits,
        measured_wire_bytes: usize,
    ) -> Result<Self, ValidationError> {
        Self::try_from_json_with_policy(value, limits, measured_wire_bytes, None, false)
    }

    fn try_from_json_with_policy(
        value: Value,
        limits: &InputLimits,
        value_byte_limit: usize,
        aggregate_byte_limit: Option<usize>,
        enforce_value_byte_limit: bool,
    ) -> Result<Self, ValidationError> {
        let Value::Object(values) = value else {
            return Err(ValidationError::new(
                "condition_context",
                ValidationReason::Inconsistent,
            ));
        };
        if values.len() > limits.context_collection_items() {
            return Err(ValidationError::new(
                "condition_context",
                ValidationReason::TooManyItems,
            ));
        }
        let mut converted = BTreeMap::new();
        for (name, value) in values {
            let name = ParameterName::parse_with_limits(&name, limits).map_err(|_| {
                ValidationError::new("context_parameter", ValidationReason::Inconsistent)
            })?;
            let value = convert_json_value(value, limits, 1, value_byte_limit)?;
            converted.insert(name, value);
        }
        Self::new_with_policy(
            converted,
            limits,
            aggregate_byte_limit,
            enforce_value_byte_limit,
        )
    }

    /// Creates an empty, valid context.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            values: BTreeMap::new(),
            fingerprint: fingerprint_context(&BTreeMap::new()),
            estimated_owned_bytes: size_of::<Self>(),
        }
    }

    /// Returns one parameter value.
    #[must_use]
    pub fn get(&self, name: &ParameterName) -> Option<&ContextValue> {
        self.values.get(name)
    }

    /// Iterates over parameters in canonical name order.
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, ParameterName, ContextValue> {
        self.values.iter()
    }

    /// Returns whether the context has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the canonical semantic fingerprint without exposing values.
    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    /// Returns a conservative estimate of heap and inline bytes owned by this context.
    #[must_use]
    pub const fn estimated_owned_bytes(&self) -> usize {
        self.estimated_owned_bytes
    }

    /// Merges a tuple context over a request context by parameter name.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] if the merged tree exceeds configured limits.
    pub fn overlay(
        &self,
        tuple_context: &Self,
        limits: &InputLimits,
    ) -> Result<Self, ValidationError> {
        let mut merged = self.values.clone();
        for (name, value) in &tuple_context.values {
            merged.insert(name.clone(), value.clone());
        }
        Self::new(merged, limits)
    }
}

impl<'a> IntoIterator for &'a ConditionContext {
    type IntoIter = std::collections::btree_map::Iter<'a, ParameterName, ContextValue>;
    type Item = (&'a ParameterName, &'a ContextValue);

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

impl Default for ConditionContext {
    fn default() -> Self {
        Self::empty()
    }
}

impl fmt::Debug for ConditionContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConditionContext")
            .field("parameters", &self.values.len())
            .finish_non_exhaustive()
    }
}

fn fingerprint_context(values: &BTreeMap<ParameterName, ContextValue>) -> Fingerprint {
    let mut builder = FingerprintBuilder::new("openfga.condition-context.v1");
    builder.write_u64(u64::try_from(values.len()).unwrap_or(u64::MAX));
    for (name, value) in values {
        builder.write_str(name.as_str());
        value.update_fingerprint(&mut builder);
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{ConditionContext, ContextString, ContextValue};
    use crate::{InputLimits, Limit, ParameterName};

    #[test]
    fn test_should_convert_and_fingerprint_context_deterministically() {
        let limits = InputLimits::default();
        let first = ConditionContext::try_from_json(
            json!({"country": "US", "count": 2, "nested": {"allowed": true}}),
            &limits,
        );
        let second = ConditionContext::try_from_json(
            json!({"nested": {"allowed": true}, "count": 2, "country": "US"}),
            &limits,
        );
        assert!(first.is_ok() && second.is_ok());
        assert_eq!(
            first.map(|context| context.fingerprint()),
            second.map(|context| context.fingerprint())
        );
    }

    #[test]
    fn test_should_overlay_tuple_context_over_request_context() {
        let limits = InputLimits::default();
        let request = ConditionContext::try_from_json(json!({"region": "us"}), &limits);
        let tuple = ConditionContext::try_from_json(json!({"region": "eu"}), &limits);
        assert!(request.is_ok() && tuple.is_ok());
        let (Ok(request), Ok(tuple)) = (request, tuple) else {
            return;
        };
        let merged = request.overlay(&tuple, &limits);
        assert!(merged.is_ok());
        let region = "region".parse::<ParameterName>();
        assert!(region.is_ok());
        let Some(region) = region.ok() else {
            return;
        };
        assert!(matches!(
            merged.ok().and_then(|context| context.get(&region).cloned()),
            Some(ContextValue::String(value)) if value.as_str() == "eu"
        ));
    }

    #[test]
    fn test_should_enforce_aggregate_context_limits_and_redact_debug() {
        let byte_limit = Limit::<32_768>::new(8);
        assert!(byte_limit.is_ok());
        let Some(byte_limit) = byte_limit.ok() else {
            return;
        };
        let limits = InputLimits::builder().context_bytes(byte_limit).build();
        assert!(ConditionContext::try_from_json(json!({"secret": "long-value"}), &limits).is_err());

        let value = ContextString::new("top-secret".to_owned(), &InputLimits::default());
        assert!(value.is_ok());
        let Some(value) = value.ok() else {
            return;
        };
        let mut values = BTreeMap::new();
        let name = "token".parse::<ParameterName>();
        assert!(name.is_ok());
        let Some(name) = name.ok() else {
            return;
        };
        values.insert(name, ContextValue::String(value));
        let context = ConditionContext::new(values, &InputLimits::default());
        assert!(context.is_ok());
        let debug = format!("{:?}", context.ok());
        assert!(!debug.contains("top-secret"));
    }
}
