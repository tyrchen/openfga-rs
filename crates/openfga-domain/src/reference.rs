//! Canonical object, subject, tuple, and condition-binding values.

use std::{collections::BTreeSet, fmt, str::FromStr};

use winnow::{
    Parser,
    combinator::{eof, opt},
    error::EmptyError,
    token::{literal, take_while},
};

use crate::{
    context::ConditionContext,
    error::{ParseError, ParseKind, ValidationError, ValidationReason},
    fingerprint::{Fingerprint, FingerprintBuilder},
    identifier::{ConditionName, ObjectId, RelationName, TypeName},
    limits::InputLimits,
};

const fn is_name_character(character: char) -> bool {
    character.is_ascii_graphic() && !matches!(character, ':' | '#' | '@')
}

fn is_object_id_character(character: char) -> bool {
    !character.is_control() && !character.is_whitespace() && !matches!(character, ':' | '#')
}

fn object_structure_error(value: &str, field: &'static str) -> ParseError {
    if value.is_empty() {
        return ParseError::new(field, 0, ParseKind::Empty);
    }
    let mut colon = None;
    for (offset, character) in value.char_indices() {
        match colon {
            None if character == ':' && offset > 0 => colon = Some(offset),
            _ if character == ':' => {
                return ParseError::new(field, offset, ParseKind::UnexpectedSeparator);
            }
            None if !is_name_character(character) => {
                return ParseError::new(field, offset, ParseKind::InvalidCharacter);
            }
            Some(_) if !is_object_id_character(character) => {
                return ParseError::new(field, offset, ParseKind::InvalidCharacter);
            }
            _ => {}
        }
    }
    match colon {
        None => ParseError::new(field, value.len(), ParseKind::MissingSeparator),
        Some(offset) if offset.saturating_add(1) == value.len() => {
            ParseError::new(field, value.len(), ParseKind::Empty)
        }
        Some(_) => ParseError::new(field, 0, ParseKind::InvalidCharacter),
    }
}

fn parse_object_parts<'a>(
    value: &'a str,
    field: &'static str,
) -> Result<(&'a str, &'a str), ParseError> {
    let mut parser = (
        take_while::<_, _, EmptyError>(1.., is_name_character),
        literal::<_, _, EmptyError>(':'),
        take_while::<_, _, EmptyError>(1.., is_object_id_character),
        eof::<_, EmptyError>,
    );
    parser
        .parse(value)
        .map(|(object_type, _, object_id, _)| (object_type, object_id))
        .map_err(|_| object_structure_error(value, field))
}

/// A concrete typed `OpenFGA` object such as `document:roadmap`.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct ObjectRef {
    object_type: TypeName,
    object_id: ObjectId,
}

impl ObjectRef {
    /// Creates a concrete object and enforces the target-object wire byte limit.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for a wildcard ID or oversized canonical value.
    pub fn new(
        object_type: TypeName,
        object_id: ObjectId,
        limits: &InputLimits,
    ) -> Result<Self, ValidationError> {
        if object_id.is_wildcard() {
            return Err(ValidationError::new(
                "object_id",
                ValidationReason::Inconsistent,
            ));
        }
        let canonical_bytes = object_type
            .as_str()
            .len()
            .checked_add(1)
            .and_then(|bytes| bytes.checked_add(object_id.as_str().len()))
            .ok_or_else(|| ValidationError::new("object", ValidationReason::TooLarge))?;
        if canonical_bytes > limits.object_ref_bytes() {
            return Err(ValidationError::new("object", ValidationReason::TooLarge));
        }
        Ok(Self {
            object_type,
            object_id,
        })
    }

    /// Parses a concrete object under the configured limits.
    ///
    /// # Errors
    ///
    /// Returns a typed grammar or validation error without retaining the input.
    pub fn parse_with_limits(
        value: &str,
        limits: &InputLimits,
    ) -> Result<Self, crate::DomainError> {
        let (object_type, object_id) = parse_object_parts(value, "object")?;
        let object_type = TypeName::parse_with_limits(object_type, limits)?;
        let object_id = ObjectId::parse_with_limits(object_id, limits)?;
        Self::new(object_type, object_id, limits).map_err(Into::into)
    }

    /// Returns the object type.
    #[must_use]
    pub const fn object_type(&self) -> &TypeName {
        &self.object_type
    }

    /// Returns the object identifier.
    #[must_use]
    pub const fn object_id(&self) -> &ObjectId {
        &self.object_id
    }
}

impl FromStr for ObjectRef {
    type Err = crate::DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse_with_limits(value, &InputLimits::default())
    }
}

impl TryFrom<&str> for ObjectRef {
    type Error = crate::DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for ObjectRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.object_type, self.object_id)
    }
}

impl fmt::Debug for ObjectRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObjectRef")
            .field("object_type", &self.object_type)
            .field("object_id_bytes", &self.object_id.as_str().len())
            .finish()
    }
}

/// A userset subject such as `group:engineering#member`.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct UsersetRef {
    object: ObjectRef,
    relation: RelationName,
}

impl UsersetRef {
    /// Creates a userset and enforces the subject wire byte limit.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] when its canonical rendering is oversized.
    pub fn new(
        object: ObjectRef,
        relation: RelationName,
        limits: &InputLimits,
    ) -> Result<Self, ValidationError> {
        let canonical_bytes = object
            .to_string()
            .len()
            .checked_add(1)
            .and_then(|bytes| bytes.checked_add(relation.as_str().len()))
            .ok_or_else(|| ValidationError::new("userset", ValidationReason::TooLarge))?;
        if canonical_bytes > limits.subject_ref_bytes() {
            return Err(ValidationError::new("userset", ValidationReason::TooLarge));
        }
        Ok(Self { object, relation })
    }

    /// Returns the userset object.
    #[must_use]
    pub const fn object(&self) -> &ObjectRef {
        &self.object
    }

    /// Returns the userset relation.
    #[must_use]
    pub const fn relation(&self) -> &RelationName {
        &self.relation
    }
}

impl fmt::Display for UsersetRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}#{}", self.object, self.relation)
    }
}

impl fmt::Debug for UsersetRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UsersetRef")
            .field("object", &self.object)
            .field("relation", &self.relation)
            .finish()
    }
}

fn subject_structure_error(value: &str) -> ParseError {
    if value.is_empty() {
        return ParseError::new("subject", 0, ParseKind::Empty);
    }
    let mut colon = None;
    let mut hash = None;
    for (offset, character) in value.char_indices() {
        if colon.is_none() {
            if character == ':' {
                if offset == 0 {
                    return ParseError::new("subject", offset, ParseKind::UnexpectedSeparator);
                }
                colon = Some(offset);
            } else if !is_name_character(character) {
                return ParseError::new("subject", offset, ParseKind::InvalidCharacter);
            }
            continue;
        }
        if hash.is_none() {
            if character == '#' {
                hash = Some(offset);
            } else if character == ':' {
                return ParseError::new("subject", offset, ParseKind::UnexpectedSeparator);
            } else if !is_object_id_character(character) {
                return ParseError::new("subject", offset, ParseKind::InvalidCharacter);
            }
            continue;
        }
        if matches!(character, '#' | ':') {
            return ParseError::new("subject", offset, ParseKind::UnexpectedSeparator);
        }
        if !is_name_character(character) {
            return ParseError::new("subject", offset, ParseKind::InvalidCharacter);
        }
    }
    match (colon, hash) {
        (None, _) => ParseError::new("subject", value.len(), ParseKind::MissingSeparator),
        (Some(offset), _) if offset.saturating_add(1) == value.len() => {
            ParseError::new("subject", value.len(), ParseKind::Empty)
        }
        (_, Some(offset)) if offset.saturating_add(1) == value.len() => {
            ParseError::new("subject", value.len(), ParseKind::Empty)
        }
        _ => ParseError::new("subject", 0, ParseKind::InvalidCharacter),
    }
}

fn parse_subject_parts(value: &str) -> Result<(&str, &str, Option<&str>), ParseError> {
    let relation = opt((
        literal::<_, _, EmptyError>('#'),
        take_while::<_, _, EmptyError>(1.., is_name_character),
    ));
    let mut parser = (
        take_while::<_, _, EmptyError>(1.., is_name_character),
        literal::<_, _, EmptyError>(':'),
        take_while::<_, _, EmptyError>(1.., is_object_id_character),
        relation,
        eof::<_, EmptyError>,
    );
    parser
        .parse(value)
        .map(|(object_type, _, object_id, relation, _)| {
            (
                object_type,
                object_id,
                relation.map(|(_, relation)| relation),
            )
        })
        .map_err(|_| subject_structure_error(value))
}

/// A concrete object, userset, or typed-wildcard subject.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum SubjectRef {
    /// One concrete object.
    Object(ObjectRef),
    /// All members of an object/relation userset.
    Userset(UsersetRef),
    /// All concrete objects of one type.
    TypedWildcard(TypeName),
}

/// Stable structural kind of a validated relationship subject.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SubjectKind {
    /// One concrete object.
    Object,
    /// An object/relation userset.
    Userset,
    /// A typed wildcard.
    TypedWildcard,
}

impl SubjectRef {
    /// Parses a subject under the configured limits.
    ///
    /// # Errors
    ///
    /// Returns a typed grammar or validation error. Untyped `*` and wildcard
    /// usersets are rejected because schema 1.1 represents only typed wildcards.
    pub fn parse_with_limits(
        value: &str,
        limits: &InputLimits,
    ) -> Result<Self, crate::DomainError> {
        if value.len() > limits.subject_ref_bytes() {
            return Err(ValidationError::new("subject", ValidationReason::TooLarge).into());
        }
        let (object_type, object_id, relation) = parse_subject_parts(value)?;
        let object_type = TypeName::parse_with_limits(object_type, limits)?;
        let object_id = ObjectId::parse_with_limits(object_id, limits)?;
        match (object_id.is_wildcard(), relation) {
            (true, None) => Ok(Self::TypedWildcard(object_type)),
            (true, Some(_)) => {
                Err(ValidationError::new("subject", ValidationReason::Inconsistent).into())
            }
            (false, None) => ObjectRef::new(object_type, object_id, limits)
                .map(Self::Object)
                .map_err(Into::into),
            (false, Some(relation)) => {
                let object = ObjectRef::new(object_type, object_id, limits)?;
                let relation = RelationName::parse_with_limits(relation, limits)?;
                UsersetRef::new(object, relation, limits)
                    .map(Self::Userset)
                    .map_err(Into::into)
            }
        }
    }

    /// Returns the subject's object type.
    #[must_use]
    pub const fn subject_type(&self) -> &TypeName {
        match self {
            Self::Object(object) => object.object_type(),
            Self::Userset(userset) => userset.object().object_type(),
            Self::TypedWildcard(object_type) => object_type,
        }
    }

    /// Returns whether this is a typed wildcard.
    #[must_use]
    pub const fn is_typed_wildcard(&self) -> bool {
        matches!(self, Self::TypedWildcard(_))
    }

    /// Returns the stable structural kind.
    #[must_use]
    pub const fn kind(&self) -> SubjectKind {
        match self {
            Self::Object(_) => SubjectKind::Object,
            Self::Userset(_) => SubjectKind::Userset,
            Self::TypedWildcard(_) => SubjectKind::TypedWildcard,
        }
    }

    /// Returns the canonical subject object ID, including `*` for a wildcard.
    #[must_use]
    pub fn object_id(&self) -> &str {
        match self {
            Self::Object(object) => object.object_id().as_str(),
            Self::Userset(userset) => userset.object().object_id().as_str(),
            Self::TypedWildcard(_) => "*",
        }
    }

    /// Returns the userset relation, or `None` for concrete objects and wildcards.
    #[must_use]
    pub const fn relation(&self) -> Option<&RelationName> {
        match self {
            Self::Userset(userset) => Some(userset.relation()),
            Self::Object(_) | Self::TypedWildcard(_) => None,
        }
    }
}

impl FromStr for SubjectRef {
    type Err = crate::DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse_with_limits(value, &InputLimits::default())
    }
}

impl TryFrom<&str> for SubjectRef {
    type Error = crate::DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for SubjectRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Object(object) => fmt::Display::fmt(object, formatter),
            Self::Userset(userset) => fmt::Display::fmt(userset, formatter),
            Self::TypedWildcard(object_type) => write!(formatter, "{object_type}:*"),
        }
    }
}

impl fmt::Debug for SubjectRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Object(_) => "object",
            Self::Userset(_) => "userset",
            Self::TypedWildcard(_) => "typed_wildcard",
        };
        formatter
            .debug_struct("SubjectRef")
            .field("kind", &kind)
            .field("subject_type", self.subject_type())
            .finish()
    }
}

fn tuple_structure_error(value: &str) -> ParseError {
    if value.is_empty() {
        return ParseError::new("tuple_key", 0, ParseKind::Empty);
    }
    let hash = value
        .char_indices()
        .find_map(|(offset, character)| (character == '#').then_some(offset));
    let Some(hash) = hash else {
        return ParseError::new("tuple_key", value.len(), ParseKind::MissingSeparator);
    };
    let at = value
        .get(hash.saturating_add(1)..)
        .and_then(|remaining| {
            remaining
                .char_indices()
                .find_map(|(offset, character)| (character == '@').then_some(offset))
        })
        .and_then(|offset| hash.checked_add(1)?.checked_add(offset));
    let Some(at) = at else {
        return ParseError::new("tuple_key", value.len(), ParseKind::MissingSeparator);
    };
    if hash == 0 || at == hash.saturating_add(1) || at.saturating_add(1) == value.len() {
        return ParseError::new("tuple_key", at, ParseKind::Empty);
    }
    ParseError::new("tuple_key", 0, ParseKind::InvalidCharacter)
}

fn parse_tuple_parts(value: &str) -> Result<(&str, &str, &str), ParseError> {
    let mut parser = (
        take_while::<_, _, EmptyError>(1.., |character: char| character != '#'),
        literal::<_, _, EmptyError>('#'),
        take_while::<_, _, EmptyError>(1.., is_name_character),
        literal::<_, _, EmptyError>('@'),
        take_while::<_, _, EmptyError>(1.., |_: char| true),
        eof::<_, EmptyError>,
    );
    parser
        .parse(value)
        .map(|(object, _, relation, _, subject, _)| (object, relation, subject))
        .map_err(|_| tuple_structure_error(value))
}

/// The canonical identity of one relationship tuple.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub struct TupleKey {
    object: ObjectRef,
    relation: RelationName,
    subject: SubjectRef,
}

impl TupleKey {
    /// Creates a tuple key from fully validated components.
    #[must_use]
    pub const fn new(object: ObjectRef, relation: RelationName, subject: SubjectRef) -> Self {
        Self {
            object,
            relation,
            subject,
        }
    }

    /// Parses the canonical `object#relation@subject` grammar.
    ///
    /// # Errors
    ///
    /// Returns a typed grammar or component validation error.
    pub fn parse_with_limits(
        value: &str,
        limits: &InputLimits,
    ) -> Result<Self, crate::DomainError> {
        let (object, relation, subject) = parse_tuple_parts(value)?;
        let object = ObjectRef::parse_with_limits(object, limits)?;
        let relation = RelationName::parse_with_limits(relation, limits)?;
        let subject = SubjectRef::parse_with_limits(subject, limits)?;
        Ok(Self::new(object, relation, subject))
    }

    /// Returns the target object.
    #[must_use]
    pub const fn object(&self) -> &ObjectRef {
        &self.object
    }

    /// Returns the target relation.
    #[must_use]
    pub const fn relation(&self) -> &RelationName {
        &self.relation
    }

    /// Returns the subject.
    #[must_use]
    pub const fn subject(&self) -> &SubjectRef {
        &self.subject
    }

    /// Returns the canonical semantic fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> Fingerprint {
        let mut builder = FingerprintBuilder::new("openfga.tuple-key.v1");
        update_tuple_key_fingerprint(self, &mut builder);
        builder.finish()
    }
}

impl FromStr for TupleKey {
    type Err = crate::DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse_with_limits(value, &InputLimits::default())
    }
}

impl TryFrom<&str> for TupleKey {
    type Error = crate::DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for TupleKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}#{}@{}",
            self.object, self.relation, self.subject
        )
    }
}

impl fmt::Debug for TupleKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TupleKey")
            .field("object_type", self.object.object_type())
            .field("relation", &self.relation)
            .field("subject", &self.subject)
            .finish_non_exhaustive()
    }
}

fn update_tuple_key_fingerprint(key: &TupleKey, builder: &mut FingerprintBuilder) {
    builder.write_str(key.object.object_type().as_str());
    builder.write_str(key.object.object_id().as_str());
    builder.write_str(key.relation.as_str());
    match &key.subject {
        SubjectRef::Object(object) => {
            builder.write_tag(0);
            builder.write_str(object.object_type().as_str());
            builder.write_str(object.object_id().as_str());
        }
        SubjectRef::Userset(userset) => {
            builder.write_tag(1);
            builder.write_str(userset.object().object_type().as_str());
            builder.write_str(userset.object().object_id().as_str());
            builder.write_str(userset.relation().as_str());
        }
        SubjectRef::TypedWildcard(object_type) => {
            builder.write_tag(2);
            builder.write_str(object_type.as_str());
        }
    }
}

/// A condition name and redacted, bounded tuple context.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct ConditionBinding {
    name: ConditionName,
    context: ConditionContext,
}

impl ConditionBinding {
    /// Creates a relationship condition binding.
    #[must_use]
    pub const fn new(name: ConditionName, context: ConditionContext) -> Self {
        Self { name, context }
    }

    /// Returns the condition name.
    #[must_use]
    pub const fn name(&self) -> &ConditionName {
        &self.name
    }

    /// Returns the bounded tuple context.
    #[must_use]
    pub const fn context(&self) -> &ConditionContext {
        &self.context
    }
}

impl fmt::Debug for ConditionBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConditionBinding")
            .field("name", &self.name)
            .field("context", &"[REDACTED]")
            .finish()
    }
}

/// Explicit condition presence for one relationship tuple.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConditionReference {
    /// The relationship is unconditional.
    #[default]
    Unconditional,
    /// The relationship is guarded by a named condition.
    Conditional(ConditionBinding),
}

impl ConditionReference {
    /// Returns the condition binding, or `None` for an unconditional tuple.
    #[must_use]
    pub const fn binding(&self) -> Option<&ConditionBinding> {
        match self {
            Self::Unconditional => None,
            Self::Conditional(binding) => Some(binding),
        }
    }
}

/// A tuple key plus optional condition metadata outside tuple identity.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct RelationshipTuple {
    key: TupleKey,
    condition: ConditionReference,
}

impl RelationshipTuple {
    /// Creates a relationship tuple.
    #[must_use]
    pub const fn new(key: TupleKey, condition: ConditionReference) -> Self {
        Self { key, condition }
    }

    /// Creates an unconditional relationship tuple.
    #[must_use]
    pub const fn unconditional(key: TupleKey) -> Self {
        Self::new(key, ConditionReference::Unconditional)
    }

    /// Returns the tuple identity.
    #[must_use]
    pub const fn key(&self) -> &TupleKey {
        &self.key
    }

    /// Returns explicit condition presence.
    #[must_use]
    pub const fn condition(&self) -> &ConditionReference {
        &self.condition
    }

    fn update_fingerprint(&self, builder: &mut FingerprintBuilder) {
        update_tuple_key_fingerprint(&self.key, builder);
        match &self.condition {
            ConditionReference::Unconditional => builder.write_tag(0),
            ConditionReference::Conditional(binding) => {
                builder.write_tag(1);
                builder.write_str(binding.name.as_str());
                builder.write_bytes(binding.context.fingerprint().as_bytes());
            }
        }
    }
}

impl fmt::Debug for RelationshipTuple {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RelationshipTuple")
            .field("key", &self.key)
            .field("condition", &self.condition)
            .finish()
    }
}

/// An immutable, bounded set of request-only relationship tuples.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub struct ContextualTuples {
    tuples: Vec<RelationshipTuple>,
    fingerprint: Fingerprint,
}

impl ContextualTuples {
    /// Validates count and tuple-key uniqueness, then computes a canonical fingerprint.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] for too many tuples or duplicate tuple identities.
    pub fn new(
        tuples: Vec<RelationshipTuple>,
        limits: &InputLimits,
    ) -> Result<Self, ValidationError> {
        if tuples.len() > limits.contextual_tuples() {
            return Err(ValidationError::new(
                "contextual_tuples",
                ValidationReason::TooManyItems,
            ));
        }
        let mut keys = BTreeSet::new();
        if tuples.iter().any(|tuple| !keys.insert(tuple.key.clone())) {
            return Err(ValidationError::new(
                "contextual_tuples",
                ValidationReason::Duplicate,
            ));
        }
        let fingerprint = fingerprint_contextual_tuples(&tuples);
        Ok(Self {
            tuples,
            fingerprint,
        })
    }

    /// Returns an empty validated tuple set.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            tuples: Vec::new(),
            fingerprint: fingerprint_contextual_tuples(&[]),
        }
    }

    /// Returns contextual tuples in caller declaration order.
    #[must_use]
    pub fn as_slice(&self) -> &[RelationshipTuple] {
        &self.tuples
    }

    /// Returns whether no contextual tuples were supplied.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tuples.is_empty()
    }

    /// Returns the order-independent canonical tuple-set fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }
}

impl Default for ContextualTuples {
    fn default() -> Self {
        Self::empty()
    }
}

impl fmt::Debug for ContextualTuples {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextualTuples")
            .field("items", &self.tuples.len())
            .finish_non_exhaustive()
    }
}

fn fingerprint_contextual_tuples(tuples: &[RelationshipTuple]) -> Fingerprint {
    let mut tuple_fingerprints = tuples
        .iter()
        .map(|tuple| {
            let mut builder = FingerprintBuilder::new("openfga.relationship-tuple.v1");
            tuple.update_fingerprint(&mut builder);
            builder.finish()
        })
        .collect::<Vec<_>>();
    tuple_fingerprints.sort_unstable();
    let mut builder = FingerprintBuilder::new("openfga.contextual-tuples.v1");
    builder.write_u64(u64::try_from(tuple_fingerprints.len()).unwrap_or(u64::MAX));
    for fingerprint in tuple_fingerprints {
        builder.write_bytes(fingerprint.as_bytes());
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::{any, proptest};

    use super::{ContextualTuples, ObjectRef, RelationshipTuple, SubjectRef, TupleKey};
    use crate::InputLimits;

    #[test]
    fn test_should_round_trip_all_subject_variants() {
        let cases = [
            "user:anne",
            "group:engineering#member",
            "user:*",
            "user:github|anne@openfga.com",
        ];
        for value in cases {
            let parsed = value.parse::<SubjectRef>();
            assert!(parsed.is_ok(), "subject fixture should parse");
            assert_eq!(
                parsed.map(|subject| subject.to_string()),
                Ok(value.to_owned())
            );
        }
        assert!("*".parse::<SubjectRef>().is_err());
        assert!("group:*#member".parse::<SubjectRef>().is_err());
    }

    #[test]
    fn test_should_round_trip_tuple_and_redact_object_ids() {
        let value = "document:roadmap#viewer@group:engineering#member";
        let tuple = value.parse::<TupleKey>();
        assert!(tuple.is_ok());
        assert_eq!(
            tuple.as_ref().map(ToString::to_string),
            Ok(value.to_owned())
        );
        let debug = format!("{:?}", tuple.ok());
        assert!(!debug.contains("roadmap"));
        assert!(!debug.contains("engineering"));
    }

    #[test]
    fn test_should_make_contextual_tuple_fingerprint_order_independent() {
        let first = "document:1#viewer@user:anne".parse::<TupleKey>();
        let second = "document:2#viewer@user:anne".parse::<TupleKey>();
        assert!(first.is_ok() && second.is_ok());
        let (Ok(first), Ok(second)) = (first, second) else {
            return;
        };
        let left = ContextualTuples::new(
            vec![
                RelationshipTuple::unconditional(first.clone()),
                RelationshipTuple::unconditional(second.clone()),
            ],
            &InputLimits::default(),
        );
        let right = ContextualTuples::new(
            vec![
                RelationshipTuple::unconditional(second),
                RelationshipTuple::unconditional(first),
            ],
            &InputLimits::default(),
        );
        assert!(left.is_ok() && right.is_ok());
        assert_eq!(
            left.map(|tuples| tuples.fingerprint()),
            right.map(|tuples| tuples.fingerprint())
        );
    }

    proptest! {
        #[test]
        fn test_should_never_panic_parsing_arbitrary_references(value in any::<String>()) {
            let _ = value.parse::<ObjectRef>();
            let _ = value.parse::<SubjectRef>();
            let _ = value.parse::<TupleKey>();
        }
    }
}
