//! Validated, versioned persistence codecs for models, tuples, and assertions.

use std::{collections::BTreeMap, sync::Arc, time::SystemTime};

use openfga_condition::{ConditionDefinition, ParameterType, ParameterTypeRef};
use openfga_domain::{
    AuthorizationModelId, ConditionBinding, ConditionContext, ConditionName, ConditionReference,
    ContextBytes, ContextKey, ContextList, ContextMap, ContextString, ContextValue,
    ContextualTuples, FiniteFloat, InputLimits, ParameterName, RelationshipTuple, StoreId,
    TupleKey,
};
use openfga_model::{
    AuthorizationModelSource, ConditionSource, DirectRestrictionSource, ModelCompiler,
    RelationSource, RestrictionKindSource, RestrictionKindSourceRef, RewriteSource,
    RewriteSourceRef, TypeDefinitionSource,
};
use openfga_storage::{Assertion, StorageError, StorageErrorKind, StoredAuthorizationModel};
use serde::{Deserialize, Serialize};

const CODEC_VERSION: u8 = 1;
const MAXIMUM_MODEL_PAYLOAD_BYTES: usize = 16 * 1_024 * 1_024;
const MAXIMUM_ASSERTION_PAYLOAD_BYTES: usize = 8 * 1_024 * 1_024;
const MAXIMUM_TUPLE_PAYLOAD_BYTES: usize = 2 * 1_024 * 1_024;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelEnvelope {
    version: u8,
    schema_version: String,
    types: Vec<TypeDto>,
    conditions: Vec<ConditionDto>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TypeDto {
    name: String,
    relations: Vec<RelationDto>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RelationDto {
    name: String,
    rewrite: RewriteDto,
    restrictions: Vec<RestrictionDto>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum RewriteDto {
    Direct,
    Computed {
        relation: String,
    },
    TupleToUserset {
        tupleset: String,
        computed: String,
    },
    Union {
        children: Vec<Self>,
    },
    Intersection {
        children: Vec<Self>,
    },
    Difference {
        base: Box<Self>,
        subtract: Box<Self>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RestrictionDto {
    subject_type: String,
    kind: RestrictionKindDto,
    condition: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "relation", rename_all = "camelCase")]
enum RestrictionKindDto {
    Object,
    Userset(String),
    Wildcard,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConditionDto {
    key: String,
    name: String,
    expression: String,
    parameters: BTreeMap<String, ParameterTypeDto>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "element", rename_all = "camelCase")]
enum ParameterTypeDto {
    Any,
    Bool,
    String,
    Int,
    Uint,
    Double,
    Bytes,
    Duration,
    Timestamp,
    IpAddress,
    List(Box<Self>),
    Map(Box<Self>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TupleEnvelope {
    version: u8,
    key: String,
    condition: Option<ConditionBindingDto>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConditionBindingDto {
    name: String,
    context: BTreeMap<String, ContextValueDto>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ContextEnvelope {
    version: u8,
    values: BTreeMap<String, ContextValueDto>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
enum ContextValueDto {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Self>),
    Map(BTreeMap<String, Self>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AssertionsEnvelope {
    version: u8,
    assertions: Vec<AssertionDto>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AssertionDto {
    tuple: String,
    expectation: bool,
    contextual_tuples: Vec<TupleEnvelope>,
    #[serde(default)]
    condition_context: BTreeMap<String, ContextValueDto>,
}

pub(crate) fn encode_model(model: &StoredAuthorizationModel) -> Result<Vec<u8>, StorageError> {
    let source = model.source();
    let envelope = ModelEnvelope {
        version: CODEC_VERSION,
        schema_version: source.schema_version().to_owned(),
        types: source
            .type_definitions()
            .iter()
            .map(TypeDto::from_source)
            .collect(),
        conditions: source
            .conditions()
            .iter()
            .map(ConditionDto::from_source)
            .collect(),
    };
    serde_json::to_vec(&envelope).map_err(codec_encode_error)
}

pub(crate) fn decode_model(
    bytes: &[u8],
    store_id: StoreId,
    model_id: AuthorizationModelId,
    written_at: SystemTime,
    compiler: &ModelCompiler,
) -> Result<Arc<StoredAuthorizationModel>, StorageError> {
    require_payload_limit(
        bytes,
        MAXIMUM_MODEL_PAYLOAD_BYTES,
        "persisted_model_payload_limit",
    )?;
    let envelope: ModelEnvelope = serde_json::from_slice(bytes).map_err(codec_decode_error)?;
    require_version(envelope.version)?;
    let source = Arc::new(AuthorizationModelSource::new(
        store_id,
        model_id,
        envelope.schema_version,
        envelope
            .types
            .into_iter()
            .map(TypeDto::into_source)
            .collect::<Result<_, _>>()?,
        envelope
            .conditions
            .into_iter()
            .map(ConditionDto::into_source)
            .collect::<Result<_, _>>()?,
    ));
    let compiled_model = compiler.compile(&source).map_err(|error| {
        StorageError::with_source(
            StorageErrorKind::Integrity,
            "persisted_model_invalid",
            error,
        )
    })?;
    StoredAuthorizationModel::new(source, compiled_model, written_at)
        .map(Arc::new)
        .map_err(|error| {
            StorageError::with_source(
                StorageErrorKind::Integrity,
                "persisted_model_identity_invalid",
                error,
            )
        })
}

pub(crate) fn encode_tuple(tuple: &RelationshipTuple) -> Result<Vec<u8>, StorageError> {
    serde_json::to_vec(&TupleEnvelope::from_tuple(tuple)?).map_err(codec_encode_error)
}

pub(crate) fn encode_condition_context(
    context: &ConditionContext,
) -> Result<Vec<u8>, StorageError> {
    let values = context
        .iter()
        .map(|(name, value)| {
            ContextValueDto::from_value(value).map(|value| (name.to_string(), value))
        })
        .collect::<Result<_, StorageError>>()?;
    serde_json::to_vec(&ContextEnvelope {
        version: CODEC_VERSION,
        values,
    })
    .map_err(codec_encode_error)
}

pub(crate) fn decode_tuple(bytes: &[u8]) -> Result<RelationshipTuple, StorageError> {
    require_payload_limit(
        bytes,
        MAXIMUM_TUPLE_PAYLOAD_BYTES,
        "persisted_tuple_payload_limit",
    )?;
    let envelope: TupleEnvelope = serde_json::from_slice(bytes).map_err(codec_decode_error)?;
    envelope.into_tuple()
}

pub(crate) fn encode_assertions(assertions: &[Assertion]) -> Result<Vec<u8>, StorageError> {
    let assertions = assertions
        .iter()
        .map(AssertionDto::from_assertion)
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_vec(&AssertionsEnvelope {
        version: CODEC_VERSION,
        assertions,
    })
    .map_err(codec_encode_error)
}

pub(crate) fn decode_assertions(bytes: &[u8]) -> Result<Arc<[Assertion]>, StorageError> {
    require_payload_limit(
        bytes,
        MAXIMUM_ASSERTION_PAYLOAD_BYTES,
        "persisted_assertion_payload_limit",
    )?;
    let envelope: AssertionsEnvelope = serde_json::from_slice(bytes).map_err(codec_decode_error)?;
    require_version(envelope.version)?;
    envelope
        .assertions
        .into_iter()
        .map(AssertionDto::into_assertion)
        .collect::<Result<Vec<_>, _>>()
        .map(Arc::from)
}

impl TypeDto {
    fn from_source(source: &TypeDefinitionSource) -> Self {
        Self {
            name: source.name().to_string(),
            relations: source
                .relations()
                .iter()
                .map(RelationDto::from_source)
                .collect(),
        }
    }

    fn into_source(self) -> Result<TypeDefinitionSource, StorageError> {
        Ok(TypeDefinitionSource::new(
            parse(&self.name, "persisted_model_type")?,
            self.relations
                .into_iter()
                .map(RelationDto::into_source)
                .collect::<Result<_, _>>()?,
        ))
    }
}

impl RelationDto {
    fn from_source(source: &RelationSource) -> Self {
        Self {
            name: source.name().to_string(),
            rewrite: RewriteDto::from_source(source.rewrite()),
            restrictions: source
                .restrictions()
                .iter()
                .map(RestrictionDto::from_source)
                .collect(),
        }
    }

    fn into_source(self) -> Result<RelationSource, StorageError> {
        Ok(RelationSource::new(
            parse(&self.name, "persisted_model_relation")?,
            self.rewrite.into_source()?,
            self.restrictions
                .into_iter()
                .map(RestrictionDto::into_source)
                .collect::<Result<_, _>>()?,
        ))
    }
}

impl RewriteDto {
    fn from_source(source: &RewriteSource) -> Self {
        match source.as_ref() {
            RewriteSourceRef::Direct => Self::Direct,
            RewriteSourceRef::Computed(relation) => Self::Computed {
                relation: relation.to_string(),
            },
            RewriteSourceRef::TupleToUserset { tupleset, computed } => Self::TupleToUserset {
                tupleset: tupleset.to_string(),
                computed: computed.to_string(),
            },
            RewriteSourceRef::Union(children) => Self::Union {
                children: children.iter().map(Self::from_source).collect(),
            },
            RewriteSourceRef::Intersection(children) => Self::Intersection {
                children: children.iter().map(Self::from_source).collect(),
            },
            RewriteSourceRef::Difference { base, subtract } => Self::Difference {
                base: Box::new(Self::from_source(base)),
                subtract: Box::new(Self::from_source(subtract)),
            },
        }
    }

    fn into_source(self) -> Result<RewriteSource, StorageError> {
        match self {
            Self::Direct => Ok(RewriteSource::Direct),
            Self::Computed { relation } => Ok(RewriteSource::Computed(parse(
                &relation,
                "persisted_computed_relation",
            )?)),
            Self::TupleToUserset { tupleset, computed } => Ok(RewriteSource::TupleToUserset {
                tupleset: parse(&tupleset, "persisted_tupleset_relation")?,
                computed: parse(&computed, "persisted_computed_relation")?,
            }),
            Self::Union { children } => Ok(RewriteSource::Union(
                children
                    .into_iter()
                    .map(Self::into_source)
                    .collect::<Result<_, _>>()?,
            )),
            Self::Intersection { children } => Ok(RewriteSource::Intersection(
                children
                    .into_iter()
                    .map(Self::into_source)
                    .collect::<Result<_, _>>()?,
            )),
            Self::Difference { base, subtract } => Ok(RewriteSource::Difference {
                base: Box::new(base.into_source()?),
                subtract: Box::new(subtract.into_source()?),
            }),
        }
    }
}

impl RestrictionDto {
    fn from_source(source: &DirectRestrictionSource) -> Self {
        let kind = match source.kind().as_ref() {
            RestrictionKindSourceRef::Object => RestrictionKindDto::Object,
            RestrictionKindSourceRef::Userset(relation) => {
                RestrictionKindDto::Userset(relation.to_string())
            }
            RestrictionKindSourceRef::Wildcard => RestrictionKindDto::Wildcard,
        };
        Self {
            subject_type: source.subject_type().to_string(),
            kind,
            condition: source.condition().map(ToString::to_string),
        }
    }

    fn into_source(self) -> Result<DirectRestrictionSource, StorageError> {
        let kind = match self.kind {
            RestrictionKindDto::Object => RestrictionKindSource::Object,
            RestrictionKindDto::Userset(relation) => {
                RestrictionKindSource::Userset(parse(&relation, "persisted_userset_relation")?)
            }
            RestrictionKindDto::Wildcard => RestrictionKindSource::Wildcard,
        };
        Ok(DirectRestrictionSource::new(
            parse(&self.subject_type, "persisted_restriction_type")?,
            kind,
            self.condition
                .map(|value| parse(&value, "persisted_restriction_condition"))
                .transpose()?,
        ))
    }
}

impl ConditionDto {
    fn from_source(source: &ConditionSource) -> Self {
        let definition = source.definition();
        Self {
            key: source.key().to_string(),
            name: definition.name().to_string(),
            expression: definition.expression().to_owned(),
            parameters: definition
                .parameters()
                .iter()
                .map(|(name, value)| (name.to_string(), ParameterTypeDto::from_type(value)))
                .collect(),
        }
    }

    fn into_source(self) -> Result<ConditionSource, StorageError> {
        Ok(ConditionSource::new(
            parse(&self.key, "persisted_condition_key")?,
            ConditionDefinition::new(
                parse(&self.name, "persisted_condition_name")?,
                self.expression,
                self.parameters
                    .into_iter()
                    .map(|(name, value)| {
                        Ok((
                            parse(&name, "persisted_parameter_name")?,
                            value.into_type()?,
                        ))
                    })
                    .collect::<Result<_, StorageError>>()?,
            ),
        ))
    }
}

impl ParameterTypeDto {
    fn from_type(value: &ParameterType) -> Self {
        match value.as_ref() {
            ParameterTypeRef::Any => Self::Any,
            ParameterTypeRef::Bool => Self::Bool,
            ParameterTypeRef::String => Self::String,
            ParameterTypeRef::Int => Self::Int,
            ParameterTypeRef::Uint => Self::Uint,
            ParameterTypeRef::Double => Self::Double,
            ParameterTypeRef::Bytes => Self::Bytes,
            ParameterTypeRef::Duration => Self::Duration,
            ParameterTypeRef::Timestamp => Self::Timestamp,
            ParameterTypeRef::IpAddress => Self::IpAddress,
            ParameterTypeRef::List(child) => Self::List(Box::new(Self::from_type(child))),
            ParameterTypeRef::Map(child) => Self::Map(Box::new(Self::from_type(child))),
        }
    }

    fn into_type(self) -> Result<ParameterType, StorageError> {
        match self {
            Self::Any => Ok(ParameterType::any()),
            Self::Bool => Ok(ParameterType::bool()),
            Self::String => Ok(ParameterType::string()),
            Self::Int => Ok(ParameterType::int()),
            Self::Uint => Ok(ParameterType::uint()),
            Self::Double => Ok(ParameterType::double()),
            Self::Bytes => Ok(ParameterType::bytes()),
            Self::Duration => Ok(ParameterType::duration()),
            Self::Timestamp => Ok(ParameterType::timestamp()),
            Self::IpAddress => Ok(ParameterType::ip_address()),
            Self::List(child) => ParameterType::list(child.into_type()?).map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::Integrity,
                    "persisted_parameter_type_invalid",
                    error,
                )
            }),
            Self::Map(child) => ParameterType::map(child.into_type()?).map_err(|error| {
                StorageError::with_source(
                    StorageErrorKind::Integrity,
                    "persisted_parameter_type_invalid",
                    error,
                )
            }),
        }
    }
}

impl TupleEnvelope {
    fn from_tuple(tuple: &RelationshipTuple) -> Result<Self, StorageError> {
        let condition = tuple
            .condition()
            .binding()
            .map(|binding| {
                Ok(ConditionBindingDto {
                    name: binding.name().to_string(),
                    context: binding
                        .context()
                        .iter()
                        .map(|(name, value)| {
                            ContextValueDto::from_value(value)
                                .map(|value| (name.to_string(), value))
                        })
                        .collect::<Result<_, StorageError>>()?,
                })
            })
            .transpose()?;
        Ok(Self {
            version: CODEC_VERSION,
            key: tuple.key().to_string(),
            condition,
        })
    }

    fn into_tuple(self) -> Result<RelationshipTuple, StorageError> {
        require_version(self.version)?;
        let key = parse(&self.key, "persisted_tuple_key")?;
        let condition = self
            .condition
            .map(ConditionBindingDto::into_binding)
            .transpose()?
            .map_or(
                ConditionReference::Unconditional,
                ConditionReference::Conditional,
            );
        Ok(RelationshipTuple::new(key, condition))
    }
}

impl ConditionBindingDto {
    fn into_binding(self) -> Result<ConditionBinding, StorageError> {
        let limits = InputLimits::default();
        let values = self
            .context
            .into_iter()
            .map(|(name, value)| {
                Ok((
                    ParameterName::parse_with_limits(&name, &limits)
                        .map_err(|error| integrity("persisted_context_parameter", error))?,
                    value.into_value(&limits)?,
                ))
            })
            .collect::<Result<_, StorageError>>()?;
        let context = ConditionContext::new(values, &limits)
            .map_err(|error| integrity("persisted_context_invalid", error))?;
        Ok(ConditionBinding::new(
            ConditionName::parse_with_limits(&self.name, &limits)
                .map_err(|error| integrity("persisted_condition_name", error))?,
            context,
        ))
    }
}

impl ContextValueDto {
    fn from_value(value: &ContextValue) -> Result<Self, StorageError> {
        match value {
            ContextValue::Null => Ok(Self::Null),
            ContextValue::Bool(value) => Ok(Self::Bool(*value)),
            ContextValue::Int(value) => Ok(Self::Int(*value)),
            ContextValue::Uint(value) => Ok(Self::Uint(*value)),
            ContextValue::Double(value) => Ok(Self::Double(value.get())),
            ContextValue::String(value) => Ok(Self::String(value.as_str().to_owned())),
            ContextValue::Bytes(value) => Ok(Self::Bytes(value.as_slice().to_vec())),
            ContextValue::List(values) => values
                .as_slice()
                .iter()
                .map(Self::from_value)
                .collect::<Result<_, _>>()
                .map(Self::List),
            ContextValue::Map(values) => values
                .iter()
                .map(|(key, value)| {
                    Self::from_value(value).map(|value| (key.as_str().to_owned(), value))
                })
                .collect::<Result<_, _>>()
                .map(Self::Map),
            _ => Err(StorageError::new(
                StorageErrorKind::Integrity,
                "context_value_variant_unknown",
            )),
        }
    }

    fn into_value(self, limits: &InputLimits) -> Result<ContextValue, StorageError> {
        match self {
            Self::Null => Ok(ContextValue::Null),
            Self::Bool(value) => Ok(ContextValue::Bool(value)),
            Self::Int(value) => Ok(ContextValue::Int(value)),
            Self::Uint(value) => Ok(ContextValue::Uint(value)),
            Self::Double(value) => FiniteFloat::new(value)
                .map(ContextValue::Double)
                .map_err(|error| integrity("persisted_context_double", error)),
            Self::String(value) => ContextString::new(value, limits)
                .map(ContextValue::String)
                .map_err(|error| integrity("persisted_context_string", error)),
            Self::Bytes(value) => ContextBytes::new(value, limits)
                .map(ContextValue::Bytes)
                .map_err(|error| integrity("persisted_context_bytes", error)),
            Self::List(values) => values
                .into_iter()
                .map(|value| value.into_value(limits))
                .collect::<Result<_, _>>()
                .and_then(|values| {
                    ContextList::new(values, limits)
                        .map(ContextValue::List)
                        .map_err(|error| integrity("persisted_context_list", error))
                }),
            Self::Map(values) => values
                .into_iter()
                .map(|(key, value)| {
                    Ok((
                        ContextKey::new(key, limits)
                            .map_err(|error| integrity("persisted_context_key", error))?,
                        value.into_value(limits)?,
                    ))
                })
                .collect::<Result<_, StorageError>>()
                .and_then(|values| {
                    ContextMap::new(values, limits)
                        .map(ContextValue::Map)
                        .map_err(|error| integrity("persisted_context_map", error))
                }),
        }
    }
}

impl AssertionDto {
    fn from_assertion(assertion: &Assertion) -> Result<Self, StorageError> {
        Ok(Self {
            tuple: assertion.tuple().to_string(),
            expectation: assertion.expectation(),
            contextual_tuples: assertion
                .contextual_tuples()
                .as_slice()
                .iter()
                .map(TupleEnvelope::from_tuple)
                .collect::<Result<_, _>>()?,
            condition_context: assertion
                .condition_context()
                .iter()
                .map(|(name, value)| {
                    ContextValueDto::from_value(value).map(|value| (name.to_string(), value))
                })
                .collect::<Result<_, _>>()?,
        })
    }

    fn into_assertion(self) -> Result<Assertion, StorageError> {
        let limits = InputLimits::default();
        let contextual_tuples = self
            .contextual_tuples
            .into_iter()
            .map(TupleEnvelope::into_tuple)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Assertion::new(
            parse::<TupleKey>(&self.tuple, "persisted_assertion_tuple")?,
            self.expectation,
            ContextualTuples::new(contextual_tuples, &limits)
                .map_err(|error| integrity("persisted_assertion_context", error))?,
            context_from_dto(self.condition_context, &limits)?,
        ))
    }
}

fn context_from_dto(
    values: BTreeMap<String, ContextValueDto>,
    limits: &InputLimits,
) -> Result<ConditionContext, StorageError> {
    let values = values
        .into_iter()
        .map(|(name, value)| {
            Ok((
                ParameterName::parse_with_limits(&name, limits)
                    .map_err(|error| integrity("persisted_context_parameter", error))?,
                value.into_value(limits)?,
            ))
        })
        .collect::<Result<_, StorageError>>()?;
    ConditionContext::new(values, limits)
        .map_err(|error| integrity("persisted_context_invalid", error))
}

fn require_version(version: u8) -> Result<(), StorageError> {
    if version == CODEC_VERSION {
        Ok(())
    } else {
        Err(StorageError::new(
            StorageErrorKind::Integrity,
            "persistence_codec_version_unsupported",
        ))
    }
}

fn require_payload_limit(
    bytes: &[u8],
    maximum: usize,
    code: &'static str,
) -> Result<(), StorageError> {
    if bytes.len() <= maximum {
        Ok(())
    } else {
        Err(StorageError::new(StorageErrorKind::Integrity, code))
    }
}

fn parse<T>(value: &str, code: &'static str) -> Result<T, StorageError>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    value.parse().map_err(|error| integrity(code, error))
}

fn integrity(
    code: &'static str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> StorageError {
    StorageError::with_source(StorageErrorKind::Integrity, code, error)
}

fn codec_encode_error(error: serde_json::Error) -> StorageError {
    StorageError::with_source(
        StorageErrorKind::Internal,
        "persistence_encode_failed",
        error,
    )
}

fn codec_decode_error(error: serde_json::Error) -> StorageError {
    integrity("persistence_decode_failed", error)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use openfga_domain::{
        ConditionBinding, ConditionContext, ConditionReference, ContextValue, InputLimits,
        ParameterName, RelationshipTuple, TupleKey,
    };
    use openfga_storage::{Assertion, StorageErrorKind};

    use super::{
        MAXIMUM_TUPLE_PAYLOAD_BYTES, decode_assertions, decode_tuple, encode_assertions,
        encode_tuple,
    };

    #[test]
    fn test_should_round_trip_assertion_condition_context() -> Result<(), Box<dyn std::error::Error>>
    {
        let limits = InputLimits::default();
        let context = ConditionContext::new(
            BTreeMap::from([(
                ParameterName::parse_with_limits("region", &limits)?,
                ContextValue::String(openfga_domain::ContextString::new(
                    "west".to_owned(),
                    &limits,
                )?),
            )]),
            &limits,
        )?;
        let assertion = Assertion::new(
            "document:roadmap#viewer@user:anne".parse()?,
            true,
            openfga_domain::ContextualTuples::empty(),
            context,
        );
        let decoded = decode_assertions(&encode_assertions(std::slice::from_ref(&assertion))?)?;
        assert_eq!(decoded.as_ref(), &[assertion]);
        Ok(())
    }

    #[test]
    fn test_should_round_trip_typed_condition_context_without_loss()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = InputLimits::default();
        let context = ConditionContext::new(
            BTreeMap::from([
                (
                    ParameterName::parse_with_limits("signed", &limits)?,
                    ContextValue::Int(-9),
                ),
                (
                    ParameterName::parse_with_limits("unsigned", &limits)?,
                    ContextValue::Uint(9),
                ),
            ]),
            &limits,
        )?;
        let tuple = RelationshipTuple::new(
            "document:roadmap#viewer@user:anne".parse::<TupleKey>()?,
            ConditionReference::Conditional(ConditionBinding::new(
                "within_limit".parse()?,
                context,
            )),
        );

        assert_eq!(decode_tuple(&encode_tuple(&tuple)?)?, tuple);
        Ok(())
    }

    #[test]
    fn test_should_reject_oversized_persisted_tuple_before_decoding() {
        let bytes = vec![b' '; MAXIMUM_TUPLE_PAYLOAD_BYTES.saturating_add(1)];
        let error = decode_tuple(&bytes).err();

        assert!(matches!(error, Some(error) if error.kind() == StorageErrorKind::Integrity));
    }
}
