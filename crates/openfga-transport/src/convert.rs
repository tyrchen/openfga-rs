//! Checked conversion between generated protocol messages and trusted domain values.

use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use openfga_condition::{ConditionDefinition, ParameterType, ParameterTypeRef};
use openfga_domain::{
    AuthorizationModelId, ConditionBinding, ConditionContext, ConditionName, ConditionReference,
    ContextValue, ContextualTuples, DomainError, InputLimits, ModelSelection, ObjectRef,
    ParameterName, ParseKind, RelationName, RelationshipTuple, StoreId, SubjectRef, TupleKey,
    TypeName, ValidationReason,
};
use openfga_model::{
    AuthorizationModelDefinition, ConditionSource, DirectRestrictionSource, RelationSource,
    RestrictionKindSource, RestrictionKindSourceRef, RewriteSource, RewriteSourceRef,
    TypeDefinitionSource,
};
use openfga_proto::openfga::{
    v1 as pb, v1::condition_param_type_ref::TypeName as WireParameterTypeName,
};
use openfga_storage::{
    Assertion, ChangeOperation, StoreRecord, StoredAuthorizationModel, StoredTuple, TupleChange,
    TupleReadFilter,
};
use serde_json::{Map, Number, Value};

use crate::ApiError;

const MAXIMUM_REWRITE_DEPTH: usize = 64;

pub(crate) fn store_id(value: &str) -> Result<StoreId, ApiError> {
    value.parse().map_err(|_| ApiError::invalid_store_id())
}

pub(crate) fn model_selection(value: &str) -> Result<ModelSelection, ApiError> {
    if value.is_empty() {
        Ok(ModelSelection::Latest)
    } else {
        value
            .parse::<AuthorizationModelId>()
            .map(ModelSelection::Explicit)
            .map_err(|error| ApiError::invalid_model_id(error.kind() == ParseKind::TooLong))
    }
}

pub(crate) fn model_id(value: &str) -> Result<AuthorizationModelId, ApiError> {
    value.parse().map_err(|error: openfga_domain::ParseError| {
        ApiError::invalid_model_id(error.kind() == ParseKind::TooLong)
    })
}

pub(crate) fn tuple_key(
    object: &str,
    relation: &str,
    user: &str,
    limits: &InputLimits,
) -> Result<TupleKey, ApiError> {
    Ok(TupleKey::new(
        ObjectRef::parse_with_limits(object, limits)
            .map_err(|error| ApiError::invalid_object(domain_parse_too_long(&error)))?,
        RelationName::parse_with_limits(relation, limits)
            .map_err(|error| ApiError::invalid_relation(error.kind() == ParseKind::TooLong))?,
        SubjectRef::parse_with_limits(user, limits).map_err(|_| ApiError::invalid_user())?,
    ))
}

fn domain_parse_too_long(error: &DomainError) -> bool {
    matches!(
        error,
        DomainError::Parse(error) if error.kind() == ParseKind::TooLong
    ) || matches!(
        error,
        DomainError::Validation(error) if error.reason() == ValidationReason::TooLarge
    )
}

pub(crate) fn relationship_tuple(
    tuple: pb::TupleKey,
    limits: &InputLimits,
) -> Result<RelationshipTuple, ApiError> {
    let key = tuple_key(&tuple.object, &tuple.relation, &tuple.user, limits)?;
    let condition = tuple
        .condition
        .map(|condition| {
            let name = ConditionName::parse_with_limits(&condition.name, limits)
                .map_err(|_| ApiError::invalid_request())?;
            let context = condition_context(condition.context, limits)?;
            Ok::<_, ApiError>(ConditionReference::Conditional(ConditionBinding::new(
                name, context,
            )))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(RelationshipTuple::new(key, condition))
}

pub(crate) fn tuple_read_filter(
    value: Option<&pb::ReadRequestTupleKey>,
    limits: &InputLimits,
) -> Result<TupleReadFilter, ApiError> {
    let Some(value) = value else {
        return Ok(TupleReadFilter::all());
    };
    let (object_type, object_id) = value
        .object
        .split_once(':')
        .ok_or_else(ApiError::invalid_request)?;
    let object_type = TypeName::parse_with_limits(object_type, limits)
        .map_err(|_| ApiError::invalid_request())?;
    let object_id = (!object_id.is_empty())
        .then(|| openfga_domain::ObjectId::parse_with_limits(object_id, limits))
        .transpose()
        .map_err(|_| ApiError::invalid_request())?;
    let relation = (!value.relation.is_empty())
        .then(|| RelationName::parse_with_limits(&value.relation, limits))
        .transpose()
        .map_err(|_| ApiError::invalid_request())?;
    let subject = (!value.user.is_empty())
        .then(|| SubjectRef::parse_with_limits(&value.user, limits))
        .transpose()
        .map_err(|_| ApiError::invalid_request())?;
    TupleReadFilter::new(object_type, object_id, relation, subject)
        .map_err(|_| ApiError::invalid_request())
}

pub(crate) fn contextual_tuples(
    tuples: Option<pb::ContextualTupleKeys>,
    limits: &InputLimits,
) -> Result<ContextualTuples, ApiError> {
    let tuples = tuples
        .map_or_else(Vec::new, |value| value.tuple_keys)
        .into_iter()
        .map(|tuple| relationship_tuple(tuple, limits))
        .collect::<Result<Vec<_>, _>>()?;
    ContextualTuples::new(tuples, limits).map_err(|_| ApiError::invalid_request())
}

pub(crate) fn condition_context(
    context: Option<pbjson_types::Struct>,
    limits: &InputLimits,
) -> Result<ConditionContext, ApiError> {
    let value = context
        .map(|context| serde_json::to_value(context).map_err(|_| ApiError::invalid_request()))
        .transpose()?
        .unwrap_or_else(|| Value::Object(Map::new()));
    ConditionContext::try_from_json(value, limits).map_err(|_| ApiError::invalid_request())
}

pub(crate) fn model_definition(
    request: &pb::WriteAuthorizationModelRequest,
    limits: &InputLimits,
) -> Result<AuthorizationModelDefinition, ApiError> {
    if request.type_definitions.len() > limits.type_definitions() {
        return Err(ApiError::invalid_request());
    }
    let types = request
        .type_definitions
        .iter()
        .map(|definition| type_definition(definition, limits))
        .collect::<Result<Vec<_>, _>>()?;
    let mut condition_entries = request.conditions.iter().collect::<Vec<_>>();
    condition_entries.sort_by_key(|(key, _)| *key);
    let conditions = condition_entries
        .into_iter()
        .map(|(key, condition)| condition_source(key, condition, limits))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AuthorizationModelDefinition::new(
        request.schema_version.clone(),
        types,
        conditions,
    ))
}

fn type_definition(
    definition: &pb::TypeDefinition,
    limits: &InputLimits,
) -> Result<TypeDefinitionSource, ApiError> {
    if definition.relations.len() > limits.relations() {
        return Err(ApiError::invalid_request());
    }
    let name = TypeName::parse_with_limits(&definition.r#type, limits)
        .map_err(|_| ApiError::invalid_request())?;
    let mut relations = definition.relations.iter().collect::<Vec<_>>();
    relations.sort_by_key(|(key, _)| *key);
    let relations = relations
        .into_iter()
        .map(|(name, rewrite)| {
            let restrictions = definition
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.relations.get(name))
                .map_or(&[][..], |metadata| {
                    metadata.directly_related_user_types.as_slice()
                })
                .iter()
                .map(|restriction| direct_restriction(restriction, limits))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(RelationSource::new(
                RelationName::parse_with_limits(name, limits)
                    .map_err(|_| ApiError::invalid_request())?,
                rewrite_source(rewrite, limits, 0)?,
                restrictions,
            ))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(TypeDefinitionSource::new(name, relations))
}

fn rewrite_source(
    rewrite: &pb::Userset,
    limits: &InputLimits,
    depth: usize,
) -> Result<RewriteSource, ApiError> {
    if depth >= MAXIMUM_REWRITE_DEPTH {
        return Err(ApiError::invalid_request());
    }
    let userset = rewrite
        .userset
        .as_ref()
        .ok_or_else(ApiError::invalid_request)?;
    match userset {
        pb::userset::Userset::This(_) => Ok(RewriteSource::Direct),
        pb::userset::Userset::ComputedUserset(value) => Ok(RewriteSource::Computed(
            RelationName::parse_with_limits(&value.relation, limits)
                .map_err(|_| ApiError::invalid_request())?,
        )),
        pb::userset::Userset::TupleToUserset(value) => {
            let tupleset = value
                .tupleset
                .as_ref()
                .ok_or_else(ApiError::invalid_request)?;
            let computed = value
                .computed_userset
                .as_ref()
                .ok_or_else(ApiError::invalid_request)?;
            Ok(RewriteSource::TupleToUserset {
                tupleset: RelationName::parse_with_limits(&tupleset.relation, limits)
                    .map_err(|_| ApiError::invalid_request())?,
                computed: RelationName::parse_with_limits(&computed.relation, limits)
                    .map_err(|_| ApiError::invalid_request())?,
            })
        }
        pb::userset::Userset::Union(value) => {
            rewrite_children(&value.child, limits, depth).map(RewriteSource::Union)
        }
        pb::userset::Userset::Intersection(value) => {
            rewrite_children(&value.child, limits, depth).map(RewriteSource::Intersection)
        }
        pb::userset::Userset::Difference(value) => Ok(RewriteSource::Difference {
            base: Box::new(rewrite_source(
                value
                    .base
                    .as_deref()
                    .ok_or_else(ApiError::invalid_request)?,
                limits,
                depth + 1,
            )?),
            subtract: Box::new(rewrite_source(
                value
                    .subtract
                    .as_deref()
                    .ok_or_else(ApiError::invalid_request)?,
                limits,
                depth + 1,
            )?),
        }),
    }
}

fn rewrite_children(
    children: &[pb::Userset],
    limits: &InputLimits,
    depth: usize,
) -> Result<Vec<RewriteSource>, ApiError> {
    if children.is_empty() || children.len() > limits.operands() {
        return Err(ApiError::invalid_request());
    }
    children
        .iter()
        .map(|child| rewrite_source(child, limits, depth + 1))
        .collect()
}

fn direct_restriction(
    restriction: &pb::RelationReference,
    limits: &InputLimits,
) -> Result<DirectRestrictionSource, ApiError> {
    let subject_type = TypeName::parse_with_limits(&restriction.r#type, limits)
        .map_err(|_| ApiError::invalid_request())?;
    let kind = match restriction.relation_or_wildcard.as_ref() {
        None => RestrictionKindSource::Object,
        Some(pb::relation_reference::RelationOrWildcard::Relation(relation)) => {
            RestrictionKindSource::Userset(
                RelationName::parse_with_limits(relation, limits)
                    .map_err(|_| ApiError::invalid_request())?,
            )
        }
        Some(pb::relation_reference::RelationOrWildcard::Wildcard(_)) => {
            RestrictionKindSource::Wildcard
        }
    };
    let condition = (!restriction.condition.is_empty())
        .then(|| ConditionName::parse_with_limits(&restriction.condition, limits))
        .transpose()
        .map_err(|_| ApiError::invalid_request())?;
    Ok(DirectRestrictionSource::new(subject_type, kind, condition))
}

fn condition_source(
    key: &str,
    condition: &pb::Condition,
    limits: &InputLimits,
) -> Result<ConditionSource, ApiError> {
    let key =
        ConditionName::parse_with_limits(key, limits).map_err(|_| ApiError::invalid_request())?;
    let name = ConditionName::parse_with_limits(&condition.name, limits)
        .map_err(|_| ApiError::invalid_request())?;
    let parameters = condition
        .parameters
        .iter()
        .map(|(name, parameter)| {
            Ok((
                ParameterName::parse_with_limits(name, limits)
                    .map_err(|_| ApiError::invalid_request())?,
                parameter_type(parameter, 0)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, ApiError>>()?;
    Ok(ConditionSource::new(
        key,
        ConditionDefinition::new(name, condition.expression.clone(), parameters),
    ))
}

fn parameter_type(
    value: &pb::ConditionParamTypeRef,
    depth: usize,
) -> Result<ParameterType, ApiError> {
    if depth >= MAXIMUM_REWRITE_DEPTH {
        return Err(ApiError::invalid_request());
    }
    let wire_type = WireParameterTypeName::try_from(value.type_name)
        .map_err(|_| ApiError::invalid_request())?;
    let scalar = |parameter| {
        if value.generic_types.is_empty() {
            Ok(parameter)
        } else {
            Err(ApiError::invalid_request())
        }
    };
    match wire_type {
        WireParameterTypeName::Any => scalar(ParameterType::any()),
        WireParameterTypeName::Bool => scalar(ParameterType::bool()),
        WireParameterTypeName::String => scalar(ParameterType::string()),
        WireParameterTypeName::Int => scalar(ParameterType::int()),
        WireParameterTypeName::Uint => scalar(ParameterType::uint()),
        WireParameterTypeName::Double => scalar(ParameterType::double()),
        WireParameterTypeName::Duration => scalar(ParameterType::duration()),
        WireParameterTypeName::Timestamp => scalar(ParameterType::timestamp()),
        WireParameterTypeName::Ipaddress => scalar(ParameterType::ip_address()),
        WireParameterTypeName::List | WireParameterTypeName::Map => {
            let [generic] = value.generic_types.as_slice() else {
                return Err(ApiError::invalid_request());
            };
            let generic = parameter_type(generic, depth + 1)?;
            if wire_type == WireParameterTypeName::List {
                ParameterType::list(generic).map_err(|_| ApiError::invalid_request())
            } else {
                ParameterType::map(generic).map_err(|_| ApiError::invalid_request())
            }
        }
        WireParameterTypeName::Unspecified => Err(ApiError::invalid_request()),
    }
}

pub(crate) fn store(record: &StoreRecord) -> Result<pb::Store, ApiError> {
    Ok(pb::Store {
        id: record.id().to_string(),
        name: record.name().as_str().to_owned(),
        created_at: Some(timestamp(record.created_at())?),
        updated_at: Some(timestamp(record.updated_at())?),
        deleted_at: None,
    })
}

pub(crate) fn create_store_response(
    record: &StoreRecord,
) -> Result<pb::CreateStoreResponse, ApiError> {
    let value = store(record)?;
    Ok(pb::CreateStoreResponse {
        id: value.id,
        name: value.name,
        created_at: value.created_at,
        updated_at: value.updated_at,
    })
}

pub(crate) fn get_store_response(record: &StoreRecord) -> Result<pb::GetStoreResponse, ApiError> {
    let value = store(record)?;
    Ok(pb::GetStoreResponse {
        id: value.id,
        name: value.name,
        created_at: value.created_at,
        updated_at: value.updated_at,
        deleted_at: None,
    })
}

pub(crate) fn stored_tuple(value: &StoredTuple) -> Result<pb::Tuple, ApiError> {
    Ok(pb::Tuple {
        key: Some(wire_tuple_key(value.tuple())?),
        timestamp: Some(timestamp(value.inserted_at())?),
    })
}

pub(crate) fn tuple_change(value: &TupleChange) -> Result<pb::TupleChange, ApiError> {
    let operation = match value.operation() {
        ChangeOperation::Write => pb::TupleOperation::Write,
        ChangeOperation::Delete => pb::TupleOperation::Delete,
        _ => return Err(ApiError::internal()),
    };
    Ok(pb::TupleChange {
        tuple_key: Some(wire_tuple_key(value.tuple())?),
        operation: operation as i32,
        timestamp: Some(timestamp(value.timestamp())?),
    })
}

pub(crate) fn wire_tuple_key(value: &RelationshipTuple) -> Result<pb::TupleKey, ApiError> {
    let condition = value
        .condition()
        .binding()
        .map(|binding| {
            Ok::<_, ApiError>(pb::RelationshipCondition {
                name: binding.name().to_string(),
                context: Some(wire_context(binding.context())?),
            })
        })
        .transpose()?;
    Ok(pb::TupleKey {
        user: value.key().subject().to_string(),
        relation: value.key().relation().to_string(),
        object: value.key().object().to_string(),
        condition,
    })
}

pub(crate) fn assertion(value: &Assertion) -> Result<pb::Assertion, ApiError> {
    Ok(pb::Assertion {
        tuple_key: Some(pb::AssertionTupleKey {
            object: value.tuple().object().to_string(),
            relation: value.tuple().relation().to_string(),
            user: value.tuple().subject().to_string(),
        }),
        expectation: value.expectation(),
        contextual_tuples: value
            .contextual_tuples()
            .as_slice()
            .iter()
            .map(wire_tuple_key)
            .collect::<Result<_, _>>()?,
        context: Some(wire_context(value.condition_context())?),
    })
}

pub(crate) fn domain_assertion(
    value: pb::Assertion,
    limits: &InputLimits,
) -> Result<Assertion, ApiError> {
    let tuple = value.tuple_key.ok_or_else(ApiError::missing_tuple_key)?;
    let contextual = value
        .contextual_tuples
        .into_iter()
        .map(|tuple| relationship_tuple(tuple, limits))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Assertion::new(
        tuple_key(&tuple.object, &tuple.relation, &tuple.user, limits)?,
        value.expectation,
        ContextualTuples::new(contextual, limits).map_err(|_| ApiError::invalid_request())?,
        condition_context(value.context, limits)?,
    ))
}

pub(crate) fn authorization_model(
    value: &StoredAuthorizationModel,
) -> Result<pb::AuthorizationModel, ApiError> {
    let source = value.source();
    Ok(pb::AuthorizationModel {
        id: value.model_id().to_string(),
        schema_version: source.schema_version().to_owned(),
        type_definitions: source
            .type_definitions()
            .iter()
            .map(wire_type_definition)
            .collect::<Result<_, _>>()?,
        conditions: source
            .conditions()
            .iter()
            .map(|condition| {
                wire_condition(condition).map(|value| (condition.key().to_string(), value))
            })
            .collect::<Result<HashMap<_, _>, _>>()?,
    })
}

fn wire_type_definition(value: &TypeDefinitionSource) -> Result<pb::TypeDefinition, ApiError> {
    let relations = value
        .relations()
        .iter()
        .map(|relation| {
            wire_rewrite(relation.rewrite(), 0)
                .map(|rewrite| (relation.name().to_string(), rewrite))
        })
        .collect::<Result<HashMap<_, _>, _>>()?;
    let relation_metadata = value
        .relations()
        .iter()
        .map(|relation| {
            let restrictions = relation
                .restrictions()
                .iter()
                .map(wire_restriction)
                .collect();
            (
                relation.name().to_string(),
                pb::RelationMetadata {
                    directly_related_user_types: restrictions,
                    module: String::new(),
                    source_info: None,
                },
            )
        })
        .collect::<HashMap<_, _>>();
    Ok(pb::TypeDefinition {
        r#type: value.name().to_string(),
        relations,
        metadata: Some(pb::Metadata {
            relations: relation_metadata,
            module: String::new(),
            source_info: None,
        }),
    })
}

fn wire_rewrite(value: &RewriteSource, depth: usize) -> Result<pb::Userset, ApiError> {
    if depth >= MAXIMUM_REWRITE_DEPTH {
        return Err(ApiError::internal());
    }
    let userset = match value.as_ref() {
        RewriteSourceRef::Direct => pb::userset::Userset::This(pb::DirectUserset {}),
        RewriteSourceRef::Computed(relation) => {
            pb::userset::Userset::ComputedUserset(pb::ObjectRelation {
                object: String::new(),
                relation: relation.to_string(),
            })
        }
        RewriteSourceRef::TupleToUserset { tupleset, computed } => {
            pb::userset::Userset::TupleToUserset(pb::TupleToUserset {
                tupleset: Some(pb::ObjectRelation {
                    object: String::new(),
                    relation: tupleset.to_string(),
                }),
                computed_userset: Some(pb::ObjectRelation {
                    object: String::new(),
                    relation: computed.to_string(),
                }),
            })
        }
        RewriteSourceRef::Union(children) => pb::userset::Userset::Union(pb::Usersets {
            child: children
                .iter()
                .map(|child| wire_rewrite(child, depth + 1))
                .collect::<Result<_, _>>()?,
        }),
        RewriteSourceRef::Intersection(children) => {
            pb::userset::Userset::Intersection(pb::Usersets {
                child: children
                    .iter()
                    .map(|child| wire_rewrite(child, depth + 1))
                    .collect::<Result<_, _>>()?,
            })
        }
        RewriteSourceRef::Difference { base, subtract } => {
            pb::userset::Userset::Difference(Box::new(pb::Difference {
                base: Some(Box::new(wire_rewrite(base, depth + 1)?)),
                subtract: Some(Box::new(wire_rewrite(subtract, depth + 1)?)),
            }))
        }
    };
    Ok(pb::Userset {
        userset: Some(userset),
    })
}

fn wire_restriction(value: &DirectRestrictionSource) -> pb::RelationReference {
    let relation_or_wildcard = match value.kind().as_ref() {
        RestrictionKindSourceRef::Object => None,
        RestrictionKindSourceRef::Userset(relation) => Some(
            pb::relation_reference::RelationOrWildcard::Relation(relation.to_string()),
        ),
        RestrictionKindSourceRef::Wildcard => Some(
            pb::relation_reference::RelationOrWildcard::Wildcard(pb::Wildcard {}),
        ),
    };
    pb::RelationReference {
        r#type: value.subject_type().to_string(),
        condition: value
            .condition()
            .map_or_else(String::new, ToString::to_string),
        relation_or_wildcard,
    }
}

fn wire_condition(value: &ConditionSource) -> Result<pb::Condition, ApiError> {
    Ok(pb::Condition {
        name: value.definition().name().to_string(),
        expression: value.definition().expression().to_owned(),
        parameters: value
            .definition()
            .parameters()
            .iter()
            .map(|(name, value)| {
                wire_parameter_type(value, 0).map(|value| (name.to_string(), value))
            })
            .collect::<Result<_, _>>()?,
        metadata: None,
    })
}

fn wire_parameter_type(
    value: &ParameterType,
    depth: usize,
) -> Result<pb::ConditionParamTypeRef, ApiError> {
    if depth >= MAXIMUM_REWRITE_DEPTH {
        return Err(ApiError::internal());
    }
    let (name, generic_types) = match value.as_ref() {
        ParameterTypeRef::Any => (WireParameterTypeName::Any, Vec::new()),
        ParameterTypeRef::Bool => (WireParameterTypeName::Bool, Vec::new()),
        ParameterTypeRef::String => (WireParameterTypeName::String, Vec::new()),
        ParameterTypeRef::Int => (WireParameterTypeName::Int, Vec::new()),
        ParameterTypeRef::Uint => (WireParameterTypeName::Uint, Vec::new()),
        ParameterTypeRef::Double => (WireParameterTypeName::Double, Vec::new()),
        ParameterTypeRef::Duration => (WireParameterTypeName::Duration, Vec::new()),
        ParameterTypeRef::Timestamp => (WireParameterTypeName::Timestamp, Vec::new()),
        ParameterTypeRef::IpAddress => (WireParameterTypeName::Ipaddress, Vec::new()),
        ParameterTypeRef::List(child) => (
            WireParameterTypeName::List,
            vec![wire_parameter_type(child, depth + 1)?],
        ),
        ParameterTypeRef::Map(child) => (
            WireParameterTypeName::Map,
            vec![wire_parameter_type(child, depth + 1)?],
        ),
        ParameterTypeRef::Bytes => return Err(ApiError::internal()),
    };
    Ok(pb::ConditionParamTypeRef {
        type_name: name as i32,
        generic_types,
    })
}

pub(crate) fn system_time(value: &pbjson_types::Timestamp) -> Result<SystemTime, ApiError> {
    if value.seconds < 0 || !(0..1_000_000_000).contains(&value.nanos) {
        return Err(ApiError::invalid_request());
    }
    UNIX_EPOCH
        .checked_add(Duration::new(
            u64::try_from(value.seconds).map_err(|_| ApiError::invalid_request())?,
            u32::try_from(value.nanos).map_err(|_| ApiError::invalid_request())?,
        ))
        .ok_or_else(ApiError::invalid_request)
}

fn timestamp(value: SystemTime) -> Result<pbjson_types::Timestamp, ApiError> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ApiError::internal())?;
    Ok(pbjson_types::Timestamp {
        seconds: i64::try_from(duration.as_secs()).map_err(|_| ApiError::internal())?,
        nanos: i32::try_from(duration.subsec_nanos()).map_err(|_| ApiError::internal())?,
    })
}

fn wire_context(value: &ConditionContext) -> Result<pbjson_types::Struct, ApiError> {
    serde_json::from_value(Value::Object(
        value
            .iter()
            .map(|(name, value)| context_value(value).map(|value| (name.to_string(), value)))
            .collect::<Result<Map<_, _>, _>>()?,
    ))
    .map_err(|_| ApiError::internal())
}

fn context_value(value: &ContextValue) -> Result<Value, ApiError> {
    match value {
        ContextValue::Null => Ok(Value::Null),
        ContextValue::Bool(value) => Ok(Value::Bool(*value)),
        ContextValue::Int(value) => Ok(Value::Number(Number::from(*value))),
        ContextValue::Uint(value) => Ok(Value::Number(Number::from(*value))),
        ContextValue::Double(value) => Number::from_f64(value.get())
            .map(Value::Number)
            .ok_or_else(ApiError::internal),
        ContextValue::String(value) => Ok(Value::String(value.as_str().to_owned())),
        ContextValue::Bytes(value) => Ok(Value::Array(
            value
                .as_slice()
                .iter()
                .copied()
                .map(Number::from)
                .map(Value::Number)
                .collect(),
        )),
        ContextValue::List(value) => value
            .as_slice()
            .iter()
            .map(context_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        ContextValue::Map(value) => value
            .iter()
            .map(|(key, value)| context_value(value).map(|value| (key.as_str().to_owned(), value)))
            .collect::<Result<Map<_, _>, _>>()
            .map(Value::Object),
        _ => Err(ApiError::internal()),
    }
}

pub(crate) fn model_filter_fingerprint() -> openfga_domain::Fingerprint {
    openfga_domain::FingerprintBuilder::new("openfga.read-authorization-models-filter.v1").finish()
}

pub(crate) fn store_filter_fingerprint(name: Option<&str>) -> openfga_domain::Fingerprint {
    let mut builder = openfga_domain::FingerprintBuilder::new("openfga.list-stores-filter.v1");
    builder.write_str(name.unwrap_or_default());
    builder.finish()
}

pub(crate) fn tuple_filter_fingerprint(filter: &TupleReadFilter) -> openfga_domain::Fingerprint {
    let mut builder = openfga_domain::FingerprintBuilder::new("openfga.read-tuples-filter.v1");
    builder.write_str(filter.object_type().map_or("", TypeName::as_str));
    builder.write_str(
        filter
            .object_id()
            .map_or("", openfga_domain::ObjectId::as_str),
    );
    builder.write_str(filter.relation().map_or("", RelationName::as_str));
    builder.write_str(
        &filter
            .subject()
            .map_or_else(String::new, ToString::to_string),
    );
    builder.finish()
}

pub(crate) fn change_filter_fingerprint(
    object_type: Option<&TypeName>,
    start_time: Option<SystemTime>,
) -> Result<openfga_domain::Fingerprint, ApiError> {
    let mut builder = openfga_domain::FingerprintBuilder::new("openfga.read-changes-filter.v1");
    builder.write_str(object_type.map_or("", TypeName::as_str));
    match start_time {
        Some(value) => {
            let duration = value
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ApiError::invalid_request())?;
            builder.write_u64(duration.as_secs());
            builder.write_u32(duration.subsec_nanos());
        }
        None => builder.write_tag(0),
    }
    Ok(builder.finish())
}
