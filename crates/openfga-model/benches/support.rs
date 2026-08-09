//! Shared multi-limit authorization-model fixture for performance gates.

use std::{collections::BTreeMap, error::Error};

use openfga_condition::{ConditionDefinition, ParameterType};
use openfga_domain::{
    AuthorizationModelId, ConditionName, ParameterName, RelationName, StoreId, TypeName,
};
use openfga_model::{
    AuthorizationModelSource, ConditionSource, DirectRestrictionSource, RelationSource,
    RestrictionKindSource, RewriteSource, TypeDefinitionSource,
};

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
const MAXIMUM_TYPES: usize = 100;
const MAXIMUM_RELATIONS: usize = 100;
const MAXIMUM_REWRITE_NODES: usize = 10_000;
const MAXIMUM_CONDITIONS: usize = 100;

pub(crate) fn maximum_supported_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    let conditions = (0..MAXIMUM_CONDITIONS)
        .map(condition)
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let operands_per_relation = MAXIMUM_REWRITE_NODES
        .checked_div(MAXIMUM_RELATIONS)
        .and_then(|nodes| nodes.checked_sub(1))
        .ok_or("maximum rewrite-node fixture is invalid")?;
    let relations = (0..MAXIMUM_RELATIONS)
        .map(|index| relation(index, operands_per_relation))
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    let mut definitions = Vec::with_capacity(MAXIMUM_TYPES);
    definitions.push(TypeDefinitionSource::new(
        "user".parse::<TypeName>()?,
        Vec::new(),
    ));
    definitions.push(TypeDefinitionSource::new(
        "document".parse::<TypeName>()?,
        relations,
    ));
    for index in 0..MAXIMUM_TYPES.saturating_sub(2) {
        definitions.push(TypeDefinitionSource::new(
            format!("resource_{index}").parse::<TypeName>()?,
            Vec::new(),
        ));
    }
    Ok(AuthorizationModelSource::new(
        STORE_ID.parse::<StoreId>()?,
        MODEL_ID.parse::<AuthorizationModelId>()?,
        "1.1".to_owned(),
        definitions,
        conditions,
    ))
}

fn condition(index: usize) -> Result<ConditionSource, Box<dyn Error>> {
    let name = format!("condition_{index}").parse::<ConditionName>()?;
    let parameters = BTreeMap::from([("x".parse::<ParameterName>()?, ParameterType::int())]);
    Ok(ConditionSource::new(
        name.clone(),
        ConditionDefinition::new(name, "x == 1".to_owned(), parameters),
    ))
}

fn relation(index: usize, operands: usize) -> Result<RelationSource, Box<dyn Error>> {
    let condition = format!("condition_{index}").parse::<ConditionName>()?;
    Ok(RelationSource::new(
        format!("relation_{index}").parse::<RelationName>()?,
        RewriteSource::Union(
            std::iter::repeat_with(|| RewriteSource::Direct)
                .take(operands)
                .collect(),
        ),
        vec![DirectRestrictionSource::new(
            "user".parse::<TypeName>()?,
            RestrictionKindSource::Object,
            Some(condition),
        )],
    ))
}
