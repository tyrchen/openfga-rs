//! Authorization-model compiler integration, property, and boundary tests.

use std::{collections::BTreeMap, error::Error};

use openfga_condition::{ConditionDefinition, ParameterType};
use openfga_domain::{
    AuthorizationModelId, ConditionName, Limit, ParameterName, RelationName, StoreId, TypeName,
};
use openfga_model::{
    AuthorizationModelSource, ConditionRequirement, ConditionSource, DirectRestrictionSource,
    ModelCompiler, ModelErrorCode, ModelLimits, RelationSource, RestrictionKind,
    RestrictionKindSource, RewriteNode, RewriteSource, TypeDefinitionSource,
};
use proptest::prelude::*;

const STORE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const MODEL_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

#[test]
fn test_should_compile_all_rewrites_conditions_and_graph_metadata() -> Result<(), Box<dyn Error>> {
    let source = valid_model()?;
    let debug = format!("{source:?}");
    assert!(!debug.contains("region =="));

    let compiled = ModelCompiler::default().compile(&source)?;
    let user = compiled.type_id(&type_name("user")?)?;
    let group = compiled.type_id(&type_name("group")?)?;
    let editor = compiled.relation_id(&type_name("document")?, &relation_name("editor")?)?;
    let viewer = compiled.relation_id(&type_name("document")?, &relation_name("viewer")?)?;
    let parent = compiled.relation_id(&type_name("document")?, &relation_name("parent")?)?;
    let member = compiled.relation_id(&type_name("group")?, &relation_name("member")?)?;

    assert!(compiled.can_reach_subject_type(viewer, user));
    assert!(compiled.can_reach_subject_type(viewer, group));
    assert!(compiled.can_reach_wildcard(viewer, user));
    assert_eq!(compiled.recursion_group(member), Some(0));
    assert!(compiled.reverse_relations(parent)?.contains(&viewer));

    let viewer_relation = compiled.relation(viewer)?;
    assert!(matches!(
        compiled.node(viewer_relation.root())?,
        RewriteNode::Union(children) if children.len() == 3
    ));
    let editor_relation = compiled.relation(editor)?;
    assert!(editor_relation.restrictions().iter().any(|restriction| {
        restriction.kind() == RestrictionKind::Userset(member)
            && matches!(restriction.condition(), ConditionRequirement::Required(_))
    }));
    assert_eq!(compiled.compiler_format_version(), 1);
    assert_eq!(compiled.schema_version(), "1.1");
    assert_eq!(compiled.store_id().to_string(), STORE_ID);
    assert_eq!(compiled.model_id().to_string(), MODEL_ID);
    assert!(!format!("{compiled:?}").contains("region =="));
    Ok(())
}

#[test]
fn test_should_be_deterministic_for_identical_ordered_source() -> Result<(), Box<dyn Error>> {
    let source = valid_model()?;
    let first = ModelCompiler::default().compile(&source)?;
    let second = ModelCompiler::default().compile(&source)?;

    assert_eq!(first.fingerprint(), second.fingerprint());
    assert_eq!(
        first.relation_id(&type_name("document")?, &relation_name("viewer")?)?,
        second.relation_id(&type_name("document")?, &relation_name("viewer")?)?,
    );
    Ok(())
}

#[test]
fn test_should_report_semantic_failures_with_stable_codes() -> Result<(), Box<dyn Error>> {
    let cases = vec![
        (
            model_with_relation(
                "viewer",
                RewriteSource::Direct,
                Vec::new(),
                vec![empty_type("user")?],
            )?,
            ModelErrorCode::AssignableWithoutRestrictions,
        ),
        (
            model_with_relation(
                "viewer",
                computed("viewer")?,
                Vec::new(),
                vec![empty_type("user")?],
            )?,
            ModelErrorCode::IllegalSelfReference,
        ),
        (
            model_with_relation(
                "viewer",
                computed("missing")?,
                Vec::new(),
                vec![empty_type("user")?],
            )?,
            ModelErrorCode::UndefinedRelation,
        ),
        (
            model_with_relation(
                "viewer",
                RewriteSource::Union(vec![RewriteSource::Direct]),
                vec![object_restriction("user", None)?],
                vec![empty_type("user")?],
            )?,
            ModelErrorCode::InvalidOperatorArity,
        ),
        (
            nonassignable_with_restriction_model()?,
            ModelErrorCode::NonAssignableWithRestrictions,
        ),
        (
            computed_cycle_model()?,
            ModelErrorCode::ForbiddenComputedCycle,
        ),
        (no_entrypoint_model()?, ModelErrorCode::NoEntrypoints),
        (
            invalid_tupleset_model()?,
            ModelErrorCode::InvalidTuplesetRelation,
        ),
        (
            invalid_ttu_target_model()?,
            ModelErrorCode::InvalidTupleToUsersetTarget,
        ),
        (
            undefined_condition_model()?,
            ModelErrorCode::UndefinedCondition,
        ),
        (invalid_condition_model()?, ModelErrorCode::InvalidCondition),
    ];

    for (model, expected) in cases {
        let codes = compile_error_codes(&model)?;
        assert!(
            codes.contains(&expected),
            "missing {expected:?} in {codes:?}"
        );
    }
    Ok(())
}

#[test]
fn test_should_bound_depth_width_and_diagnostics() -> Result<(), Box<dyn Error>> {
    let mut deep = RewriteSource::Direct;
    for _ in 0..70 {
        deep = RewriteSource::Difference {
            base: Box::new(deep),
            subtract: Box::new(RewriteSource::Direct),
        };
    }
    let deep_model = model_with_relation(
        "viewer",
        deep,
        vec![object_restriction("user", None)?],
        vec![empty_type("user")?],
    )?;
    assert!(compile_error_codes(&deep_model)?.contains(&ModelErrorCode::RewriteLimitExceeded));

    let wide_model = model_with_relation(
        "viewer",
        RewriteSource::Union((0..101).map(|_| RewriteSource::Direct).collect()),
        vec![object_restriction("user", None)?],
        vec![empty_type("user")?],
    )?;
    assert!(compile_error_codes(&wide_model)?.contains(&ModelErrorCode::RewriteLimitExceeded));

    let invalid = source_model(
        "1.1",
        vec![empty_type("this")?, empty_type("this")?],
        Vec::new(),
    )?;
    let Err(errors) = ModelCompiler::default().compile(&invalid) else {
        return Err("invalid model unexpectedly compiled".into());
    };
    assert_eq!(errors.errors().len(), 3);
    assert!(
        errors
            .errors()
            .windows(2)
            .all(|pair| matches!(pair, [left, right] if left.path() <= right.path()))
    );
    Ok(())
}

#[test]
fn test_should_reject_and_drop_adversarial_depth_without_stack_overflow()
-> Result<(), Box<dyn Error>> {
    let mut rewrite = RewriteSource::Direct;
    for _ in 0..50_000 {
        rewrite = RewriteSource::Difference {
            base: Box::new(rewrite),
            subtract: Box::new(RewriteSource::Direct),
        };
    }
    let source = model_with_relation(
        "viewer",
        rewrite,
        vec![object_restriction("user", None)?],
        vec![empty_type("user")?],
    )?;

    assert!(
        compile_error_codes(&source)?.contains(&ModelErrorCode::RewriteLimitExceeded),
        "adversarial rewrite depth must be rejected deterministically",
    );
    drop(source);
    Ok(())
}

#[test]
fn test_should_require_ttu_target_relation_on_every_permitted_type() -> Result<(), Box<dyn Error>> {
    let source = source_model(
        "1.1",
        vec![
            type_source(
                "folder",
                vec![relation_source(
                    "viewer",
                    RewriteSource::Direct,
                    vec![object_restriction("folder", None)?],
                )?],
            )?,
            empty_type("portfolio")?,
            type_source(
                "document",
                vec![
                    relation_source(
                        "parent",
                        RewriteSource::Direct,
                        vec![
                            object_restriction("folder", None)?,
                            object_restriction("portfolio", None)?,
                        ],
                    )?,
                    relation_source("viewer", ttu("parent", "viewer")?, Vec::new())?,
                ],
            )?,
        ],
        Vec::new(),
    )?;

    assert!(compile_error_codes(&source)?.contains(&ModelErrorCode::InvalidTupleToUsersetTarget),);
    Ok(())
}

#[test]
fn test_should_accept_computed_cycle_with_concrete_entrypoints() -> Result<(), Box<dyn Error>> {
    let source = source_model(
        "1.1",
        vec![
            empty_type("user")?,
            type_source(
                "account",
                vec![
                    relation_source(
                        "admin",
                        RewriteSource::Union(vec![RewriteSource::Direct, computed("member")?]),
                        vec![object_restriction("user", None)?],
                    )?,
                    relation_source(
                        "member",
                        RewriteSource::Union(vec![RewriteSource::Direct, computed("admin")?]),
                        vec![object_restriction("user", None)?],
                    )?,
                ],
            )?,
        ],
        Vec::new(),
    )?;

    let model = ModelCompiler::default().compile(&source)?;
    let admin = model.relation_id(&type_name("account")?, &relation_name("admin")?)?;
    let member = model.relation_id(&type_name("account")?, &relation_name("member")?)?;
    assert_eq!(model.recursion_group(admin), model.recursion_group(member));
    assert!(model.recursion_group(admin).is_some());
    Ok(())
}

#[test]
fn test_should_report_error_cap_exhaustion_explicitly() -> Result<(), Box<dyn Error>> {
    let source = source_model(
        "invalid",
        vec![empty_type("this")?, empty_type("this")?],
        Vec::new(),
    )?;
    let limits = ModelLimits::builder()
        .model_errors(Limit::<1_000>::new(2)?)
        .build();
    let Err(errors) = ModelCompiler::new(limits).compile(&source) else {
        return Err("invalid model unexpectedly compiled".into());
    };

    assert!(errors.is_truncated());
    assert!(
        errors
            .errors()
            .iter()
            .any(|error| error.code() == ModelErrorCode::TooManyModelErrors),
    );
    Ok(())
}

#[test]
fn test_should_reject_schema_condition_mismatch_and_redact_large_schema()
-> Result<(), Box<dyn Error>> {
    let condition = condition_source("key", "name", "true", BTreeMap::new())?;
    let source = source_model(
        &"sensitive-schema".repeat(64),
        vec![empty_type("user")?],
        vec![condition],
    )?;
    let debug = format!("{source:?}");
    assert!(!debug.contains("sensitive-schema"));
    let codes = compile_error_codes(&source)?;
    assert!(codes.contains(&ModelErrorCode::InvalidSchemaVersion));
    assert!(codes.contains(&ModelErrorCode::ConditionNameMismatch));
    Ok(())
}

proptest! {
    #[test]
    fn test_should_compile_generated_computed_chains_without_nondeterminism(length in 1_usize..32) {
        let source = generated_chain(length);
        prop_assert!(source.is_ok());
        if let Ok(source) = source {
            let first = ModelCompiler::default().compile(&source);
            let second = ModelCompiler::default().compile(&source);
            prop_assert!(first.is_ok());
            prop_assert!(second.is_ok());
            if let (Ok(first), Ok(second)) = (first, second) {
                prop_assert_eq!(first.fingerprint(), second.fingerprint());
            }
        }
    }
}

fn generated_chain(length: usize) -> Result<AuthorizationModelSource, Box<dyn Error>> {
    let mut relations = Vec::with_capacity(length.saturating_add(1));
    relations.push(relation_source(
        "r0",
        RewriteSource::Direct,
        vec![object_restriction("user", None)?],
    )?);
    for index in 1..=length {
        relations.push(relation_source(
            &format!("r{index}"),
            computed(&format!("r{}", index.saturating_sub(1)))?,
            Vec::new(),
        )?);
    }
    source_model(
        "1.1",
        vec![empty_type("user")?, type_source("document", relations)?],
        Vec::new(),
    )
}

fn valid_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    let user = empty_type("user")?;
    let group = type_source(
        "group",
        vec![relation_source(
            "member",
            RewriteSource::Direct,
            vec![
                object_restriction("user", None)?,
                userset_restriction("group", "member", None)?,
            ],
        )?],
    )?;
    let folder = type_source(
        "folder",
        vec![relation_source(
            "viewer",
            RewriteSource::Direct,
            vec![object_restriction("user", None)?],
        )?],
    )?;
    let document = type_source(
        "document",
        vec![
            relation_source(
                "parent",
                RewriteSource::Direct,
                vec![object_restriction("folder", None)?],
            )?,
            relation_source(
                "editor",
                RewriteSource::Direct,
                vec![
                    object_restriction("user", None)?,
                    userset_restriction("group", "member", Some("in_region"))?,
                ],
            )?,
            relation_source(
                "viewer",
                RewriteSource::Union(vec![
                    RewriteSource::Direct,
                    computed("editor")?,
                    ttu("parent", "viewer")?,
                ]),
                vec![wildcard_restriction("user")?],
            )?,
            relation_source(
                "restricted",
                RewriteSource::Direct,
                vec![object_restriction("user", None)?],
            )?,
            relation_source(
                "can_view",
                RewriteSource::Difference {
                    base: Box::new(computed("viewer")?),
                    subtract: Box::new(computed("restricted")?),
                },
                Vec::new(),
            )?,
            relation_source(
                "can_edit",
                RewriteSource::Intersection(vec![computed("editor")?, computed("viewer")?]),
                Vec::new(),
            )?,
        ],
    )?;
    let parameters =
        BTreeMap::from([("region".parse::<ParameterName>()?, ParameterType::string())]);
    source_model(
        "1.1",
        vec![user, group, folder, document],
        vec![condition_source(
            "in_region",
            "in_region",
            "region == 'us'",
            parameters,
        )?],
    )
}

fn model_with_relation(
    name: &str,
    rewrite: RewriteSource,
    restrictions: Vec<DirectRestrictionSource>,
    mut other_types: Vec<TypeDefinitionSource>,
) -> Result<AuthorizationModelSource, Box<dyn Error>> {
    other_types.push(type_source(
        "document",
        vec![relation_source(name, rewrite, restrictions)?],
    )?);
    source_model("1.1", other_types, Vec::new())
}

fn nonassignable_with_restriction_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    source_model(
        "1.1",
        vec![
            empty_type("user")?,
            type_source(
                "document",
                vec![
                    relation_source(
                        "editor",
                        RewriteSource::Direct,
                        vec![object_restriction("user", None)?],
                    )?,
                    relation_source(
                        "viewer",
                        computed("editor")?,
                        vec![object_restriction("user", None)?],
                    )?,
                ],
            )?,
        ],
        Vec::new(),
    )
}

fn computed_cycle_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    source_model(
        "1.1",
        vec![type_source(
            "document",
            vec![
                relation_source("a", computed("b")?, Vec::new())?,
                relation_source("b", computed("a")?, Vec::new())?,
            ],
        )?],
        Vec::new(),
    )
}

fn no_entrypoint_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    model_with_relation(
        "viewer",
        RewriteSource::Direct,
        vec![userset_restriction("document", "viewer", None)?],
        Vec::new(),
    )
}

fn invalid_tupleset_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    source_model(
        "1.1",
        vec![
            empty_type("user")?,
            type_source(
                "folder",
                vec![relation_source(
                    "viewer",
                    RewriteSource::Direct,
                    vec![object_restriction("user", None)?],
                )?],
            )?,
            type_source(
                "document",
                vec![
                    relation_source(
                        "parent",
                        RewriteSource::Union(vec![RewriteSource::Direct, RewriteSource::Direct]),
                        vec![object_restriction("folder", None)?],
                    )?,
                    relation_source("viewer", ttu("parent", "viewer")?, Vec::new())?,
                ],
            )?,
        ],
        Vec::new(),
    )
}

fn invalid_ttu_target_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    source_model(
        "1.1",
        vec![
            empty_type("folder")?,
            type_source(
                "document",
                vec![
                    relation_source(
                        "parent",
                        RewriteSource::Direct,
                        vec![object_restriction("folder", None)?],
                    )?,
                    relation_source("viewer", ttu("parent", "viewer")?, Vec::new())?,
                ],
            )?,
        ],
        Vec::new(),
    )
}

fn undefined_condition_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    model_with_relation(
        "viewer",
        RewriteSource::Direct,
        vec![object_restriction("user", Some("missing"))?],
        vec![empty_type("user")?],
    )
}

fn invalid_condition_model() -> Result<AuthorizationModelSource, Box<dyn Error>> {
    let user = empty_type("user")?;
    let document = type_source(
        "document",
        vec![relation_source(
            "viewer",
            RewriteSource::Direct,
            vec![object_restriction("user", Some("broken"))?],
        )?],
    )?;
    source_model(
        "1.1",
        vec![user, document],
        vec![condition_source(
            "broken",
            "broken",
            "missing == true",
            BTreeMap::new(),
        )?],
    )
}

fn source_model(
    schema: &str,
    types: Vec<TypeDefinitionSource>,
    conditions: Vec<ConditionSource>,
) -> Result<AuthorizationModelSource, Box<dyn Error>> {
    Ok(AuthorizationModelSource::new(
        store_id()?,
        model_id()?,
        schema.to_owned(),
        types,
        conditions,
    ))
}

fn empty_type(name: &str) -> Result<TypeDefinitionSource, Box<dyn Error>> {
    type_source(name, Vec::new())
}

fn type_source(
    name: &str,
    relations: Vec<RelationSource>,
) -> Result<TypeDefinitionSource, Box<dyn Error>> {
    Ok(TypeDefinitionSource::new(type_name(name)?, relations))
}

fn relation_source(
    name: &str,
    rewrite: RewriteSource,
    restrictions: Vec<DirectRestrictionSource>,
) -> Result<RelationSource, Box<dyn Error>> {
    Ok(RelationSource::new(
        relation_name(name)?,
        rewrite,
        restrictions,
    ))
}

fn condition_source(
    key: &str,
    name: &str,
    expression: &str,
    parameters: BTreeMap<ParameterName, ParameterType>,
) -> Result<ConditionSource, Box<dyn Error>> {
    Ok(ConditionSource::new(
        key.parse::<ConditionName>()?,
        ConditionDefinition::new(
            name.parse::<ConditionName>()?,
            expression.to_owned(),
            parameters,
        ),
    ))
}

fn object_restriction(
    subject_type: &str,
    condition: Option<&str>,
) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    restriction(subject_type, RestrictionKindSource::Object, condition)
}

fn userset_restriction(
    subject_type: &str,
    relation: &str,
    condition: Option<&str>,
) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    restriction(
        subject_type,
        RestrictionKindSource::Userset(relation_name(relation)?),
        condition,
    )
}

fn wildcard_restriction(subject_type: &str) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    restriction(subject_type, RestrictionKindSource::Wildcard, None)
}

fn restriction(
    subject_type: &str,
    kind: RestrictionKindSource,
    condition: Option<&str>,
) -> Result<DirectRestrictionSource, Box<dyn Error>> {
    let condition = condition.map(str::parse::<ConditionName>).transpose()?;
    Ok(DirectRestrictionSource::new(
        type_name(subject_type)?,
        kind,
        condition,
    ))
}

fn computed(relation: &str) -> Result<RewriteSource, Box<dyn Error>> {
    Ok(RewriteSource::Computed(relation_name(relation)?))
}

fn ttu(tupleset: &str, computed: &str) -> Result<RewriteSource, Box<dyn Error>> {
    Ok(RewriteSource::TupleToUserset {
        tupleset: relation_name(tupleset)?,
        computed: relation_name(computed)?,
    })
}

fn compile_error_codes(
    model: &AuthorizationModelSource,
) -> Result<Vec<ModelErrorCode>, Box<dyn Error>> {
    match ModelCompiler::default().compile(model) {
        Ok(_) => Err("invalid model unexpectedly compiled".into()),
        Err(errors) => Ok(errors
            .errors()
            .iter()
            .map(openfga_model::ModelError::code)
            .collect()),
    }
}

fn store_id() -> Result<StoreId, Box<dyn Error>> {
    STORE_ID.parse().map_err(Into::into)
}

fn model_id() -> Result<AuthorizationModelId, Box<dyn Error>> {
    MODEL_ID.parse().map_err(Into::into)
}

fn type_name(value: &str) -> Result<TypeName, Box<dyn Error>> {
    value.parse().map_err(Into::into)
}

fn relation_name(value: &str) -> Result<RelationName, Box<dyn Error>> {
    value.parse().map_err(Into::into)
}
