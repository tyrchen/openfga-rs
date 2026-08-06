//! Executable `OpenFGA` condition compatibility and resource-bound matrix.

use std::{collections::BTreeMap, error::Error};

use openfga_condition::{
    CancellationToken, CompileErrorKind, ConditionCompiler, ConditionDefinition, ConditionLimits,
    EvaluationBudget, EvaluationErrorKind, ParameterType,
};
use openfga_domain::{ConditionContext, ConditionName, InputLimits, Limit, ParameterName};
use proptest::proptest;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    cases: Vec<FixtureCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureCase {
    name: String,
    expression: String,
    expected_baseline: Outcome,
    expected_rust: Outcome,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Outcome {
    kind: OutcomeKind,
    value: Option<bool>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
enum OutcomeKind {
    Bool,
    Error,
}

#[test]
fn test_should_match_shared_openfga_literal_matrix() -> Result<(), Box<dyn Error>> {
    let fixture: Fixture = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/cel-conformance/cases.json"
    )))?;
    assert!(!fixture.cases.is_empty() && fixture.cases.len() <= 64);
    for case in fixture.cases {
        assert!(!case.name.is_empty() && case.expression.len() <= 4_096);
        let definition = definition(&case.name, &case.expression, BTreeMap::new())?;
        assert_eq!(case.expected_rust, case.expected_baseline);
        let actual =
            match ConditionCompiler::default().compile(&definition, &ConditionLimits::default()) {
                Ok(compiled) => compiled
                    .evaluate(
                        &ConditionContext::empty(),
                        &ConditionContext::empty(),
                        EvaluationBudget::new(10_000)?,
                        &CancellationToken::new(),
                    )
                    .map_or(
                        Outcome {
                            kind: OutcomeKind::Error,
                            value: None,
                        },
                        |outcome| Outcome {
                            kind: OutcomeKind::Bool,
                            value: Some(outcome.condition_met()),
                        },
                    ),
                Err(_) => Outcome {
                    kind: OutcomeKind::Error,
                    value: None,
                },
            };
        assert_eq!(actual, case.expected_rust, "shared case {}", case.name);
    }
    Ok(())
}

#[test]
fn test_should_type_parameters_and_overlay_tuple_context() -> Result<(), Box<dyn Error>> {
    let parameters = BTreeMap::from([
        (ParameterName::try_from("x")?, ParameterType::int()),
        (ParameterName::try_from("label")?, ParameterType::string()),
    ]);
    let compiled = ConditionCompiler::default().compile(
        &definition("overlay", "x < 100 && label == 'tuple'", parameters)?,
        &ConditionLimits::default(),
    )?;
    let limits = InputLimits::default();
    let request = ConditionContext::try_from_json(json!({"x": 90, "label": "request"}), &limits)?;
    let tuple = ConditionContext::try_from_json(json!({"label": "tuple"}), &limits)?;
    let outcome = compiled.evaluate(
        &request,
        &tuple,
        EvaluationBudget::new(100)?,
        &CancellationToken::new(),
    )?;
    assert!(outcome.condition_met());
    assert!(outcome.cost() > 0);
    Ok(())
}

#[test]
fn test_should_preserve_partial_missing_parameter_logic() -> Result<(), Box<dyn Error>> {
    let parameters = BTreeMap::from([
        (ParameterName::try_from("first")?, ParameterType::string()),
        (ParameterName::try_from("second")?, ParameterType::string()),
    ]);
    let limits = InputLimits::default();
    let second_true = ConditionContext::try_from_json(json!({"second": "ok"}), &limits)?;
    let second_false = ConditionContext::try_from_json(json!({"second": "no"}), &limits)?;
    let or_condition = ConditionCompiler::default().compile(
        &definition(
            "partial_or",
            "first == 'ok' || second == 'ok'",
            parameters.clone(),
        )?,
        &ConditionLimits::default(),
    )?;
    let and_condition = ConditionCompiler::default().compile(
        &definition("partial_and", "first == 'ok' && second == 'ok'", parameters)?,
        &ConditionLimits::default(),
    )?;
    let budget = EvaluationBudget::new(100)?;
    assert!(
        or_condition
            .evaluate(
                &second_true,
                &ConditionContext::empty(),
                budget,
                &CancellationToken::new()
            )?
            .condition_met()
    );
    assert!(
        !and_condition
            .evaluate(
                &second_false,
                &ConditionContext::empty(),
                budget,
                &CancellationToken::new()
            )?
            .condition_met()
    );
    let missing = or_condition.evaluate(
        &second_false,
        &ConditionContext::empty(),
        budget,
        &CancellationToken::new(),
    );
    assert!(
        matches!(missing, Err(error) if error.kind() == EvaluationErrorKind::MissingParameters && error.missing_parameter_count() == 1)
    );

    let comprehension_parameters = BTreeMap::from([
        (
            ParameterName::try_from("items")?,
            ParameterType::list(ParameterType::int())?,
        ),
        (ParameterName::try_from("missing")?, ParameterType::int()),
    ]);
    let partial_exists = ConditionCompiler::default().compile(
        &definition(
            "partial_exists",
            "items.exists(i, i == 2 || missing == i)",
            comprehension_parameters,
        )?,
        &ConditionLimits::default(),
    )?;
    let items = ConditionContext::try_from_json(json!({"items": [1, 2]}), &limits)?;
    assert!(
        partial_exists
            .evaluate(
                &items,
                &ConditionContext::empty(),
                EvaluationBudget::new(500)?,
                &CancellationToken::new(),
            )?
            .condition_met()
    );
    Ok(())
}

#[test]
fn test_should_evaluate_openfga_parameter_types_and_helpers() -> Result<(), Box<dyn Error>> {
    let parameters = BTreeMap::from([
        (ParameterName::try_from("when")?, ParameterType::timestamp()),
        (ParameterName::try_from("ttl")?, ParameterType::duration()),
        (ParameterName::try_from("ip")?, ParameterType::ip_address()),
        (
            ParameterName::try_from("tags")?,
            ParameterType::list(ParameterType::string())?,
        ),
        (
            ParameterName::try_from("attributes")?,
            ParameterType::map(ParameterType::int())?,
        ),
    ]);
    let expression = "when < timestamp('2025-01-01T00:00:00Z') && ttl == duration('1h') && \
                      ip.in_cidr('192.168.0.0/24') && 'admin' in tags && attributes.level == 3";
    let compiled = ConditionCompiler::default().compile(
        &definition("types", expression, parameters)?,
        &ConditionLimits::default(),
    )?;
    let context = ConditionContext::try_from_json(
        json!({
            "when": "2024-01-01T00:00:00Z",
            "ttl": "1h",
            "ip": "::ffff:192.168.0.8",
            "tags": ["reader", "admin"],
            "attributes": {"level": 3}
        }),
        &InputLimits::default(),
    )?;
    assert!(
        compiled
            .evaluate(
                &context,
                &ConditionContext::empty(),
                EvaluationBudget::new(500)?,
                &CancellationToken::new()
            )?
            .condition_met()
    );
    Ok(())
}

#[test]
fn test_should_evaluate_comprehensions_without_recursion() -> Result<(), Box<dyn Error>> {
    let parameters = BTreeMap::from([(
        ParameterName::try_from("items")?,
        ParameterType::list(ParameterType::int())?,
    )]);
    let compiled = ConditionCompiler::default().compile(
        &definition(
            "mapping",
            "items.map(i, i * 2).map(i, i * i).size() > 0",
            parameters,
        )?,
        &ConditionLimits::default(),
    )?;
    let context =
        ConditionContext::try_from_json(json!({"items": [1, 2, 3]}), &InputLimits::default())?;
    assert!(
        compiled
            .evaluate(
                &context,
                &ConditionContext::empty(),
                EvaluationBudget::new(1_000)?,
                &CancellationToken::new()
            )?
            .condition_met()
    );
    Ok(())
}

#[test]
fn test_should_enforce_cost_and_cancellation() -> Result<(), Box<dyn Error>> {
    let compiled = ConditionCompiler::default().compile(
        &definition("budget", "1 < 2 && 3 < 4", BTreeMap::new())?,
        &ConditionLimits::default(),
    )?;
    let cost_error = compiled.evaluate(
        &ConditionContext::empty(),
        &ConditionContext::empty(),
        EvaluationBudget::new(1)?,
        &CancellationToken::new(),
    );
    assert!(matches!(cost_error, Err(error) if error.kind() == EvaluationErrorKind::CostExceeded));
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = compiled.evaluate(
        &ConditionContext::empty(),
        &ConditionContext::empty(),
        EvaluationBudget::new(100)?,
        &cancellation,
    );
    assert!(matches!(cancelled, Err(error) if error.kind() == EvaluationErrorKind::Cancelled));
    Ok(())
}

#[test]
fn test_should_match_openfga_actual_cost_cases() -> Result<(), Box<dyn Error>> {
    let limits = InputLimits::default();
    let integer_parameters = BTreeMap::from([
        (ParameterName::try_from("x")?, ParameterType::int()),
        (ParameterName::try_from("y")?, ParameterType::int()),
    ]);
    let integer_condition = ConditionCompiler::default().compile(
        &definition("integer_cost", "x < y", integer_parameters)?,
        &ConditionLimits::default(),
    )?;
    let integers = ConditionContext::try_from_json(json!({"x": 1, "y": 2}), &limits)?;
    let integer_outcome = integer_condition.evaluate(
        &integers,
        &ConditionContext::empty(),
        EvaluationBudget::new(3)?,
        &CancellationToken::new(),
    )?;
    assert_eq!(integer_outcome.cost(), 3);
    assert!(matches!(
        integer_condition.evaluate(
            &integers,
            &ConditionContext::empty(),
            EvaluationBudget::new(2)?,
            &CancellationToken::new(),
        ),
        Err(error) if error.kind() == EvaluationErrorKind::CostExceeded
    ));

    let string_parameters = BTreeMap::from([
        (ParameterName::try_from("x")?, ParameterType::string()),
        (ParameterName::try_from("y")?, ParameterType::string()),
    ]);
    let string_condition = ConditionCompiler::default().compile(
        &definition("string_cost", "x == y", string_parameters)?,
        &ConditionLimits::default(),
    )?;
    let strings = ConditionContext::try_from_json(json!({"x": "ab", "y": "ab"}), &limits)?;
    let string_outcome = string_condition.evaluate(
        &strings,
        &ConditionContext::empty(),
        EvaluationBudget::new(3)?,
        &CancellationToken::new(),
    )?;
    assert_eq!(string_outcome.cost(), 3);
    assert!(matches!(
        string_condition.evaluate(
            &strings,
            &ConditionContext::empty(),
            EvaluationBudget::new(2)?,
            &CancellationToken::new(),
        ),
        Err(error) if error.kind() == EvaluationErrorKind::CostExceeded
    ));

    let list_parameters = BTreeMap::from([(
        ParameterName::try_from("items")?,
        ParameterType::list(ParameterType::string())?,
    )]);
    let list_condition = ConditionCompiler::default().compile(
        &definition("list_cost", "'d' in items", list_parameters)?,
        &ConditionLimits::default(),
    )?;
    let items = ConditionContext::try_from_json(json!({"items": ["a", "b", "c"]}), &limits)?;
    let list_outcome = list_condition.evaluate(
        &items,
        &ConditionContext::empty(),
        EvaluationBudget::new(4)?,
        &CancellationToken::new(),
    )?;
    assert_eq!(list_outcome.cost(), 4);
    assert!(matches!(
        list_condition.evaluate(
            &items,
            &ConditionContext::empty(),
            EvaluationBudget::new(3)?,
            &CancellationToken::new(),
        ),
        Err(error) if error.kind() == EvaluationErrorKind::CostExceeded
    ));
    Ok(())
}

#[test]
fn test_should_enforce_runtime_value_limits() -> Result<(), Box<dyn Error>> {
    let value_limits = ConditionLimits::builder()
        .runtime_value_bytes(Limit::<4_194_304>::new(3)?)
        .runtime_collection_items(Limit::<100_000>::new(1)?)
        .build();
    for expression in [
        "'abcd' == 'abcd'",
        "[1, 2] == [1, 2]",
        "'ab' + 'cd' == 'abcd'",
        "[1] + [2] == [1, 2]",
    ] {
        let compiled = ConditionCompiler::default().compile(
            &definition("runtime_limit", expression, BTreeMap::new())?,
            &value_limits,
        )?;
        let exceeded = compiled.evaluate(
            &ConditionContext::empty(),
            &ConditionContext::empty(),
            EvaluationBudget::new(100)?,
            &CancellationToken::new(),
        );
        assert!(
            matches!(exceeded, Err(error) if error.kind() == EvaluationErrorKind::ValueLimitExceeded)
        );
    }
    Ok(())
}

#[test]
fn test_should_reject_invalid_static_programs_and_redact_debug() -> Result<(), Box<dyn Error>> {
    let unknown = ConditionCompiler::default().compile(
        &definition("unknown", "missing == 1", BTreeMap::new())?,
        &ConditionLimits::default(),
    );
    assert!(matches!(unknown, Err(error) if error.kind() == CompileErrorKind::UnknownIdentifier));
    let non_boolean = ConditionCompiler::default().compile(
        &definition("non_boolean", "1", BTreeMap::new())?,
        &ConditionLimits::default(),
    );
    assert!(
        matches!(non_boolean, Err(error) if error.kind() == CompileErrorKind::NonBooleanResult)
    );
    let unsupported = ConditionCompiler::default().compile(
        &definition("unsupported", "Message{secret: 1}", BTreeMap::new())?,
        &ConditionLimits::default(),
    );
    assert!(matches!(unsupported, Err(error) if error.kind() == CompileErrorKind::Unsupported));
    let redacted_definition =
        definition("redacted", "'top-secret' == 'top-secret'", BTreeMap::new())?;
    assert!(!format!("{redacted_definition:?}").contains("top-secret"));
    let compiled =
        ConditionCompiler::default().compile(&redacted_definition, &ConditionLimits::default())?;
    assert!(!format!("{compiled:?}").contains("top-secret"));
    assert_send_sync(&compiled);

    let recompiled =
        ConditionCompiler::default().compile(&redacted_definition, &ConditionLimits::default())?;
    let changed = ConditionCompiler::default().compile(
        &definition("redacted", "'top-secret' != 'other'", BTreeMap::new())?,
        &ConditionLimits::default(),
    )?;
    assert_eq!(compiled.fingerprint(), recompiled.fingerprint());
    assert_ne!(compiled.fingerprint(), changed.fingerprint());
    Ok(())
}

#[test]
fn test_should_enforce_compile_limits_and_generic_depth() -> Result<(), Box<dyn Error>> {
    let expression_limits = ConditionLimits::builder()
        .expression_bytes(Limit::<16_384>::new(8)?)
        .build();
    let oversized = ConditionCompiler::default().compile(
        &definition("oversized", "true && true", BTreeMap::new())?,
        &expression_limits,
    );
    assert!(matches!(oversized, Err(error) if error.kind() == CompileErrorKind::LimitExceeded));

    let node_limits = ConditionLimits::builder()
        .ast_nodes(Limit::<16_384>::new(2)?)
        .build();
    let too_many_nodes = ConditionCompiler::default().compile(
        &definition("nodes", "1 < 2", BTreeMap::new())?,
        &node_limits,
    );
    assert!(
        matches!(too_many_nodes, Err(error) if error.kind() == CompileErrorKind::LimitExceeded)
    );

    let mut nested = ParameterType::int();
    for _ in 0..15 {
        nested = ParameterType::list(nested)?;
    }
    let too_deep = ParameterType::list(nested);
    assert!(matches!(too_deep, Err(error) if error.kind() == CompileErrorKind::LimitExceeded));
    Ok(())
}

#[test]
fn test_should_reject_invalid_eager_parameters_and_runtime_values() -> Result<(), Box<dyn Error>> {
    let parameters = BTreeMap::from([
        (ParameterName::try_from("used")?, ParameterType::bool()),
        (ParameterName::try_from("unused")?, ParameterType::int()),
    ]);
    let compiled = ConditionCompiler::default().compile(
        &definition("eager", "used", parameters)?,
        &ConditionLimits::default(),
    )?;
    let context = ConditionContext::try_from_json(
        json!({"used": true, "unused": false}),
        &InputLimits::default(),
    )?;
    let invalid = compiled.evaluate(
        &context,
        &ConditionContext::empty(),
        EvaluationBudget::new(100)?,
        &CancellationToken::new(),
    );
    assert!(matches!(invalid, Err(error) if error.kind() == EvaluationErrorKind::InvalidParameter));

    let numeric_string = ConditionCompiler::default().compile(
        &definition(
            "numeric_string",
            "value == 1000",
            BTreeMap::from([(ParameterName::try_from("value")?, ParameterType::int())]),
        )?,
        &ConditionLimits::default(),
    )?;
    let numeric_context =
        ConditionContext::try_from_json(json!({"value": "1e3"}), &InputLimits::default())?;
    assert!(
        numeric_string
            .evaluate(
                &numeric_context,
                &ConditionContext::empty(),
                EvaluationBudget::new(100)?,
                &CancellationToken::new(),
            )?
            .condition_met()
    );

    for expression in [
        "timestamp('2024-01-01T00:00:60Z') == timestamp('2024-01-01T00:00:00Z')",
        "{'key': 1, 'key': 2}.key == 1",
    ] {
        let compiled = ConditionCompiler::default().compile(
            &definition("runtime_invalid", expression, BTreeMap::new())?,
            &ConditionLimits::default(),
        )?;
        let invalid = compiled.evaluate(
            &ConditionContext::empty(),
            &ConditionContext::empty(),
            EvaluationBudget::new(100)?,
            &CancellationToken::new(),
        );
        assert!(matches!(invalid, Err(error) if error.kind() == EvaluationErrorKind::InvalidValue));
    }
    Ok(())
}

#[test]
fn test_should_accept_go_duration_and_rfc3339_offset_forms() -> Result<(), Box<dyn Error>> {
    let expression = "duration('10μs') == duration('10us') && \
                      timestamp('2024-01-01T01:00:00+01:00') == timestamp('2024-01-01T00:00:00Z')";
    let compiled = ConditionCompiler::default().compile(
        &definition("time_forms", expression, BTreeMap::new())?,
        &ConditionLimits::default(),
    )?;
    assert!(
        compiled
            .evaluate(
                &ConditionContext::empty(),
                &ConditionContext::empty(),
                EvaluationBudget::new(100)?,
                &CancellationToken::new(),
            )?
            .condition_met()
    );
    Ok(())
}

proptest! {
    #[test]
    fn test_should_preserve_bounded_integer_ordering(left in -1_000_000_i64..=1_000_000, right in -1_000_000_i64..=1_000_000) {
        if let Ok(definition) = definition("property", &format!("{left} <= {right}"), BTreeMap::new())
            && let Ok(compiled) = ConditionCompiler::default().compile(&definition, &ConditionLimits::default())
            && let Ok(budget) = EvaluationBudget::new(100)
            && let Ok(outcome) = compiled.evaluate(
                &ConditionContext::empty(),
                &ConditionContext::empty(),
                budget,
                &CancellationToken::new(),
            )
        {
            assert_eq!(outcome.condition_met(), left <= right);
        }
    }
}

fn definition(
    name: &str,
    expression: &str,
    parameters: BTreeMap<ParameterName, ParameterType>,
) -> Result<ConditionDefinition, Box<dyn Error>> {
    Ok(ConditionDefinition::new(
        ConditionName::try_from(name)?,
        expression.to_owned(),
        parameters,
    ))
}

fn assert_send_sync<T: Send + Sync>(_: &T) {}
