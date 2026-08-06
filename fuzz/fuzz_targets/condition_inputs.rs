#![no_main]
// `fuzz_target!` owns its artifact file; production condition code performs no file I/O here.
#![allow(clippy::disallowed_types)]

use std::collections::BTreeMap;

use libfuzzer_sys::fuzz_target;
use openfga_condition::{
    CancellationToken, ConditionCompiler, ConditionDefinition, ConditionLimits, EvaluationBudget,
    ParameterType,
};
use openfga_domain::{ConditionContext, ConditionName, InputLimits, ParameterName};

fuzz_target!(init: std::panic::set_hook(Box::new(|_| {})), |data: &[u8]| {
    let Some(expression) = std::str::from_utf8(data).ok() else {
        return;
    };
    let Some(parameters) = parameters() else {
        return;
    };
    let Some(name) = ConditionName::try_from("fuzz_condition").ok() else {
        return;
    };
    let definition = ConditionDefinition::new(name, expression.to_owned(), parameters);
    let Ok(compiled) = ConditionCompiler::default().compile(&definition, &ConditionLimits::default()) else {
        return;
    };
    let context = serde_json::from_slice::<serde_json::Value>(data)
        .ok()
        .and_then(|value| ConditionContext::try_from_json(value, &InputLimits::default()).ok())
        .unwrap_or_else(ConditionContext::empty);
    let cancellation = CancellationToken::new();
    if data.first() == Some(&0xff) {
        cancellation.cancel();
    }
    let Some(budget) = EvaluationBudget::new(10_000).ok() else {
        return;
    };
    let _ = compiled.evaluate(&context, &ConditionContext::empty(), budget, &cancellation);
});

fn parameters() -> Option<BTreeMap<ParameterName, ParameterType>> {
    let list = ParameterType::list(ParameterType::int()).ok()?;
    Some(BTreeMap::from([
        (ParameterName::try_from("x").ok()?, ParameterType::int()),
        (ParameterName::try_from("y").ok()?, ParameterType::int()),
        (ParameterName::try_from("items").ok()?, list),
        (
            ParameterName::try_from("param1").ok()?,
            ParameterType::string(),
        ),
        (
            ParameterName::try_from("param2").ok()?,
            ParameterType::string(),
        ),
        (ParameterName::try_from("data").ok()?, ParameterType::any()),
        (
            ParameterName::try_from("ip").ok()?,
            ParameterType::ip_address(),
        ),
    ]))
}
