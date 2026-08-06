//! Executable Phase 0 evidence for rejecting `cel-interpreter` as the `OpenFGA` evaluator.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
};

use cel_interpreter::{Context, Program, Value};

#[test]
fn test_should_confirm_candidate_baseline_scalar_behavior() {
    let mut context = Context::default();
    context.add_variable_from_value("x", 1_i64);
    context.add_variable_from_value("y", 2_i64);
    let program = Program::compile("x < y && 'a' in ['a', 'b']");
    assert!(matches!(
        program.map(|compiled| compiled.execute(&context)),
        Ok(Ok(Value::Bool(true)))
    ));
}

#[test]
fn test_should_record_candidate_value_matrix() {
    let expressions = [
        "true && !false",
        "1 < 2",
        "1.5 < 2.0",
        "b'abc' == b'abc'",
        "duration('1h') < duration('2h')",
        "timestamp('2024-01-01T00:00:00Z') < timestamp('2025-01-01T00:00:00Z')",
        "'a' in ['a', 'b']",
        "'key' in {'key': 1}",
        "null == null",
    ];
    for expression in expressions {
        let outcome =
            Program::compile(expression).map(|program| program.execute(&Context::default()));
        assert!(
            matches!(outcome, Ok(Ok(Value::Bool(true)))),
            "candidate value-matrix failure for {expression}"
        );
    }
}

#[test]
fn test_should_expose_missing_openfga_ipaddress_function() {
    let outcome = Program::compile("ipaddress('192.168.0.1').in_cidr('192.168.0.0/24')")
        .map(|program| program.execute(&Context::default()));
    assert!(matches!(outcome, Ok(Err(_))));
}

#[test]
fn test_should_expose_missing_static_result_type_check() {
    // OpenFGA rejects a condition whose output type is not Boolean. The candidate accepts it at
    // compile time because Program::compile only parses the expression.
    let program = Program::compile("param1");
    assert!(program.is_ok());
}

#[test]
fn test_should_expose_reachable_candidate_panic() {
    // The parser accepts a map with an identifier field; the evaluator reaches an internal panic
    // instead of returning a typed error. Catching the dependency panic makes the rejection
    // evidence executable without allowing it to escape the test process.
    let program = Program::compile("Message{field: 1}");
    assert!(program.is_ok());
    let outcome = program
        .map(|compiled| catch_unwind(AssertUnwindSafe(|| compiled.execute(&Context::default()))));
    assert!(matches!(outcome, Ok(Err(_))));
}

#[test]
fn test_should_expose_absent_runtime_cost_budget() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&calls);
    let mut context = Context::default();
    context.add_function("charge", move |_value: i64| {
        observed_calls.fetch_add(1, Ordering::Relaxed);
        true
    });
    let program = Program::compile("[1, 2, 3].all(item, charge(item))");
    assert!(matches!(
        program.map(|compiled| compiled.execute(&context)),
        Ok(Ok(Value::Bool(true)))
    ));
    // OpenFGA's cost budget of one would stop before all three calls. The candidate has no budget
    // parameter and therefore executes the complete comprehension.
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

#[test]
fn test_should_expose_absent_in_evaluator_cancellation() {
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));
    let function_started = Arc::clone(&started);
    let function_release = Arc::clone(&release);
    let mut context = Context::default();
    context.add_function("blocking", move || {
        function_started.store(true, Ordering::Release);
        while !function_release.load(Ordering::Acquire) {
            thread::yield_now();
        }
        true
    });
    let program = Program::compile("blocking()");
    assert!(program.is_ok());
    let Some(program) = program.ok() else {
        return;
    };
    let evaluator =
        thread::spawn(move || matches!(program.execute(&context), Ok(Value::Bool(true))));
    while !started.load(Ordering::Acquire) {
        thread::yield_now();
    }
    cancelled.store(true, Ordering::Release);
    for _ in 0..100 {
        thread::yield_now();
    }
    assert!(cancelled.load(Ordering::Acquire));
    assert!(!evaluator.is_finished());
    release.store(true, Ordering::Release);
    assert!(matches!(evaluator.join(), Ok(true)));
}
