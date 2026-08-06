//! Executable Phase 0 evidence for rejecting `cel-interpreter` as the `OpenFGA` evaluator.

// The cancellation candidate is deliberately isolated in a killable test subprocess.
#[allow(clippy::disallowed_types)]
use std::process::{Command, Stdio};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use cel_interpreter::{Context, Program, Value};
use serde::Deserialize;

const CANCELLATION_HELPER_ENV: &str = "OPENFGA_CEL_CANCELLATION_HELPER";
const MAX_FIXTURE_CASES: usize = 64;
const MAX_FIXTURE_EXPRESSION_BYTES: usize = 4_096;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Fixture {
    cases: Vec<FixtureCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FixtureCase {
    name: String,
    expression: String,
    expected_baseline: Outcome,
    expected_candidate: Outcome,
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
    NonBoolean,
}

#[test]
fn test_should_execute_shared_openfga_candidate_matrix() {
    let fixture = serde_json::from_str::<Fixture>(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/cel-conformance/cases.json"
    )));
    assert!(fixture.is_ok());
    let Some(fixture) = fixture.ok() else {
        return;
    };
    assert!(!fixture.cases.is_empty());
    assert!(fixture.cases.len() <= MAX_FIXTURE_CASES);
    for case in fixture.cases {
        assert!(!case.name.is_empty() && case.name.len() <= 128);
        assert!(!case.expression.is_empty());
        assert!(case.expression.len() <= MAX_FIXTURE_EXPRESSION_BYTES);
        assert!(matches!(
            case.expected_baseline.kind,
            OutcomeKind::Bool | OutcomeKind::Error
        ));
        let outcome = normalize_candidate(&case.expression);
        assert!(
            outcome == case.expected_candidate,
            "candidate matrix mismatch for {}: expected {:?}, found {:?}",
            case.name,
            case.expected_candidate,
            outcome
        );
    }
}

fn normalize_candidate(expression: &str) -> Outcome {
    let Ok(program) = Program::compile(expression) else {
        return Outcome {
            kind: OutcomeKind::Error,
            value: None,
        };
    };
    match program.execute(&Context::default()) {
        Ok(Value::Bool(value)) => Outcome {
            kind: OutcomeKind::Bool,
            value: Some(value),
        },
        Ok(_) => Outcome {
            kind: OutcomeKind::NonBoolean,
            value: None,
        },
        Err(_) => Outcome {
            kind: OutcomeKind::Error,
            value: None,
        },
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
    let outcome = run_bounded_cancellation_helper();
    assert!(outcome.is_ok(), "{outcome:?}");
}

#[allow(clippy::disallowed_types)]
fn run_bounded_cancellation_helper() -> Result<(), String> {
    let executable = std::env::current_exe()
        .map_err(|error| format!("failed to resolve the test executable: {error}"))?;
    let mut child = Command::new(executable)
        .args([
            "--ignored",
            "--exact",
            "test_should_run_candidate_cancellation_helper",
            "--nocapture",
        ])
        .env(CANCELLATION_HELPER_ENV, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("failed to start cancellation helper: {error}"))?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match child
            .try_wait()
            .map_err(|error| format!("failed to wait for cancellation helper: {error}"))?
        {
            Some(status) if status.success() => return Ok(()),
            Some(status) => return Err(format!("cancellation helper exited with {status}")),
            None => thread::sleep(Duration::from_millis(10)),
        }
    }
    child
        .kill()
        .map_err(|error| format!("failed to kill timed-out cancellation helper: {error}"))?;
    child
        .wait()
        .map_err(|error| format!("failed to reap timed-out cancellation helper: {error}"))?;
    Err("candidate cancellation evidence exceeded its five-second process limit".to_owned())
}

#[test]
#[ignore = "executed in a bounded child process by the cancellation evidence test"]
fn test_should_run_candidate_cancellation_helper() {
    assert_eq!(std::env::var(CANCELLATION_HELPER_ENV).as_deref(), Ok("1"));
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));
    let function_started = Arc::clone(&started);
    let function_release = Arc::clone(&release);
    let mut context = Context::default();
    context.add_function("blocking", move || {
        function_started.store(true, Ordering::Release);
        let deadline = Instant::now() + Duration::from_secs(2);
        while !function_release.load(Ordering::Acquire) && Instant::now() < deadline {
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
    let start_deadline = Instant::now() + Duration::from_secs(1);
    while !started.load(Ordering::Acquire) && Instant::now() < start_deadline {
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
