# Spike: OpenFGA CEL conformance

Status: Accepted — `cel-interpreter` rejected · Baseline: `cel-go 0.30.0`

## Decision

Do not use `cel-interpreter 0.10.0` as the OpenFGA condition evaluator. Use `cel-parser 0.10.1`
behind the project-owned condition boundary, then implement a typed, bounded evaluator over its
AST in Phase 1. No third-party evaluator value, error, or AST type may escape `openfga-condition`.

The selected implementation must support the exact expression/type/function surface exercised by
the pinned OpenFGA baseline. Syntax outside that reviewed surface is rejected during compilation;
conditions are never disabled or interpreted approximately.

## Executable matrix

`tests/cel-conformance/cases.json` is a shared, bounded fixture corpus transformed from
`vendors/openfga/internal/condition/condition_test.go` and the design acceptance matrix.
`make cel-spike` first executes every case through the exact vendored OpenFGA condition package
and its `cel-go 0.30.0`, then executes the same expressions through the Rust candidate and compares
both normalized outcomes with the fixture. This covers positive scalar/value behavior, the
OpenFGA IP extension, and compile-time Boolean result enforcement.

| Capability | Candidate result | OpenFGA requirement | Decision impact |
| --- | --- | --- | --- |
| Boolean/scalar comparison and short circuit | Pass | Required | Parser/evaluator semantics are useful prior art. |
| String, bytes, duration, timestamp, list, map, membership, null | Pass for the tested matrix | Required | Retain equivalent tests for the project evaluator. |
| OpenFGA `ipaddress(...).in_cidr(...)` | Runtime undeclared-function error | Required | Implement typed IP address value/function in project code. |
| Declared parameter and Boolean result typing | Non-Boolean `param1` compiles | Required at model publication | Implement deterministic static type checking. |
| Deterministic runtime cost budget | No budget input; all comprehension calls execute | Required | Charge every AST operation and collection step in project code. |
| Prompt in-evaluator cancellation | No cancellation input or loop hook | Required | Check cancellation at every node and comprehension iteration. |
| Panic freedom on parsed hostile input | `Message{field: 1}` reaches dependency `todo!` | Required | Third-party evaluator is disqualified. |

## Cancellation and cost evidence

The cancellation test installs a blocking candidate function, begins evaluation, marks a request
cancelled, and proves the evaluator remains active until an unrelated release signal arrives. The
whole dependency call runs in a child test process with a five-second parent deadline and forced
termination on overrun, so dependency drift fails instead of hanging CI. The cost test assigns a
conceptual budget of one to a three-item comprehension and observes all three function calls
because the candidate API has no cost channel.

These are structural gaps, not adapter error-mapping gaps. Wrapping `Program::execute` with a
deadline would return early while leaving blocking evaluation alive, violating the project's
joined-work and resource-bound rules.

## Selected Phase 1 shape

```text
untrusted expression + declared parameter types
                  │
                  ▼
       bounded cel-parser parse
                  │ parse errors copied into owned/redacted project errors
                  ▼
      project static type checker
       │ Boolean result required
       │ allowlisted functions/types only
       │ AST/depth/identifier/literal bounds
       ▼
 immutable project CompiledCondition
                  │
request context ──┼── tuple context overlays request context
budget + cancel ──┘
                  ▼
     project-owned iterative evaluator
       │ charge before each operation/traversal
       │ check cancellation at bounded intervals
       │ typed outcome/error; no panic paths
       ▼
          Boolean condition outcome
```

The evaluator supports Boolean/scalar values, strings, bytes, duration, timestamp, IP address,
list, map, dynamic/any, comparisons, membership, OpenFGA functions, and the comprehension forms
present in the vendored corpus. Missing parameters, lossy conversion, non-Boolean results, budget
exhaustion, cancellation, and runtime failures remain distinct typed errors.

`cel-parser` parse errors are not `Send`; the boundary converts them immediately into bounded,
owned project diagnostics. Compiled conditions retain only immutable, thread-safe project state.

## Reproduction

```bash
make cel-spike
```

Observed on 2026-08-05: all eleven shared baseline cases matched their normalized OpenFGA and
candidate expectations, and every disqualifying compatibility/safety test passed. The dependency
is retained only as a test candidate; production `openfga-condition` depends on the parser, not
the rejected evaluator.

`cel-interpreter` brings the unmaintained `paste 1.0.15` transitively. `cargo audit` reports
RUSTSEC-2024-0436 as an allowed unmaintained warning (not a vulnerability), and `cargo-deny`
records a narrow, reasoned exception for this test-only rejection evidence. Phase 1 removes the
candidate and that exception when the project-owned evaluator replaces the spike matrix.
