# Condition engine design

Status: Proposed · Depends on: [`10-domain-model-design.md`](10-domain-model-design.md)

## Compatibility boundary

Project-owned interfaces isolate CEL:

```text
ConditionCompiler::compile(definition, limits) -> CompiledCondition
CompiledCondition::evaluate(request_context, tuple_context, budget, cancellation)
  -> ConditionOutcome
```

Phase 0 rejected `cel-interpreter 0.10.0` as the evaluator and selected a project-owned bounded evaluator over `cel-parser 0.10.1`; the evidence is in [`spike-cel-openfga-conformance.md`](../docs/research/spike-cel-openfga-conformance.md). No CEL crate type appears in domain, storage, model, or service public APIs. Project crates remain `#![forbid(unsafe_code)]`; an implementation requiring application FFI/unsafe is rejected unless repository policy explicitly changes.

## Compile semantics

Model publication parses and type-checks expressions, validates declared parameter names/types, installs only allowlisted OpenFGA functions/types, requires Boolean result type, and records a deterministic compiled fingerprint. Expression bytes, AST nodes, nesting, identifiers, literals, and comprehension structure are bounded.

Supported semantics MUST match the baseline for Boolean/scalar values, signed/unsigned/double conversions, strings, bytes, duration, timestamp, IP address, list, map, dynamic/any, comparisons, membership, and OpenFGA helper functions. Unsupported CEL extensions are rejected at compilation rather than behaving differently at evaluation.

## Evaluation semantics

Request context is validated first; tuple context overlays it by key. Values are converted to declared types without lossy coercion. Missing required parameters, invalid conversion, non-Boolean result, cost exhaustion, cancellation, and runtime CEL failure are typed errors—not false.

Each evaluation has a deterministic cost budget. The adapter charges at least for AST operations, collection traversal, function calls, and comprehensions; wall-clock deadline and cancellation are checked at bounded intervals. Context value bytes, nesting, and collection sizes remain bounded after merging.

Evaluation never logs expression context or secrets. Metrics use condition name/fingerprint, result class, cost, and duration only.

## Phase 0 selection gate

The Phase 0 selection gate in [`spike-cel-openfga-conformance.md`](../docs/research/spike-cel-openfga-conformance.md) required `cel-interpreter` to demonstrate:

- pass/fail parity on vendored OpenFGA condition tests and relevant CEL conformance cases;
- OpenFGA type/function compatibility, unknown/partial semantics where observable, and precedence rules;
- deterministic cost enforcement and prompt cancellation;
- no unsafe code in project crates and acceptable dependency/license/security posture.

It failed the static-typing, cost, cancellation, OpenFGA IP address, and panic-freedom gates. The selected project evaluator closes those gaps over the parser AST, supports the exact pinned baseline surface, and rejects unsupported extensions at compilation; conditions are never silently disabled.

## Acceptance criteria

- Every baseline condition fixture has matching compile/evaluate outcome and error category.
- Tuple context override and missing-parameter behavior have explicit tests.
- Property/fuzz inputs cannot panic, recurse without bound, or exceed configured allocation/cost ceilings.
- Compiled conditions are immutable, thread-safe, cacheable with their model, and redact source/context in `Debug`.

## Engineering norms

All repository `AGENTS.md` engineering sections bind this crate. In particular, condition failures are `thiserror` variants with sources, hostile values are bounded before compilation, cancellation is explicit, no unsafe/panic path is permitted, structured telemetry is redacted, and public compiler/evaluator contracts are documented and property/fuzz tested. **Serialization & Data** applies only to validated context conversion; compiled CEL internals are never serialized.

## Cross-references

- ← Depends on: [`10-domain-model-design.md`](10-domain-model-design.md)
- → Consumed by: [`12-model-compiler-design.md`](12-model-compiler-design.md), [`14-check-engine-design.md`](14-check-engine-design.md), [`15-list-queries-design.md`](15-list-queries-design.md)
- ↔ Research: [`../docs/research/study-openfga-implementation.md`](../docs/research/study-openfga-implementation.md), [`../docs/research/survey-rust-ecosystem.md`](../docs/research/survey-rust-ecosystem.md)
- ↔ Prior art: lazy typed compilation in `vendors/openfga/internal/condition/condition.go:51` and bounded evaluation in `vendors/openfga/internal/condition/eval/eval.go:49`
