# Check engine design

Status: Proposed · Depends on: [`12-model-compiler-design.md`](12-model-compiler-design.md), [`13-storage-design.md`](13-storage-design.md), [`11-condition-engine-design.md`](11-condition-engine-design.md)

## Engine contract

`CheckEvaluator` receives a fully validated command, explicit `Arc<CompiledModel>`, `Arc<dyn TupleReader>`, and request budget. It returns `CheckOutcome { allowed, resolution, metadata }` or a typed error. `resolution` is internal/diagnostic and never leaks condition context.

The initial evaluator is deliberately direct and remains the permanent compatibility oracle. Optimized evaluators implement the same trait and may run only under configured strategy selection with immediate rollback.

## Subproblem state

One root owns immutable contextual-tuple indexes (by object/relation and by subject), condition context, consistency, cancellation token, deadline, and atomic/budget counters. A subproblem is `(object, relation, subject, branch_path)`. Memoization keys include store, explicit model ID/fingerprint, tuple, contextual-tuple fingerprint, condition-context fingerprint, and consistency/revision inputs.

Traversal uses an explicit work graph/futures, never unbounded Rust-stack recursion. Branch-local visited sets are persistent/copied per branch. Re-entering the same semantic subproblem on one path yields deny for that path; it does not poison siblings. Depth exhaustion, dispatch exhaustion, read exhaustion, deadline, and cancellation are resource errors.

## Rewrite semantics

- **Direct:** merge matching persisted and contextual tuples, filter conditions, match exact subject and typed wildcards, and recursively evaluate userset subjects.
- **Computed:** evaluate the named relation on the same object/subject.
- **TTU:** read the tupleset relation, then evaluate the computed relation on every referenced target object permitted by the compiled model.
- **Union:** run operands under a bounded semaphore. First allow is decisive and cancels siblings. If none allows, return an error if any operand errored; otherwise deny.
- **Intersection:** first deny is decisive and cancels siblings. If every operand allows, allow. Otherwise, with no deny and at least one error, return an error.
- **Difference:** semantically `base && !subtract`. Base deny or subtract allow is decisive deny. Base allow plus subtract deny is allow. Any remaining combination containing an error returns error.

Cancellation after a decisive result still joins all spawned tasks and closes all tuple streams. Error selection is deterministic: resource/cancellation caused by sibling short-circuit is suppressed; otherwise choose the lowest operand index and preserve its typed source.

## Concurrency and budgets

Each root owns a `JoinSet`/structured equivalent and child cancellation token. Per-request evaluator semaphore, datastore semaphore, server-wide in-flight semaphore, dispatch count, datastore query count, tuple item count, depth, and elapsed deadline are independent finite bounds. Configuration validates them against storage pool/queue capacity.

No lock is held across `.await`. Shared request counters are atomics; branch state is owned. Contextual tuple indexes and compiled models are immutable `Arc`s. Any spawned task panic is caught at the join boundary and becomes a redacted internal error; applications may then trigger supervised health degradation according to runtime policy.

## Fast paths and rollout

Reachability rejection may skip storage only after the compiler property proves it conservative. Direct exact lookup, weight-two joins, rewrite flattening, decision caching, and future adaptive planning are strategies, not semantics. A strategy graduates through unit proof, upstream conformance, randomized differential comparison with the oracle, cancellation/fault tests, shadow production observation, and a measured performance win. Mismatch disables it automatically and emits a safe high-severity metric/log.

## Observability

One root span records store/model identifiers, object/subject types (not IDs), consistency, strategy, allowed/error class, cache status, dispatch/query/item counts, throttling, cycle count, and duration. High-cardinality IDs, tuples, condition values, and credentials are excluded.

## Acceptance criteria

- Truth tables and error precedence above are exhaustively tested.
- All vendored Check/BatchCheck fixtures match upstream, including contextual tuples, wildcards, conditions, cycles, depth, and consistency.
- Loom/model tests cover reducer state where useful; stress tests find no leaked tasks/streams on short-circuit or cancellation.
- Random model/tuple generators produce identical outcomes between oracle and every enabled strategy.
- Unreachable-subject rejection performs zero datastore reads and never changes an allow to deny.

## Engineering norms

All repository `AGENTS.md` engineering sections bind the evaluator. Most critically: message-passing/owned branch state replaces shared mutable maps; every task panic is handled and every task joined; checked counters bound hostile work; errors remain domain-specific; `Debug`/tracing redact tuple IDs and contexts; performance changes require profiles; and public traits/outcomes/errors have docs and examples. Serde is N/A inside the evaluator because transport conversion occurs outside it.

## Cross-references

- ← Depends on: [`11-condition-engine-design.md`](11-condition-engine-design.md), [`12-model-compiler-design.md`](12-model-compiler-design.md), [`13-storage-design.md`](13-storage-design.md)
- → Consumed by: [`15-list-queries-design.md`](15-list-queries-design.md), [`16-cache-consistency-design.md`](16-cache-consistency-design.md), [`20-api-transport-design.md`](20-api-transport-design.md)
- ↔ Research: [`../docs/research/study-openfga-implementation.md`](../docs/research/study-openfga-implementation.md)
- ↔ Prior art: bounded union reduction in `vendors/openfga/internal/check/check.go:202` and branch-local visited state in `vendors/openfga/internal/graph/resolve_check_request.go:119`
