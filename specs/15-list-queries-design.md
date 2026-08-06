# Enumeration and expansion design

Status: Proposed · Depends on: [`14-check-engine-design.md`](14-check-engine-design.md)

## Shared rules

ListObjects, StreamedListObjects, ListUsers, and Expand share validated domain types, compiled models, condition evaluation, tuple readers, contextual indexes, consistency propagation, budgets, and observability. They do not implement their semantics by pretending every problem is Check. Every result stream is bounded, cancellable, deduplicated, and joined/drained on termination.

## ListObjects

The baseline algorithm compiles a reverse traversal from `(target_type, relation)` toward the concrete subject. It uses reverse tuple indexes to enumerate candidates. Union-only complete paths may emit final candidates; intersection, difference, cycles, conditions, or otherwise ambiguous paths emit residual candidates that the oracle Check evaluator verifies.

Correctness invariant:

```text
emitted objects = { o in bounded reachable candidates | Check(o, relation, subject) = allow }
```

Candidate generation MUST be conservative: false positives are permitted before residual Check; false negatives are not. A per-request canonical object set deduplicates candidates/results. Candidate count, residual Check concurrency, storage reads/items, results, depth, and deadline are separately capped. Hitting the public result cap is successful truncation where the API defines it; hitting internal safety budgets is a resource error.

Unary and streamed APIs use the same engine. Unary collects up to the response cap. Streaming applies backpressure through a bounded channel and stops promptly when the client disconnects. Ordering follows upstream observable guarantees only.

The [Phase 0 algorithm spike](../docs/research/spike-listobjects-algorithm.md) measured a material worker-pipeline win but found unsupported shapes and panic-only failure branches. Reverse traversal plus residual Check is therefore the authoritative baseline. The weighted worker pipeline remains experimental until the optimization graduation gates prove complete equivalent behavior.

## ListUsers

ListUsers forward-expands the requested object/relation into subjects of a requested type. Direct users, recursive usersets, computed/TTU rewrites, conditions, and typed wildcards are handled explicitly. Intersection tracks subject membership per operand; one wildcard may satisfy its type operand without fabricating concrete identities. Difference tracks base and subtract membership, including wildcard exclusion.

The engine uses canonical `SubjectRef` keys, branch-local visited usersets, and bounded maps. It deduplicates final users, applies filters before emission when sound, and returns typed resource errors rather than partial unmarked success when internal limits are exhausted.

## Expand

Expand constructs the baseline-compatible userset tree for a concrete object/relation and model. Nodes are typed direct/computed/TTU/union/intersection/difference expansions. It observes conditions and tuple reads as upstream specifies but is diagnostic: it does not claim its tree alone is an authorization decision. Node count, depth, tuple reads, and serialized response bytes are bounded; branch cycles produce the baseline-compatible terminal representation.

## Acceptance criteria

- Upstream list/expand fixtures match result sets, error categories, truncation, and stream termination.
- For generated finite datasets, every ListObjects result passes Check and every Check-allowed object in the enumerated universe appears when no public limit truncates output.
- ListUsers set-algebra property tests cover wildcard, userset, intersection, and exclusion combinations.
- Slow consumer, disconnect, timeout, and storage-error tests leave no producers, Check tasks, tuple streams, or channel buffers alive.
- Reverse query plans use the required storage indexes; enumeration never scans an unbounded store universe.

## Engineering norms

All repository `AGENTS.md` engineering sections bind enumeration engines. Bounded channels and joined producers satisfy the actor/structured-concurrency policy; checked counters protect every hostile cardinality; errors are typed rather than partial silent output; logs redact identities; iterators minimize cloning and allocate with known bounds; public streams document cancellation and errors. Serde is N/A inside engines because wire mapping belongs to transport.

## Cross-references

- ← Depends on: [`14-check-engine-design.md`](14-check-engine-design.md)
- → Consumed by: [`20-api-transport-design.md`](20-api-transport-design.md), [`71-performance-design.md`](71-performance-design.md), [`72-compatibility-testing-verification-plan.md`](72-compatibility-testing-verification-plan.md)
- ↔ Research: [`../docs/research/study-openfga-implementation.md`](../docs/research/study-openfga-implementation.md)
- ↔ Prior art: reverse-edge pruning in `vendors/openfga/internal/graph/graph.go:118` and ListUsers set reduction in `vendors/openfga/pkg/server/commands/listusers/list_users_rpc.go:564`
