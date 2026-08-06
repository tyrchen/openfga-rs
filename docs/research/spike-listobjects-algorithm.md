# Spike: ListObjects baseline algorithm

Status: Accepted — reverse traversal plus residual Check selected

## Decision

Use conservative reverse candidate traversal with the permanent correctness-first Check evaluator
as the Rust ListObjects baseline. The weighted worker pipeline remains an optional, shadowed
optimization and cannot become authoritative without the Phase 6 zero-mismatch graduation gate.

The baseline is selected even though the pipeline is materially faster on the upstream benchmark:
its current supported-shape surface and failure behavior do not meet the compatibility bar.

## Compared algorithms

```text
Reverse + residual Check (authoritative)       Worker pipeline (experimental)

(type, relation, subject)                      compiled weighted graph
          │                                             │
          ▼                                             ▼
conservative reverse edges                    worker per reachable graph node
          │                                             │ bounded channels
          ├─ complete union-only ─▶ result              ▼
          │                                   union/intersection/difference
          ▼                                             │
ambiguous candidate set                                 ▼
          │                                   deduplicated output stream
          ▼
oracle Check per candidate
          │
          ▼
deduplicated output stream
```

Reverse traversal may overproduce candidates but must never omit a potentially allowed object.
Intersection, difference, conditions, cycles, and any incomplete path are residual-Check cases.
This yields the inspectable invariant:

```text
output = { candidate | oracle Check(candidate, relation, subject) = allow }
```

## Executable comparison

`make listobjects-spike` runs the vendored benchmark with one measured iteration per case. Every
variant asserts that it returns the same 5,000-object cardinality before reporting latency. The
command executes the standard reverse/Check path and worker pipeline on direct, computed, two-hop
TTU, three-hop TTU, and recursive TTU datasets.

Observed on an Apple M5 Pro on 2026-08-05:

| Shape | Standard | Pipeline | Approximate speedup |
| --- | ---: | ---: | ---: |
| Direct | 4.61 ms | 2.24 ms | 2.1× |
| Computed | 4.62 ms | 2.14 ms | 2.2× |
| Two-hop TTU | 35.24 ms | 15.70 ms | 2.2× |
| Three-hop TTU | 57.20 ms | 16.98 ms | 3.4× |
| Recursive TTU | 41.98 ms | 14.64 ms | 2.9× |

These single-iteration numbers establish a material optimization opportunity, not a performance
budget or statistically stable benchmark claim.

## Why the faster pipeline is not the baseline

- The vendored pipeline suite explicitly removes unsupported userset-as-user and computed-userset
  cases (`vendors/openfga/internal/listobjects/pipeline/pipeline_test.go:792` and `:906`).
- Pipeline construction contains panic branches for unsupported operator/node types
  (`vendors/openfga/internal/listobjects/pipeline/pipeline.go:216`). Rust project policy forbids
  equivalent panic paths on model or request input.
- A worker network has more lifecycle, buffering, cycle-group, and error-precedence surface than
  the reverse/Check baseline. Its speed does not prove complete candidate semantics.
- The permanent Check oracle is already required by KD-002; using it for ambiguous candidates
  produces a simpler correctness argument and differential oracle for later optimization.

## Phase 3 implementation contract

Candidate generation uses only indexed reverse reads and carries a `requires_check` marker. It has
independent caps for candidates, depth, storage reads/items, residual Check concurrency, results,
and deadline. Candidate/result sets are canonical and deduplicated. Every task and stream is
cancelled, drained, and joined. No store-wide object scan is permitted.

Generated finite-universe tests must prove both directions: every emitted object passes Check and
every Check-allowed reachable object appears when the public result limit does not truncate. The
pipeline may be implemented later behind a strategy boundary, shadow comparison, kill switch, and
instant fallback to this baseline.

## Reproduction

```bash
make listobjects-spike
```
