# Phase 1 Check differential report

Status: Passing complete vendored Check corpus · Date: 2026-08-05

## Scope and provenance

The Phase 1 differential harness compares Rust with the vendored Go server at commit
`4e4f79ed841513dfd61746a75ef473f6198299f7` in two complementary ways. A live HTTP corpus sends
seventeen named Check cases and one five-item BatchCheck to both servers. A complete fixture replay
records the causal setup and Check event stream produced by every case in the pinned upstream
`tests/check` package—including its consolidated YAML assets, ABAC assets, standard and contextual
matrices, and dynamic `any` condition suite—and replays it through the Rust service.

Together they cover direct membership and denial, typed wildcards, usersets, computed usersets,
tuple-to-userset, union, intersection, difference, tuple-bound conditions with request-context
overlay, cyclic userset graphs, contextual tuples, higher consistency, cross-numeric CEL boundary
behavior, invalid Check input, and correlated BatchCheck item-local failures. The fixture recorder
uses a Go overlay, leaving the vendored submodule unchanged, and preserves event order so every Rust
query observes the same preceding stores, models, and tuples as Go.

This report does not claim full wire parity before the Phase 2 transport. `make check-oracle`
covers finite budgets, storage failures, root cancellation and deadlines during condition
evaluation, and joined-task cleanup in Rust. The differential gates establish public decision and
error-class parity for the complete pinned upstream Check fixture corpus plus the live Check and
BatchCheck service/probe paths.

## Normalization and safety contract

For every live Check case, the comparator preserves and compares HTTP status, optional `allowed`
value, and a low-cardinality error class. For BatchCheck it additionally canonicalizes the result
map by correlation ID and compares each allowed decision or item-local error class. The complete
fixture replay normalizes each Go result to allowed, denied, validation, or resource-exhausted and
compares it with the Rust service result. Its report contains only event indices and normalized
outcomes—never store/model/object/subject identifiers or condition contexts—and caps both input
size and reported mismatches.

For the live gate, both origins must be credential-free loopback IP-literal HTTP URLs, redirects
are disabled, requests time out after five seconds, and every response is capped at 8 KiB while
streaming.

The Rust probe accepts only the Check and BatchCheck request subsets exercised by this corpus. It
applies a 32 KiB body cap, a concurrency cap of 16, a five-second request timeout, strict JSON
fields, bounded domain conversion, immutable model resolution through `openfga-service`, and the
correctness-first evaluator over actor-owned memory storage.

## Result

The complete fixture replay processed 3,420 ordered events and 2,226 Check evaluations with zero
mismatches. The live corpus produced zero mismatches for seventeen Check cases and one five-item
BatchCheck:

| Case | Go | Rust |
| --- | --- | --- |
| `direct_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `direct_deny` | HTTP 200, denied | HTTP 200, denied |
| `typed_wildcard_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `userset_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `computed_userset_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `tuple_to_userset_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `intersection_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `intersection_deny` | HTTP 200, denied | HTTP 200, denied |
| `difference_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `difference_deny` | HTTP 200, denied | HTTP 200, denied |
| `condition_tuple_context_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `condition_deny` | HTTP 200, denied | HTTP 200, denied |
| `userset_cycle_with_direct_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `userset_cycle_deny` | HTTP 200, denied | HTTP 200, denied |
| `contextual_tuple_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `condition_dynamic_numeric_boundary_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `invalid_object_error` | HTTP 400, validation | HTTP 400, validation |
| `correlated_mixed_batch` | HTTP 200; allow, deny, condition, contextual, item error | Same normalized map |

The machine-readable report printed by the command includes the pinned baseline commit, corpus
source, per-case normalized observations, the canonicalized BatchCheck result map, and an empty
`mismatches` array.

## Reproduction

```bash
make check-corpus-differential
make check-differential
```

The corpus target uses the checksum-verified pinned Go toolchain and a temporary overlay to record
the complete upstream fixture stream, then replays it in Rust and removes all temporary data. The
live target builds the exact vendored source, starts both servers on loopback, waits for readiness,
runs the comparator, then terminates and joins both processes through an exit trap. `make
check-spike` composes the upstream baseline, Rust oracle, complete fixture replay, and live
differential gate.
