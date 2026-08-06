# Phase 1 Check differential report

Status: Passing bounded corpus · Date: 2026-08-05

## Scope and provenance

The Phase 1 differential harness sends the same normalized Check cases to the vendored Go server
at commit `4e4f79ed841513dfd61746a75ef473f6198299f7` and the Rust Check probe. The corpus is a bounded,
inspectable transformation of the memory scenarios in `vendors/openfga/tests/check/check.go` and
`vendors/openfga/tests/check/check_test.go`: direct membership, a negative decision, typed wildcard,
userset membership, contextual tuples, and invalid Check input. The model and tuples are recreated
independently in each server; request identifiers differ, but the semantic question is identical.

This report does not claim full wire or endpoint parity. The vendored upstream Check suites remain
the broad baseline gate through `make check-baseline`, while `make check-oracle` covers all Phase 1
rewrite forms, conditions, cycles, budgets, cancellation, and cleanup in Rust. This differential
slice proves that the delivered service/probe path agrees with Go on the normalized public outcome.

## Normalization and safety contract

For every named case, the comparator preserves and compares HTTP status, optional `allowed` value,
and a low-cardinality error class. It emits a separate mismatch for each differing field. It never
records store/model/object/subject identifiers, condition context, or response bodies. Both origins
must be credential-free loopback IP-literal HTTP URLs, redirects are disabled, requests time out
after five seconds, and every response is capped at 8 KiB while streaming.

The Rust probe accepts only the Check request subset exercised by this corpus. It applies a 32 KiB
body cap, a concurrency cap of 16, a five-second request timeout, strict JSON fields, bounded domain
conversion, immutable model resolution through `openfga-service`, and the correctness-first
evaluator over actor-owned memory storage.

## Result

The six cases produced zero mismatches:

| Case | Go | Rust |
| --- | --- | --- |
| `direct_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `direct_deny` | HTTP 200, denied | HTTP 200, denied |
| `typed_wildcard_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `userset_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `contextual_tuple_allow` | HTTP 200, allowed | HTTP 200, allowed |
| `invalid_object_error` | HTTP 400, validation | HTTP 400, validation |

The machine-readable report printed by the command includes the pinned baseline commit, corpus
source, per-case normalized observations, and an empty `mismatches` array.

## Reproduction

```bash
make check-differential
```

The target builds the exact vendored source using the checksum-verified pinned Go toolchain, starts
both servers on loopback, waits for readiness, runs the comparator, then terminates and joins both
processes through an exit trap. `make check-spike` composes the upstream baseline, Rust oracle, and
this live differential gate.
