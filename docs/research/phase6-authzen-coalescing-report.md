# Phase 6 AuthZEN and Check-coalescing graduation report

Status: Passing locally · Date: 2026-08-08

## Pinned compatibility surface

The implementation retains the OpenFGA behavioral oracle at
`4e4f79ed841513dfd61746a75ef473f6198299f7` and generates the AuthZEN v1 protocol from the
already pinned OpenFGA API source at `f153694bfc20f7be303e33cabe72b668596c5a06`. The AuthZEN
input checksum and generated aggregate checksum are recorded in
`crates/openfga-proto/proto.lock.json`; `make proto` reproduces the descriptor, Rust messages,
JSON adapters, both Tonic service sides, and all six HTTP routes.

The supported mapping is Evaluation → Check, execute-all Evaluations → BatchCheck,
short-circuit Evaluations → ordered Check, Subject Search → ListUsers, Resource Search →
StreamedListObjects, and Action Search → model relations plus BatchCheck. The optional
`Openfga-Authorization-Model-Id` header pins a valid ULID and otherwise falls back to the latest
model, matching the vendored server. Subject/resource/action properties use the upstream prefixes;
explicit request context has final precedence. Search pagination is accepted and intentionally
ignored because the pinned implementation does not paginate these operations.

Discovery publishes only the configured canonical base URL. It never derives authority from the
HTTP Host header. Production rejects a configured non-HTTPS discovery URL; enabled evaluation and
search remain usable when discovery is unconfigured, while discovery returns FailedPrecondition.

## Compatibility evidence

`make authzen-conformance` first passes the complete vendored Go AuthZEN package, then applies a
temporary connection-only adapter and runs the exact same five files and 70 named subtests against
enabled and disabled Rust servers on real HTTP/gRPC listeners:

```text
ok github.com/openfga/openfga/tests/authzen 90.552s
ok github.com/openfga/openfga/.authzen-external.<random> 6.354s
```

`make authzen-differential` starts the exact Go and Rust binaries, provisions semantically
equivalent stores/models/tuples, and compares 19 normalized HTTP cases. It covers all six
operations, allow/deny/computed/TTU/difference decisions, execute-all and deny-first evaluation
semantics, permit-first evaluation, a valid model pin, latest-model fallback, ignored search
pagination, malformed input for every POST operation, embedded batch-item error status, and
canonical discovery. Result: zero mismatches. The harness originally caught omitted
`decision: false` JSON; the deterministic generator now explicitly emits this AuthZEN-required
default and guards the transformation shape.

Transport tests additionally exercise all mappings directly, property precedence, disabled-mode
error precedence, malformed model headers, Host-header poisoning, and all six operations over a
real-TCP gRPC listener. The vendored corpus supplies its ABAC scalar/list/map property matrix,
hierarchy/group/exclusion/intersection scenarios, short-circuit permutations, search cases, store
ID errors, experimental gating, and model pinning unchanged.

## Optimization proposal and invariant

The selected optimization is bounded coalescing of simultaneous identical Check requests on a
decision-cache miss. The measured waste was one complete datastore traversal per concurrent caller
for the same semantic key. The invariant is: sharing may reduce duplicated successful computation,
but must not alter the Boolean decision, error class/code, request budget, higher-consistency
behavior, aggregate work accounting, cancellation, or deadline observed by any caller.

The coalescing key includes the complete process-keyed decision identity, every per-root budget
that affects completion, the tuple-reader identity, and the shared mutable-cache invalidation
generation. Higher-consistency and aggregate-work-meter requests bypass. A leader receives its
exact typed failure; that failure is never shared with a healthy follower, which re-evaluates the
oracle with its own controls. Request-local cancellation and deadline exits also delegate to the
oracle with the caller's already-cancelled or expired controls, preserving its stable error kind
and the canonical `check_cancelled` or `check_deadline_elapsed` code across direct and cached
evaluation. The number of tracked keys is finite. BatchCheck remains on the permanent oracle.

Rollout modes are `disabled`, `shadow`, and `enabled`. Shadow returns the oracle result, compares
decision or stable error kind/code, emits low-cardinality metrics, and atomically disables the
strategy on any mismatch with a redacted high-severity log. Enabled mode retains one verification
sample per 64 authoritative coalesced roots and trips the same process-local kill switch. The
stable `openfga.check.coalescing.killed` gauge remains at one until restart. `disabled` is the
explicit startup rollback, and the public runtime disable control is immediate. Checked-in
development profiles remain in `shadow` so observation precedes promotion in each deployment.

## Correctness, fault, and cancellation evidence

Focused tests prove 16 simultaneous identical successes execute one shared root plus the enabled
verification sample; leader cancellation does not cancel a follower; a leader's injected storage
failure is preserved while followers re-evaluate; distinct budgets cannot share; post-write and
post-delete requests cannot join a pre-mutation computation; higher-consistency and work-meter
calls bypass; production-stack mid-flight cancellation/deadline retain their exact canonical codes
without tripping the kill switch; a rewrite matrix stays equal in shadow; and injected shadow and
enabled mismatches trip the kill switch. The existing
oracle, cache invalidation, full vendored Check corpus, and live AuthZEN differential remain the
authoritative semantic gates.

There are zero accepted or quarantined mismatches. Any future mismatch keeps the oracle
authoritative and is release-blocking.

## Performance evidence

Criterion was run on an Apple M5 Pro, arm64 macOS 26.5, Rust
`1.97.1 (8bab26f4f)`, release profile, in-memory actor storage, one direct tuple, 32 simultaneous
identical callers, 20 statistical samples, one-second warm-up, and five-second measurement:

| Workload | 95% estimate interval |
| --- | ---: |
| 32 direct identical checks | 130.17–130.84 µs |
| 32 coalesced identical checks | 100.43–100.67 µs |

The midpoint fell from 130.51 µs to 100.55 µs: a 23.0% latency reduction (1.30× equivalent
throughput) while retaining enabled-mode verification sampling. The instrumented first-sample test
reduced duplicated roots from 16 to one shared root plus one oracle verification. The same run
measured direct single Check p95 at 14.375 µs and a warm decision-cache hit p95 at 709 ns,
both within the existing gates. Coalescing targets only concurrent cold misses and does not replace
the faster warm cache path.

Reproduce with:

```text
cargo bench -p openfga-check --bench check_latency -- --noplot
```

## Graduation and rollback

The strategy meets the Phase 6 local graduation gate: complete semantic identity, bounded memory,
zero known mismatches, explicit error/cancellation/budget tests, material measured improvement,
shadow comparison, low-cardinality telemetry, automatic mismatch kill, and a configuration rollback
to `disabled`. Deployment promotion from `shadow` to `enabled` still requires observing zero
mismatches for the deployment's representative traffic; this is an operational rollout condition,
not an implementation gap.
