# Phase 4 scale and reference benchmark report

Status: Passing on 2026-08-09

## Scope and reproducibility

This report records the Phase 4 cache-consistency and production-scale gate. The pinned comparison
is the vendored OpenFGA commit `4e4f79ed841513dfd61746a75ef473f6198299f7`; the Rust runtime is the
release build containing this report. Both servers receive the same schema 1.1 models, tuple
corpora, and requests over loopback HTTP. IDs differ only because each process owns an isolated
store.

Run the release evidence with:

```text
make phase4-scale
make phase4-local-postgres-scale-smoke
```

The first command writes machine-readable artifacts to `target/phase4`; the second creates a
temporary local PostgreSQL cluster, runs its storage contract/fault/migration/plan gate, checks SQLx
metadata against the live schema, runs the TLS HTTP/gRPC Go compatibility matrix, writes scale
artifacts to `target/phase4-postgres`, and removes the cluster after shutdown. The shorter developer
gate is `make phase4-scale-smoke`. The harness starts exact binaries directly, permits only
IP-literal loopback origins, and bounds clients, requests, durations, response bodies, and readiness
waits.

## Declared environment and limits

| Dimension | Value |
| --- | --- |
| Host | Apple M5 Pro, 18 logical CPUs, 64 GiB RAM |
| OS | macOS 26.5.2, Darwin 25.5.0, arm64 |
| Rust | 1.97.1, LLVM 22.1.6, optimized `release` profile |
| Go | 1.26.5, darwin/arm64 |
| Network | Loopback HTTP; no TLS or proxy |
| Primary scale backend | Actor-owned in-memory storage |
| PostgreSQL smoke | Temporary local PostgreSQL; 16-connection pool; startup migrations |
| Server bounds | Production and PostgreSQL: 64 endpoint permits; in-memory comparison only: 256 endpoint permits; 16 PostgreSQL work permits; eight Check reads; eight BatchCheck/residual roots |
| Dataset | Deterministic checked-in shallow, eight-way wide, deep, recursive, conditioned, contextual, dense, and sparse fixtures; no random generator seed |
| Harness policy | Per-class admission rate ceilings and in-memory endpoint permits raised only inside the harness to isolate service work from intentional shedding; PostgreSQL and deployment defaults remain at their validated bounds |

Default service-cache byte budgets are 64 MiB for model source, 128 MiB for compiled models, 64 MiB
for compiled conditions, 16 MiB for decisions, and 128 MiB for tuples. The 10,000-entry latest-model
alias cache is budgeted at 256 bytes per entry, making the service-cache aggregate approximately
402.4 MiB. The transport additionally has a 64 MiB combined normalization/validation cache, for an
approximately 466.4 MiB total default cache budget. Each service byte-weighted cache rejects a
configuration above 512 MiB and server configuration rejects a service-cache aggregate above 1
GiB; the independently bounded transport wire cache rejects a configuration above 1 GiB. Weights
are conservative owned-byte estimates that include retained payload capacity and explicit entry
overhead; tuple result sets also have a 10,000-result default limit.

## Correctness, invalidation, and fault results

| Backend and mutation path | Concurrent sequences | Higher-consistency observations | Minimize-latency observations | Stale results |
| --- | ---: | ---: | ---: | ---: |
| In-memory, mutation through the reader process | 32 | 64 | 64 | 0 |
| PostgreSQL, independent writer process through the shared changelog | 8 | 16 | 2,219 | 0 |

Each sequence checked deny, committed a write, observed allow, committed a delete, and observed deny.
PostgreSQL `MINIMIZE_LATENCY` reads additionally had to converge through changelog polling; the
independent writer shared no process-local invalidation state with the reader.

The cache suite passed timeout, lag/backlog, duplicate/order, registration-overflow, conservative
flush, cold-latest publication races, coalescing, higher-consistency bypass, local-write
invalidation, actor restart, and bounded stop/join cases. Transport metrics classify only a bounded
completion result, and the dashboard/alerts expose controller poll freshness rather than implying
that a poll timestamp is an applied watermark. The rolling-drain test admitted a deliberately
nonterminating request, initiated graceful shutdown, enforced the drain deadline, released the
client, and joined every owned listener task.

Feature parity remained green after optimization. The pinned Go differential gates reported zero
mismatches for the official SDK/management surface, 2,226 Check comparisons across 3,420 corpus
events, BatchCheck, ListObjects and streaming, ListUsers, and Expand. The complete upstream AuthZEN
corpus and the six-operation AuthZEN HTTP differential also passed with zero mismatches. The live
PostgreSQL compatibility run repeated the HTTP/gRPC management and decision paths against the same
Go baseline.

## Pinned Go reference matrix

The matrix contains 132 rows across 18 workload names: 16 matrix workloads plus a post-tuple-
invalidation check and a distinct cold-store Check. Warm rows use 1, 10, and 100 clients; initial
rows isolate the first operation. The final repeated comparison used 50 requests per client for
Check, BatchCheck, and explicit model load, five for enumeration, and 20 for mutation workloads.
Model publication populates the
immutable model caches by design, so its first explicit HTTP load is honestly labeled
`post-publish`; a separate engine benchmark below measures a genuinely cold storage-backed load.
The two ListObjects profiles record exact 0% and 100% residual-Check ratios. The complete artifact
contains p50/p95/p99 for successful responses and never folds overload into latency.

Across the final full in-memory matrix, all 134,346 requests produced the expected successful result
with zero semantic mismatches or overload responses. Every successful response was parsed into a
typed operation-specific result; each loaded row was checked against the successfully matched
Go/Rust warm semantic oracle. Representative warm 100-client rows are:

| Workload | Go allowed / overload | Go p95 | Rust allowed / overload | Rust p95 |
| --- | ---: | ---: | ---: | ---: |
| Direct exact Check | 5,000 / 0 | 3,796 µs | 5,000 / 0 | 2,494 µs |
| Recursive userset Check | 5,000 / 0 | 338,063 µs | 5,000 / 0 | 5,444 µs |
| Deep recursive userset Check | 5,000 / 0 | 760,487 µs | 5,000 / 0 | 5,391 µs |
| Eight-way union Check | 5,000 / 0 | 5,333 µs | 5,000 / 0 | 1,875 µs |
| ListUsers set algebra | 500 / 0 | 5,635 µs | 500 / 0 | 1,781 µs |
| ListObjects, 0% residual | 500 / 0 | 3,232 µs | 500 / 0 | 1,729 µs |
| ListObjects, 100% residual | 500 / 0 | 5,198 µs | 500 / 0 | 1,840 µs |
| Explicit model load, post-publish | 5,000 / 0 | 6,974 µs | 5,000 / 0 | 2,101 µs |
| Model compile and publish | 2,000 / 0 | 16,965 µs | 2,000 / 0 | 2,273 µs |
| Tuple write and changelog | 2,000 / 0 | 22,771 µs | 2,000 / 0 | 2,106 µs |

These are small, same-host, end-to-end HTTP orientation samples. They are not engine-latency
evidence, a portable service-level objective, or a noise-aware regression threshold. In this run,
Rust is faster on every representative row. Across every paired row having at least 50 samples,
Rust also had lower p50, p95, and p99 and higher throughput in this repeated run. Each row records
ending RSS for its process. CPU is computed
from before/after process-time deltas and is `null` for intervals shorter than 100 ms or samples
exceeding the host logical-CPU ceiling, because centisecond process accounting cannot attribute
those rows reliably.

## Profile-guided optimization findings

The original slow direct, union, enumeration, and mutation rows shared a nearly constant 15–20 ms
tail even though the direct evaluator benchmark was approximately 9 µs and a warm decision-cache
hit was below 1 µs. A sampled full-server CPU profile identified request validation, not graph
evaluation or storage, as the dominant cost. Every reflected request type independently decoded the
same embedded protobuf descriptor set, every validation walk decoded PGV field-rule extensions, and
every pattern check rebuilt its regex automata.

Generated messages now use one process-wide immutable descriptor pool. Transport validation builds
one immutable schema containing decoded field rules and compiled Go-compatible regexes, then reuses
it on every request. A second sample after those changes showed repeated JSON normalization and
reflection/transcoding still dominated the model-publication path. The transport therefore uses a
bounded, byte-weighted wire cache keyed by the exact message type, HTTP route, and raw body. HTTP
route identity matters because path fields are injected after body normalization, while protobuf
map encoding is not a stable request identity. Only successful normalizations and validations are
retained; failures are always revalidated, and messages containing `lt_now` timestamp rules (plus
all containing message types) bypass validation memoization. Authorization, eager compilation, and
storage publication remain on every request. Ordered exhaustive validation semantics are unchanged,
and the pinned 42-test HTTP/gRPC validation suite covers nested messages, route/body cache isolation,
map ordering, repeated fields, timestamps, multi-rule failures, and RE2/ASCII class behavior.

The remaining model-publication tail scaled almost linearly with client count. Publication was
recompiling the same CEL conditions for every semantically identical model; `cel-parser` shares
lock-protected parser DFA state, so concurrent recompilation amplified contention. `ModelCompiler`
now owns a configurable byte-weighted cache of immutable compiled conditions, keyed by the complete
condition definition and every compilation limit. Identical misses are coalesced, successful values
are shared by `Arc`, and failures are not cached. The persistent compiler is shared by publication
and model loading. Separate burst benchmarks ruled out model-ID allocation and storage/cache writes
as the limiting stages.

## Engine latency and allocation budgets

Fixed-sample checks measure individual operations and fail the gate at the Phase 4 engineering
budgets. Criterion supplies a separate statistically sampled estimate:

| Operation | Fixed-sample p95 | Budget | Criterion estimate | Result |
| --- | ---: | ---: | ---: | --- |
| Cold explicit model load and compile | 18.042 µs | Informational | 46.275–46.495 µs | Pass |
| Warm explicit model-cache hit | 0.792 µs | Informational | 0.710–0.716 µs | Pass |
| Direct in-memory Check | 13.375 µs | ≤ 1 ms | 8.526–8.608 µs | Pass |
| Warm decision-cache hit | 0.791 µs | ≤ 250 µs | 0.716–0.718 µs | Pass |
| Maximum supported model compile | 476.000 µs | ≤ 250 ms | 461.610–464.290 µs | Pass |

The maximum-model compile retained its output while `dhat` measured a 788,751-byte peak heap,
against the 64 MiB temporary-memory budget. `dhat` is a release-benchmark-only development
dependency; it is not linked into the server. The compile fixture exercises the maximum supported
100 types, 100 relations, 10,000 rewrite nodes, and 100 conditions simultaneously. The cold model-
load benchmark pre-populates the underlying actor-owned store before constructing the cache, so its
first explicit read performs a real source load and compilation. These direct measurements, rather
than HTTP scheduling and serialization, are the evidence for the engine budgets.

At a burst of 100 operations, maximum-model compilation sustained 20.8–21.0 thousand models/s, raw
in-memory publication sustained 338.8–341.2 thousand models/s, cached publication sustained
201.2–205.1 thousand models/s, and model-ID allocation sustained 394.3–396.7 thousand IDs/s. These
isolate the components behind the end-to-end publication row and confirm that its former 161 ms p95
came from repeated CEL parsing contention rather than identifier generation or persistence.

## Thirty-minute in-memory release soak

One hundred clients were paced to a combined ceiling of 10,000 attempts/s. The server ran for
1,800.016 seconds and completed 10,760,241 expected allows at 5,977 requests/s with zero overload.
The maximum individual request time was 212.500 ms, and all 1,801 sampled readiness probes returned
`SERVING`.

| Resource | Baseline | High-water | Post-drain | Bound/result |
| --- | ---: | ---: | ---: | --- |
| Runtime tasks | 7 | 109 | 7 | Post-drain tolerance: 8; pass |
| Endpoint permits | 64 available | 18 in flight | 64 available | Capacity: 64; pass |
| RSS | 109,584 KiB | 115,712 KiB | 115,712 KiB | Growth: 6,128 KiB ≤ 65,536 KiB; pass |
| Threads | 19 | 19 | 19 | No growth; pass |

The harness recorded 1,802 resource samples. In-memory storage has no SQL pool or storage-work
semaphore, so those artifact fields are explicitly `null`/zero rather than inferred.

## PostgreSQL authoritative-read smoke

The fresh local PostgreSQL run used `HIGHER_CONSISTENCY`, bypassing mutable caches and routing reads
through the global storage-work semaphore and 16-connection pool. Sixteen clients ran for 30.004
seconds: 39,352 expected allows, zero overload, 1,311 requests/s, 10.010 ms maximum request time, and
31 successful readiness probes.

| Resource | Baseline | High-water | Post-drain | Result |
| --- | ---: | ---: | ---: | --- |
| Runtime tasks | 7 | 55 | 7 | Returned to baseline; pass |
| Endpoint permits | 64 available | 16 in flight | 64 available | Fully returned; pass |
| Storage-work permits | 16 available | 16 in flight | 16 available | Fully returned; pass |
| Pool connections | 16 open / 16 idle | 16 in use | 16 open / 16 idle | Fully returned; pass |
| RSS | 79,088 KiB | 81,168 KiB | 81,168 KiB | Growth: 2,080 KiB ≤ 65,536 KiB; pass |
| Threads | 20 | 20 | 19 | No growth; pass |

The direct PostgreSQL Check reference p95 was 0.340 ms at one client and 1.800 ms at ten clients,
both within the 10 ms local-network budget. At 100 clients, finite admission shed 281 of 500
requests and allowed-result p95 was 17.369 ms; that deliberate overload point is outside the budget's
stated scope. Before load, the same disposable cluster passed the complete PostgreSQL storage
contract, transaction fault-injection, migration, concurrency, query-plan, SQLx live-schema, and
Go/Rust TLS compatibility gates.

## Interpretation and limits

- Phase 4's consistency, conservative-failure, finite-resource, release-soak, live-PostgreSQL, and
  rolling-drain exit gates pass on the declared host.
- Loopback results do not characterize remote database latency, TLS, authentication, OTLP export,
  multi-node invalidation, or production traffic distributions.
- The HTTP comparison is retained without folding overload into successful-response latency. The
  optimization is additionally supported by a sampled CPU profile and isolated engine/component
  benchmarks; the loopback ranking alone is not treated as proof.
- RSS is process-level evidence for this workload, not proof that every possible cache key mix
  reaches the configured byte ceiling safely; owned-byte estimators and capacity/eviction tests
  provide the complementary cardinality bound.
- Production deployments must repeat the worksheet in the
  [capacity runbook](../operations/capacity-runbook.md) with their actual model mix, database, and
  network RTT.
