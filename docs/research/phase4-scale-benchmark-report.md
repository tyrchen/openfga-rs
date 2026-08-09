# Phase 4 scale and reference benchmark report

Status: Passing on 2026-08-08

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
temporary local PostgreSQL cluster, runs its storage contract/fault/migration/plan gate, writes
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
| Server bounds | 64 endpoint permits; 16 PostgreSQL work permits; eight Check reads; eight BatchCheck/residual roots |
| Dataset | Deterministic checked-in shallow, eight-way wide, deep, recursive, conditioned, contextual, dense, and sparse fixtures; no random generator seed |
| Harness policy | Per-class admission rate ceilings raised only inside the harness to isolate concurrency; endpoint/pool/evaluator concurrency bounds and deployment defaults are unchanged |

Default cache byte budgets are 64 MiB for model source, 128 MiB for compiled models, 16 MiB for
decisions, and 128 MiB for tuples. The 10,000-entry latest-model alias cache is budgeted at 256 bytes
per entry, making the default aggregate approximately 338.4 MiB. Each byte-weighted cache rejects a
configuration above 512 MiB and server configuration rejects an aggregate above 1 GiB. Weights are
conservative owned-byte estimates that include retained payload capacity and explicit entry
overhead; tuple result sets also have a 10,000-result default limit.

## Correctness, invalidation, and fault results

| Backend and mutation path | Concurrent sequences | Higher-consistency observations | Minimize-latency observations | Stale results |
| --- | ---: | ---: | ---: | ---: |
| In-memory, mutation through the reader process | 32 | 64 | 64 | 0 |
| PostgreSQL, independent writer process through the shared changelog | 8 | 16 | 1,890 | 0 |

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

## Pinned Go reference matrix

The matrix contains 132 rows across 18 workload names: 16 matrix workloads plus a post-tuple-
invalidation check and a distinct cold-store Check. Warm rows use 1, 10, and 100 clients; initial
rows isolate the first operation. Check, BatchCheck, and explicit model load use up to 25 requests
per client, enumeration uses five, and mutation workloads use one. Model publication populates the
immutable model caches by design, so its first explicit HTTP load is honestly labeled
`post-publish`; a separate engine benchmark below measures a genuinely cold storage-backed load.
The two ListObjects profiles record exact 0% and 100% residual-Check ratios. The complete artifact
contains p50/p95/p99 for successful responses and never folds overload into latency.

Across the full in-memory matrix, all 64,860 requests produced the expected successful result with
zero semantic mismatches or overload responses. Every successful response was parsed into a typed
operation-specific result; each loaded row was checked against the successfully matched Go/Rust
warm semantic oracle. Representative warm 100-client rows are:

| Workload | Go allowed / overload | Go p95 | Rust allowed / overload | Rust p95 |
| --- | ---: | ---: | ---: | ---: |
| Direct exact Check | 2,500 / 0 | 4,353 µs | 2,500 / 0 | 22,939 µs |
| Recursive userset Check | 2,500 / 0 | 233,494 µs | 2,500 / 0 | 24,084 µs |
| Deep recursive userset Check | 2,500 / 0 | 541,830 µs | 2,500 / 0 | 23,399 µs |
| Eight-way union Check | 2,500 / 0 | 5,200 µs | 2,500 / 0 | 24,629 µs |
| ListUsers set algebra | 500 / 0 | 6,497 µs | 500 / 0 | 19,004 µs |
| ListObjects, 0% residual | 500 / 0 | 3,996 µs | 500 / 0 | 10,376 µs |
| ListObjects, 100% residual | 500 / 0 | 5,685 µs | 500 / 0 | 9,655 µs |
| Explicit model load, post-publish | 2,500 / 0 | 6,804 µs | 2,500 / 0 | 1,915 µs |
| Model compile and publish | 100 / 0 | 9,597 µs | 100 / 0 | 161,445 µs |
| Tuple write and changelog | 100 / 0 | 4,194 µs | 100 / 0 | 6,245 µs |

These are small, same-host, end-to-end HTTP orientation samples. They are not engine-latency
evidence, a portable service-level objective, or a noise-aware regression threshold. They show that
the relative result depends strongly on the operation: Rust is materially faster on recursive
fixtures and post-publication explicit model reads, while Go is faster on direct/wide checks,
enumeration, and mutations. Each row records ending RSS for its process. CPU is computed from
before/after process-time deltas and is `null` for intervals shorter than 100 ms or samples exceeding
the host logical-CPU ceiling, because centisecond process accounting cannot attribute those rows
reliably; 14 longer rows produced attributable CPU values.

## Engine latency and allocation budgets

Fixed-sample checks measure individual operations and fail the gate at the Phase 4 engineering
budgets. Criterion supplies a separate statistically sampled estimate:

| Operation | Fixed-sample p95 | Budget | Criterion estimate | Result |
| --- | ---: | ---: | ---: | --- |
| Cold explicit model load and compile | 17.750 µs | Informational | 44.865–45.543 µs | Pass |
| Warm explicit model-cache hit | 0.834 µs | Informational | 0.675–0.677 µs | Pass |
| Direct in-memory Check | 13.375 µs | ≤ 1 ms | 8.787–8.830 µs | Pass |
| Warm decision-cache hit | 0.792 µs | ≤ 250 µs | 0.708–0.710 µs | Pass |
| Maximum supported model compile | 952.042 µs | ≤ 250 ms | 850.740–861.240 µs | Pass |

The maximum-model compile retained its output while `dhat` measured a 610,855-byte peak heap,
against the 64 MiB temporary-memory budget. `dhat` is a release-benchmark-only development
dependency; it is not linked into the server. The compile fixture exercises the maximum supported
100 types, 100 relations, 10,000 rewrite nodes, and 100 conditions simultaneously. The cold model-
load benchmark pre-populates the underlying actor-owned store before constructing the cache, so its
first explicit read performs a real source load and compilation. These direct measurements, rather
than HTTP scheduling and serialization, are the evidence for the engine budgets.

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
through the global storage-work semaphore and 16-connection pool. Sixteen clients ran for 30.002
seconds: 40,431 expected allows, zero overload, 1,347 requests/s, 11.910 ms maximum request time, and
31 successful readiness probes.

| Resource | Baseline | High-water | Post-drain | Result |
| --- | ---: | ---: | ---: | --- |
| Runtime tasks | 7 | 45 | 7 | Returned to baseline; pass |
| Endpoint permits | 64 available | 16 in flight | 64 available | Fully returned; pass |
| Storage-work permits | 16 available | 16 in flight | 16 available | Fully returned; pass |
| Pool connections | 16 open / 16 idle | 16 in use | 16 open / 16 idle | Fully returned; pass |
| RSS | 118,000 KiB | 127,808 KiB | 127,808 KiB | Growth: 9,808 KiB ≤ 65,536 KiB; pass |
| Threads | 20 | 20 | 19 | No growth; pass |

The direct PostgreSQL Check reference p95 was 1.778 ms at one client and 6.186 ms at ten clients,
both within the 10 ms local-network budget. At 100 clients, finite admission shed 246 of 500
requests and allowed-result p95 was 22.897 ms; that deliberate overload point is outside the budget's
stated scope. Before load, the same disposable cluster passed the complete PostgreSQL storage
contract, transaction fault-injection, migration, concurrency, and query-plan gate.

## Interpretation and limits

- Phase 4's consistency, conservative-failure, finite-resource, release-soak, live-PostgreSQL, and
  rolling-drain exit gates pass on the declared host.
- Loopback results do not characterize remote database latency, TLS, authentication, OTLP export,
  multi-node invalidation, or production traffic distributions.
- The HTTP comparison is intentionally retained without hiding slower Rust rows or overload. No
  optimization is graduated from this evidence alone.
- RSS is process-level evidence for this workload, not proof that every possible cache key mix
  reaches the configured byte ceiling safely; owned-byte estimators and capacity/eviction tests
  provide the complementary cardinality bound.
- Production deployments must repeat the worksheet in the
  [capacity runbook](../operations/capacity-runbook.md) with their actual model mix, database, and
  network RTT.
