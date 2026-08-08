# Phase 4 scale and reference benchmark report

Status: Passing on 2026-08-08

## Scope and reproducibility

This report records the Phase 4 cache-consistency and production-scale gate. The Go comparison is
the vendored OpenFGA commit `4e4f79ed841513dfd61746a75ef473f6198299f7`; the Rust runtime is the
release build containing this report, built with `rustc 1.97.1 (8bab26f4f 2026-07-14)`. Both servers
receive the same schema 1.1 model, tuple corpus, and direct-allow Check request over loopback HTTP.
Store and model identifiers differ because each server creates its own isolated store.

Run the exact release gate with:

```text
make phase4-scale
make phase4-local-postgres-scale-smoke
```

The first command writes machine-readable artifacts to `target/phase4`; the second writes
PostgreSQL artifacts to `target/phase4-postgres`. `make phase4-scale-smoke` is the short developer
gate. The harness starts exact binaries directly, restricts targets to IP-literal loopback origins,
bounds all client/request/duration inputs and response bodies, and removes temporary PostgreSQL data
after shutdown.

## Declared environment

| Dimension | Value |
| --- | --- |
| Host | Apple M5 Pro, 18 logical CPUs, 64 GiB RAM |
| OS | macOS 26.5.2 (Darwin 25.5.0, arm64) |
| Rust | 1.97.1, LLVM 22.1.6, optimized `release` profile |
| Go | 1.26.5 darwin/arm64 |
| Network | Loopback HTTP; no TLS or proxy |
| Primary scale backend | Actor-owned in-memory storage |
| PostgreSQL smoke | Temporary local PostgreSQL, 16-connection pool, migrations on startup |
| Rust bounds | 64 endpoint permits; eight Check reads; eight BatchCheck/residual roots |
| Cache policy | Checked-in model, decision, tuple, and controller defaults |
| Harness rate policy | Authentication-attempt and Check ceilings raised to 1,000,000/window to isolate concurrency; deployment defaults unchanged |

## Correctness and fault results

The full run executed 32 concurrent independent deny/write/read/delete/read sequences. All 64
post-mutation `HIGHER_CONSISTENCY` Checks observed the completed write or delete; stale outcomes were
zero. The PostgreSQL smoke repeated eight sequences and 16 higher-consistency observations with zero
stale outcomes.

The cache suite separately passed timeout, lag/backlog, duplicate/order fault, registration overflow,
conservative flush, coalescing, higher-consistency bypass, local write invalidation, actor restart,
and bounded stop/join cases. The rolling-drain gate admitted a deliberately nonterminating request,
marked the listener for graceful shutdown, enforced the drain deadline, released the client, and
joined every owned listener task.

## Pinned Go reference

Each client issued 25 sequential requests after warmup. Latency is reported only for successful
allows; shed responses are counted separately. These small same-host samples are orientation data,
not a portable service-level objective or a noise-aware regression threshold.

| Clients | Implementation | Requests | Allowed / overloaded | Requests/s | p50 | p95 | p99 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Go | 25 | 25 / 0 | 2,515 | 327 µs | 755 µs | 1,383 µs |
| 1 | Rust | 25 | 25 / 0 | 581 | 1,695 µs | 1,973 µs | 2,314 µs |
| 10 | Go | 250 | 250 / 0 | 19,508 | 392 µs | 1,067 µs | 1,475 µs |
| 10 | Rust | 250 | 250 / 0 | 4,724 | 1,936 µs | 2,504 µs | 3,985 µs |
| 100 | Go | 2,500 | 2,500 / 0 | 34,925 | 2,428 µs | 4,691 µs | 10,566 µs |
| 100 | Rust | 2,500 | 2,500 / 0 | 7,295 | 11,820 µs | 24,050 µs | 32,156 µs |

There were no authorization mismatches. The Go server was 4.1–4.8 times faster by observed
throughput in this narrow workload, and Rust allowed-result p95 was 2.3–5.1 times higher. This is an
explicit optimization opportunity, not a reason to weaken consistency, admission, or resource
bounds. The deterministic transport test proves protocol-compatible shedding by holding all 64
endpoint permits; the reference sample did not keep 64 requests simultaneously in service long
enough to shed.

## Thirty-minute release soak

The 100 clients were paced to a combined ceiling of 10,000 attempts/s. The optimized Rust server ran
for 1,800.015 seconds and completed 11,587,169 allowed Checks at 6,437 requests/s with zero denials,
unexpected errors, or overload responses. The maximum individual request time was 68.362 ms.
Readiness remained `SERVING` at sampled checkpoints throughout the run.

Post-warm RSS rose from 87,568 KiB to 106,560 KiB: 18,992 KiB growth against a 65,536 KiB gate. RSS
reached its plateau early and remained stable through the end. The workload uses one hot semantic
key, so bounded cache cardinality is additionally covered by cache capacity tests rather than
inferred from this memory result.

## PostgreSQL authoritative-read smoke

An isolated temporary PostgreSQL cluster exercised `HIGHER_CONSISTENCY` for every soak Check, so
model and tuple reads bypassed mutable caches and passed through the global storage work semaphore
and 16-connection pool. Sixteen clients ran for 30.005 seconds: 40,400 allows, zero overload or
semantic failures, 1,346 requests/s, and 18.613 ms maximum request time. All 31 readiness probes
passed. RSS grew 28,256 KiB, below the same 65,536 KiB gate. Pool and work cardinality cannot exceed
the configured 16 permits/connections; the load produced no acquire deadline, cancellation, or
availability failure. The same temporary cluster passed the complete PostgreSQL storage contract,
transaction fault-injection, migration, concurrency, and query-plan gate before the load run.

## Interpretation and limits

- The Phase 4 correctness, finite-resource, release-soak, live PostgreSQL, and rolling-drain gates
  pass on the declared host.
- The comparison covers one hot direct-allow HTTP Check. It does not characterize every reference
  workload in the performance design, remote-database latency, TLS, authentication, OTLP export, or
  multi-node behavior.
- The measured Rust/Go gap is retained honestly. No performance optimization graduates from these
  data, and no numerical service objective is recalibrated here.
- PostgreSQL data was disposable and local. Production capacity must repeat the worksheet in the
  [capacity runbook](../operations/capacity-runbook.md) against its real model mix and network RTT.
