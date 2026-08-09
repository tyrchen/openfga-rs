# Capacity and overload runbook

This runbook defines the finite concurrency envelope for the OpenFGA Rust server and the evidence
required before changing it. Authorization correctness limits are never relaxed to recover latency.

## Default envelope

The PostgreSQL profile starts with 16 connections, 64 admitted requests, eight concurrent reads per
Check root, eight BatchCheck roots, and eight ListObjects residual checks. Configuration validation
enforces these relationships:

- one Check root leaves two pool slots within a pool-sized scheduling wave for changelog, health,
  and administrative work when the pool has at least three connections;
- BatchCheck and ListObjects fan-out cannot exceed the pool size;
- nested fan-out (`root concurrency × Check read concurrency`) cannot exceed four times the pool;
- admitted transport concurrency cannot exceed four times the pool.

The PostgreSQL storage semaphore remains the final work bound. Transport admission uses a nonwaiting
permit: excess requests receive the protocol-compatible overload response before service execution.
Per-principal fixed-window rates provide a separate abuse and burst boundary.

## Capacity worksheet

Record one row per tested deployment shape. Do not extrapolate between materially different models,
tuple density, database latency, CPU limits, or cache policy.

| Input or result | Required value |
| --- | --- |
| Binary commit and Rust version | Exact identifiers |
| Host/container CPU and memory | Allocated and observed |
| PostgreSQL version/network RTT | Version plus p50/p95 RTT |
| Pool maximum/minimum | Configured counts |
| Transport, Check, BatchCheck, residual limits | Configured counts |
| Cache weights/TTLs/controller lag | Configured values |
| Workload/model/tuple seed | Reproducible identifier |
| Clients and request mix | 1/10/100 and endpoint percentages |
| Throughput and p50/p95/p99 | Successful and overloaded separately |
| Pool wait, utilization, cache hit ratio | Dashboard values |
| RSS/task/connection high-water marks | Baseline, peak, post-test |

Use the smallest of these constraints as the initial concurrency ceiling:

1. the point before PostgreSQL work-wait p95 rises sharply;
2. the point before request p95 violates the service objective;
3. the point before overload exceeds the accepted shed budget;
4. the point that preserves the two-slot per-root headroom and normally retains idle pool capacity;
5. the point whose 30-minute soak returns tasks, permits, and memory to baseline.

## Tuning sequence

1. Hold model, tuple corpus, database, and cache policy fixed. Warm the process and record the
   declared environment.
2. Increase clients through 1, 10, and 100 while keeping `maximumConcurrency` at or below
   `4 × maxConnections`.
3. If database work wait saturates first, scale the database/pool or reduce nested fan-out. Raising
   transport concurrency only grows queued request state.
4. If CPU saturates while pool wait remains low, add replicas or lower per-request evaluator and
   residual concurrency.
5. If overload occurs with healthy latency and spare resources, raise transport concurrency in one
   measured step without crossing the validated pool multiple.
6. Run the consistency fault suite and a 30-minute soak after the chosen values. Compare memory,
   tasks, pool state, p95/p99, cache hit ratio, and shed ratio with the pre-test baseline.
7. Roll out by canary. Roll back on authorization mismatch, controller unready/lag, sustained pool
   exhaustion, or a memory/task count that does not return to baseline.

## Executable gates

`make phase4-scale-smoke` runs a five-second release smoke. `make phase4-scale` runs the required
30-minute, 100-client release soak, pinned Go comparison, concurrent consistency sequences, cache
fault suite, RSS bound, and in-flight drain test. Results are written beneath `target/phase4`.

`make phase4-local-postgres-scale-smoke` creates an isolated temporary local PostgreSQL cluster,
runs a 30-second higher-consistency load through the 16-connection pool, then stops the cluster and
removes its temporary data. Operators with an existing isolated database use
`make phase4-postgres-scale-smoke POSTGRES_TEST_URL=...`; never point a destructive test at a shared
or production database.

During these loopback development gates, `GET /capacityz` samples Tokio task count, endpoint and
storage permit availability, and PostgreSQL pool open/idle counts once per second. The route is not
registered in the production profile. Each soak records baseline, high-water, and post-drain values
and fails unless permits return to capacity, every open pool connection is idle, and task count
returns within the declared tolerance. A separate one-second process sampler records RSS and OS
thread baseline, peak, and post-drain values.

The release target lifts authentication-attempt and Check rate ceilings to their validated maximum
only for this loopback harness, isolating endpoint and storage capacity. It does not alter the
checked-in deployment defaults. `PHASE4_SOAK_SECONDS`, `PHASE4_SOAK_CLIENTS`, and the other
`PHASE4_*` variables provide bounded, explicit overrides.

## Incident containment

When overload is sustained, preserve the bounds and shed upstream traffic. Lower BatchCheck or
ListObjects fan-out before reducing the per-root control-plane headroom. A zero available-work gauge
with rising wait latency means the database work envelope is saturated; a high transport shed ratio
with low database wait indicates the endpoint limit is the active boundary. Follow the
[failure response runbook](failure-response-runbook.md) for recovery and the
[observability runbook](observability-runbook.md) for signal definitions.
