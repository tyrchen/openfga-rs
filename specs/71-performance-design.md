# Performance design

Status: Proposed · Depends on: semantic component designs

## Principle

Correctness, cancellation, and finite resource use are prerequisites. Performance claims are measured against a declared environment and dataset; optimizations do not alter observable authorization semantics. The oracle evaluator remains available even after faster strategies graduate.

## Reference workloads

Benchmarks include direct exact checks, userset recursion, TTU, wide union, intersection/difference with early decisive operands, conditioned tuples, contextual tuples, batch checks with repeated subproblems, ListObjects reverse/residual ratios, ListUsers set algebra, model compile/load, tuple write/changelog, and cache cold/warm/invalidated states.

Datasets span shallow/wide/deep/recursive models; sparse and dense tuples; hot/cold stores; 1/10/100 concurrent clients; and PostgreSQL network latency. Each result records CPU/memory, Rust/tool versions, machine, backend/schema, pool and evaluator limits, model/tuple generator seed, and p50/p95/p99.

## Initial budgets

These are engineering targets, not current claims, measured on the Phase 1 declared reference host:

- Direct in-memory Check, warm compiled model, no condition: p95 ≤ 1 ms and no datastore-equivalent heap allocation proportional to store size.
- Direct PostgreSQL Check on local network: p95 ≤ 10 ms excluding deliberate queue overload.
- Warm decision-cache hit: p95 ≤ 250 µs.
- Model compilation for maximum supported model: ≤ 250 ms and ≤ 64 MiB temporary memory.
- Cancellation/disconnect stops new storage dispatch within 10 ms and joins within the endpoint shutdown budget.
- At configured concurrency, resident memory remains within cache + queue + in-flight budget with no unbounded growth over a 30-minute soak.

Phase 1 may recalibrate numerical targets once hardware and upstream comparison are recorded; any change is a decision-log update, not silent test weakening.

## Allocation and concurrency policy

Parse once and borrow where ownership permits. Root requests allocate contextual indexes once; child work shares immutable `Arc`s. Dense relation/node IDs and pre-sized vectors are preferred for compiled graphs. Tuple streams process bounded pages. Bytes payloads use `Bytes` where transport/storage ownership benefits; cloning raw contexts/tuples per branch is prohibited.

Concurrency is controlled at server, request, storage, and residual-Check levels. Defaults derive from CPU count and pool size but validate against finite ceilings. Queue time is measured separately from execution. Semaphore fairness, head-of-line blocking, and overload rejection are benchmarked.

## Optimization graduation

An optimization proposal supplies profile evidence and names its semantic invariant. It passes:

1. oracle differential tests on upstream and generated cases;
2. error-precedence, cancellation, budget, and fault tests;
3. shadow comparison with mismatch kill switch;
4. statistically useful Criterion/load evidence showing material p95/p99, throughput, or memory improvement;
5. rollback configuration and observability.

Weight-two joins, rewrite flattening, reverse worker pipelines, granular cache invalidation, request coalescing, and adaptive planning follow this gate. `inline`, SmallVec, ArcSwap, and additional caches require profile evidence; unsafe is never an optimization option.

## Acceptance criteria

- CI catches functional regressions; scheduled/release benchmarks detect material performance regressions with noise-aware thresholds.
- Flamegraphs/profiles identify actual bottlenecks before optimization work.
- Load and soak tests prove queue/cache/task/memory bounds and graceful overload.
- Performance reports always state compatibility result, because a faster incorrect deny/allow is a failure.
