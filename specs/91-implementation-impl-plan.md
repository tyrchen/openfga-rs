# Engineering implementation plan

Status: OpenFGA v1 GA and Phase 6 complete 2026-08-08; Phases 7–8 planned · Depends on: all preceding specs

## Readiness assessment

Phases 0–6 are complete: the exact protocol/CEL baseline, semantic engines, secure durable API,
enumeration, consistency/scale controls, PostgreSQL/MySQL/SQLite matrix, upstream conversion, GA
release evidence, separately pinned AuthZEN surface, and one graduated Check-coalescing strategy
are implemented and independently reviewed. The source-pinned
[`DynamoDB storage study`](../docs/research/study-dynamodb-storage.md) closes the design assumptions
for Phases 7–8; the backend remains unadvertised until Phase 8 evidence is complete.

Before each implementation phase, re-check dependency versions and security posture; research versions are dated evidence. Every phase is an end-to-end, independently reviewed change set with no incomplete code or temporary fallback.

## Why engineering order differs from feature order

- Stakeholders see “Check” as M1, but validated tuple/model types, CEL compilation, storage capabilities, and a compiled graph must land first or the evaluator contract remains provisional.
- Stakeholders see the complete API in M2, but transport adapters land after service/domain errors so HTTP and gRPC cannot dictate engine semantics.
- Stakeholders see caching as scale work in M4, but tuple/changelog atomicity lands in M1/M2 because retrofitting a consistency signal after caches exist is unsafe.
- ListObjects is a user-visible API, yet it follows the Check oracle: residual candidate verification cannot be correct without that permanent reference implementation.
- DynamoDB looks like one backend feature, but Phase 7 must land shared codec/key/transaction contracts before runtime wiring, and Phase 8 must run real-AWS operational/release evidence after local correctness is stable. Combining them would make emulator success indistinguishable from a production support claim.

## Effort and build graph

One experienced engineer is estimated at 43–64 weeks through the existing GA (M0–M5), plus 8–12 weeks for DynamoDB preview and production graduation (M7–M8), including tests, docs, review, and 25% collaboration overhead. Two engineers can overlap real-AWS infrastructure/evidence with local backend work after Phase 7's contracts stabilize, but transaction, cursor, and manifest semantics do not parallelize safely.

```text
Phase 0: protocol/CEL/list proofs
             │
             ▼
Phase 1: domain ─▶ condition ─▶ model ─▶ memory storage ─▶ Check oracle
             │                                  │              │
             ▼                                  └──────┬───────┘
Phase 2: PostgreSQL + services + transport + auth/runtime
             │                                         │
             └──────────────────┬──────────────────────┘
                                ▼
Phase 3: reverse enumeration + ListUsers + Expand
                                │
                                ▼
Phase 4: changelog cache controller + consistency + scale
                                │
                                ▼
Phase 5: MySQL/SQLite + migration + GA evidence
                                │
             ┌──────────────────┼──────────────────────┐
             ▼                  ▼                      ▼
    Phase 6a AuthZEN   Phase 6b proven fast paths   Phase 7 DynamoDB preview
                                                         │
                                                         ▼
                                             Phase 8 production graduation
```

## Phase 0 — De-risk compatibility foundations (M0, complete 2026-08-05)

| # | Deliverable | Specs/research | Effort |
| --- | --- | --- | ---: |
| 0.1 | Pin Rust `1.97.1`; establish crate/lint/Makefile skeleton; replace placeholder package metadata. | 60, 61, KD-017 | 3–4 days |
| 0.2 | Produce `spike-openfga-proto-generation.md`: pin/checksum API source and protoc, prove deterministic Tonic/Prost and HTTP route metadata, run SDK smoke. | 20, ecosystem survey | 4–6 days |
| 0.3 | Produce `spike-cel-openfga-conformance.md`: execute baseline/CEL matrices and select or reject the pure-Rust adapter with cost/cancellation evidence. | 11, implementation study | 6–10 days |
| 0.4 | Produce `spike-listobjects-algorithm.md`: executable reverse-plus-Check versus worker-pipeline comparison and select conservative baseline. | 15, implementation study | 5–8 days |
| 0.5 | Makefile differential harness starts the vendored Go server and a minimal Rust probe; report normalized mismatches. | 72 | 4–6 days |

Exit gate: every spike resolves its decision (no “evaluate later”); generation reproduces from a clean checkout; dependency audit/deny passes; decisions/protocol pins are updated with evidence.

Verification: artifact regeneration diff, spike test commands, harness smoke, documentation/link checks, full Rust gates for new workspace/API code, `cargo audit`, and `cargo deny check`.

Completion evidence: the accepted [protocol](../docs/research/spike-openfga-proto-generation.md), [CEL](../docs/research/spike-cel-openfga-conformance.md), and [ListObjects](../docs/research/spike-listobjects-algorithm.md) spikes; the passing [differential report](../docs/research/phase0-differential-report.md); and finalized protocol/algorithm entries in `99-key-decisions.md`.

## Phase 1 — Semantic spine and local Check (M1, complete 2026-08-06)

| # | Task | Spec | Effort |
| --- | --- | --- | ---: |
| 1.1 | Implement validated newtypes/parsers, bounded contexts/commands/errors, canonical fingerprints/tokens, property/fuzz corpus. | 10 | 1.5–2 weeks |
| 1.2 | Implement selected bounded condition compiler/evaluator and conformance suite. | 11 | 1.5–2 weeks |
| 1.3 | Implement deterministic model validation, rewrite IR, reachability/reverse metadata, fingerprints, immutable handles. | 12 | 2–3 weeks |
| 1.4 | Define narrow storage traits and actor-owned memory backend with atomic indexes/changelog and shared contracts. | 13 | 1.5–2 weeks |
| 1.5 | Implement correctness-first Check and BatchCheck: all rewrites, conditions, contextual tuples, wildcards, branch cycles, budgets, joined concurrency. | 14 | 2.5–4 weeks |
| 1.6 | Add thin service/probe path and differential report against vendored Check scenarios. | 20, 72 | 4–6 days |

Exit gate: upstream model/condition/Check outcomes match; generated/reference properties pass; cancellation/fault tests return task/stream counters to zero; no production unsafe/unwrap/expect/panic; M1 demo works from a clean checkout.

Verification: focused unit/property/fuzz suites during development, then full Rust gates, vendored Check differential suite, memory storage contract/fault suite, and relevant security limits.

## Phase 2 — Durable API, PostgreSQL, and secure runtime (M2, complete 2026-08-06)

| # | Task | Spec | Effort |
| --- | --- | --- | ---: |
| 2.1 | Implement store/model/assertion/tuple/change service use cases and exhaustive domain error mapping. | 20 | 1.5–2 weeks |
| 2.2 | Add PostgreSQL schema/migrations, checked hot queries, atomic tuple/changelog writes, primary/replica consistency, query-plan/fault tests. | 13 | 3–4 weeks |
| 2.3 | Implement every M2 Tonic/Axum endpoint, validation, pagination tokens, middleware, BatchCheck behavior, and protocol goldens. | 20 | 2.5–3.5 weeks |
| 2.4 | Implement YAML configuration, CLI/migration commands, TLS, telemetry, health, supervision, graceful drain. | 21 | 1.5–2.5 weeks |
| 2.5 | Implement OIDC/JWKS and preshared authentication plus store/action policy and redaction tests. | 70 | 2–3 weeks |
| 2.6 | Publish operator configuration, migration, backup/restore, auth, and failure runbooks. | 21, 60 | 4–6 days |

Exit gate: official SDK/API differential suite passes for delivered endpoints; PostgreSQL transaction/migration/query-plan gates pass; security/redaction/failure/shutdown suites pass; M2 runs with TLS/auth by secure default.

Verification: full Rust gates; PostgreSQL contract/fault/migration suites; gRPC/HTTP goldens and SDK matrix; OIDC/JWKS/IDOR/limit tests; dependency audit/deny for new dependencies.

## Phase 3 — Enumeration and Expand (M3, complete 2026-08-06)

| # | Task | Spec | Effort |
| --- | --- | --- | ---: |
| 3.1 | Implement conservative reverse graph/candidate traversal and indexed PostgreSQL reverse reads. | 12, 13, 15 | 2–3 weeks |
| 3.2 | Implement residual-Check ListObjects with dedup, independent budgets, unary collection, and backpressured streaming. | 15 | 2–3 weeks |
| 3.3 | Implement ListUsers forward expansion and wildcard/intersection/difference set algebra. | 15 | 2–3 weeks |
| 3.4 | Implement bounded Expand tree and response-size/cycle behavior. | 15, 20 | 1–1.5 weeks |
| 3.5 | Complete differential/generated-set, slow-client, disconnect, cleanup, and query-plan evidence. | 72 | 1–2 weeks |

Exit gate: no false negatives in bounded generated universes; every emitted object passes Check; vendored list/expand outcomes match; slow/disconnected clients leak no work; no store-wide scans.

Verification: full Rust gates, upstream enumeration differential suite, set properties, stream cancellation/fault suite, and PostgreSQL query plans/load samples.

## Phase 4 — Cache consistency and production scale (M4, complete 2026-08-08)

| # | Task | Spec | Effort |
| --- | --- | --- | ---: |
| 4.1 | Add immutable model/source/latest-alias caches with bounded weights and coalesced compile/load. | 12, 16 | 1–1.5 weeks |
| 4.2 | Add complete-key decision/tuple caches and higher-consistency bypass/primary propagation. | 16 | 1.5–2 weeks |
| 4.3 | Implement supervised changelog invalidation, watermark/lag policy, gap/overflow conservative flush, restart/shutdown. | 16, 21 | 1.5–2.5 weeks |
| 4.4 | Tune finite overload/budget defaults against pool capacity; add dashboards, alerts, and incident/capacity runbooks. | 21, 71 | 1–1.5 weeks |
| 4.5 | Run consistency faults, load/soak, rolling drain, and reference benchmark report. | 71, 72 | 1–1.5 weeks |

Exit gate: no stale result under higher consistency; cache gaps/failure are conservative; memory/tasks/pool stay bounded; M4 scale demonstration and benchmark report are reproducible.

Verification: full Rust gates, concurrent write/check and invalidation model/fault suites, actor lifecycle tests, load/soak, and dependency audit/deny for cache additions.

## Phase 5 — Backend matrix and GA hardening (M5, complete 2026-08-08)

| # | Task | Spec | Effort |
| --- | --- | --- | ---: |
| 5.1 | Implement MySQL migrations/queries and pass shared contract/differential/fault suite. | 13, 72 | 2–3 weeks |
| 5.2 | Implement SQLite migrations/queries and pass shared contract/differential/fault suite. | 13, 72 | 1.5–2.5 weeks |
| 5.3 | Build/test declared upstream schema/data migration, backup/restore, upgrade/rollback drills. | 21 | 1.5–2 weeks |
| 5.4 | Close threat model, security remediation, SBOM/provenance, license/advisory/secret gates. | 70 | 1–1.5 weeks |
| 5.5 | Trace every normative requirement to tests/evidence; complete docs/examples/compatibility matrix; remove obsolete placeholder core. | 60–72 | 1.5–2.5 weeks |
| 5.6 | Run release artifact matrix and independent final spec/code review; fix every valid finding. | all | 1–1.5 weeks |

Exit gate: zero unexplained differential mismatches across advertised backends; migration/disaster/rollback drills pass; full security/supply-chain/performance/release gates pass; compatibility statement names exact pins.

Verification: all Rust gates; every backend contract/API/differential/fault/migration suite; security/audit/deny/secret/SBOM gates; docs/link checks; release-artifact load/soak.

## Phase 6 — AuthZEN and proven optimization tracks (M6, complete 2026-08-08)

| # | Independent track | Spec | Effort |
| --- | --- | --- | ---: |
| 6.1 | Pin and implement AuthZEN transport/service mapping over existing semantics; run compatibility suite. | 00, 20, 72 | 6–10 weeks |
| 6.2 | Propose one profiled strategy (weighted Check, list pipeline, granular invalidation, coalescing, or safe actor-owned planner), shadow oracle, add kill switch. | 14–16, 71 | 4–8 weeks each |
| 6.3 | Run zero-mismatch differential/fault/cancellation and material performance graduation dossier before defaulting a strategy on. | 71, 72 | 2–4 weeks each |

Exit gate: AuthZEN evidence names its pin. Each optimization has an independent zero-mismatch dossier, measurable win, observability, and instant rollback. Phase 6 is not required for OpenFGA v1 GA correctness.

Completion evidence: the [Phase 6 AuthZEN and Check-coalescing report](../docs/research/phase6-authzen-coalescing-report.md), `make authzen-conformance`, `make authzen-differential`, real HTTP/gRPC transport tests, coalescing shadow/fault/cancellation/budget/mutation-race tests, and Criterion evidence showing a 23.0% reduction for 32 simultaneous identical cold checks while retaining enabled-mode oracle sampling.

## Phase 7 — DynamoDB backend preview (M7, estimated 5–7 weeks)

| # | Task | Spec/research | Effort |
| --- | --- | --- | ---: |
| 7.1 | Recheck current AWS SDK/Rustack versions and security posture; extract the byte-identical v1 persistence codec into `openfga-storage`; add the isolated `openfga-storage-dynamodb` crate with explicit Tokio/rustls-aws-lc features. | 17 § 2/7, storage study | 4–6 days |
| 7.2 | Implement validated config, versioned binary key/shard codec, item families, exact size accounting, and cross-shard cursor/query planners with exhaustive property tests. | 17 § 2–5 | 1–1.5 weeks |
| 7.3 | Implement all tuple reads plus conditional forward/reverse/changelog/HEAD transactions, idempotent unknown-result retry, conflict reclassification, typed errors, cancellation, metrics, and deterministic fault tests. | 13, 16, 17 § 4–6 | 1.5–2 weeks |
| 7.4 | Implement store lifecycle, model staging/commit chunks, assertion generation replacement, schema metadata, health, and initial provision/status commands with state-machine fault tests. | 17 § 7–9 | 1–1.5 weeks |
| 7.5 | Add YAML/runtime composition and Makefile-owned pinned Rustack lifecycle; run shared storage, official-SDK full API, differential, pagination, cancellation, and two-process invalidation preview gates. | 17 § 8–10, 21, 61, 72 | 1–1.5 weeks |
| 7.6 | Run the isolated real-AWS storage contract in a dedicated account/Region, inspect the complete diff, and perform an independent review against specs/research; fix every valid finding. | 17 § 10, 70–72 | 4–6 days |

Exit gate: every storage capability and full API path passes locally on pinned Rustack; deterministic fault/property suites cover every planner/state transition; the isolated real-AWS storage contract proves base-table strong reads and atomic transactions; 49 mutations succeed and 50 reject before dispatch; no DynamoDB support claim appears in GA compatibility/release artifacts.

Verification: focused crate tests during development; then full Rust build/test/nightly-fmt/clippy-pedantic gates, `cargo audit`, `cargo deny check`, codec regression fixtures for SQL, `make dynamodb-storage-rustack`, the explicit opt-in real-AWS storage target, API differential, cancellation, and two-process invalidation tests. Record any unavailable AWS operational evidence as an M8 gate, never as a waived M7 assertion.

## Phase 8 — DynamoDB production graduation (M8, estimated 3–5 weeks)

| # | Task | Spec | Effort |
| --- | --- | --- | ---: |
| 8.1 | Complete real-AWS concurrency/failure matrix: HEAD races, tuple conflicts, rollback, identical-token timeout recovery, service size/page limits, throttling/retry/deadline/cancellation, interrupted durable garbage collection, corrupt data, and readiness errors. | 17 § 6/7/10, 72 | 1–1.5 weeks |
| 8.2 | Run the complete OpenFGA/official-SDK/differential suite through two DynamoDB-backed server replicas, including higher consistency, packed-changelog cache invalidation, management, Check/BatchCheck, enumeration, and streaming disconnect cleanup. | 16, 17 § 10, 20, 72 | 4–6 days |
| 8.3 | Land least-privilege runtime/provisioning IAM examples and DynamoDB configuration, capacity, failure, migration, backup/restore runbooks; prove KMS, PITR restore-to-new-table, schema validation, and denied Scan/control-plane actions. | 17 § 8–10, 21, 70 | 4–6 days |
| 8.4 | Execute the declared real-AWS p50/p95/p99, RCU/WCU, 49-mutation, hot-store, 1/10/100-client, 30-minute soak, overload, drain, and cost evidence; tune only within fixed semantic/schema contracts. | 17 § 11, 71 | 1–1.5 weeks |
| 8.5 | Update compatibility/dependency/release docs, normative traceability, dashboards/alerts, SBOM/provenance inputs, and backend matrix; run the exact release artifact gate. | 60–72 | 3–5 days |
| 8.6 | Perform independent final spec/code/security/operations review and fix every valid finding before changing the backend status from preview to supported. | all DynamoDB-linked specs | 3–5 days |

Exit gate: real AWS proves every [`17-dynamodb-storage-design.md` acceptance criterion](17-dynamodb-storage-design.md#13-acceptance-criteria); there are zero unexplained compatibility or authorization mismatches; IAM/KMS/PITR/restore/load/soak/cost evidence is checked in; release artifacts and compatibility matrix advertise the exact Region/topology/limit contract.

Verification: full Rust and dependency gates; Rustack regression; complete real-AWS storage/fault/security/restore/performance suite; full API/differential/official-client matrix; two-process cache consistency; release artifact/SBOM/provenance gate; documentation/link/traceability checks.

## Per-phase completion discipline

- Inspect the complete diff; remove dead/incomplete code; update specs, decisions, and research when evidence changes.
- Run the smallest focused checks while developing, then the full Rust gate (`cargo build`, `cargo test`, `cargo +nightly fmt`, `cargo clippy -- -D warnings`, plus pedantic where it adds signal) for Rust/API/manifest/generated changes.
- Run `cargo audit` and `cargo deny check` for dependency/lock/license/release changes.
- Run linked conformance, differential, backend, security, cancellation, and performance gates in proportion to touched risk.
- Perform an independent review against phase specs and fix every valid finding before completion.
- Record exact commands, justified skipped heavyweight gates, upstream pin, and release evidence.

This order is correct because it lands trusted types before behavior, the permanent oracle before enumerators/optimizers, transactional consistency signals before caches, and operational/security controls before a production claim.
