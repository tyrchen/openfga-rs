# Engineering implementation plan

Status: Proposed · Depends on: all preceding specs

## Readiness assessment

The architectural contracts, upstream pin, research evidence, and release gates are ready. Production implementation is intentionally blocked on three Phase 0 facts: exact API/proto generation, OpenFGA-compatible CEL behavior, and the ListObjects baseline algorithm. The current workspace crates are placeholders and are not treated as reusable API.

Before each implementation phase, re-check dependency versions and security posture; research versions are dated evidence. Every phase is an end-to-end, independently reviewed change set with no incomplete code or temporary fallback.

## Why engineering order differs from feature order

- Stakeholders see “Check” as M1, but validated tuple/model types, CEL compilation, storage capabilities, and a compiled graph must land first or the evaluator contract remains provisional.
- Stakeholders see the complete API in M2, but transport adapters land after service/domain errors so HTTP and gRPC cannot dictate engine semantics.
- Stakeholders see caching as scale work in M4, but tuple/changelog atomicity lands in M1/M2 because retrofitting a consistency signal after caches exist is unsafe.
- ListObjects is a user-visible API, yet it follows the Check oracle: residual candidate verification cannot be correct without that permanent reference implementation.

## Effort and build graph

One experienced engineer is estimated at 43–64 weeks through GA (M0–M5), including tests, docs, review, and 25% collaboration overhead. Two engineers can overlap transport/backends with conformance work after Phase 1, yielding an estimated 28–40 weeks; semantic dependencies below do not parallelize safely.

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
                   ┌────────────┴────────────┐
                   ▼                         ▼
          Phase 6a AuthZEN         Phase 6b proven fast paths
```

## Phase 0 — De-risk compatibility foundations (M0, 3–5 weeks)

| # | Deliverable | Specs/research | Effort |
| --- | --- | --- | ---: |
| 0.1 | Pin Rust `1.97.1`; establish crate/lint/Makefile skeleton; replace placeholder package metadata. | 60, 61, KD-017 | 3–4 days |
| 0.2 | Produce `spike-openfga-proto-generation.md`: pin/checksum API source and protoc, prove deterministic Tonic/Prost and HTTP route metadata, run SDK smoke. | 20, ecosystem survey | 4–6 days |
| 0.3 | Produce `spike-cel-openfga-conformance.md`: execute baseline/CEL matrices and select or reject the pure-Rust adapter with cost/cancellation evidence. | 11, implementation study | 6–10 days |
| 0.4 | Produce `spike-listobjects-algorithm.md`: executable reverse-plus-Check versus worker-pipeline comparison and select conservative baseline. | 15, implementation study | 5–8 days |
| 0.5 | Makefile differential harness starts the vendored Go server and a minimal Rust probe; report normalized mismatches. | 72 | 4–6 days |

Exit gate: every spike resolves its decision (no “evaluate later”); generation reproduces from a clean checkout; dependency audit/deny passes; decisions/protocol pins are updated with evidence.

Verification: artifact regeneration diff, spike test commands, harness smoke, documentation/link checks, full Rust gates for new workspace/API code, `cargo audit`, and `cargo deny check`.

## Phase 1 — Semantic spine and local Check (M1, 8–12 weeks)

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

## Phase 2 — Durable API, PostgreSQL, and secure runtime (M2, 10–14 weeks)

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

## Phase 3 — Enumeration and Expand (M3, 8–12 weeks)

| # | Task | Spec | Effort |
| --- | --- | --- | ---: |
| 3.1 | Implement conservative reverse graph/candidate traversal and indexed PostgreSQL reverse reads. | 12, 13, 15 | 2–3 weeks |
| 3.2 | Implement residual-Check ListObjects with dedup, independent budgets, unary collection, and backpressured streaming. | 15 | 2–3 weeks |
| 3.3 | Implement ListUsers forward expansion and wildcard/intersection/difference set algebra. | 15 | 2–3 weeks |
| 3.4 | Implement bounded Expand tree and response-size/cycle behavior. | 15, 20 | 1–1.5 weeks |
| 3.5 | Complete differential/generated-set, slow-client, disconnect, cleanup, and query-plan evidence. | 72 | 1–2 weeks |

Exit gate: no false negatives in bounded generated universes; every emitted object passes Check; vendored list/expand outcomes match; slow/disconnected clients leak no work; no store-wide scans.

Verification: full Rust gates, upstream enumeration differential suite, set properties, stream cancellation/fault suite, and PostgreSQL query plans/load samples.

## Phase 4 — Cache consistency and production scale (M4, 6–9 weeks)

| # | Task | Spec | Effort |
| --- | --- | --- | ---: |
| 4.1 | Add immutable model/source/latest-alias caches with bounded weights and coalesced compile/load. | 12, 16 | 1–1.5 weeks |
| 4.2 | Add complete-key decision/tuple caches and higher-consistency bypass/primary propagation. | 16 | 1.5–2 weeks |
| 4.3 | Implement supervised changelog invalidation, watermark/lag policy, gap/overflow conservative flush, restart/shutdown. | 16, 21 | 1.5–2.5 weeks |
| 4.4 | Tune finite overload/budget defaults against pool capacity; add dashboards, alerts, and incident/capacity runbooks. | 21, 71 | 1–1.5 weeks |
| 4.5 | Run consistency faults, load/soak, rolling drain, and reference benchmark report. | 71, 72 | 1–1.5 weeks |

Exit gate: no stale result under higher consistency; cache gaps/failure are conservative; memory/tasks/pool stay bounded; M4 scale demonstration and benchmark report are reproducible.

Verification: full Rust gates, concurrent write/check and invalidation model/fault suites, actor lifecycle tests, load/soak, and dependency audit/deny for cache additions.

## Phase 5 — Backend matrix and GA hardening (M5, 8–12 weeks)

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

## Phase 6 — AuthZEN and proven optimization tracks (M6, 6–12 weeks each)

| # | Independent track | Spec | Effort |
| --- | --- | --- | ---: |
| 6.1 | Pin and implement AuthZEN transport/service mapping over existing semantics; run compatibility suite. | 00, 20, 72 | 6–10 weeks |
| 6.2 | Propose one profiled strategy (weighted Check, list pipeline, granular invalidation, coalescing, or safe actor-owned planner), shadow oracle, add kill switch. | 14–16, 71 | 4–8 weeks each |
| 6.3 | Run zero-mismatch differential/fault/cancellation and material performance graduation dossier before defaulting a strategy on. | 71, 72 | 2–4 weeks each |

Exit gate: AuthZEN evidence names its pin. Each optimization has an independent zero-mismatch dossier, measurable win, observability, and instant rollback. Phase 6 is not required for OpenFGA v1 GA correctness.

## Per-phase completion discipline

- Inspect the complete diff; remove dead/incomplete code; update specs, decisions, and research when evidence changes.
- Run the smallest focused checks while developing, then the full Rust gate (`cargo build`, `cargo test`, `cargo +nightly fmt`, `cargo clippy -- -D warnings`, plus pedantic where it adds signal) for Rust/API/manifest/generated changes.
- Run `cargo audit` and `cargo deny check` for dependency/lock/license/release changes.
- Run linked conformance, differential, backend, security, cancellation, and performance gates in proportion to touched risk.
- Perform an independent review against phase specs and fix every valid finding before completion.
- Record exact commands, justified skipped heavyweight gates, upstream pin, and release evidence.

This order is correct because it lands trusted types before behavior, the permanent oracle before enumerators/optimizers, transactional consistency signals before caches, and operational/security controls before a production claim.
