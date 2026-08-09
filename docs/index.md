# Documentation Index

## Operations

| Runbook | Purpose |
| --- | --- |
| [Server configuration](./operations/configuration-runbook.md) | Secure YAML/secret setup, validation, deployment, health, restart, and rollback |
| [SQL and upstream migrations](./operations/migration-runbook.md) | PostgreSQL/MySQL/SQLite schema states, upstream data conversion, upgrades, and rollback |
| [SQL backup and restore](./operations/backup-restore-runbook.md) | PostgreSQL/MySQL/SQLite backup, restore verification, and promotion |
| [Authentication and authorization](./operations/authentication-runbook.md) | Disabled/preshared/OIDC operation, rotation, JWKS outages, and store/action policy |
| [Failure response](./operations/failure-response-runbook.md) | Triage, fail-safe behavior, diagnosis, graceful restart, and escalation evidence |
| [Capacity and overload](./operations/capacity-runbook.md) | Pool-derived concurrency envelope, load worksheet, tuning sequence, and containment |
| [Observability and alerts](./operations/observability-runbook.md) | OTLP metric contract, Grafana dashboard, alert rules, and redacted response procedures |

## Release and security

| Document | Purpose |
| --- | --- |
| [Compatibility matrix](./compatibility.md) | Exact upstream, protocol, toolchain, SDK and backend pins behind the GA compatibility claim |
| [Threat model](./security/threat-model.md) | Assets, trust boundaries, abuse cases, controls, verification and residual risk |
| [Normative traceability](./verification/normative-requirements.md) | Every specification MUST mapped to automated or inspectable release evidence |

## Research

| Memo | Purpose | Status |
| --- | --- | --- |
| [OpenFGA implementation study](./research/study-openfga-implementation.md) | Source-pinned architecture, hot paths, data structures, algorithms, adopt/avoid decisions | Done |
| [Rust ecosystem survey](./research/survey-rust-ecosystem.md) | Current Rust toolchain, framework, storage, cache, security, CEL, and testing choices | Done |
| [OpenFGA protocol generation spike](./research/spike-openfga-proto-generation.md) | Exact API/protoc pins, deterministic Tonic/Prost generation, routes, and SDK smoke | Accepted |
| [OpenFGA CEL conformance spike](./research/spike-cel-openfga-conformance.md) | Executable candidate matrix and bounded project-evaluator decision | Accepted |
| [ListObjects algorithm spike](./research/spike-listobjects-algorithm.md) | Reverse-plus-Check versus worker-pipeline comparison and baseline selection | Accepted |
| [Phase 0 differential report](./research/phase0-differential-report.md) | Vendored Go/Rust probe lifecycle, normalization contract, and SDK smoke | Passing |
| [Phase 1 Check differential report](./research/phase1-check-differential-report.md) | Complete vendored corpus plus live Go/Rust Check and BatchCheck parity | Passing |
| [Phase 3 enumeration differential report](./research/phase3-enumeration-differential-report.md) | Generated-set equivalence, live Go/Rust list/Expand parity, disconnect cleanup, and query-plan controls | Passing |
| [Phase 4 scale and benchmark report](./research/phase4-scale-benchmark-report.md) | Cross-process consistency, full Go reference matrix, p95/heap budgets, 30-minute soak, PostgreSQL pool smoke, and rolling drain | Passing |
| [Phase 5 GA release evidence](./research/phase5-ga-release-evidence.md) | Backend matrix, upstream conversion, supply-chain controls, artifact contract, and release blockers | Passing locally; live MySQL enforced in CI |
| [Phase 6 AuthZEN and Check-coalescing report](./research/phase6-authzen-coalescing-report.md) | Pinned AuthZEN parity plus the zero-mismatch, fault, rollback, observability, and performance dossier for identical-Check coalescing | Passing locally |

The OpenFGA study is pinned to `vendors/openfga` commit `4e4f79ed841513dfd61746a75ef473f6198299f7`. The Rust survey was checked on 2026-08-05 and must be refreshed before dependency changes.
