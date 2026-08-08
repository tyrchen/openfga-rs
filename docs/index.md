# Documentation Index

## Operations

| Runbook | Purpose |
| --- | --- |
| [Server configuration](./operations/configuration-runbook.md) | Secure YAML/secret setup, validation, deployment, health, restart, and rollback |
| [PostgreSQL migrations](./operations/migration-runbook.md) | Schema states, planned upgrades, failure handling, and forward-only rollback |
| [PostgreSQL backup and restore](./operations/backup-restore-runbook.md) | Logical backup, PITR expectations, restore verification, and promotion |
| [Authentication and authorization](./operations/authentication-runbook.md) | Disabled/preshared/OIDC operation, rotation, JWKS outages, and store/action policy |
| [Failure response](./operations/failure-response-runbook.md) | Triage, fail-safe behavior, diagnosis, graceful restart, and escalation evidence |
| [Capacity and overload](./operations/capacity-runbook.md) | Pool-derived concurrency envelope, load worksheet, tuning sequence, and containment |
| [Observability and alerts](./operations/observability-runbook.md) | OTLP metric contract, Grafana dashboard, alert rules, and redacted response procedures |

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

The OpenFGA study is pinned to `vendors/openfga` commit `4e4f79ed841513dfd61746a75ef473f6198299f7`. The Rust survey was checked on 2026-08-05 and must be refreshed before dependency changes.
