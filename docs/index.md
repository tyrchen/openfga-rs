# Documentation Index

## Project guides

| Guide | Purpose |
| --- | --- |
| [Architecture](./architecture.md) | Crate boundaries, request lifecycle, consistency, concurrency, security, performance, and extension points |
| [Dependency policy](./dependencies.md) | Current direct-dependency audit, load-bearing choices, update policy, and required gates |
| [Release process](./releasing.md) | Release preparation, tag workflow, artifact contents, verification, and rollback |
| [GitHub governance](./repository-governance.md) | Branch protection, Actions/release policy, security features, maintainers, and periodic audit |
| [Contributing](../CONTRIBUTING.md) | Development workflow, change-specific evidence, pull requests, and attribution |
| [Security policy](../SECURITY.md) | Supported versions, private vulnerability reporting, and disclosure expectations |

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
| [DynamoDB operations](./operations/dynamodb-runbook.md) | Regional topology, YAML, least-privilege IAM, provisioning, KMS/PITR restore, failure response, cleanup, and verification |

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
| [DynamoDB storage study](./research/study-dynamodb-storage.md) | AWS limits, physical access paths, Rust SDK choice, and Rustack/local-versus-cloud verification boundary | Done |
| [OpenFGA protocol generation spike](./research/spike-openfga-proto-generation.md) | Exact API/protoc pins, deterministic Tonic/Prost generation, routes, and SDK smoke | Accepted |
| [OpenFGA CEL conformance spike](./research/spike-cel-openfga-conformance.md) | Executable candidate matrix and bounded project-evaluator decision | Accepted |
| [ListObjects algorithm spike](./research/spike-listobjects-algorithm.md) | Reverse-plus-Check versus worker-pipeline comparison and baseline selection | Accepted |
| [Phase 0 differential report](./research/phase0-differential-report.md) | Vendored Go/Rust probe lifecycle, normalization contract, and SDK smoke | Passing |
| [Phase 1 Check differential report](./research/phase1-check-differential-report.md) | Complete vendored corpus plus live Go/Rust Check and BatchCheck parity | Passing |
| [Phase 3 enumeration differential report](./research/phase3-enumeration-differential-report.md) | Generated-set equivalence, live Go/Rust list/Expand parity, disconnect cleanup, and query-plan controls | Passing |
| [Phase 4 scale and benchmark report](./research/phase4-scale-benchmark-report.md) | Cross-process consistency, full Go reference matrix, p95/heap budgets, 30-minute soak, PostgreSQL pool smoke, and rolling drain | Passing |
| [Phase 5 GA release evidence](./research/phase5-ga-release-evidence.md) | Backend matrix, upstream conversion, supply-chain controls, artifact contract, and release blockers | Passing locally; live MySQL enforced in CI |
| [Phase 6 AuthZEN and Check-coalescing report](./research/phase6-authzen-coalescing-report.md) | Pinned AuthZEN parity plus the zero-mismatch, fault, rollback, observability, and performance dossier for identical-Check coalescing | Passing locally |
| [Phase 7 DynamoDB preview report](./research/phase7-dynamodb-preview-report.md) | Pinned Rustack storage/full-API evidence and the explicit boundary around unavailable real-AWS promotion evidence | Local preview passing; AWS promotion blocked |

The OpenFGA study is pinned to `vendors/openfga` commit `4e4f79ed841513dfd61746a75ef473f6198299f7`. The DynamoDB study is pinned to `vendors/rustack` commit `ab8bc61a3e45058c7d42de8443f9d215cc110b18`. The Rust survey was checked on 2026-08-05 and must be refreshed before dependency changes.
