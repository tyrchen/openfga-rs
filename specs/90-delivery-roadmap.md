# Delivery roadmap

Status: M0–M6 complete 2026-08-08; M7–M8 planned · Audience: stakeholders and release owners

This roadmap states usable outcomes and release evidence. Detailed build order is in [`91-implementation-impl-plan.md`](91-implementation-impl-plan.md); milestone numbers map one-to-one to its phases.

## Calendar assumptions

Estimates are elapsed engineering weeks for one experienced Rust engineer working primarily on this project, including tests, docs, review fixes, and 25% collaboration/operational overhead. They assume access to PostgreSQL/MySQL and a dedicated real-AWS DynamoDB test environment plus an OpenFGA compatibility owner for questions. Two engineers can parallelize backend/transport and test-harness work after M1, but the semantic spine remains serial; DynamoDB preview and production graduation add 8–12 one-engineer weeks after the existing GA baseline.

```text
M0 risk closure ─▶ M1 semantic spine ─▶ M2 durable API ─▶ M3 enumeration
  3–5 weeks          8–12 weeks          10–14 weeks       8–12 weeks
                                                               │
                                                               ▼
                                                M4 consistency / scale
                                                               │
                                                               ▼
                                                    M5 existing GA
                                                     /          \
                                                    ▼            ▼
                                      M6 AuthZEN / fast paths   M7 DynamoDB preview
                                       6–12 weeks per track       5–7 weeks
                                                                  │
                                                                  ▼
                                                        M8 DynamoDB production
                                                               3–5 weeks
```

| Milestone | Stakeholder outcome | Estimate | Demonstration | Exit evidence |
| --- | --- | ---: | --- | --- |
| M0 — Compatibility foundation (complete 2026-08-05) | Technical risks and baseline are closed before product claims. | 3–5 weeks | Reproducible proto generation, CEL decision, ListObjects baseline proof, conformance harness boots Go/Rust targets. | Three accepted spike reports, pinned protocols/tools, passing differential report. |
| M1 — Model and local decisions (complete 2026-08-06) | Developers can publish models/tuples and obtain correct Check decisions in a single process. | 8–12 weeks | In-memory store; model validation; conditions; Check/BatchCheck against upstream scenarios. | Domain/compiler/condition/oracle suites and differential Check report. |
| M2 — Durable control/data plane (complete 2026-08-06) | Operators can run the complete OpenFGA v1 API on PostgreSQL with migrations and security. | 10–14 weeks | gRPC + HTTP, stores/models/assertions/tuples/changes, PostgreSQL, OIDC/preshared auth, TLS. | API/SDK, storage transaction/migration, security, and failure reports. |
| M3 — Enumeration parity (complete 2026-08-06) | Applications can use ListObjects, streaming, ListUsers, and Expand with bounded behavior. | 8–12 weeks | Complex recursive/wildcard/intersection/difference cases under client cancellation. | Differential enumeration report, set properties, stream cleanup, query plans. |
| M4 — Consistency and scale (complete 2026-08-08) | Production workloads gain safe caching, replicas, graceful operations, and measured capacity. | 6–9 weeks | Higher-consistency read after write, cache invalidation/lag failure, overload, rolling shutdown. | Consistency faults, soak/load, telemetry/redaction, runbooks. |
| M5 — Backend and GA compatibility (complete 2026-08-08) | OpenFGA v1 compatibility is GA across advertised databases. | 8–12 weeks | PostgreSQL/MySQL/SQLite/in-memory matrix and upstream migration drill. | Full release gate, SBOM/audit, migration matrix, published compatibility statement. |
| M6 — AuthZEN and proven optimizations (complete 2026-08-08) | Additional compatibility and faster strategies ship without changing decisions. | 6–12 weeks per track | AuthZEN client scenarios; shadowed strategies with instant rollback. | Passing AuthZEN differential and Check-coalescing graduation report. |
| M7 — DynamoDB backend preview | Developers and platform teams can run the complete storage/API contract locally on pinned Rustack and in an isolated regional DynamoDB table. | 5–7 weeks | Create a store/model/assertions, atomically write/read forward/reverse tuples and changes, run Check/List APIs, and prove 49/50 action limits and sharded pagination. | Shared/fault/property suites, Rustack official-SDK gate, isolated real-AWS storage contract, dependency audit/deny, and independent review; backend remains preview. |
| M8 — DynamoDB production graduation | Operators can deploy and recover a least-privilege, measured regional DynamoDB-backed server and rely on an advertised compatibility claim. | 3–5 weeks | Two server replicas, concurrent writers, higher-consistency read-after-write, cache convergence, IAM denial, PITR restore, load/soak, and full API differential. | Real-AWS failure/idempotency/consistency evidence, KMS/PITR restore drill, performance/cost report, runbooks/compatibility matrix, release gate, and independent final review. |

## Release policy

M0–M1 are experimental, M2–M4 preview, and M5 is the first GA candidate. M6 items release independently after GA. M7 is explicitly a DynamoDB preview and MUST NOT alter the GA claim; M8 graduates DynamoDB independently. A milestone is complete only when its demonstration and exit evidence are checked in; partial feature code does not advance status.

Security or authorization mismatches stop promotion. Performance shortfall may defer a scale claim but cannot waive correctness/resource bounds. Backend support is advertised individually.
