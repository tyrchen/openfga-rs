# Delivery roadmap

Status: Active · Audience: stakeholders and release owners

This roadmap states usable outcomes and release evidence. Detailed build order is in [`91-implementation-impl-plan.md`](91-implementation-impl-plan.md); milestone numbers map one-to-one to its phases.

## Calendar assumptions

Estimates are elapsed engineering weeks for one experienced Rust engineer working primarily on this project, including tests, docs, review fixes, and 25% collaboration/operational overhead. They assume access to PostgreSQL/MySQL test environments and an OpenFGA compatibility owner for questions. Two engineers can parallelize backend/transport and test-harness work after M1, but the semantic spine remains serial; a realistic two-engineer GA range is 28–40 weeks rather than half the one-engineer estimate.

```text
M0 risk closure ─▶ M1 semantic spine ─▶ M2 durable API ─▶ M3 enumeration
  3–5 weeks          8–12 weeks          10–14 weeks       8–12 weeks
                                                               │
                                                               ▼
M6 AuthZEN/optimization ◀─ M5 GA hardening ◀─ M4 consistency/scale
  6–12 weeks each          8–12 weeks         6–9 weeks
  (post-GA, independent)
```

| Milestone | Stakeholder outcome | Estimate | Demonstration | Exit evidence |
| --- | --- | ---: | --- | --- |
| M0 — Compatibility foundation (complete 2026-08-05) | Technical risks and baseline are closed before product claims. | 3–5 weeks | Reproducible proto generation, CEL decision, ListObjects baseline proof, conformance harness boots Go/Rust targets. | Three accepted spike reports, pinned protocols/tools, passing differential report. |
| M1 — Model and local decisions | Developers can publish models/tuples and obtain correct Check decisions in a single process. | 8–12 weeks | In-memory store; model validation; conditions; Check/BatchCheck against upstream scenarios. | Domain/compiler/condition/oracle suites and differential Check report. |
| M2 — Durable control/data plane | Operators can run the complete OpenFGA v1 API on PostgreSQL with migrations and security. | 10–14 weeks | gRPC + HTTP, stores/models/assertions/tuples/changes, PostgreSQL, OIDC/preshared auth, TLS. | API/SDK, storage transaction/migration, security, and failure reports. |
| M3 — Enumeration parity | Applications can use ListObjects, streaming, ListUsers, and Expand with bounded behavior. | 8–12 weeks | Complex recursive/wildcard/intersection/difference cases under client cancellation. | Differential enumeration report, set properties, stream cleanup, query plans. |
| M4 — Consistency and scale | Production workloads gain safe caching, replicas, graceful operations, and measured capacity. | 6–9 weeks | Higher-consistency read after write, cache invalidation/lag failure, overload, rolling shutdown. | Consistency faults, soak/load, telemetry/redaction, runbooks. |
| M5 — Backend and GA compatibility | OpenFGA v1 compatibility is GA across advertised databases. | 8–12 weeks | PostgreSQL/MySQL/SQLite/in-memory matrix and upstream migration drill. | Full release gate, SBOM/audit, migration matrix, published compatibility statement. |
| M6 — AuthZEN and proven optimizations | Additional compatibility and faster strategies ship without changing decisions. | 6–12 weeks per track | AuthZEN client scenarios; shadowed strategies with instant rollback. | AuthZEN differential report and per-optimization graduation dossiers. |

## Release policy

M0–M1 are experimental, M2–M4 preview, and M5 is the first GA candidate. M6 items release independently after GA. A milestone is complete only when its demonstration and exit evidence are checked in; partial feature code does not advance status.

Security or authorization mismatches stop promotion. Performance shortfall may defer a scale claim but cannot waive correctness/resource bounds. Backend support is advertised individually.
