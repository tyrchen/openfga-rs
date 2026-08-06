# Key decisions

Status: Accepted for implementation planning · Decision date: 2026-08-05

Each decision may be revisited only when its trigger occurs and the replacement preserves the PRD and verification gates.

| ID | Decision and why | Alternatives considered | Pinned by | Revisit trigger |
| --- | --- | --- | --- | --- |
| KD-001 | Target the exact vendored commit and name it in releases; “latest compatible” is not testable. | Floating main; latest release tag. | [PRD](00-openfga-prd.md), [verification](72-compatibility-testing-verification-plan.md) | Upstream baseline upgrade. |
| KD-002 | Keep a simple correctness-first Check evaluator permanently so optimization always has a stable oracle. | Replace oracle after optimization; only upstream integration tests. | [Check](14-check-engine-design.md), [performance](71-performance-design.md) | Never remove; implementation may be simplified. |
| KD-003 | Parse wire strings once into private-field newtypes/enums to prevent malformed states and repeated parsing. | Raw strings throughout; validate at each use. | [domain](10-domain-model-design.md), [storage](13-storage-design.md) | Wire grammar changes. |
| KD-004 | Compile immutable models before publication and cache by `(store, model)` to move validation/graph work off queries. | Interpret source per request; mutable compiled graph. | [compiler](12-model-compiler-design.md), [cache](16-cache-consistency-design.md) | Mutable model protocol appears upstream. |
| KD-005 | Give Check, ListObjects, ListUsers, and Expand separate algorithms over shared primitives because enumeration is not Boolean evaluation. | One universal evaluator; scan-and-Check. | [Check](14-check-engine-design.md), [list](15-list-queries-design.md) | A proven abstraction preserves every gate with less complexity. |
| KD-006 | Use narrow capability traits composed at server assembly so algorithms require only operations they use. | Giant datastore trait; concrete backend dependencies. | [storage](13-storage-design.md), [crates](61-workspace-crates-design.md) | Stable plugin ABI demands a different boundary. |
| KD-007 | Use actor ownership for in-memory multi-index storage/controllers to make multi-state transitions atomic without lock choreography. | `Mutex`/`RwLock` maps; DashMap for every index. | [storage](13-storage-design.md), [cache](16-cache-consistency-design.md), [runtime](21-runtime-operations-design.md) | Profiling proves serialized ownership is the bottleneck. |
| KD-008 | Use SQLx and explicit backend-specific hot queries because query shapes/indexes are load-bearing. | ORM; handwritten driver layer. | [storage](13-storage-design.md), [ecosystem research](../docs/research/survey-rust-ecosystem.md) | SQLx cannot support a required backend/feature. |
| KD-009 | Commit tuple mutation and changelog append in one transaction; cache and ReadChanges correctness depend on it. | Best-effort/outbox after commit; periodic full scan. | [storage](13-storage-design.md), [cache](16-cache-consistency-design.md) | Never while changelog is the consistency signal. |
| KD-010 | Use Tonic/Prost gRPC plus explicit Axum HTTP adapters to share Tower policy while preserving exact HTTP behavior. | Generic transcoder; gRPC-only server. | [transport](20-api-transport-design.md), [ecosystem research](../docs/research/survey-rust-ecosystem.md) | Generated transcoding proves equivalent and simpler. |
| KD-011 | Own CEL compiler/evaluator traits and select implementation after Phase 0 because pure-Rust baseline compatibility is unproven. | Bind directly to cel-interpreter; C++ FFI; omit conditions. | [conditions](11-condition-engine-design.md), [Phase 0](91-implementation-impl-plan.md) | Phase 0 selects/rejects an adapter or upstream semantics change. |
| KD-012 | Forbid unsafe in all project crates; soundness/repository policy outweigh speculative fast paths. | Audited local unsafe; port Go raw-pointer planner. | [security](70-security-design.md), [developer experience](60-developer-experience-design.md) | Explicit repository policy change only. |
| KD-013 | Bound and join all concurrency to prevent hostile fan-out and teardown leaks. | Detached task per tuple; only global pool caps. | [Check](14-check-engine-design.md), [runtime](21-runtime-operations-design.md) | Bounds may be tuned, never removed. |
| KD-014 | Higher consistency bypasses mutable tuple-derived caches and reads primary to prevent stale authorization decisions. | TTL-only staleness; replica barrier without proof. | [storage](13-storage-design.md), [cache](16-cache-consistency-design.md) | A stronger revision/coherence protocol is proven. |
| KD-015 | Separate cache namespaces/policies by query shape to avoid key/policy ambiguity. | One shared first-writer-configured iterator cache. | [cache](16-cache-consistency-design.md) | Unified cache proves deterministic policy and better results. |
| KD-016 | Production requires OIDC or preshared authentication; disabled auth is explicit loopback development only. | Upstream-style optional auth default; network-position trust. | [security](70-security-design.md), [runtime](21-runtime-operations-design.md) | Threat model changes through security review. |
| KD-017 | Pin Rust `1.97.1`/edition 2024 and centralize reviewed features for reproducibility. | Unpinned stable; per-crate versions/features. | [developer experience](60-developer-experience-design.md), [ecosystem research](../docs/research/survey-rust-ecosystem.md) | Toolchain/dependency upgrade phase. |
| KD-018 | Optimize behind strategy traits, shadow comparison, kill switch, and rollback because authorization correctness dominates latency. | Replace oracle in place; benchmark-only graduation. | [Check](14-check-engine-design.md), [performance](71-performance-design.md), [verification](72-compatibility-testing-verification-plan.md) | Graduation thresholds may tighten, controls remain. |
| KD-019 | Ship OpenFGA v1 GA before AuthZEN to keep the primary scope independently verifiable. | Include AuthZEN in first GA; omit permanently. | [PRD](00-openfga-prd.md), [roadmap](90-delivery-roadmap.md) | Product priority changes with schedule re-baseline. |
| KD-020 | Version, authenticate, scope, and bound continuation tokens to prevent tamper, replay, and parser abuse. | Plain serialized cursor; backend-native opaque token. | [domain](10-domain-model-design.md), [transport](20-api-transport-design.md), [security](70-security-design.md) | Exact upstream token compatibility documents an exception. |

## Rejected alternatives

- A monolithic `core` crate: encourages cycles and lets transport/storage types leak into semantics.
- A giant datastore mock interface: weakens interface segregation and makes algorithm tests brittle.
- CEL through C++ FFI by default: adds unsafe/native supply-chain surface contrary to project policy.
- Blind port of upstream adaptive raw-pointer atomics: unsafe and premature.
- Treating errors as deny: hides outages and changes union/intersection semantics.
- Running ListObjects by scanning every object and calling Check: correct only at unacceptable, unbounded cost.
