# Product requirements: openfga-rs

Status: Proposed · Audience: product, platform, security, and implementation teams

## Product statement

openfga-rs is a production-grade Rust implementation of the OpenFGA authorization server. Existing OpenFGA clients and models should work without application changes while operators gain a memory-safe, resource-bounded, observable service with first-class PostgreSQL, MySQL, SQLite, and in-memory deployments. A separately gated DynamoDB extension becomes an advertised backend only after the M7–M8 delivery in [`17-dynamodb-storage-design.md`](17-dynamodb-storage-design.md).

The compatibility baseline is the vendored OpenFGA commit recorded in [`../docs/research/study-openfga-implementation.md`](../docs/research/study-openfga-implementation.md). A release MUST name its upstream baseline and MUST NOT claim compatibility beyond tested behavior.

## Users and jobs

- Application developers ask Check/BatchCheck and enumeration questions through existing OpenFGA SDKs.
- Authorization-model authors publish, inspect, and test immutable models and assertions.
- Platform operators deploy, migrate, scale, observe, back up, and upgrade the service.
- Security teams require authenticated administration, store-scoped authorization, auditability, and fail-closed limits.
- Contributors need deterministic builds and a small set of explicit engine and storage contracts.

## Required capabilities

The GA product MUST provide:

1. OpenFGA v1 gRPC and HTTP/JSON behavior for store, model, assertion, tuple, changelog, Check, BatchCheck, Expand, ListObjects, StreamedListObjects, and ListUsers APIs.
2. Authorization-model schema `1.1`, including direct relations, computed usersets, tuple-to-userset, union, intersection, difference, typed wildcards, usersets, contextual tuples, and conditions.
3. Immutable model publication with deterministic validation and latest-model selection.
4. In-memory and SQL storage with atomic tuple/changelog mutation, stable pagination, and higher-consistency reads.
5. Bounded execution: request deadlines, body and collection limits, recursion and dispatch budgets, bounded storage I/O, bounded queues, and joined tasks.
6. OIDC and preshared-key authentication, store-scoped service authorization, TLS, secret redaction, and an explicit loopback-only unauthenticated development mode.
7. Structured logs, traces, metrics, readiness/liveness, graceful shutdown, schema migration, and YAML configuration with environment overrides.
8. Compatibility evidence produced by the verification plan, including differential execution against the vendored Go server.

AuthZEN compatibility is a post-GA compatibility milestone. It MUST reuse the same service and policy core, but MUST NOT delay or weaken OpenFGA v1 compatibility.

DynamoDB compatibility is a post-GA backend milestone. It MUST preserve the same logical storage and authorization semantics in one writable Region. It MAY advertise a lower write-batch limit where DynamoDB imposes a hard transaction ceiling, but MUST preserve all-or-nothing mutation/changelog behavior and cannot graduate on emulator-only evidence.

## Success criteria

- All applicable upstream API and model test vectors pass for every supported backend.
- Differential tests find no unexplained decision, error-code, pagination, or ordering mismatch against the pinned upstream server.
- Cancellation and fault-injection tests show no leaked evaluator tasks, database rows, connections, or cache actors.
- Security limits reject hostile inputs before business logic and production startup rejects unauthenticated configuration.
- The documented performance budgets are met on the declared reference environment without semantic shortcuts.
- Operators can migrate from a supported OpenFGA schema snapshot through a documented, tested procedure.
- DynamoDB operators can provision, validate, back up, restore, capacity-test, and run the complete API with workload identity and least-privilege IAM; emulator-only evidence is insufficient.

## Compatibility policy

Wire compatibility includes field presence, defaults, validation timing, status/error mapping, continuation-token scope, streaming termination, and consistency behavior—not only successful decision values. Response ordering is guaranteed only where the upstream contract guarantees it; tests compare sets otherwise.

Upstream changes enter through a deliberate baseline update: update the submodule, inventory protocol/model/storage changes, update specs and decisions, regenerate fixtures, and run the full compatibility gate.

## Non-goals

- A new authorization language or a wire-incompatible ergonomic API.
- A distributed SQL database, globally coherent cache, policy editor, control-plane UI, or client SDK suite.
- Cross-Region strongly consistent DynamoDB/global-table semantics, DAX, or DynamoDB Streams as an application changelog.
- Reproducing Go implementation details that are not observable contracts.
- Shipping experimental weighted/adaptive algorithms on the authoritative path before differential and shadow gates pass.
- Allowing user-provided CEL programs, tuple sets, or requests to consume unbounded CPU, memory, storage reads, or tasks.

## Release boundaries

- **Experimental:** no compatibility promise; used for Phase 0 evidence.
- **Preview:** API shape frozen to a named upstream pin; migrations may require operator action.
- **GA:** compatibility and migration policy enforced; security/operations gates mandatory.

The milestone outcomes are defined in [`90-delivery-roadmap.md`](90-delivery-roadmap.md); engineering order is defined separately in [`91-implementation-impl-plan.md`](91-implementation-impl-plan.md).
