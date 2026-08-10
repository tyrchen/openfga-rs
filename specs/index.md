# openfga-rs specification set

Status: Proposed · Baseline: OpenFGA `4e4f79ed841513dfd61746a75ef473f6198299f7`

These documents specify an API-compatible, safe Rust implementation of OpenFGA. Normative words **MUST**, **SHOULD**, and **MAY** have their RFC 2119 meanings. Research evidence is in [`../docs/research/`](../docs/research/).

## Reading order: product and architecture

1. [`00-openfga-prd.md`](00-openfga-prd.md) — product scope, users, success criteria, and non-goals.
2. [`10-domain-model-design.md`](10-domain-model-design.md) — validated identifiers, tuples, requests, and rewrite IR.
3. [`11-condition-engine-design.md`](11-condition-engine-design.md) — CEL compatibility boundary and evaluation rules.
4. [`12-model-compiler-design.md`](12-model-compiler-design.md) — authorization-model validation and immutable compilation.
5. [`13-storage-design.md`](13-storage-design.md) — capability traits, indexes, transactions, consistency, and backends.
6. [`14-check-engine-design.md`](14-check-engine-design.md) — correctness oracle, recursion, reduction, and concurrency.
7. [`15-list-queries-design.md`](15-list-queries-design.md) — ListObjects, StreamedListObjects, ListUsers, and Expand.
8. [`16-cache-consistency-design.md`](16-cache-consistency-design.md) — cache identities, invalidation, and consistency policy.
9. [`17-dynamodb-storage-design.md`](17-dynamodb-storage-design.md) — no-index table layout, transactions, cursors, blob manifests, AWS security, and verification.
10. [`20-api-transport-design.md`](20-api-transport-design.md) — protobuf, gRPC, HTTP/JSON, errors, pagination, and streaming.
11. [`21-runtime-operations-design.md`](21-runtime-operations-design.md) — process lifecycle, configuration, migration, telemetry, and resilience.

## Reading order: delivery and cross-cutting contracts

12. [`60-developer-experience-design.md`](60-developer-experience-design.md) — repository policy, automation, dependency governance, and documentation.
13. [`61-workspace-crates-design.md`](61-workspace-crates-design.md) — crate graph and allowed dependency directions.
14. [`70-security-design.md`](70-security-design.md) — trust boundaries, authentication, authorization, and resource limits.
15. [`71-performance-design.md`](71-performance-design.md) — budgets, measurement, and optimization graduation.
16. [`72-compatibility-testing-verification-plan.md`](72-compatibility-testing-verification-plan.md) — conformance, differential, property, failure, and backend tests.
17. [`80-openfga-glossary.md`](80-openfga-glossary.md) — canonical terminology.
18. [`90-delivery-roadmap.md`](90-delivery-roadmap.md) — stakeholder milestones and release outcomes.
19. [`91-implementation-impl-plan.md`](91-implementation-impl-plan.md) — dependency-ordered engineering phases and exit gates.
20. [`99-key-decisions.md`](99-key-decisions.md) — accepted decisions and revisitation triggers.

## Catalogue

| Range | Type | Authoritative questions answered |
| --- | --- | --- |
| 00 | PRD | Who is the system for, what must it do, and what proves success? |
| 10–17 | Foundation/component designs | What are the domain, CEL, compiler, storage, decision, enumeration, cache, and DynamoDB contracts? |
| 20–21 | Integration/runtime designs | How are those contracts exposed, secured, configured, supervised, and operated? |
| 60–72 | Cross-cutting designs | Which repository, crate, security, performance, and verification gates bind every phase? |
| 80 | Glossary | Which overloaded OpenFGA terms have one project meaning? |
| 90–91 | Delivery contracts | What outcome lands in each milestone, in what engineering order, and with what evidence? |
| 99 | Decisions | Why were load-bearing alternatives accepted or rejected, and what would reopen them? |

## Build-order graph

```text
┌──────────────────┐
│ 00 Product       │
│ scope + measures │
└────────┬─────────┘
         ▼
┌──────────────────┐      ┌────────────────────┐
│ 10 Domain        │─────▶│ 11 CEL boundary    │
│ validated values │      │ compile/evaluate   │
└───────┬──────────┘      └─────────┬──────────┘
        │                            │
        ├────────────────────────────▼──┐
        │ 12 Immutable model compiler   │
        │ IR · validation · reachability│
        └──────────────┬────────────────┘
                       │
┌──────────────────────▼──┐
│ 13 Storage contracts    │
│ transactions + indexes  │
└─────────────┬───────────┘
              ▼
┌─────────────────────────┐       ┌────────────────────────┐
│ 14 Check oracle         │──────▶│ 15 List / Expand       │
│ bounded rewrite engine  │       │ reverse + set engines  │
└─────────────┬───────────┘       └───────────┬────────────┘
              ├──────────────┬─────────────────┘
              ▼              ▼
┌──────────────────┐   ┌──────────────────────────────┐
│ 16 Cache /       │   │ 20 gRPC + HTTP API           │
│ consistency      │   │ auth middleware + streaming  │
└────────┬─────────┘   └──────────────┬───────────────┘
         │                            │
         ├──────────────┐             │
         ▼              │             │
┌──────────────────┐    │             │
│ 17 DynamoDB      │    │             │
│ storage profile  │    │             │
└────────┬─────────┘    │             │
         └──────────────┴─────────────┘
                         ▼
              ┌───────────────────────┐
              │ 21 Runtime / ops      │
              │ actors · config · CLI │
              └───────────┬───────────┘
                          ▼
       ┌────────────────────────────────────────┐
       │ 60/61/70/71/72 cross-cutting gates     │
       │ repository · crates · security · proof │
       └───────────────────┬────────────────────┘
                           ▼
                 ┌──────────────────┐
                 │ 90/91 delivery   │
                 │ M0…M8 / phases   │
                 └──────────────────┘
```

## Traceability

Every implementation phase links to component requirements and verification gates. Every public behavior traces to the vendored protocol or an explicitly documented project policy. Backend-specific profiles, including DynamoDB, must preserve the shared storage semantics and satisfy both their dedicated evidence gates and the common compatibility suite. When code and a proposed spec disagree, the spec is updated or accepted through the decision log before release; compatibility is never changed silently.
