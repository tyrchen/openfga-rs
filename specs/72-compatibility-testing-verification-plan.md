# Compatibility and testing verification plan

Status: Proposed · Verifies: all component specs

## Test layers

1. **Domain unit/property/fuzz:** parsers, newtypes, tokens, limits, redaction, and canonical round trips over arbitrary bytes.
2. **Compiler/condition fixtures:** valid/invalid upstream models, graph metadata, CEL types/functions/cost/unknown/error behavior, deterministic fingerprints.
3. **Engine unit/model:** exhaustive reducer truth/error tables, direct/computed/TTU/wildcard/condition/cycle/depth behavior, set algebra, cancellation, and budgets.
4. **Storage contract:** one reusable suite against memory, PostgreSQL, MySQL, SQLite, and DynamoDB; transactions, conflicts, ordering, pagination, consistency routing, stream cleanup, and migrations/provisioning. DynamoDB runs both a pinned Rustack local profile and an authoritative real-AWS profile.
5. **Service/transport golden:** protobuf/JSON, routes, defaults, validation, status/error bodies, middleware order, pagination, BatchCheck correlation, and streaming.
6. **Differential:** send the same normalized corpus to the vendored Go binary and Rust server; compare decisions, result sets, error classifications, metadata where public, pagination behavior, and terminal stream behavior.
7. **Official-client/end-to-end:** supported OpenFGA SDK smoke and scenario suites over gRPC and HTTP for every backend profile.
8. **Security/resilience:** auth/authz, hostile boundaries, rate/size/depth limits, fault injection, actor restart, outages, cancellation, and graceful shutdown.
9. **Performance:** Criterion microbenchmarks, load/soak/query-plan tests under [`71-performance-design.md`](71-performance-design.md).

## Upstream corpus governance

Fixtures copied or transformed from `vendors/openfga` record source path, pinned commit, license notice, and transformation. Prefer invoking upstream suites/server through a Makefile harness to reduce drift. Updating the vendor pin produces an inventory diff before expected outputs change.

Differential cases include every endpoint and model feature, malformed boundary cases, condition values, contextual tuples, consistency settings, cycles, errors under injected storage failure, duplicate/missing writes, pages/tokens, client cancellation, and stream limits. Nondeterministic ordering is normalized only when the protocol does not guarantee order.

## Generated semantic testing

Bounded generators create valid models, tuples, contexts, and queries. Independent reference set evaluation is used for small acyclic universes. Core properties:

- oracle and optimized Check outcomes/errors match;
- union/intersection/difference laws hold where error-free;
- ListObjects equals Check-filtered bounded universe;
- ListUsers membership agrees with Check for enumerable concrete subjects, with explicit wildcard rules;
- reachability rejection never removes an allow;
- adding unrelated tuples does not change a decision;
- higher consistency observes completed writes according to the transaction contract.

Every failure records a reproducible seed and minimized model without secrets.

## Failure and concurrency matrix

Inject failure before/during/after each storage read, iterator page, condition evaluation, task spawn/join, cache access/invalidation, changelog commit, response send, and shutdown stage. Test decisive sibling cancellation versus real errors. Repeated tests assert pool permits, actor/task counts, channels, and memory return to baseline.

Use Tokio paused time for deadlines/backoff, Loom for small reducer/actor state machines where valuable, and real runtime stress for stream/database behavior. Tests do not rely on arbitrary sleeps.

For DynamoDB, a private API fault fake covers every SDK boundary deterministically; Rustack covers official-SDK protocol/query/condition/transaction integration on loopback; real DynamoDB alone proves strong consistency, AWS transaction rollback/idempotency, service size/page behavior, throttling, IAM, KMS, PITR, restore, and interrupted durable garbage collection. Rustack gaps are recorded in [`../docs/research/study-dynamodb-storage.md`](../docs/research/study-dynamodb-storage.md) and MUST NOT be waived as “emulator differences.” Real-AWS tests use OIDC, a dedicated account/Region, unique allowlisted table names, cost/concurrency ceilings, and exact-target cleanup.

## Phase/release gates

- A phase passes only its linked spec acceptance criteria plus relevant regression suites.
- A backend cannot be advertised until the shared contract, migrations/provisioning, differential suite, and fault suite pass. DynamoDB additionally requires Rustack and real-AWS gates, two-process invalidation, load/soak, PITR restore, and least-privilege IAM evidence.
- An optimization cannot default on until the graduation gate passes with zero unexplained mismatches.
- GA requires all applicable upstream, differential, official-client, security, migration, shutdown, and performance gates on the exact release artifacts.
- Any unexplained authorization mismatch is release-blocking. Quarantined tests require an owner, written root cause, and non-GA status; flaky retries do not constitute a pass.

## Required reports

CI/release artifacts include upstream pin, proto pin, toolchain/dependency lock, backend matrix, test seeds, conformance diff summary, ignored/quarantined inventory, security/audit results, migration matrix, and benchmark environment/results.

## Acceptance criteria

- Each normative MUST in the spec set maps to at least one automated test or an explicit inspectable release control.
- Fresh-checkout Makefile commands reproduce all non-secret test stages.
- Differential harness reports field-level mismatches without logging sensitive contexts.
- Test coverage includes success, typed error, cancellation, limit, and cleanup behavior for each public operation.
