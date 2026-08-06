# Storage design

Status: Proposed · Depends on: [`10-domain-model-design.md`](10-domain-model-design.md)

## Capability interfaces

Consumers depend on the smallest object-safe async capability: `TupleReader`, `TupleWriter`, `ModelReader`, `ModelWriter`, `StoreReader`, `StoreWriter`, `AssertionReader`, `AssertionWriter`, `ChangeReader`, and `HealthCheck`. Dynamic server assembly composes `Arc<dyn ... + Send + Sync>`; modules document the `async-trait` exception required for object safety. Static helpers use native async trait methods where possible.

Tuple readers expose semantic methods, not free-form filters:

- exact tuple lookup;
- read by object and relation with optional subject restrictions;
- read userset tuples;
- reverse read starting with bounded subject/object-type filters;
- existence/count operations used by validated fast paths.

Results are a project-owned fallible `TupleStream` whose `close` is idempotent. Dropping or cancelling it releases database rows and pool connections. APIs accept a deadline/cancellation context, explicit consistency, and bounded page/read options.

## Mutation transaction

`write_tuples(store, deletes, writes, options)` MUST in one transaction:

1. validate caps and reject a key appearing in conflicting operations;
2. sort canonical tuple keys to produce deterministic lock order;
3. lock/read affected keys;
4. enforce configurable duplicate-write/missing-delete policy;
5. delete, insert, and append ordered changelog rows;
6. commit atomically or roll back everything.

Read-committed is the baseline isolation when deterministic key locking prevents lost updates. Backend-specific tests exercise concurrent overlapping writes. Change IDs are monotonic ULIDs; tuple and changelog timestamps use the same injected transaction clock.

## Logical schema and indexes

The canonical tuple key is `(store_id, object_type, object_id, relation, subject_kind, subject_type, subject_id, subject_relation)`, with condition name/context stored outside identity exactly as the upstream contract requires. Binary/case-sensitive collation is mandatory.

Every SQL backend provides equivalent indexes for:

- primary/exact tuple key;
- `(store, object_type, object_id, relation, subject...)` forward reads;
- `(store, subject..., object_type, relation, object_id)` reverse reads;
- userset subject queries;
- changelog `(store, change_id)` ordering;
- models `(store, model_id)`, latest-model ordering, assertions, and stores.

Query-plan fixtures verify hot queries use intended indexes at representative cardinalities. Migrations are versioned, checksum-verified, forward-only in production, and transactional where the backend permits. The server refuses a schema newer than it understands and reports an actionable older-schema readiness failure.

## Backends

- **Memory:** one supervised actor owns all maps, forward/reverse indexes, models, assertions, and changelog. Commands use bounded channels; snapshot reads are returned as owned bounded batches. A mutation updates all indexes and changelog in one actor turn.
- **PostgreSQL:** primary GA backend with optional read replica. Higher consistency always selects primary. Pool size, statement timeouts, and server semaphores are coordinated.
- **MySQL and SQLite:** compatibility backends with backend-specific SQL/migrations; SQLite writes are serialized and documented for embedded scale.

No backend returns database-specific errors across the storage boundary. `StorageError` distinguishes not found, already exists, conflict, invalid continuation, timeout, unavailable, integrity, and internal failures, preserving a redacted source.

A canonical store ID also acts as a data namespace independently of the store-directory record.
Models, assertions, tuples, and changes can be persisted and read without a prior CreateStore.
DeleteStore is idempotent and removes or hides only the directory record; namespace data remains
available, matching the pinned OpenFGA lifecycle contract. Backends therefore do not foreign-key
namespace tables to the store-directory table.

## Pagination and consistency

Pagination uses stable canonical sort keys plus versioned integrity-protected tokens. Tokens bind store, operation, normalized filter fingerprint, backend-independent cursor, and expiry. Decoded bytes and field sizes are bounded before allocation. Invalid, expired, cross-store, or cross-filter tokens are errors.

`HigherConsistency` routes mutable reads to the primary and carries through every nested evaluator read. `MinimizeLatency` may use replicas subject to configured lag policy. Immutable model-by-ID reads may use replicas/caches under either preference after publication durability.

## Acceptance criteria

- Shared contract tests pass unchanged against all four backends.
- Fault injection proves tuple/changelog atomicity and resource release on every failure/cancellation point.
- Pagination has no duplicates/omissions across static datasets and rejects token replay across scopes.
- Concurrent mutation tests prove deterministic conflict/ignore semantics and absence of deadlocks.
- Query plans and pool-use tests catch missing indexes and leaked streams.

## Engineering norms

All repository `AGENTS.md` engineering sections bind storage crates. The object-safe `async-trait` boundary is the documented exception to native async traits; backend failures use `thiserror` with redacted sources, SQL is parameterized, all I/O is timed/cancellable, streams are owned and closed, observability is structured, and public fallible APIs document `# Errors`. Serialization rules apply to persisted canonical records and tokens, never raw database-specific values.

## Cross-references

- ← Depends on: [`10-domain-model-design.md`](10-domain-model-design.md)
- → Consumed by: [`14-check-engine-design.md`](14-check-engine-design.md), [`15-list-queries-design.md`](15-list-queries-design.md), [`16-cache-consistency-design.md`](16-cache-consistency-design.md), [`21-runtime-operations-design.md`](21-runtime-operations-design.md)
- ↔ Research: [`../docs/research/study-openfga-implementation.md`](../docs/research/study-openfga-implementation.md), [`../docs/research/survey-rust-ecosystem.md`](../docs/research/survey-rust-ecosystem.md)
- ↔ Prior art: capability interfaces in `vendors/openfga/pkg/storage/storage.go:144` and PostgreSQL atomic write path in `vendors/openfga/pkg/storage/postgres/postgres.go:584`
