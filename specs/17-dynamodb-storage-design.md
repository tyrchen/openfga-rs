# DynamoDB storage design

Status: Proposed · Owner: openfga-rs · Depends on: [`10-domain-model-design.md`](10-domain-model-design.md), [`13-storage-design.md`](13-storage-design.md), [`16-cache-consistency-design.md`](16-cache-consistency-design.md)

## 1. Purpose and scope

`openfga-storage-dynamodb` implements every existing `openfga-storage` capability against one Amazon DynamoDB table without changing service or engine semantics. It is an optional durable backend selected only by `openfga-server`; no domain, model, Check, list, cache, service, or transport crate depends on AWS types.

The backend MUST preserve:

- atomic tuple mutation plus ordered changelog visibility;
- exact, forward, userset, reverse, store, model, assertion, and change reads without `Scan`;
- `HigherConsistency` through strongly consistent base-table reads;
- stable bounded pagination across physical shards;
- backend-neutral typed errors, cancellation, deadlines, and redaction;
- the store-namespace lifecycle in [`13-storage-design.md`](13-storage-design.md#backends).

The first release supports one writable AWS account/Region per table. DynamoDB global tables, DAX, Streams-driven changelog construction, cross-table joins, and automatic import from SQL are non-goals because none can strengthen the required single-Region transaction and consistency contract.

## 2. Crate and interface

The new crate implements the unchanged object-safe traits from `openfga-storage`:

```rust
#[non_exhaustive]
#[derive(Clone, Debug, TypedBuilder)]
pub struct DynamoDbStorageConfig {
    pub table_name: DynamoDbTableName,
    pub region: RegionName,
    pub endpoint: Option<DevelopmentEndpoint>,
    pub maximum_in_flight: NonZeroU32,
    pub attempt_timeout: Duration,
    pub operation_timeout: Duration,
    pub maximum_attempts: NonZeroU32,
    pub maximum_conflict_retries: NonZeroU32,
    pub maximum_tuple_mutations: DynamoDbMutationLimit,
    pub garbage_collection: DynamoDbGarbageCollectionConfig,
}

#[non_exhaustive]
#[derive(Debug)]
pub struct DynamoDbStorage { /* private AWS client, codec, and semaphore */ }
```

`DynamoDbMutationLimit` accepts `1..=49`; its default is 49. Table, Region, and development-endpoint values are private-field newtypes validated at configuration conversion. The optional endpoint is accepted only in the development profile, must use an IPv4/IPv6 loopback literal over HTTP, and must contain no userinfo, query, or fragment; it is used by Rustack. Production uses normal AWS endpoint resolution and HTTPS.

The crate uses `aws-sdk-dynamodb`, `aws-config`, and the Smithy HTTP client with explicit Tokio and `rustls-aws-lc` features. It uses the AWS default credential/Region provider chain unless the YAML supplies an explicit Region. Credentials are never fields of `DynamoDbStorageConfig`, never printable, and never project-managed refresh state.

No new storage trait is introduced. Backend testability comes from pure key/item/planner validators,
the local official-SDK contract, and a private unit-only dispatch fault injector that can fail an
operation immediately before dispatch or after a successful response (the unknown-commit case).
The fault surface MUST NOT escape the crate or exist in production builds.

The fallible constructor returns the shared storage capabilities plus an opaque `DynamoDbRuntime` lifecycle handle. Application supervision starts, health-checks, stops, and joins that handle; dropping it requests shutdown but is not the normal completion path. This keeps cleanup ownership explicit without leaking AWS client types or adding lifecycle methods to semantic storage traits.

## 3. Physical architecture

One table uses a string partition key `pk` and binary sort key `sk`. It has no LSI or GSI. All production calls are `GetItem`, `BatchGetItem`, `Query`, conditional `PutItem`/`UpdateItem`/`DeleteItem`, `TransactWriteItems`, and control-plane health/provisioning operations. Application code MUST NOT issue `Scan`.

```text
                                      one regional DynamoDB table
┌──────────────────────────────────────────────────────────────────────────────────┐
│ pk (String) + sk (Binary)                                                        │
│                                                                                  │
│  ┌──────────────────────────┐      ┌──────────────────────────┐                  │
│  │ F#store#object-shard     │      │ R#store#subject-shard    │                  │
│  │ sk: object/relation/user │      │ sk: user/type/rel/object │                  │
│  │ canonical tuple payload │      │ identical tuple payload  │                  │
│  └─────────────┬────────────┘      └─────────────┬────────────┘                  │
│                └──────── atomic mutation ────────┘                               │
│                                  │                                               │
│                    ┌─────────────▼─────────────┐                                 │
│                    │ H#store / change head     │                                 │
│                    │ C#store#change-shard      │                                 │
│                    │ packed ordered changes    │                                 │
│                    └───────────────────────────┘                                 │
│                                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐ │
│  │ S#directoryShard│  │ M#store         │  │ B#kind#store#identity#generation│ │
│  │ active stores   │  │ model manifests │  │ bounded immutable blob chunks   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘ │
│                                                                                  │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │ A#store#model: assertion HEAD + committed generation manifest/chunks       │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│  G#gcShard: durable staging/retirement cleanup work ordered by not-before time    │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### 3.1 Schema constants

The schema version fixes these compile-time constants:

| Constant | v1 value | Reason |
| --- | ---: | --- |
| Tuple forward shards | 32 | Bound store-wide listing fan-out while spreading object keys. |
| Tuple reverse shards | 32 | Spread subjects while allowing an exact subject to select one shard. |
| Change shards | 4 | Spread packed batches; bounded four-way ordered merge. |
| Store-directory shards | 16 | Avoid a global directory hot key; bounded listing merge. |
| Garbage-collection shards | 16 | Bound cleanup fan-out without one global write key. |
| Blob chunk payload | 256 KiB | Remain comfortably below the 400 KiB item limit. |
| Model payload | 16 MiB | Preserve the existing persistence-codec ceiling; at most 64 chunks. |
| Assertion payload | 8 MiB | Preserve the existing ceiling; at most 32 chunks. |
| Tuple item | 350 KiB | Leave key/attribute overhead below 400 KiB. |
| Packed change batch | 350 KiB | Leave key/attribute overhead below 400 KiB. |
| Transaction aggregate items | 3.5 MiB local ceiling | Conservatively remain below the 4 MiB service limit. |

Shard counts are data-layout ABI, not runtime tuning. A schema metadata item records the version and counts; startup fails with `schema_incompatible` if compiled constants differ. Changing them requires an explicit online migration with dual-read/dual-write evidence and a new schema version.

### 3.2 Key codec

`pk` prefixes and `sk` bytes are versioned. Tuple sort keys use a terminal-safe conceptual grammar but are encoded as binary:

```text
forward sk = v1 | object_type | object_id | relation | subject_kind | subject_type | subject_id | subject_relation
reverse sk = v1 | subject_kind | subject_type | subject_id | subject_relation | object_type | relation | object_id
```

The first byte is the codec version. Each following validated UTF-8 segment is encoded as its raw
bytes followed by `NUL`; domain identifier allowlists make `NUL` unrepresentable, so the terminator
is unambiguous and the raw byte order remains memcomparable across variable-length fields. Subject
kind is an explicit segment. Reserved minimum/maximum sentinels exist only in query-bound builders
and cannot be produced by a domain value. The encoding MUST:

- preserve one deterministic byte order on every architecture;
- permit exact keys and `begins_with` ranges for every access pattern;
- remain at most 896 bytes under the maximum `InputLimits`, reserving enough of the 1,024-byte `StorageCursor` ceiling for cursor version/operation/length framing;
- round-trip every valid tuple and reject noncanonical or oversized persisted bytes;
- use SHA-256 of the canonical object or subject encoding, truncated to the shard mask, so shard selection is stable across releases.

The forward and reverse records contain the same versioned tuple payload, insertion timestamp, and payload digest. A mutation pre-read validates both copies before classifying the tuple; any missing or mismatched peer is `Integrity`. Ordinary one-direction queries reject malformed observed records but do not double their read cost merely to prove that the unqueried peer exists. An offline verifier therefore samples or exhaustively compares both copies and is required after restore or suspected out-of-band modification.

### 3.3 Item families and access paths

| Logical data | Partition/sort key | Read path |
| --- | --- | --- |
| Forward tuple | `F#store#sha256(object)%32` / forward key | Exact Get; object/relation Query; store-wide 32-way Query+merge. |
| Reverse tuple | `R#store#sha256(subject)%32` / reverse key | One object-type/relation-prefixed Query per exact subject; exact object restrictions use BatchGet. |
| Change head | `H#store` / fixed `head` | Strong Get plus conditional transaction Update. |
| Change batch | `C#store#sha256(lastChangeId)%4` / last change ID | Four-way Query+merge; flatten changes after cursor. |
| Store directory | `S#sha256(store)%16` / store ID | Exact Get; 16-way Query+merge for ListStores. |
| Model manifest | `M#store` / model ID | Exact Get; descending Query for latest/list. |
| Blob chunk | `B#kind#store#identity#generation` / chunk number | strong/eventual BatchGet after manifest. |
| Assertion HEAD | `A#store#model` / fixed `head` | Get current committed generation. |
| Assertion manifest | `A#store#model` / generation ID | Resolve state/checksum/chunk metadata after HEAD. |
| Garbage-collection work | `G#sha256(generation)%16` / not-before time + identity | Query due work; idempotently delete unreachable blob partitions. |
| Schema metadata | fixed deployment key | Strong Get at startup/readiness. |

Filter expressions MAY reduce returned bytes but never establish security, identity, ordering, or correctness. Because DynamoDB applies `Limit` before filters, planners track both emitted items and evaluated items. Each public method has a finite evaluated-item/query/byte ceiling derived from existing request budgets; it may return an empty page with a continuation when filters consumed a physical page.

## 4. Tuple read behavior

`HigherConsistency` sets `ConsistentRead=true` on every Get, BatchGet, and Query. `MinimizeLatency` sets it to false; existing mutable cache policy remains authoritative over whether such results may be cached. No method silently falls back from strong to eventual reads.

| Capability | Plan |
| --- | --- |
| `read_exact_tuple` / `tuple_exists` | Compute the forward shard and Get the exact forward key. |
| `read_object_relation` | Query one forward shard by object/relation prefix; use BatchGet for a nonempty exact-subject allowlist. |
| `read_userset_tuples` | Query the object/relation prefix and apply bounded subject-kind/type/relation and condition filters while paging. |
| `read_reverse_tuples` | Query one reverse shard per distinct subject using its object-type/relation prefix; use BatchGet when object IDs are supplied; deduplicate and merge by the shared subject-then-object ordering. |
| `count_object_relation` | Use strong/eventual `Select=COUNT` Query pages when no client filter is needed; otherwise count decoded bounded matches. |
| `read_tuples` | Always use canonical forward order: one shard when the full object is present, otherwise all 32 forward shards with the longest available prefix and bounded filtering. Subject-only filters do not switch to reverse order because that would change pagination semantics. |

Multi-branch reads use a bounded `JoinSet` owned by the caller operation and acquire the backend semaphore per AWS request. Cancellation or deadline drops outstanding SDK futures, joins local tasks, and returns the existing typed error. A `TupleStream` is constructed only after a complete bounded owned snapshot has been decoded, preserving current close/drop behavior.

Every BatchGet loop explicitly retries returned `UnprocessedKeys` with bounded jitter inside the same operation deadline. It accepts only requested keys, rejects duplicate/malformed response keys, and returns `Unavailable` rather than treating exhausted unprocessed keys as absence.

BatchGet planners deduplicate keys and issue at most 100 unique keys per call. Any subject/object-ID Cartesian product uses checked multiplication and must fit the operation's configured key/read/byte budget before dispatch; it returns `ResourceExhausted` instead of creating unbounded request batches.

## 5. Stable sharded pagination

Sharded tuple, store, and model pages use `DynamoCursorV1 { operation, last_canonical_key }`, encoded within `StorageCursor` and then protected/scoped by the existing transport token. `ChangeReader` preserves the shared `StorageCursor(ChangeId bytes)` convention required by `PageOptions::after_change_id`; its single validated ID is also the canonical merge boundary. Neither form contains an AWS credential, table ARN, raw `LastEvaluatedKey`, or tuple payload.

```text
shard/subject queries          merge owner                 Page<T>
       │                           │                          │
       │ 1. Query after translated │                          │
       │    canonical boundary ───▶│                          │
       │                           │                          │
       │ 2. bounded sorted runs ──▶│                          │
       │                           │ 3. validate + k-way merge│
       │                           │    dedupe physical copies│
       │                           │                          │
       │                           │ 4. emit <= page limit ──▶│
       │                           │    cursor = safe boundary│
       │                           │                          │
       │ 5. no emitted match but   │                          │
       │    evaluated progress ───▶│ 6. empty page + progress│
```

Every paginated branch preserves the logical order in its sort key: forward tuples use the full canonical tuple key, store shards use store ID, change shards use change ID, and models use model ID. The same exclusive logical boundary can therefore be encoded as the sort-key lower bound on every relevant shard. Reverse tuple reads return bounded snapshots rather than a continuation and use their separate subject-first order.

The merge owns one matching lookahead or one evaluated physical frontier per live branch. It never emits a candidate beyond the smallest unresolved branch frontier. When a filter yields no match before the per-call evaluation budget, every non-exhausted branch must have advanced and the continuation is the minimum of their last evaluated canonical keys; faster branches may be re-read on the next page, but no candidate is skipped. A full page uses its last emitted key; a fully exhausted merge has no continuation. Results at or below the cursor are rejected defensively. Property tests MUST prove no duplicate or omission for every page size, shard boundary, asymmetric branch progress, filter shape, empty filtered page, and cancellation point.

DynamoDB's `LastEvaluatedKey` is consumed only inside one SDK-query loop. It is never exposed as the project cursor, because a full page can span many physical requests and branches.

## 6. Atomic tuple mutation and changelog protocol

One mutation contains at most 49 distinct tuple keys and passes the documented DynamoDB item-size calculation before network I/O. The calculation includes attribute names, keys, and current/resulting values for every action—including ConditionChecks—and enforces a conservative 3.5 MiB aggregate-item ceiling below AWS's 4 MiB limit. Expression names/values and the SDK request structure have separate bounded-size checks. Conflicting delete/write keys are rejected before the pre-read.

```text
Caller                 DynamoDbStorage                    DynamoDB table
  │                           │                                  │
  │ 1. write_tuples ─────────▶│                                  │
  │                           │ 2. strong BatchGet F + R state   │
  │                           │─────────────────────────────────▶│
  │                           │◀─────────────────────────────────│
  │                           │ 3. classify apply/no-op/conflict │
  │                           │    validate encoded byte limits  │
  │                           │                                  │
  │                           │ 4. strong Get change HEAD ──────▶│
  │                           │◀─────────────────────────────────│
  │                           │ 5. allocate strictly greater IDs │
  │                           │                                  │
  │                           │ 6. TransactWriteItems ──────────▶│
  │                           │    - conditional F actions       │
  │                           │    - matching R actions          │
  │                           │    - no-op condition checks      │
  │                           │    - conditional HEAD update     │
  │                           │    - packed change batch         │
  │                           │    - ClientRequestToken          │
  │                           │                                  │
  │                           │ 7a. committed ◀──────────────────│
  │◀──────────────────────────│     ordered ChangeIds            │
  │                           │                                  │
  │                           │ 7b. HEAD/state conflict ◀────────│
  │                           │     re-read/reclassify/retry      │
  │                           │                                  │
  │                           │ 7c. ambiguous transport failure  │
  │                           │     retry identical request/token│
```

### 6.1 Optimistic classification

A strong BatchGet reads both forward and reverse records for every requested key (at most 98 items) and first verifies that the pair is either absent or byte-identical in tuple identity/digest. A one-sided or mismatched pair returns `Integrity`; it is never silently overwritten or repaired. Valid pairs are then classified:

- applied write: conditional Put forward (`attribute_not_exists`) plus unconditional Put reverse;
- applied delete: conditional Delete forward (existence and expected payload digest) plus Delete reverse;
- ignored duplicate write: only when the requested payload digest equals the existing tuple; otherwise return `Conflict`. ConditionCheck that the same forward digest still exists;
- ignored missing delete: ConditionCheck that the forward record remains absent;
- error-policy duplicate/missing: return `Conflict` without a write.

If a condition check fails because state changed after classification, the whole request is re-read and rebuilt within `maximumConflictRetries` and the original deadline. An all-no-op request executes only its condition checks and returns an empty `MutationOutcome`; it does not advance the changelog.

### 6.2 Change ordering

Every nonempty mutation strongly reads a per-store HEAD. Applied deletes precede applied writes and each group is in canonical tuple-key order, matching the shared contract. The allocator forms a CSPRNG ULID at the injected clock, takes the greater of that value and HEAD, then advances the checked 128-bit value once per applied tuple. This produces a contiguous lexicographic set strictly greater than HEAD; overflow is `Integrity`. All tuple/change timestamps use the same injected transaction clock. The transaction conditionally advances HEAD with the last ID while writing tuple records and one packed change batch. A competing writer loses the HEAD condition and retries from the new head. Gaps after abandoned pre-dispatch allocations are harmless; committed IDs are strictly increasing across server replicas.

The batch sort key is its last change ID and its payload stores ordered individual `TupleChange` records. `read_changes(after)` queries all four change shards for batches with `lastChangeId > after`, flattens only individual IDs greater than `after`, merges them, and applies object-type/start-time filters. Its continuation is the last emitted or evaluated individual change ID, so `PageOptions::after_change_id` remains valid.

### 6.3 Idempotency and error classification

Each built transaction receives a CSPRNG-generated client request token. An SDK/transport timeout with unknown commit status retries byte-identical actions with the same token within the request deadline and AWS ten-minute idempotency window. A semantic/head conflict rebuilds the request with a new token. Tokens are structured tracing values only as hashes and are never returned or logged raw.

`TransactionCanceledException` cancellation reasons are mapped by action position:

- tuple error-policy condition → `Conflict` with the validated tuple attached;
- optimistic no-op or HEAD condition → internal retry, then `Conflict` if the finite retry budget is exhausted;
- throttling/capacity/transient service error → `Unavailable` after SDK retries;
- item/transaction size → pre-dispatch `ResourceExhausted`; an AWS disagreement is `Integrity`;
- access denied, missing table, wrong Region, or incompatible schema → `Unavailable` with a stable readiness code;
- corrupt item/checksum/key/version → `Integrity`;
- elapsed deadline/caller signal → `Timeout`/`Cancelled`.

AWS messages, request bodies, keys, tuple values, endpoints, account IDs, and credentials never cross `StorageError` display/debug or tracing fields.

## 7. Models and assertions larger than one item

The versioned persistence codec currently owned by `openfga-storage-sql` moves to a backend-neutral module in `openfga-storage`. SQL and DynamoDB use the same model, tuple, condition-context, and assertion envelopes; moving it MUST preserve codec v1 bytes and SQL compatibility.

### 7.1 Immutable model publication

1. Encode and checksum the model; split it into at most 64 chunks.
2. In one small transaction, conditionally create a `STAGING` manifest and a durable garbage-collection record with a bounded not-before time.
3. Put each immutable chunk without a DynamoDB TTL in a two-action transaction: a ConditionCheck that the manifest is still the exact `STAGING` generation plus a conditional chunk Put. This prevents a late uploader from recreating chunks after the collector claims the generation. Ambiguous retries reuse the identical request token; an already-present chunk is accepted only after a strong byte/digest match.
4. After every chunk is durable, use one small idempotent transaction to change the manifest to `COMMITTED` and delete its garbage-collection record, guarded by manifest state/checksum. The transaction never touches chunk items and therefore remains below 4 MiB.
5. Readers/list/latest queries ignore non-committed manifests. Decode requires exact chunk count, total bytes, SHA-256 checksum, codec version, and identity.

A failed publication leaves an unreachable `STAGING` generation plus durable cleanup work; no partial payload is visible. DynamoDB TTL is not used for blob correctness or discovery because deletion is asynchronous and cannot cascade from a manifest to its chunks.

### 7.2 Atomic assertion replacement

Assertions use immutable generation manifests/chunks plus an assertion HEAD pointer. Replacement stages a new generation, then one transaction:

- commits the new manifest;
- conditionally moves HEAD from the previously read generation;
- deletes the new generation's staging cleanup record;
- marks the old manifest `RETIRED` and creates a retirement cleanup record after the configured rollback window.

This transaction contains only small metadata items, independent of payload size. A HEAD race retries against the new generation. Assertion readers always resolve HEAD and its manifest/chunks strongly, then read only the immutable `COMMITTED` generation named by HEAD; this prevents an eventual stale HEAD from racing retirement cleanup. Empty assertions are a committed zero-chunk generation, not absence/error.

### 7.3 Durable garbage collection

A supervised, backend-owned cleanup task queries due work from all 16 garbage-collection shards under its own semaphore share and deadline. For each record it strongly reads the manifest and assertion HEAD. Before deleting data, one transaction claims the manifest as `DELETING`: expired `STAGING` is conditionally changed so a publisher can no longer commit it; `RETIRED` is changed only with a simultaneous condition that HEAD does not name that generation. The assertion retirement delay exceeds the maximum storage-operation deadline, so a reader that resolved the formerly active generation has drained before deletion begins. The collector then queries the generation's blob partition, issues bounded idempotent `DeleteItem` calls, deletes the manifest, and conditionally deletes the exact cleanup record. A crash at any step leaves the cleanup record retryable; an already-`DELETING` or missing manifest with the same internal work identity resumes orphan deletion, while a stale record for a committed/active generation is removed without touching data.

Every manifest/HEAD/cleanup transaction follows the same byte-identical `ClientRequestToken` unknown-outcome rule as tuple transactions.

The task has explicit start, shutdown, join, and restart behavior in application supervision. Interval, batch size, concurrency, staging age, and assertion rollback retention are bounded validated YAML values. Cleanup lag/bytes/items/failures are observable; lag never changes read correctness, but exceeding the configured storage-leak alert threshold makes readiness degraded until an operator restores cleanup or runs the same idempotent collector through a Makefile-owned maintenance target.

## 8. Stores, deletion, and schema lifecycle

Store creation conditionally puts one active directory record. Rename conditionally updates name/timestamp. `list_stores` queries all 16 directory shards, applies an exact-name filter after decoding, and merges by ascending store ID. Delete removes only the directory record and is idempotent; tuple/model/assertion/change namespace data remains, exactly as required by the pinned OpenFGA lifecycle.

The server commands are:

- `migrate status`: DescribeTable plus strong metadata validation; no mutation;
- `migrate up`: create the initial table or apply an explicitly versioned forward migration under a conditional schema lease;
- `validate-config`: validate names/Region/endpoint/timeouts/limits without AWS calls.

Production `migrateOnStart` remains opt-in. Initial provisioning supports on-demand billing by default, AWS-owned or customer-managed KMS encryption, PITR, deletion protection, and required tags. Runtime startup does not silently modify billing, KMS, PITR, deletion protection, IAM, or tags.

Readiness requires table `ACTIVE`, readable compatible metadata, and a bounded strong Get. Liveness is process-only. Backup/restore graduation uses PITR into a new table, verifies metadata/manifests/checksums and representative semantic reads, then changes the configured table name through a bounded restart; in-place destructive restore is not supported.

## 9. Security and operational boundary

### 9.1 Runtime IAM

The runtime role is scoped to the exact table ARN and may use only `DescribeTable`, `GetItem`, `BatchGetItem`, `Query`, `PutItem`, `UpdateItem`, `DeleteItem`, and `TransactWriteItems`. It has no `Scan`, `CreateTable`, `DeleteTable`, backup/restore, IAM, or wildcard-resource permission. The migration/provisioning role is separate and minimally adds required table/tag/PITR/deletion-protection operations.

If a customer KMS key is selected, key policy grants only the DynamoDB service/runtime path required by AWS and the operator backup/restore path. Table names, Region, endpoint, account, KMS key IDs, and ARNs are bounded configuration/telemetry values; none is accepted from an OpenFGA request.

### 9.2 Capacity and observability

The backend semaphore caps total AWS requests, including fan-out and retries. Metrics have bounded labels (`operation`, `consistency`, `result`, `retry_class`) and include request/attempt latency, consumed RCU/WCU when returned, throttles, unprocessed BatchGet keys, transaction cancellations, HEAD retries, evaluated/emitted items, shard fan-out, encoded bytes, blob chunks, garbage-collection lag/work/failures, and readiness state. Identifiers and physical keys are forbidden labels.

The per-store HEAD is an intentional serialization point that establishes changelog order. Capacity documentation MUST treat its hot-key ceiling as a backend limit, measure it, and alert before sustained throttling. No claim above the measured per-store mutation rate is made.

## 10. Testing and verification

### 10.1 Deterministic/unit layer

- key/shard/codec property tests over every valid boundary and arbitrary hostile bytes;
- transaction planner/model tests for conflict classification, 49/50 limits, the 100-action
  structural ceiling, 400 KiB/3.5 MiB/4 MiB boundaries, and ambiguous outcomes;
- k-way pagination model tests for no duplicates/omissions and bounded progress;
- blob/garbage-collection state-machine tests at every stage/commit/HEAD/cleanup failure;
- error classification plus before/after-dispatch unknown-outcome tests through the private fault
  injector;
- the unchanged shared storage contract plus expanded model/store/assertion/change contracts.

### 10.2 Rustack local layer

`make dynamodb-storage-rustack` builds the pinned submodule with `cargo build --manifest-path vendors/rustack/Cargo.toml -p rustack-cli --no-default-features --features dynamodb`, starts its `rustack` binary with `SERVICES=dynamodb` and `GATEWAY_LISTEN=127.0.0.1:<reserved-port>`, waits on health, creates an isolated table in the fresh emulator, runs the storage contract through the official AWS Rust SDK, and always stops/joins the process. `make dynamodb-api-rustack` separately runs the full two-replica server scale/consistency/drain smoke against the same kind of isolated emulator. Tests use explicit dummy credentials and the loopback endpoint override. They cover primary-key Query pagination, empty filtered pages, conditional transactions, packed changes, chunks, garbage collection, namespace/directory lifecycle, and API wiring. They do not claim IAM, AWS consistency, idempotency, throttling, rollback-on-application-failure, PITR, or KMS evidence.

### 10.3 Real AWS layer

`make dynamodb-storage-aws` requires an explicit opt-in environment and workload identity, resolves one unique allowlisted table name, provisions through the test role, and deletes only that table after artifacts are captured. The future graduation environment uses OIDC and a dedicated account/Region with a cost/concurrency ceiling. Graduation proves:

- strong read-after-write for forward, reverse, changes, HEAD, models, assertions, and stores;
- conditional transaction rollback, concurrent HEAD ordering, identical-token idempotency, timeout recovery, and error classification;
- count/item/transaction size rejection and filtered 1 MiB pagination behavior;
- throttling/retry/timeout/cancellation bounds and consumed-capacity metrics;
- least-privilege runtime IAM denial of `Scan` and every unneeded control-plane or wildcard-resource operation;
- interrupted garbage-collection recovery without active-generation deletion, KMS/PITR status, backup/restore to a new table, and restored semantic checks.

The DynamoDB backend is not advertised until the Rustack gate, real-AWS gate, full OpenFGA differential/API suite, 30-minute load/soak, cache invalidation across two server processes, and independent review all pass on the same release source.

## 11. Performance budgets

On the declared AWS test environment after warmup and excluding deliberate admission shedding:

- direct `HigherConsistency` exact tuple read: p95 ≤ 20 ms;
- direct `HigherConsistency` Check with a warm model and one tuple read: p95 ≤ 35 ms;
- forward/reverse single-branch read of ≤100 returned tuples: p95 ≤ 30 ms;
- 49-tuple mutation including changelog: p95 ≤ 100 ms without throttling;
- unfiltered store-wide tuple page: at most 32 shard queries and configured concurrency;
- read/write amplification and consumed capacity are reported per workload, not hidden in latency;
- a 30-minute bounded soak returns tasks/permits to baseline and grows RSS by ≤64 MiB excluding configured caches.

Budgets are initial engineering gates, not public AWS-wide SLOs. The release report records Region, table mode/capacity, item sizes, network placement, SDK versions, concurrency, retries, p50/p95/p99, RCU/WCU, throttles, and cost estimate.

## 12. Engineering norms

| AGENTS.md area | Binding rule |
| --- | --- |
| Error Handling | Per § Error Handling: library `thiserror` sources remain redacted behind `StorageError`; application assembly adds `anyhow::Context`. |
| Async & Concurrency | Per § Async & Concurrency: Tokio, owned bounded `JoinSet` fan-out, semaphore admission, cancellation/deadline on every SDK call, and an explicitly supervised cleanup task with start/shutdown/join/restart behavior. Mutable authorization state remains externally transactional. |
| Type Design & API | Per § Type Design & API: private validated newtypes, `TypedBuilder` for config, `TryFrom`/`FromStr`, non-exhaustive public types, safe `Debug`. |
| Safety & Security | Per § Safety & Security: `forbid(unsafe_code)`, checked sizes/arithmetic, boundary validation, aws-lc rustls, credential-chain secrets, least-privilege IAM, no Scan/SSRF. |
| Serialization & Data | Per § Serialization & Data: shared versioned Serde envelopes; DynamoDB attributes are private physical representation; decode validates version, identity, checksum, count, and byte ceilings immediately. |
| Testing | Per § Testing: same-file unit tests, `test_should_` names, property/fault tests, Rustack integration, and authoritative real-AWS tests. |
| Logging & Observability | Per § Logging & Observability: structured `tracing`, bounded labels, redacted keys/tuples/tokens/credentials, spans around logical capability calls rather than every item. |
| Performance | Per § Performance: profile and record capacity before tuning; preallocate known action/chunk counts; bounded borrowed encoding where practical; no speculative unsafe/inline. |
| Documentation | Per § Documentation: module/public-item docs, examples, `# Errors`, config/runbook/index updates, and no public API without compile-tested documentation. |

## 13. Acceptance criteria

- Every `openfga-storage` capability passes the shared and DynamoDB-specific contract on Rustack and real DynamoDB.
- No runtime code calls `Scan`, depends on GSI/LSI/Streams, or accepts a non-loopback custom endpoint in production.
- Concurrent replicas never commit a tuple without its reverse record and changelog, never publish nonmonotonic changes, and never expose a partial blob generation.
- `HigherConsistency` uses strong reads for every mutable access path and produces zero stale authorization results after completed writes.
- Pagination and filtered progress have no duplicates/omissions and remain within cursor/query/item/byte budgets.
- Every SDK error, retry, timeout, cancellation, unknown commit, corruption, and cleanup failure maps to a tested typed outcome without sensitive output.
- IAM, KMS, PITR, restore, readiness, load/soak, cross-process cache invalidation, differential API, dependency audit/deny, and release evidence pass before support is advertised.

## 14. Cross-references

- ← Depends on: [`10-domain-model-design.md`](10-domain-model-design.md), [`13-storage-design.md`](13-storage-design.md), [`16-cache-consistency-design.md`](16-cache-consistency-design.md)
- → Consumed by: [`21-runtime-operations-design.md`](21-runtime-operations-design.md), [`61-workspace-crates-design.md`](61-workspace-crates-design.md), [`70-security-design.md`](70-security-design.md), [`71-performance-design.md`](71-performance-design.md), [`72-compatibility-testing-verification-plan.md`](72-compatibility-testing-verification-plan.md)
- ↔ Research: [`../docs/research/study-dynamodb-storage.md`](../docs/research/study-dynamodb-storage.md), [`../docs/research/study-openfga-implementation.md`](../docs/research/study-openfga-implementation.md)
- ↔ Prior art: Rustack operations at `vendors/rustack/crates/rustack-dynamodb-model/src/operations.rs:7`, query path at `vendors/rustack/crates/rustack-dynamodb-core/src/provider.rs:1365`, and transaction path at `vendors/rustack/crates/rustack-dynamodb-core/src/provider.rs:2382`
