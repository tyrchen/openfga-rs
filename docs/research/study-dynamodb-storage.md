# Study: DynamoDB storage constraints and Rustack test coverage

Status: Done · Owner: openfga-rs · Date: 2026-08-09 · Vendor pin: `vendors/rustack` @ `ab8bc61a3e45058c7d42de8443f9d215cc110b18` (`v0.9.1`)

## Why this study

This study answers one design question: **which DynamoDB physical model and test stack can implement the existing `openfga-storage` contracts without weakening atomic tuple/changelog writes, `HigherConsistency`, bounded pagination, or failure behavior?** The result feeds [`../../specs/17-dynamodb-storage-design.md`](../../specs/17-dynamodb-storage-design.md).

The upstream OpenFGA study already established the semantic requirements: narrow storage capabilities, purpose-built forward/reverse access paths, atomic tuple/changelog mutation, higher-consistency cache bypass, and stable pagination. This study does not revisit those decisions. It evaluates DynamoDB and Rustack against them.

Sources were checked on 2026-08-09. AWS limits and SDK versions are current-service facts and must be rechecked before implementation.

## Architecture map

```text
openfga-storage capability call
            │
            ▼
┌──────────────────────────────────────────────────────────────┐
│ openfga-storage-dynamodb                                     │
│                                                              │
│  codec + key encoder ─▶ bounded query planner ─▶ k-way merge │
│           │                         │                        │
│           └──────────────┬──────────┘                        │
│                          ▼                                   │
│       conditional TransactWriteItems + ClientRequestToken    │
└──────────────────────────┬───────────────────────────────────┘
                           │ AWS JSON 1.0 / SigV4
                ┌──────────┴───────────┐
                ▼                      ▼
      ┌──────────────────┐   ┌────────────────────────┐
      │ Local fast gate  │   │ Authoritative gate     │
      │ Rustack v0.9.1   │   │ real regional DynamoDB │
      │ query/conditions │   │ consistency, retries,  │
      │ transaction shape│   │ limits, atomic failure │
      └──────────────────┘   └────────────────────────┘
```

Rustack is a useful local protocol implementation, not an AWS semantic oracle. The test strategy therefore has two levels rather than silently treating an emulator pass as proof of cloud behavior.

## AWS constraints that determine the design

### Transactions and item size

AWS currently limits `TransactWriteItems` to 100 unique items and 4 MiB, forbids two actions against the same item, and limits an item—including attribute names—to 400 KiB. Transactional writes cost twice the normal write units because DynamoDB performs prepare and commit work. Transactions are ACID only in the Region where they originate; global-table replicas can observe partial propagation. Sources: [DynamoDB constraints](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html), [transactions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html), and [read/write consumption](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html).

Consequences:

1. One logical tuple needs a forward item and a reverse item if both access paths must be strongly queryable. One packed changelog item and one per-store change-head action leave `2N + 2 <= 100`, so the backend maximum is **49 tuple mutations**.
2. Transaction and item size are validated locally before dispatch. Count alone is insufficient.
3. Models and assertion snapshots need chunked immutable blobs with a committed manifest; the SQL codec permits payloads far larger than one DynamoDB item. A commit transaction cannot update every chunk because their aggregate size would exceed 4 MiB, so only small manifest/HEAD/cleanup metadata participates in visibility transactions.
4. The first supported topology is one writable Region. Global tables are not an acceptable implementation of `HigherConsistency`.

### Read consistency and indexes

Base-table and local-secondary-index queries can be strongly consistent. Global secondary indexes are updated asynchronously and support eventual reads only. An LSI also imposes a 10 GiB limit for every partition-key item collection. Sources: [read consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html), [secondary-index comparison](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html), and [LSI limits](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html).

The design therefore rejects a single tuple item plus a reverse GSI: it would make reverse `HigherConsistency` impossible. It also rejects a store-wide partition plus reverse LSI: the 10 GiB item-collection ceiling and a single hot store key would become product limits. Two transactionally maintained base-table records preserve strong reads and distribute forward and reverse traffic independently.

### Pagination, capacity, and retries

`Query` returns at most 1 MiB per call; `LastEvaluatedKey` must be supplied as `ExclusiveStartKey`, and a nonempty last key does not prove that another matching item exists after filtering. A query `Limit` applies before filter evaluation. Sources: [query pagination](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.Pagination.html) and [query behavior](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.Other.html).

A physical partition is bounded at roughly 3,000 read units and 1,000 write units per second; adaptive capacity cannot remove a single-key ceiling. Source: [partition-key guidance](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html). Fixed write shards and bounded fan-out are therefore schema, not tuning trivia.

The AWS SDK for Rust defaults to the standard retry strategy with three total attempts and jitter. It has no default operation or attempt timeout, so the backend must configure both and intersect them with `OperationContext` deadlines. Adaptive mode can delay initial attempts and is not the default choice. Sources: [SDK retries](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/retries.html) and [SDK timeouts](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/timeouts.html).

`TransactWriteItems.ClientRequestToken` makes a request idempotent for ten minutes. The same token and different parameters are rejected. The backend must reuse a token only for an identical retry after an ambiguous transport outcome; an optimistic change-head conflict builds a new transaction and token. Source: [transaction idempotency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html).

## Current Rust SDK choice

The crates.io versions resolved on 2026-08-09 are `aws-sdk-dynamodb 1.120.0`, `aws-config 1.10.1`, `aws-smithy-runtime 1.12.1`, and `aws-smithy-http-client 1.2.0`; the first two declare Rust 1.94.1 and therefore fit the repository's Rust 1.97.1 pin. `aws-smithy-http-client` exposes the explicit `rustls-aws-lc` feature, so the implementation can satisfy the repository's crypto-provider rule without enabling its Ring feature.

Adopt the low-level generated DynamoDB client. An ORM or single-table helper would obscure conditional expressions, exact consumed-capacity reporting, request-token reuse, cancellation reasons, and the project-owned cross-shard pagination contract.

## Rustack architecture and hot path

Rustack v0.9.1 exposes the required protocol operations—table management, item CRUD, `Query`, `BatchGetItem`, and transactional get/write—in one operation enum (`vendors/rustack/crates/rustack-dynamodb-model/src/operations.rs:7`). Its query model includes index name, key/filter expressions, `ExclusiveStartKey`, `Limit`, and `ConsistentRead` (`vendors/rustack/crates/rustack-dynamodb-model/src/input.rs:332`). Its transaction model accepts `ClientRequestToken` (`vendors/rustack/crates/rustack-dynamodb-model/src/input.rs:573`). Existing integration tests drive `Query` and transactions through the official AWS Rust SDK (`vendors/rustack/tests/integration/src/test_dynamodb.rs:349`, `vendors/rustack/tests/integration/src/test_dynamodb.rs:792`).

The pinned repository declares the MIT license at its workspace root. It is a development/test submodule, not a linked production dependency; its license is compatible with this project's Apache-2.0 distribution policy and its notices remain in the vendored checkout.

The dominant Rustack query path parses the key expression, extracts partition/sort constraints, queries its ordered in-memory table storage, applies filters after selection, and returns a last-evaluated key (`vendors/rustack/crates/rustack-dynamodb-core/src/provider.rs:1536`). This matches the APIs needed by the proposed no-secondary-index physical model. The pinned core suite passed locally on 2026-08-09: 133 unit tests, zero failures, including ordered binary/string keys, range/prefix queries, pagination, item sizing, and condition parsing/evaluation.

The repository's `rustack-cli` package exposes a `rustack` binary with a DynamoDB-only feature build. Its documented `SERVICES=dynamodb` and `GATEWAY_LISTEN` settings let the Make target bind an isolated loopback port without Docker or globally installed tools.

## Rustack gaps that shape verification

Rustack stores LSI declarations at table creation (`vendors/rustack/crates/rustack-dynamodb-core/src/provider.rs:570`), but indexed queries resolve only `gsi_definitions` (`vendors/rustack/crates/rustack-dynamodb-core/src/provider.rs:1483`). The proposed production schema deliberately needs neither GSI nor LSI, so this gap does not block local contract tests.

Three gaps prevent Rustack from being the only acceptance environment:

1. `ClientRequestToken` is modeled but not read by the DynamoDB provider; source search finds it only in the model and an unimplemented planning note. Rustack cannot prove ambiguous-result idempotency.
2. Transaction conditions are evaluated before a sequential application loop (`vendors/rustack/crates/rustack-dynamodb-core/src/provider.rs:2477`). An application-stage error can therefore occur after prior writes, unlike AWS rollback semantics.
3. `ConsistentRead` is accepted in the wire model but the in-memory provider has no replica/eventual-consistency behavior. Rustack cannot prove consistency routing, GSI lag (unused here), throttling, consumed capacity, IAM, KMS, PITR, or regional behavior.

These are test-scope limitations, not reasons to fork Rustack or weaken production behavior.

## What we will adopt

1. One DynamoDB table with only base `pk`/`sk` keys; no `Scan`, GSI, LSI, or Streams dependency.
2. Transactionally duplicated forward and reverse tuple items, fixed deterministic shards, and one packed changelog batch plus change-head action per nonempty mutation.
3. Strong base-table reads for `HigherConsistency`; eventual base-table reads for `MinimizeLatency`.
4. A versioned, NUL-terminated memcomparable binary key codec whose validated segments cannot
   contain NUL, whose forward and reverse suffixes retain canonical tuple ordering, and whose bytes
   remain under DynamoDB key-size limits.
5. Chunked model/assertion blobs with a checksum-bearing staged manifest, a small atomic visibility transition, and durable sharded garbage-collection records. Chunk correctness and discovery do not depend on asynchronous DynamoDB TTL.
6. Standard SDK retries, explicit attempt/operation deadlines, a backend semaphore, idempotent identical transaction retries, and typed redacted error mapping.
7. Rustack for fast local official-SDK and storage-contract tests; a disposable real DynamoDB table for authoritative transaction, consistency, capacity/error, IAM, PITR, restore, and durable-cleanup evidence.

## What we will avoid

1. **Reverse GSI as authority:** it cannot satisfy strong reverse reads.
2. **Store-wide Scan:** it is unbounded in cost and incompatible with the existing query budgets.
3. **DynamoDB Streams as changelog:** stream reads are eventual and tuple state plus changelog would no longer commit under one application-visible transaction.
4. **BatchWriteItem for semantic writes:** it lacks the conditional all-or-nothing contract and can return unprocessed items.
5. **Adaptive retry mode by default:** initial requests may be delayed and its per-resource client-pooling assumptions are easy to violate.
6. **Long-lived AWS keys in YAML:** use the SDK credential chain/workload identity; local endpoints receive explicit test credentials only.
7. **Emulator-only support claims:** Rustack exercises protocol shape and happy-path algorithms but does not establish AWS failure semantics.

## Decision

**GO with a two-tier verification stack.** DynamoDB can implement the complete storage capability set within explicit backend limits: at most 49 tuple mutations, bounded encoded bytes, one writable Region, fixed schema shards, and no store-wide scans. Rustack v0.9.1 is suitable for local integration tests of the operations the design uses, but real DynamoDB evidence is mandatory before the backend is advertised.

## Risks identified

- A packed change batch may hit 400 KiB before the tuple-count limit; validate exact encoded size and return `ResourceExhausted` before any write.
- The per-store change head serializes mutation transactions. Benchmark the declared ceiling and alert on head-conflict retry rate; do not claim unlimited per-store write scaling.
- Cross-shard merge/pagination is correctness-sensitive. Model it independently and property-test no duplicates/omissions under page boundaries, filtering, concurrent readers, and cancellation.
- Chunk-manifest cleanup must never delete committed/active data. Claim expired generations conditionally, retain replaced assertions beyond the maximum operation deadline, and test every failure point around staging, commit, HEAD replacement, retirement, and idempotent deletion.
- Rustack may add missing behavior after this pin. Refresh the memo rather than encoding version-specific workarounds in production.
