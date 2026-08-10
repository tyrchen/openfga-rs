# Phase 7 DynamoDB preview report

Status: local preview gates passing; authoritative AWS evidence unavailable.

Date: 2026-08-09

## Scope and pins

The implementation uses `aws-sdk-dynamodb 1.120.0`, `aws-config 1.10.1`, and
`aws-smithy-http-client 1.2.0` with an explicit Rustls/AWS-LC client. Local integration uses the
Rustack `v0.9.1` submodule at `ab8bc61a3e45058c7d42de8443f9d215cc110b18`. The backend owns one
regional on-demand table with string `pk` and binary `sk`; it has no indexes or Streams and its
runtime path never calls `Scan`.

## Local evidence

The following gates passed from the same working tree:

- Rustack's DynamoDB core suite: 133 tests;
- `make dynamodb-storage-rustack`: shared tuple contract, strong reads, model and assertion chunk
  publication, assertion generation replacement and retired-generation cleanup, ordered packed
  changes including mid-batch pagination, forward/userset/reverse/count snapshots, store rename and
  directory-only deletion, 49-mutation success, pre-dispatch rejection of 50 mutations, and
  adversarial wrong-shard tuple/store/change rejection;
- `make dynamodb-api-rustack`: two DynamoDB-backed Rust server processes, cross-process changelog
  invalidation, higher-consistency mutation/read sequences, Go/Rust Check, BatchCheck,
  ListObjects, ListUsers, model publication/load, tuple-write/changelog reference workloads,
  bounded load, drain, and the existing Criterion gate;
- focused unit/property tests for endpoint/table/Region/provisioning validation, versioned
  memcomparable forward/reverse keys and cursors, tuple/checksum codecs, strict packed-changelog
  order, bounded blob layout/allocation, transaction byte boundaries, cancellation classes,
  before/after-dispatch ambiguity, table scalar types, and every GC manifest decision;
- two independent code-review passes followed by fixes for namespace lifecycle, sharded planner
  bounds, canonical physical identity, corrupt blob/work/change handling, transaction accounting and
  retry classification, assertion retention, fair garbage collection, provisioning, and bounded
  operational telemetry;
- strict Clippy on the DynamoDB crate and server touched surface. AWS SDK futures are deliberately
  exempted from `large_futures`; boxing every generated SDK future would add one allocation per
  request without changing the object-safe storage trait futures.

The first full-API run found two actionable defects: the generic scale fixture wrote 100 tuples in
one request although DynamoDB's atomic limit is 49, and 100 concurrent writers contended on the
per-store changelog HEAD. The fixture now batches at 49, the API advertises the configured DynamoDB
limit, and semantic transaction conflicts use bounded cancellation/deadline-aware jittered retry.
The repeated gate passed.

## Production-evidence boundary

The local AWS credential prerequisite check reported an expired session (`aws login` is required),
so the explicit real-AWS target was not dispatched. Consequently this report does **not** claim IAM
denial behavior, KMS, PITR/restore,
AWS strong-consistency and idempotency behavior, throttling/capacity classification, the declared
AWS latency/cost matrix, or the 30-minute AWS soak. The compatibility matrix therefore keeps
`dynamodb` in preview and excludes it from the advertised production backend table. Those items
remain mandatory Phase 8 promotion gates; Rustack evidence cannot waive them.

## Reproduction

```sh
make dynamodb-storage-rustack
make dynamodb-api-rustack
```

After authenticating a dedicated AWS test identity and selecting a uniquely prefixed table:

```sh
OPENFGA_DYNAMODB_AWS_TEST=1 \
OPENFGA_DYNAMODB_TEST_TABLE=openfga-aws-test-contract \
AWS_REGION=us-west-2 \
make dynamodb-storage-aws
```

The AWS target treats the configured value as an allowlisted prefix, appends a random suffix,
requires the exact generated table to be absent, and deletes only that table after the contract.
