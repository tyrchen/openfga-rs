# DynamoDB operations runbook

Status: preview until the real-AWS graduation evidence in `specs/17-dynamodb-storage-design.md` is complete.

## Deployment contract

The backend owns one regional DynamoDB table with a string partition key named `pk` and a binary
sort key named `sk`. It has no secondary indexes or Streams and never calls `Scan`. Deploy exactly
one writable Region per table. Global Tables and DAX are not supported.

Select the backend with `storage.backend: dynamodb`. The complete non-secret YAML surface is shown
in `config/openfga-development.yaml`; credentials are deliberately absent. The AWS SDK default
credential chain supplies short-lived workload credentials. `endpoint` is accepted only in the
development profile and only for a literal loopback HTTP address.

Production defaults should be reviewed explicitly:

- `maximumInFlight` bounds all SDK work; start at 64 and size it below the deployment's aggregate
  request concurrency.
- `attemptTimeoutMs`, `operationTimeoutMs`, and `maximumAttempts` bound Smithy retries. Keep the
  attempt timeout below the operation timeout and the service request deadline above both.
- `maximumTupleMutations` is fixed within `1..=49`; 50 is rejected before dispatch.
- cleanup interval, grace period, batch size, and shutdown timeout control the supervised durable
  generation collector. Never use DynamoDB TTL as a replacement for this actor.
- `garbageCollectionMaximumWorkLagMs` degrades readiness when the latest successful collector pass
  sees older overdue work; set it above the cleanup interval and alert before it is reached.
- `assertionRollbackRetentionMs` must remain strictly greater than the maximum public request
  deadline so a reader that resolved the previous assertion generation drains before collection.
- `kmsKeyIdentifier`, `pointInTimeRecovery`, `deletionProtection`, and `tags` are provisioning-role
  controls. Keep ownership/cost tags nonempty and use deletion protection for production tables.
- leave `provisionOnStart: false` in production. Provision with a separate control-plane identity.

## Provision and validate

With the provisioning role active:

```sh
cargo run -p openfga-server -- migrate --config /absolute/path/openfga.yaml up
cargo run -p openfga-server -- migrate --config /absolute/path/openfga.yaml status
```

`up` creates only the exact configured table, using on-demand billing, the configured tags,
deletion protection and encryption, enables PITR when configured, then writes immutable schema
metadata. `status` strongly reads the metadata and fails for absent, transitioning, or incompatible
schema. Normal startup validates the same metadata but does not alter the table unless the explicit
development-only `provisionOnStart` setting is enabled.

Apply the runtime and provisioning examples in `deploy/aws/` only after replacing the account,
Region, and table placeholders. The runtime policy intentionally denies control-plane operations
and grants no `Scan`. A separate organization/SCP or permissions boundary should explicitly deny
`dynamodb:Scan`, table deletion, and policy mutation to the runtime role.

## Encryption, deletion protection, and recovery

Provision with deletion protection, PITR, and either the AWS-owned DynamoDB key or an approved
customer-managed KMS key. The runtime role does not need direct plaintext key material.
The KMS key policy must allow DynamoDB in the selected account/Region while denying unrelated
principals. Record the table ARN, key ARN, PITR status, deletion-protection status, and latest restore
drill in deployment evidence.

Restore to a new table; never overwrite the live table:

1. Stop writers or fence them at the deployment layer and record the last observed change ID.
2. Restore PITR to a unique allowlisted table name in the same Region.
3. Run `migrate status` against the restored table. An incompatible result is a hard stop.
4. Run the storage contract and offline forward/reverse verifier, then sample models, assertions,
   changes, and authorization decisions with higher consistency.
5. Point a canary at the restored table, then roll replicas. Do not run two writable tables.
6. Retain the old table until cache convergence, audit comparison, and rollback windows close.

## Readiness and incident response

Readiness strongly checks table state and schema metadata. Treat these stable states separately:

- `dynamodb_table_missing`: configuration/Region/account error or an unauthorized deletion;
- `dynamodb_table_transitioning`: wait for the control-plane transition within the deployment
  deadline;
- `dynamodb_schema_incompatible`: stop rollout and inspect metadata; never rewrite it manually;
- `dynamodb_gc_lag_exceeded`: cleanup is making insufficient progress; reduce write pressure,
  inspect throttling/capacity, and increase collector throughput before serving more traffic;
- operation timeout/throttling: reduce admission, inspect consumed capacity and hot partitions,
  then raise capacity or timeouts only with evidence;
- tuple peer/digest/blob integrity error: stop writes, preserve CloudTrail and backups, run the
  offline verifier, and restore if out-of-band modification is confirmed.

The cleanup actor is supervised and joined during drain. A failed pass is retried from durable
sharded work records. Alert on repeated cleanup failures, growing work age, throttling, transaction
conflicts, p95/p99 operation duration, and readiness failures. Logs and metrics contain stable codes,
operation labels, and shard-independent counts; they never contain tuples, keys, tokens, endpoints,
or credentials.

## Verification

Local development:

```sh
make dynamodb-storage-rustack
make dynamodb-api-rustack
```

Authoritative AWS verification requires an authenticated workload identity and explicit opt-in:

```sh
OPENFGA_DYNAMODB_AWS_TEST=1 \
OPENFGA_DYNAMODB_TEST_TABLE=openfga-aws-test-contract \
AWS_REGION=us-west-2 \
make dynamodb-storage-aws
```

The AWS test treats `OPENFGA_DYNAMODB_TEST_TABLE` as a prefix, appends a cryptographically random
suffix, requires the generated name to be absent, and deletes only the exact table it created after
the contract. Rustack proves fast API wiring and transactions, but it does not prove
AWS IAM, KMS, PITR, strong-consistency, throttling, rollback, or idempotency behavior. Those remain
release blockers until captured in the DynamoDB graduation report.
