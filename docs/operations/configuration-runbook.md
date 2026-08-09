# Server configuration runbook

This runbook covers configuration validation, secret injection, deployment, and rollback for
`openfga-server`. The checked-in examples are
[`openfga-development.yaml`](../../config/openfga-development.yaml) and
[`openfga-preshared-development.yaml`](../../config/openfga-preshared-development.yaml).

> The project is not yet production-ready. A production profile enables the implemented safety
> checks, but it is not a GA support statement.

## Safety invariants

- YAML is the canonical configuration format. Unknown fields, invalid ranges, files over 1 MiB,
  and invalid UTF-8 are rejected.
- `profile: production` always requires TLS, even on loopback. Any non-loopback listener also
  requires TLS.
- `auth.mode: disabled` is accepted only with `profile: development` and two loopback listeners.
- Secret values are never stored in YAML. YAML names the environment variables from which the
  runtime loads them.
- TLS files use absolute paths without `..`. A supervised owner reloads a complete validated
  certificate/key pair atomically for both listeners; malformed replacements retain the active
  identity. Other referenced secrets, authorization policy, and OIDC bootstrap state require a
  coordinated restart.
- `storage.backend: memory` is volatile. Use PostgreSQL for any data that must survive restart.
- PostgreSQL starts only against the exact schema version embedded in the binary unless
  `migrateOnStart` is explicitly enabled.

## Production baseline

Start from a repository-owned file and keep environment-specific values in deployment
configuration. This example uses PostgreSQL, TLS, and preshared authentication:

```yaml
profile: production
listeners:
  http: 0.0.0.0:8080
  grpc: 0.0.0.0:8081
tls:
  enabled: true
  certificatePath: /run/openfga/tls/tls.crt
  privateKeyPath: /run/openfga/tls/tls.key
  reloadIntervalSeconds: 30
storage:
  backend: postgres
  memory:
    actorCapacity: 256
  postgres:
    primaryUrlEnv: OPENFGA_DATABASE_URL
    replicaUrlEnv: OPENFGA_DATABASE_REPLICA_URL
    maxConnections: 32
    minConnections: 4
    acquireTimeoutMs: 5000
    statementTimeoutMs: 5000
    replicaMaxLagMs: 1000
    maxTupleMutations: 100
    migrateOnStart: false
cache:
  model:
    sourceWeight: 67108864
    compiledWeight: 134217728
    latestAliases: 10000
    immutableTtlSeconds: 604800
    latestAliasTtlSeconds: 10
  decision:
    weight: 16777216
    ttlSeconds: 10
  tuple:
    weight: 134217728
    maximumResults: 10000
    ttlSeconds: 10
  controller:
    channelCapacity: 1024
    pageSize: 100
    pollIntervalMs: 1000
    readTimeoutMs: 1000
    maximumLagMs: 10000
auth:
  mode: preshared
  preshared:
    keys:
      - id: deployment-operator
        keyEnv: OPENFGA_PRESHARED_KEY
  authorization:
    bindings:
      - principal: deployment-operator
        actions: [check, batchCheck, read]
        stores: [01ARZ3NDEKTSV4RRFFQ69G5FAV]
transport:
  defaultPageSize: 50
  requestTimeoutMs: 5000
  tokenTtlSeconds: 86400
  maximumMessageBytes: 1048576
  maximumConcurrency: 64
  tokenKeyId: primary
  tokenKeyEnv: OPENFGA_TOKEN_KEY
  tokenVerificationKeys:
    - id: prior
      keyEnv: OPENFGA_TOKEN_KEY_PRIOR
  admission:
    windowSeconds: 60
    authenticationAttempts: 20000
    authenticationFailures: 2000
    globalAuthenticationAttempts: 200000
    globalAuthenticationFailures: 20000
    administration: 1000
    reads: 10000
    writes: 2000
    checks: 20000
    enumeration: 1000
evaluator:
  depth: 25
  dispatches: 10000
  datastoreQueries: 100
  tupleItems: 10000
  conditionCost: 100000
  concurrentReads: 8
  batchConcurrency: 8
telemetry:
  logFormat: json
  logFilter: info
  otlpEndpoint: https://otel-collector.internal:4317
  exportTimeoutMs: 5000
shutdown:
  drainTimeoutMs: 10000
  healthIntervalMs: 1000
```

Omit `replicaUrlEnv` when there is no replica. Latency-preferring reads use a configured replica
only while its replay lag is within `replicaMaxLagMs`; otherwise they conservatively use the
primary.

Omit `telemetry.otlpEndpoint` to disable trace and metric export. When configured, it must identify
an OTLP gRPC collector; route the collector's metric pipeline to the Prometheus datasource used by
the checked-in dashboard and alerts.

Authentication attempt and failure limits apply independently to each direct TCP peer IP. The
global authentication limits are separate emergency ceilings across all peers. Forwarded client-IP
headers are deliberately ignored; a trusted reverse proxy must enforce its own original-client
limits because this service only trusts the socket peer address.

## Secret inventory

| Reference | Required material | Rotation effect |
| --- | --- | --- |
| `storage.postgres.primaryUrlEnv` | PostgreSQL URL for the writable primary | New process/pool required |
| `storage.postgres.replicaUrlEnv` | Optional read-only replica URL | New process/pool required |
| `transport.tokenKeyEnv` | Standard-base64 encoding of the active 32–64-byte signing key | New tokens use this key ID |
| `transport.tokenVerificationKeys[].keyEnv` | Standard-base64 encoding of prior 32–64-byte keys | Existing tokens remain valid during a bounded overlap window |
| `auth.preshared.keys[].keyEnv` | 32–256 ASCII-graphic bytes from a CSPRNG-backed secret | Follow the overlap procedure in the authentication runbook |

Environment-reference names must be 1–128 bytes, begin with an uppercase ASCII letter, and
contain only uppercase ASCII letters, digits, and underscores. Inject actual secret values through
the deployment secret manager. Do not place them in command arguments, generated effective
configuration, logs, tickets, or shell history.

## Settings and limits

| Area | Important constraints |
| --- | --- |
| PostgreSQL | `maxConnections` is 1–65,536; `minConnections` cannot exceed it; acquisition and statement timeouts are 1 ms–5 minutes; tuple mutations are 1–5,000 |
| Model cache | source/compiled byte weights are positive and each at most 512 MiB; immutable TTL is at most 30 days; the mutable latest alias is at most 5 minutes and is bypassed by higher-consistency reads |
| Mutable caches | decision/tuple byte weights are positive and each at most 512 MiB; TTL is at most 24 hours; tuple entries hold at most 100,000 rows; higher-consistency reads bypass and do not populate either cache |
| Aggregate cache | configured source, compiled, decision, tuple, and estimated latest-alias capacity is at most 1 GiB per process |
| Cache controller | channel capacity also bounds tracked stores; page size is positive and ≤1,000; poll/read/lag are ≤5 minutes; maximum lag bounds four sequential changelog reads and cannot exceed shutdown drain timeout; overflow, detectable gap/order fault, timeout, lag, failure, and restart conservatively flush and disable mutable entries until a healthy poll |
| TLS | reload interval is 1–3,600 seconds; each complete pair is bounded and validated before one atomic publication to HTTP and gRPC |
| Transport | page size is 1–100; timeout is 1 ms–5 minutes; token TTL is 1 second–720 hours; message size is 1–16 MiB; concurrency is 1–65,536 and, for PostgreSQL, ≤4× pool size; admission rates are 1–1,000,000 per 1–3,600-second window |
| Evaluator | every budget is positive; depth ≤1,000, dispatches/items/cost ≤1,000,000, datastore queries ≤100,000, reads ≤1,024, batch concurrency ≤1,000; PostgreSQL per-root read budgets leave two pool slots and nested fan-out is bounded to 4× pool size |
| Telemetry | `logFormat` is `pretty` or `json`; the log filter must parse; export timeout is 1 ms–1 minute; OTLP is a credential-free HTTP(S) origin without path/query/fragment |
| Shutdown | drain timeout is 1 ms–5 minutes; health interval is 1 ms–1 minute |

Authentication-specific settings are documented in the
[authentication runbook](authentication-runbook.md).

## Environment overrides

Scalar YAML fields can be overridden with `OPENFGA__`, double-underscore path separators, and
camel-case field names. Prefer changing the reviewed YAML for lists and nested policy structures.
Examples:

```text
OPENFGA__TRANSPORT__MAXIMUM_CONCURRENCY=512
OPENFGA__TELEMETRY__LOG_FILTER=openfga_server=debug,info
OPENFGA__SHUTDOWN__DRAIN_TIMEOUT_MS=30000
```

These overrides are configuration values, not the separately referenced secret values such as
`OPENFGA_TOKEN_KEY`.

## Validate and deploy

1. Validate the exact mounted file and environment overrides without opening storage or listeners:

   ```sh
   make validate-config CONFIG=/absolute/path/openfga.yaml
   ```

2. Inspect the merged, secret-free representation. Treat principal/store policy as operationally
   sensitive even though credential values are absent:

   ```sh
   make print-effective-config CONFIG=/absolute/path/openfga.yaml
   ```

3. For PostgreSQL, require a current schema before admitting traffic:

   ```sh
   make migrate-status CONFIG=/absolute/path/openfga.yaml
   ```

4. Start one canary, then check `GET /healthz`, `GET /readyz`, and the standard gRPC health
   service. `healthz` is process liveness; `readyz` includes storage and authentication readiness.
   Health endpoints intentionally do not require API credentials, so restrict them at the network
   boundary.
5. Exercise one authenticated read and one denied cross-store request before expanding rollout.
6. Send `SIGTERM`/`SIGINT` for shutdown. The process marks itself unready, stops through its
   supervisor, drains for `shutdown.drainTimeoutMs`, joins owned actors, flushes telemetry, and
   closes storage.

## Continuation-token key rotation

1. Add the new secret and move the current `tokenKeyId`/`tokenKeyEnv` pair into
   `tokenVerificationKeys`.
2. Configure the new key as the active pair and restart. New tokens use it while tokens signed by
   the prior key continue to verify.
3. Retain prior keys for at least `tokenTtlSeconds` plus deployment and clock skew, then remove them
   in a later coordinated restart. Key IDs must be unique and the total key set is capped at 16.

## TLS rotation

Replace the certificate and key files as one filesystem-level deployment unit. Within
`tls.reloadIntervalSeconds`, the owner bounded-reads both files, validates the pair, and atomically
publishes one rustls configuration to new HTTP and gRPC connections. A partial or malformed update
is rejected and the prior identity remains active; inspect the generic reload warning and repair
the files without restarting or weakening TLS.

## Rollback

Retain the previous binary, YAML, secret versions, and database schema compatibility result for
each deployment. Configuration-only rollback is a restart with the prior validated YAML/secrets.
Never roll an older binary onto a database whose migration status is `tooNew`; use the
[migration rollback procedure](migration-runbook.md#rollback) instead.
