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
- TLS files use absolute paths without `..`. They, referenced secrets, authorization policy, and
  OIDC bootstrap state are loaded at startup; changing them requires a coordinated restart.
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
  maximumConcurrency: 1024
  tokenKeyId: primary
  tokenKeyEnv: OPENFGA_TOKEN_KEY
evaluator:
  depth: 25
  dispatches: 10000
  datastoreQueries: 100
  tupleItems: 10000
  conditionCost: 100000
  concurrentReads: 16
  batchConcurrency: 16
telemetry:
  logFormat: json
  logFilter: info
  exportTimeoutMs: 5000
shutdown:
  drainTimeoutMs: 10000
  healthIntervalMs: 1000
```

Omit `replicaUrlEnv` when there is no replica. Latency-preferring reads use a configured replica
only while its replay lag is within `replicaMaxLagMs`; otherwise they conservatively use the
primary.

## Secret inventory

| Reference | Required material | Rotation effect |
| --- | --- | --- |
| `storage.postgres.primaryUrlEnv` | PostgreSQL URL for the writable primary | New process/pool required |
| `storage.postgres.replicaUrlEnv` | Optional read-only replica URL | New process/pool required |
| `transport.tokenKeyEnv` | Standard-base64 encoding of 32–64 random bytes | Existing continuation tokens become invalid because this release loads one active token key |
| `auth.preshared.keys[].keyEnv` | 32–256 ASCII-graphic bytes from a CSPRNG-backed secret | Follow the overlap procedure in the authentication runbook |

Environment-reference names must be 1–128 bytes, begin with an uppercase ASCII letter, and
contain only uppercase ASCII letters, digits, and underscores. Inject actual secret values through
the deployment secret manager. Do not place them in command arguments, generated effective
configuration, logs, tickets, or shell history.

## Settings and limits

| Area | Important constraints |
| --- | --- |
| PostgreSQL | `maxConnections` is 1–65,536; `minConnections` cannot exceed it; acquisition and statement timeouts are 1 ms–5 minutes; tuple mutations are 1–5,000 |
| Transport | page size is 1–100,000; timeout is 1 ms–5 minutes; token TTL is 1 second–720 hours; message size is 1–16 MiB; concurrency is 1–65,536 |
| Evaluator | every budget is positive; depth ≤1,000, dispatches/items/cost ≤1,000,000, datastore queries ≤100,000, reads ≤1,024, batch concurrency ≤1,000 |
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

## Rollback

Retain the previous binary, YAML, secret versions, and database schema compatibility result for
each deployment. Configuration-only rollback is a restart with the prior validated YAML/secrets.
Never roll an older binary onto a database whose migration status is `tooNew`; use the
[migration rollback procedure](migration-runbook.md#rollback) instead.
