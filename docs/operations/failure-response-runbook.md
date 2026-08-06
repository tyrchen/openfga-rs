# Failure response runbook

This runbook covers detection, containment, diagnosis, and recovery for the M2 server. Prefer
fail-closed recovery over bypassing authentication, schema checks, TLS, budgets, or redaction.

## First five minutes

1. Stop automated rollout and record UTC start time, affected version/config revision, region, and
   symptom.
2. Check process status, `GET /healthz`, `GET /readyz`, and gRPC health separately. A live but
   unready process should be removed from admission, not restarted in a loop without diagnosis.
3. Determine whether the event affects startup, all requests, one transport, one store/action,
   PostgreSQL, OIDC/JWKS, or telemetry.
4. Preserve redacted structured logs and platform events. Never attach configuration secrets,
   authorization headers, DSNs, tokens, tuple/context payloads, object IDs, or subject IDs.
5. Contain writes when schema integrity, ambiguous database commit, backup corruption, or restore
   correctness is in doubt.

## Failure matrix

| Signal | Fail-safe behavior | Operator action |
| --- | --- | --- |
| Configuration/TLS/secret invalid | Startup fails before API listener readiness | Fix the referenced value; validate config; do not weaken profile/TLS/auth |
| TLS reload invalid | The replacement pair is rejected and both listeners retain the last valid identity | Repair and atomically replace both files; verify the next reload event and new connection certificate |
| PostgreSQL unreachable at startup | Assembly fails; listeners do not become ready | Check DNS/network/TLS/credentials/pool limits and primary role |
| Schema `fresh`/`pending`/`tooNew` or checksum mismatch | PostgreSQL backend refuses service | Follow the migration runbook; never edit metadata/checksums |
| Primary outage after startup | Readiness probe fails; storage requests return redacted failures | Remove from admission, restore primary service, verify schema and write/read probes |
| Replica unavailable or beyond lag ceiling | Latency-preferring reads fall back to primary | Check replay lag and primary capacity; do not raise the lag ceiling without consistency analysis |
| OIDC unavailable at startup | Discovery/JWKS bootstrap fails; no listeners | Validate issuer, public DNS answers, CA chain, allowlist, and provider documents |
| OIDC refresh failing | Last verified keys remain until stale grace; after that readiness fails and auth is unavailable | Restore provider/network before grace expires; do not extend grace during an unexplained key event |
| Preshared authentication failures spike | Invalid requests get a generic 401 until the bounded failure bucket sheds excess attempts | Check client rollout and active key labels; rotate through overlap, never log/compare raw keys |
| Admission rate exceeded | Requests fail with a bounded 429/resource-exhausted response before expensive work | Identify the authentication/principal/endpoint class; adjust only from measured legitimate demand |
| Authorization denials spike | Requests fail before store lookup with generic 403 | Compare reviewed bindings/actions/stores and deployment revision; do not add wildcard as a diagnostic shortcut |
| Request timeout/resource limit | Bounded request fails; cancellation propagates to owned work | Identify load/input class, inspect pool and evaluator budgets, shed traffic; do not remove ceilings during incident response |
| Supervised task exits or panics | Runtime supervisor initiates shutdown; no silent partial service | Preserve panic-safe logs, drain/restart only after identifying the task and trigger |
| Telemetry exporter unavailable | Application keeps local structured diagnostics; export/flush errors are reported | Restore collector separately; do not route secrets into ad hoc logs |
| Memory backend restart | All memory data is lost by design | Use only for development; there is no recovery path without an external fixture |

## PostgreSQL diagnosis

1. Confirm the configured endpoint is the writable primary and that the secret version matches the
   deployment. Do not print the URL.
2. Check server reachability, connection saturation, statement timeout, locks, disk space, WAL,
   replication state, and recent failover/migration events using database-native tooling.
3. Run `make migrate-status CONFIG=...` only when the database is stable enough for a read-only
   diagnostic.
4. For an ambiguous tuple write, do not automatically retry until the caller reconciles the tuple
   and changelog state. Retrying a non-idempotent mutation after an unknown commit can duplicate
   intent even when storage constraints prevent duplicate rows.
5. After recovery, require readiness plus authenticated store/model/tuple/Check probes before
   restoring admission.

Use the [migration runbook](migration-runbook.md) for schema failures and the
[backup/restore runbook](backup-restore-runbook.md) for corruption, operator error, or disaster
recovery.

## Authentication diagnosis

For preshared mode, distinguish missing headers, client rollout mistakes, inactive identity labels,
and server configuration revisions without collecting the key. Confirm by secret version/metadata,
not by copying material into a terminal or ticket.

For OIDC, check in this order:

1. configured issuer is exact and HTTPS;
2. discovery issuer equals it exactly;
3. `jwks_uri` host is the issuer host or an exact `allowedHosts` entry;
4. every DNS answer is public and the certificate is valid for the DNS name;
5. response size/key count, unique bounded `kid`, signing use/operations, and algorithm family;
6. token issuer/audience/authorized party/time claims and signing-key overlap.

Authentication errors deliberately collapse details at the client boundary. Use redacted
`error_kind` fields from server logs and provider-side request logs; do not turn on payload/header
logging.

## Graceful restart and rollback

1. Remove the instance from external admission and wait for readiness routing to converge.
2. Send `SIGTERM` or `SIGINT`; do not use an unconditional kill unless the bounded drain exceeds
   the incident's safety limit.
3. Confirm process exit and that HTTP/gRPC ports are closed before replacement.
4. For binary/config rollback, validate the old configuration and require its migration status to
   be `current`. A `tooNew` database requires restore, not an older binary restart.
5. Start one canary, verify both transports as applicable, then restore traffic gradually.

## Escalation evidence

Capture only bounded, redacted evidence:

- binary version and artifact digest;
- configuration revision and output of `print-effective-config` after reviewing policy identifiers;
- migration status JSON and PostgreSQL version;
- health transition times and generic error classes;
- task/connection/in-flight counts, timeout and resource-limit counters;
- OIDC error kind, last successful refresh time, and issuer-side correlation IDs when available;
- shutdown signal, drain duration, and exit status.

After recovery, document the trigger, invariant that contained it, recovery point, data-integrity
checks, and an automated regression or alert. If recovery required bypassing a documented invariant,
treat that as a separate security incident.
