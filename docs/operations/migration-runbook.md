# PostgreSQL migration runbook

`openfga-server` embeds forward-only SQLx migrations. It verifies the SQLx migration history,
checksums, and the project-owned `openfga_schema_metadata` version before serving PostgreSQL-backed
traffic.

## Commands and states

Use the application configuration so the command reads the same primary URL reference and pool
policy as the server:

```sh
make migrate-status CONFIG=/absolute/path/openfga.yaml
make migrate-up CONFIG=/absolute/path/openfga.yaml
```

`migrate status` is read-only and prints JSON containing `current`, `target`, and `state`. It exits
nonzero unless the state is `current`, which makes it suitable for a deployment gate.

| State | Meaning | Required action |
| --- | --- | --- |
| `fresh` | No SQLx migration table exists | Back up if the database is not known-empty, then run `migrate up` |
| `pending` | The database is older than the binary | Back up, stop old writers, then run `migrate up` |
| `current` | History, checksums, and logical schema version match | The binary may start |
| `tooNew` | A newer binary migrated the database | Do not start this binary; deploy a compatible binary or restore |

The status command also fails closed on an interrupted migration, checksum mismatch, missing
embedded migration, invalid metadata, or database connectivity failure.

## Planned upgrade

1. Read the release notes and inspect every new migration. Confirm PostgreSQL-version support,
   lock behavior, storage growth, expected duration, and whether old and new binaries can safely
   overlap. If compatibility is not explicitly proven, plan a write outage.
2. Record the current binary digest, configuration revision, `migrate status` JSON, PostgreSQL
   server version, database size, and replica lag.
3. Take and verify a recoverable backup using the
   [backup/restore runbook](backup-restore-runbook.md). A snapshot without a tested restore is not
   rollback evidence.
4. Set `storage.postgres.migrateOnStart: false`. Keep it false for multi-replica production so a
   dedicated migration job owns the change.
5. Remove old application instances from admission and stop writes. Wait for in-flight requests to
   drain.
6. Run exactly one migration job:

   ```sh
   make migrate-up CONFIG=/absolute/path/openfga.yaml
   ```

   SQLx serializes migration application with the backend lock and verifies prior checksums. Do not
   treat that lock as proof that mixed application versions are semantically compatible.
7. Require a clean postcondition:

   ```sh
   make migrate-status CONFIG=/absolute/path/openfga.yaml
   ```

8. Start one new binary, require ready health, then run authenticated store/model/tuple write-read
   probes and a Check request. Expand rollout only after those pass.
9. Keep the pre-migration backup and previous release until the observation window and restore
   drill requirements are met.

For a single-instance development environment, `migrateOnStart: true` applies pending migrations
during connection setup. It is deliberately opt-in and is not the production procedure.

## Failed migration

- Stop rollout and keep all application instances unready.
- Preserve the command output, redacted server logs, binary digest, and database logs.
- Run `migrate status`; do not rerun blindly if it reports an interrupted record, checksum
  mismatch, missing migration, or `tooNew`.
- A connectivity/timeout failure may be retried only after confirming the first command did not
  commit. The embedded migrations are expected to be transactional, but verify state rather than
  assuming.
- Never edit `_sqlx_migrations`, its checksums, or `openfga_schema_metadata` by hand. Those checks
  are corruption and compatibility barriers.

## Rollback

There is no `migrate down` command. Rolling back a binary is safe only when `migrate status` from
that binary reports `current`.

For an incompatible schema rollback:

1. Stop admission and all writers.
2. Preserve the failed/new database for investigation.
3. Restore the pre-migration backup into a new, empty database or managed-service instance.
4. Point the previous binary's secret reference to the restored primary.
5. Run the previous binary's `migrate status`; require `current`.
6. Start a canary, verify health and authenticated read/write/Check behavior, then shift traffic.

Do not destructively rewrite the active database to imitate a down migration.
