# PostgreSQL backup and restore runbook

This runbook defines logical backup, point-in-time recovery expectations, verification, and restore
for the PostgreSQL backend. Set service-specific recovery point and recovery time objectives before
production use; the commands below do not choose them for you.

PostgreSQL documents that `pg_dump` creates a consistent single-database export while traffic is
concurrent, and that custom archives are portable and inspectable with `pg_restore`. For regular
production recovery with a low RPO, use managed snapshots or base backups plus continuous WAL
archiving and PITR in addition to logical dumps. See the official
[`pg_dump`](https://www.postgresql.org/docs/current/app-pgdump.html),
[`pg_restore`](https://www.postgresql.org/docs/current/app-pgrestore.html), and
[PITR](https://www.postgresql.org/docs/current/continuous-archiving.html) documentation.

## Scope and credentials

- Give OpenFGA a dedicated database. A full database dump must include project tables,
  `_sqlx_migrations`, `openfga_schema_metadata`, and `openfga_change_sequence`.
- Use a supported `pg_dump` client compatible with the source server. Check both versions before
  the backup.
- Keep database passwords out of command arguments and shell history. Configure a libpq
  [service file](https://www.postgresql.org/docs/current/libpq-pgservice.html) plus a protected
  [password file](https://www.postgresql.org/docs/current/libpq-pgpass.html), or use equivalent
  managed-service identity. Password-file permissions must be `0600` on Unix.
- Encrypt backup artifacts at rest, restrict restore permission, and treat a dump as hostile unless
  its provenance and integrity are verified. Restoring a dump executes SQL chosen by the source.

The examples assume libpq services named `openfga-primary` and `openfga-restore`; neither command
contains a password.

## Create a logical backup

1. Record application version/digest, configuration revision, PostgreSQL server and client
   versions, UTC time, and migration state:

   ```sh
   make migrate-status CONFIG=/absolute/path/openfga.yaml
   pg_dump --version
   ```

2. Create a custom-format dump. `--lock-wait-timeout` makes conflicting DDL fail instead of
   hanging indefinitely:

   ```sh
   pg_dump \
     --dbname=service=openfga-primary \
     --format=custom \
     --file=openfga.dump \
     --lock-wait-timeout=30s \
     --verbose
   ```

3. Require exit status zero and inspect every warning. Do not use `--no-sync` for a production
   backup.
4. Verify that the archive is readable and contains the schema sentinels:

   ```sh
   pg_restore --list openfga.dump > openfga.dump.list
   rg '_sqlx_migrations|openfga_schema_metadata|openfga_change_sequence|TABLE DATA.*tuples' openfga.dump.list
   ```

5. Generate a cryptographic checksum with the platform-approved tool, store it separately from the
   dump, and copy both to immutable encrypted storage.
6. Restore the dump into an isolated database and execute the verification procedure below. A list
   check alone is not a restore test.

`pg_dump` is a point-in-time logical snapshot, not continuous recovery. Configure and continuously
test managed PITR or PostgreSQL base-backup/WAL archiving when the required RPO is smaller than the
logical-backup interval.

## Restore into a new database

Prefer a new, empty database or instance. Do not use `--clean` against the active production
database.

1. Stop application admission and writers if this restore will become authoritative. Record the
   intended recovery point and preserve the current database.
2. Verify the backup's provenance, checksum, encryption metadata, and `pg_restore --list` output.
3. Create an empty target database owned by the intended application role.
4. Restore atomically and fail at the first error:

   ```sh
   pg_restore \
     --dbname=service=openfga-restore \
     --exit-on-error \
     --single-transaction \
     --no-owner \
     --no-privileges \
     --verbose \
     openfga.dump
   ```

5. Point a copy of the OpenFGA YAML at an environment reference for the restored database. Never
   overwrite the secret for the active deployment during validation.
6. Run schema status with the binary that will serve the restore:

   ```sh
   make migrate-status CONFIG=/absolute/path/openfga-restore.yaml
   ```

   If the result is `pending`, take a snapshot of the restored database and follow the migration
   runbook. If it is `tooNew`, use a compatible binary. Do not change metadata manually.
7. Start an isolated server and verify:

   - liveness and readiness;
   - store counts and selected store/model/assertion reads;
   - selected tuple reads and changelog continuity;
   - an authenticated Check allow case and deny case;
   - one reversible write/read/delete exercise in a designated recovery-test store.

8. Compare row counts and business invariants to the backup manifest. Measure the actual restore
   time against the RTO.
9. Promote by changing the database secret reference and restarting a canary. Shift traffic only
   after readiness and application probes pass. Retain the previous database until the rollback
   window closes.

## PITR recovery

Use the database provider's documented procedure or PostgreSQL base backup plus archived WAL. Stop
at a point before the destructive event, recover onto a new timeline/instance, then perform the same
schema and application verification as a logical restore. Do not direct OpenFGA at a database still
in recovery or at an unverified replica.

## Routine evidence

- Automate backups outside this repository with the platform's approved scheduler and secret
  manager.
- Alert on missed backups, failed WAL archival, age beyond RPO, checksum failure, and restore-test
  failure.
- Run isolated restore drills on a fixed cadence and before schema migrations.
- Record backup duration, restore duration, data size, recovery point, tool versions, and verifier
  results without recording DSNs, tokens, tuple contents, object IDs, or subject IDs.
