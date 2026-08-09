# Phase 5 GA release evidence

Status: implemented; release-candidate gates are reproducible from the exact source revision.

## Delivered surface

- MySQL 8.4 and bundled SQLite 3.51 storage implement the complete shared storage capability set,
  backend-specific migrations, atomic tuple/changelog mutations, bounded pools/work, classified
  errors, query-plan assertions, and failure injection at every mutation stage.
- The same secure HTTP/gRPC and official JavaScript SDK compatibility harness used for PostgreSQL
  runs against MySQL and SQLite. SQLite additionally exercises a real backup/restore and too-new
  rollback barrier.
- `openfga-upstream-migrate` performs a bounded offline conversion from the exact vendored OpenFGA
  SQLite schema. It takes a write-blocking source snapshot, validates the schema, requires an empty
  destination, and reuses production wire-to-domain validation. Original upstream changelog rows
  are intentionally not copied; imported tuples create a fresh cutover changelog watermark.
- The release gate includes advisory/license policy, full-history secret detection, CycloneDX/SPDX
  SBOMs, SHA-256 manifests, two-OS artifact builds, and GitHub OIDC/Sigstore build and SBOM
  attestations.

## Reproduction commands

```sh
make sqlite-storage upstream-migration-drill phase5-sqlite-compatibility
make mysql-storage phase5-mysql-compatibility \
  MYSQL_TEST_URL='mysql://USER:PASSWORD@HOST/openfga'
make postgres-storage phase2-compatibility \
  POSTGRES_TEST_URL='postgres://USER:PASSWORD@HOST/openfga'
make check clippy-strict audit deny secret-scan release-artifacts
```

`make phase5-ga-evidence` runs the differential, enumeration, supply-chain, artifact, and release
binary load/soak gates. `make phase5-release-gate` composes it with every backend control and
requires both external SQL URLs. Keep secrets out of command history in production automation; the
literals above are grammar examples, not credentials.

The SQLite/API, upstream migration, secret scan and local Darwin arm64 release artifact controls
were executed during implementation. The strengthened MySQL storage/migration suite passed ten
consecutive runs and the complete secure SDK/gRPC/runtime gate against a temporary Oracle MySQL
8.4.10 distribution. PostgreSQL storage and the same API gate passed on the final tree against local
PostgreSQL 17.10; CI independently requires the advertised PostgreSQL 18.4 image. The complete GA
evidence target passed with 3,420 Check corpus events and all enumeration cases at zero mismatches,
then exercised the exact release binary under consistency, load, soak, drain, and resource-bound
checks. A second independent Phase 5 review reported no remaining actionable findings.

## Artifact contract

`make release-artifacts` emits a platform-named server archive, CycloneDX JSON, SPDX JSON, and a
platform-named `SHA256SUMS` manifest under `target/phase5`. Tag builds cannot start the Linux/macOS
artifact matrix until the dedicated GA job has run the complete differential and enumeration
suites, security controls, and release binary scale/soak harness. The matrix then attests each
archive and its SPDX SBOM with `actions/attest`, uploads the complete set, and attaches it to the
GitHub release. Verify a downloaded archive with the corresponding checksum and
`gh attestation verify` before installation.

## Release blockers

There are no accepted semantic mismatches or quarantined Phase 5 tests. Any backend contract/API
failure, ignored MySQL test outside its dedicated CI job, migration/restore failure, advisory or
license denial, secret finding, missing SBOM/checksum, or attestation failure blocks publication.
