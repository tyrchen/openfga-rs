# Release process

This project publishes tag-triggered, checksummed binaries only after semantic, backend,
supply-chain, and release-binary gates pass. Release tags use `vMAJOR.MINOR.PATCH` and must match the
workspace version in `Cargo.toml`.

## Prepare

1. Confirm the compatibility pins and supported profiles in [compatibility.md](compatibility.md).
2. Update `CHANGELOG.md`, the workspace version, affected runbooks, and release evidence.
3. Refresh direct dependency and GitHub Action versions using
   [dependencies.md](dependencies.md); review every upstream release note.
4. Regenerate protocol output only if its pinned inputs changed, then run `make check-proto`.
5. Verify the working tree contains only the intended release changes.

Run the local release gate with real external PostgreSQL and MySQL instances:

```sh
make phase5-release-gate \
  POSTGRES_TEST_URL='postgres://USER:PASSWORD@HOST/openfga' \
  MYSQL_TEST_URL='mysql://USER:PASSWORD@HOST/openfga'
```

Keep actual secrets out of shell history and CI logs. Use a secret manager or protected CI values in
real release automation. The command above documents URL grammar only.

The gate includes full Rust checks and strict lints; SQLite, PostgreSQL, and MySQL contracts,
migrations, fault injection, query plans, API compatibility, and restore/migration drills; pinned Go
differential and AuthZEN evidence; advisory/license/source policy; full-history secret scanning;
SBOM/checksum generation; and the exact release binary consistency/load/soak/drain checks.

## Tag and publish

Create an annotated, signed tag only from the reviewed release commit:

```sh
git tag -s v0.1.0 -m 'openfga-rs v0.1.0'
git push origin v0.1.0
```

Do not move or reuse a published version tag. A tag starts these ordered GitHub Actions jobs:

1. normal Rust, compatibility, PostgreSQL, MySQL, and SQLite jobs;
2. the GA release gate over the exact tag revision;
3. Linux and macOS release builds;
4. per-platform CycloneDX/SPDX SBOMs and SHA-256 manifests;
5. GitHub OIDC/Sigstore build and SBOM attestations;
6. artifact collection and GitHub Release publication with generated changelog notes.

Every third-party action is pinned to an immutable commit. Release jobs have only the permissions
needed for attestations or publication.

## Artifact contents

Each platform archive contains:

- `openfga-server`;
- the Apache-2.0 `LICENSE`;
- `NOTICE` attribution; and
- `README.md` plus memory and preshared-key configuration examples.

The release also contains a CycloneDX JSON SBOM, an SPDX JSON SBOM, and a platform-specific
`SHA256SUMS` file. `make release-artifacts` reproduces the local platform set under `target/phase5`.

## Verify a release

Download all files for one platform, then verify the checksums:

```sh
shasum -a 256 -c SHA256SUMS-darwin-arm64
```

On Linux, `sha256sum -c` is equivalent. Verify provenance against the repository identity with the
GitHub CLI:

```sh
gh attestation verify openfga-server-darwin-arm64.tar.gz --repo OWNER/REPOSITORY
```

Replace `OWNER/REPOSITORY` with the repository from which the release was downloaded. Inspect the
SPDX/CycloneDX documents and confirm the archive includes `LICENSE` and `NOTICE` before promotion.

## Failure and rollback

Any semantic mismatch, backend contract failure, ignored database test outside its dedicated job,
advisory/license denial, secret finding, missing artifact, checksum mismatch, or attestation failure
blocks publication. Fix the cause and create a new commit; never weaken or skip the failed gate.

If publication occurred with incorrect artifacts, mark the release as affected, remove the unsafe
downloads, preserve the tag and audit trail, publish operator guidance, and issue a new patch version.
Application rollback follows the migration and backup/restore runbooks; never roll a binary back
across a schema compatibility barrier without their explicit checks.
