# Contributing to openfga-rs

Thank you for improving `openfga-rs`. Correct authorization decisions and conservative failure
behavior take precedence over convenience or benchmark wins. Small, focused changes with explicit
evidence are easiest to review.

By intentionally submitting a contribution, you agree that it is licensed under the repository's
[Apache License 2.0](LICENSE), as described by section 5 of that license. No additional contributor
license agreement is currently required.

## Before starting

- Search existing issues and pull requests before proposing overlapping work.
- Open an issue for a new public API, compatibility change, storage backend, migration, security
  boundary, or architectural change before investing in a large implementation.
- Never include credentials, production data, private models, exploit details, or customer
  identifiers in an issue, test, trace, or commit.
- Follow [SECURITY.md](SECURITY.md) for vulnerabilities and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
  for community expectations.

## Development setup

Install Git and rustup, then initialize the pinned compatibility sources:

```sh
git submodule update --init --recursive
rustup show
make check
```

The repository pins stable Rust in `rust-toolchain.toml`, a separate nightly rustfmt, and a
checksum-verified Actionlint binary in the Makefile. Differential tests bootstrap a checksummed Go
toolchain into ignored `.tools/` storage. Official SDK smoke tests require the Node.js version
declared in the compatibility matrix.

Do not use `cargo clean`; the workspace is large and the project workflow does not require it.

## Making a change

1. Keep crate dependency direction consistent with the [architecture guide](docs/architecture.md).
2. Validate hostile input at its boundary and use domain newtypes after validation.
3. Bound bodies, strings, collections, work, concurrency, queues, caches, recursion, and I/O time.
4. Preserve cancellation and join owned tasks; do not detach unbounded work.
5. Parameterize SQL and preserve tuple/changelog transaction atomicity.
6. Add descriptive `test_should_...` coverage for success, failure, limits, cancellation, and
   compatibility-sensitive diagnostics.
7. Update public docs, configuration examples, compatibility pins, runbooks, specifications, and
   generated artifacts when their contracts change.
8. Profile before optimizing and state which correctness/architecture invariants remain intact.

Project Rust code forbids `unsafe`. Production code must not use `unwrap`, `expect`, `todo`, hostile
input indexing, or panic as error handling. Libraries use typed `thiserror` errors; application
assembly may add `anyhow` context.

## Verification

During development, run the narrowest check that exercises the touched behavior. Before requesting
review, use the full gate required by the change:

| Change | Minimum final evidence |
| --- | --- |
| Rust source, tests, examples, manifests, generated Rust | `make check clippy-strict` |
| Dependency, lockfile, license, supply chain, packaging | `make check clippy-strict audit deny` and affected release/security target |
| Documentation only | `make check-docs` and manual rendered-Markdown review |
| Protocol inputs/generator | `make check-proto check` plus affected SDK/differential suites |
| Check/CEL semantics | `make conformance check-spike` |
| Enumeration semantics | `make enumeration-differential` plus relevant engine tests |
| Transport/API/errors | `make differential-smoke phase2-compatibility` with a supported SQL backend |
| Storage/migrations | Backend contract, fault, migration, query-plan, compatibility, and restore gates |
| Cache/consistency/performance | `make phase4-scale` and relevant live-PostgreSQL evidence |
| Security boundary | `make security` plus targeted adversarial tests and threat-model update |

The full PostgreSQL developer gate can use Postgres.app on macOS:

```sh
PATH="/Applications/Postgres.app/Contents/Versions/latest/bin:$PATH" \
  make phase4-local-postgres-scale-smoke
```

If a heavyweight gate is not applicable or cannot run locally, say exactly why in the pull request;
CI remains authoritative for its advertised database images and tag-only release jobs.

## Dependencies and generated code

Read [docs/dependencies.md](docs/dependencies.md) before changing a version or adding a crate/action.
Use `make proto` to regenerate protocol output and `make check-proto` to prove reproducibility. Do
not hand-edit files under `crates/openfga-proto/src/generated`.

## Commits and pull requests

Use focused [Conventional Commits](https://www.conventionalcommits.org/) so release notes can be
generated consistently. A pull request should include:

- the problem, approach, and user-visible behavior;
- the pinned upstream/API surface affected, if any;
- security, consistency, migration, and compatibility implications;
- benchmark methodology for performance claims;
- exact commands and environments used for verification;
- documentation and changelog updates where users or operators are affected.

Keep unrelated formatting or dependency churn out of the same pull request. Review feedback should
be resolved with code/evidence or a recorded decision, not hidden by weakening a test or lint.

## Licensing and attribution

New files are covered by Apache-2.0 without per-file headers unless their format or upstream license
requires one. Preserve third-party copyright, license, and NOTICE material. Any copied or transformed
fixture must record its repository, commit, source path, license, and transformation.
