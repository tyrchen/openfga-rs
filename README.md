# openfga-rs

`openfga-rs` is a safe-Rust reimplementation of [OpenFGA](https://openfga.dev/). It targets behavioral parity with the exact Go source and API revisions vendored in this repository. The [compatibility matrix](docs/compatibility.md) defines the supported API and memory, PostgreSQL, MySQL, and SQLite deployment profiles.

Phases 0–5 establish the semantic core, complete API, secure runtime, enumeration, bounded caching and GA backend/release evidence. The dependency-ordered roadmap is in [the implementation plan](specs/91-implementation-impl-plan.md), and the current decisions are recorded in [the key-decisions log](specs/99-key-decisions.md).

## Workspace

- `apps/openfga-server`: server composition and the Phase 0 differential probe.
- `crates/`: narrowly scoped domain, protocol, condition, storage, evaluation, service, and transport crates.
- `tools/openfga-proto-codegen`: deterministic OpenFGA protocol generator.
- `tools/openfga-upstream-migrate`: bounded offline migration from the pinned upstream SQLite schema.
- `vendors/openfga`: pinned Go compatibility oracle.
- `vendors/openfga-api`: pinned OpenFGA protocol source.
- `specs/` and [`docs/`](docs/index.md): design contracts, operator runbooks, and implementation evidence.

## Development

Rust is pinned in [`rust-toolchain.toml`](rust-toolchain.toml). The compatibility targets bootstrap a checksum-verified Go toolchain under the ignored `.tools/` directory; the SDK smoke also requires Node.js and npm.

```bash
make check                 # proto reproducibility, build, tests, format, lint, docs
make clippy-strict         # boundary-oriented panic/index/unwrap linting
make conformance           # CEL and ListObjects Phase 0 evidence
make differential-smoke    # Go/Rust health comparison and official SDK smoke
make sqlite-storage        # SQLite contracts, fault injection, plans, and restore drill
make upstream-migration-drill
make secret-scan
make release-artifacts     # binary archive, SBOMs, and SHA-256 manifest
make audit
make deny
```

Generated Rust protocol artifacts are committed. Regenerate them with `make proto` and prove that regeneration is clean with `make check-proto`.

## License

Distributed under the [MIT License](LICENSE.md).
