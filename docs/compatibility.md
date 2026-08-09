# Compatibility matrix

This release implements the OpenFGA v1 API against one immutable upstream baseline. Compatibility
claims apply only to the operations and environments in this matrix; AuthZEN is not part of the GA
claim.

## Exact pins

| Surface | Pin |
| --- | --- |
| OpenFGA behavioral oracle | `4e4f79ed841513dfd61746a75ef473f6198299f7` |
| OpenFGA API source | `f153694bfc20f7be303e33cabe72b668596c5a06` |
| Rust | `1.97.1` (`8bab26f4f`, edition 2024) |
| SQLx | `0.9.0`, locked by `Cargo.lock` |
| PostgreSQL CI | `18.4` |
| MySQL CI | `8.4.10` |
| SQLite | bundled SQLite `3.51.3` from `libsqlite3-sys 0.37.0` |
| JavaScript SDK smoke | `@openfga/sdk 0.9.6`, Node.js 24 |
| Dependency security tools | `cargo-audit 0.22.2`, `cargo-deny 0.20.2` |
| Release security tools | `gitleaks 8.30.1`, `syft 1.50.0` |

Protocol source integrity, protoc version, imported module commits, licenses, and all input hashes
are recorded in `crates/openfga-proto/proto.lock.json` and reproduced by `make check-proto`.

## Advertised profiles

| Backend | Deployment profile | Required evidence |
| --- | --- | --- |
| Memory | Development, tests, ephemeral single process | Shared contract, full workspace tests, differential suites |
| PostgreSQL 18.4 | Production, primary plus optional bounded-lag read replica | Contract, migration, transaction fault, query-plan, SDK/API, consistency and scale suites |
| MySQL 8.4.10 | Production, writable primary; no replica routing | Shared portable contract, migration, transaction fault, query-plan and full SDK/API suite |
| SQLite 3.51.3 | Embedded/single-process deployments | Shared portable contract, migration, transaction fault, backup/restore and full SDK/API suite |

SQLite is intentionally configured with exactly one database connection. It is not an HA or
multi-process writer profile. MySQL and SQLite use the same semantic storage traits as PostgreSQL;
backend-specific SQL is confined to migrations, locking, upsert syntax, and error classification.

## API scope

The GA surface is the generated OpenFGA v1 HTTP/gRPC service: stores, authorization models,
assertions, relationship tuples and changes, Check, BatchCheck, Expand, ListObjects,
StreamedListObjects, and ListUsers. Official-client and differential tests normalize ordering only
where the protocol does not define order. An unexplained decision, result-set, public error,
pagination, or terminal-stream mismatch is release-blocking.

The vendored Go server remains the executable oracle. Run `make differential-smoke`,
`make check-spike`, and `make enumeration-differential` to reproduce semantic evidence. For a SQL
backend, also run its storage target and the backend-specific Phase 2 compatibility target described
in the [Phase 5 release evidence](research/phase5-ga-release-evidence.md).

## Compatibility changes

Changing either upstream commit, any protocol input, database major/minor pin, Rust pin, or SDK
version requires a new inventory diff and all affected conformance, migration, security, and
performance gates. A release must name the new pins; it must not inherit this statement implicitly.
