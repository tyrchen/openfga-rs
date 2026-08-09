# openfga-rs

`openfga-rs` is an independent, safe-Rust implementation of the
[OpenFGA](https://openfga.dev/) authorization service. It implements the OpenFGA v1 HTTP and gRPC
APIs, the AuthZEN Authorization API surface, CEL conditions, and memory, PostgreSQL, MySQL, and
SQLite storage profiles.

The project is source-pinned and differential-tested against a specific OpenFGA Go revision. It is
not maintained by or affiliated with the OpenFGA project. Compatibility claims apply to the exact
versions in the [compatibility matrix](docs/compatibility.md), rather than every past or future
OpenFGA release.

## Highlights

- Complete generated OpenFGA v1 HTTP/gRPC surface: stores, models, assertions, tuples, changes,
  Check, BatchCheck, Expand, ListObjects, StreamedListObjects, and ListUsers.
- Optional AuthZEN Evaluation, batch Evaluation, Subject/Resource/Action Search, and discovery.
- Bounded evaluators, admission control, request deadlines, graceful drain, cache invalidation, and
  explicit overload behavior.
- Preshared-key and OIDC authentication with store/action authorization; TLS uses rustls with the
  AWS-LC backend.
- Actor-owned in-memory storage and production SQL profiles for PostgreSQL and MySQL, plus a
  single-process SQLite profile.
- Deterministic protocol generation, Go differential suites, backend contracts, fuzz targets,
  supply-chain policy, SBOMs, checksums, and GitHub build attestations.
- `#![forbid(unsafe_code)]` throughout project crates.

## Performance compared with OpenFGA Go

The release benchmark starts the Rust server and OpenFGA Go commit
`4e4f79ed841513dfd61746a75ef473f6198299f7` as separate optimized processes on the same host,
configures identical in-memory fixtures, warms both implementations, and compares typed successful
responses. The final repeated matrix completed 134,346 requests with no overload or semantic
mismatch. For every paired row with at least 50 samples, Rust had lower p50, p95, and p99 latency
and higher throughput in that run.

Representative warm 100-client results:

| Workload | OpenFGA Go p95 | `openfga-rs` p95 | Relative p95 |
| --- | ---: | ---: | ---: |
| Direct Check | 3.796 ms | 2.494 ms | Rust 1.52× faster |
| Recursive userset | 338.063 ms | 5.444 ms | Rust 62.1× faster |
| Deep recursive userset | 760.487 ms | 5.391 ms | Rust 141× faster |
| Wide union | 5.333 ms | 1.875 ms | Rust 2.84× faster |
| ListObjects, residual-heavy | 5.198 ms | 1.840 ms | Rust 2.83× faster |
| ListUsers set algebra | 5.635 ms | 1.781 ms | Rust 3.16× faster |
| Explicit model-cache load | 6.974 ms | 2.101 ms | Rust 3.32× faster |
| Model compile and publish | 16.965 ms | 2.273 ms | Rust 7.46× faster |
| Tuple write and changelog | 22.771 ms | 2.106 ms | Rust 10.81× faster |

These loopback results are orientation evidence, not a portable service-level objective. Recursive
fixtures magnify implementation differences, while database RTT, TLS, authentication, telemetry,
model shape, and traffic distribution can change production results. The
[full benchmark report](docs/research/phase4-scale-benchmark-report.md) records the pinned Go commit,
host, toolchains, request counts, p50/p95/p99, throughput, component benchmarks, PostgreSQL smoke,
memory bounds, soak results, methodology, and limitations. Reproduce it with `make phase4-scale`.

## Architecture

The codebase separates wire protocol, validated domain types, model/CEL compilation, storage
capabilities, evaluation, enumeration, caching, use cases, transport, and application composition.
Semantic crates do not depend on Axum, Tonic, SQLx, or concrete storage backends unless that
framework is their explicit responsibility.

```text
HTTP/gRPC -> transport -> service -> check/list/cache -> model/condition -> storage traits
                |                         |                    |
              proto                    domain          memory/PostgreSQL/MySQL/SQLite
```

The [architecture guide](docs/architecture.md) explains dependency direction, request flow,
consistency, concurrency, security boundaries, and extension points. Detailed design contracts live
under [`specs/`](specs/index.md), and operator documentation starts at [`docs/`](docs/index.md).

## Quick start

Prerequisites are Git and the Rust toolchain manager. Go and Node.js are needed only for differential
and SDK compatibility suites. Clone submodules before building:

```sh
git submodule update --init --recursive
```

Run the local memory profile with a fresh continuation-token key:

```sh
export OPENFGA_TOKEN_KEY="$(openssl rand -base64 32)"
cargo run --release -p openfga-server -- run --config config/openfga-development.yaml
```

The default listeners are HTTP `127.0.0.1:8080` and gRPC `127.0.0.1:8081`:

```sh
curl --fail http://127.0.0.1:8080/healthz
curl --fail http://127.0.0.1:8080/readyz
```

The development profile disables authentication and TLS and must not be exposed to an untrusted
network. Start from [`config/openfga-preshared-development.yaml`](config/openfga-preshared-development.yaml)
or the [configuration runbook](docs/operations/configuration-runbook.md) for a secured deployment.

To use PostgreSQL and apply embedded migrations on startup:

```sh
export OPENFGA_TOKEN_KEY="$(openssl rand -base64 32)"
export OPENFGA_DATABASE_URL='postgres://USER:PASSWORD@HOST/openfga'
export OPENFGA__STORAGE__BACKEND=postgres
export OPENFGA__STORAGE__POSTGRES__MIGRATE_ON_START=true
cargo run --release -p openfga-server -- run --config config/openfga-development.yaml
```

Use explicit `migrate status`/`migrate up` commands and reviewed secrets in production; see the
[migration](docs/operations/migration-runbook.md) and
[backup/restore](docs/operations/backup-restore-runbook.md) runbooks.

## Workspace

| Path | Responsibility |
| --- | --- |
| `apps/openfga-server` | Configuration, composition, lifecycle, migrations, and verification probes |
| `crates/openfga-domain` | Validated identifiers, references, commands, limits, and fingerprints |
| `crates/openfga-proto` | Pinned generated OpenFGA/AuthZEN protobuf, JSON, and gRPC types |
| `crates/openfga-model`, `openfga-condition` | Authorization-model and CEL compilation/evaluation |
| `crates/openfga-storage*` | Capability traits, contracts, and memory/SQL implementations |
| `crates/openfga-check`, `openfga-list` | Check, BatchCheck, ListObjects, ListUsers, and Expand engines |
| `crates/openfga-cache` | Bounded decision/model/tuple caches and changelog invalidation |
| `crates/openfga-auth` | Authentication and store/action authorization policy |
| `crates/openfga-service`, `openfga-transport` | Transport-neutral use cases and HTTP/gRPC adapters |
| `tools/` | Deterministic protocol generation, documentation checks, migration, and gRPC probes |
| `vendors/` | Immutable OpenFGA Go/API compatibility sources |

## Development and verification

Rust is pinned in [`rust-toolchain.toml`](rust-toolchain.toml). Generated protocol artifacts are
committed and reproducible. The most useful targets are:

```sh
make check                 # generated code/docs, GitHub Actions, build, tests, fmt, clippy, rustdoc
make clippy-strict         # panic, unwrap, indexing, and pedantic boundary lints
make differential-smoke    # pinned Go health/API and official JavaScript SDK smoke
make check-spike           # complete pinned Check/BatchCheck differential corpus
make enumeration-differential
make postgres-storage      # live PostgreSQL contracts, faults, migrations, and plans
make phase4-scale          # full Go/Rust benchmark and consistency/soak evidence
make audit deny            # RustSec and dependency/license/source policy
make secret-scan           # committed history and working-tree secret scan
make release-artifacts     # binary, Apache notices, SBOMs, and SHA-256 manifests
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for change-specific gates and
[dependency policy](docs/dependencies.md) for version and supply-chain governance.

## Security

Do not report exploitable vulnerabilities in a public issue. Follow [SECURITY.md](SECURITY.md) for
private reporting and supported-version policy. The project threat model and operator response
procedures are in [`docs/security/`](docs/security/threat-model.md) and
[`docs/operations/`](docs/index.md#operations).

## License

Licensed under the [Apache License, Version 2.0](LICENSE). Attribution notices for generated and
vendored upstream material are recorded in [NOTICE](NOTICE). OpenFGA names and marks belong to their
respective owners; the license does not grant trademark rights.
