# Dependency and supply-chain policy

Status: direct dependencies reviewed on 2026-08-09.

`Cargo.toml` and `Cargo.lock` are the authoritative dependency inventory. This document explains why
the major dependencies exist, how versions are selected, and which checks are required before a
dependency change is accepted.

## Current audit

All 54 direct third-party workspace requirements were compared with the stable releases published
by crates.io on 2026-08-09. This review updated `async-trait` 0.1.91 → 0.1.92, `clap` 4.6.5 →
4.6.6, `rustls` 0.23.35 → 0.23.43, and `thiserror` 2.0.19 → 2.0.20. Every other direct requirement
was already at the current stable release. The registry advertises a `rustls` 0.24 development
prerelease; the project remains on stable `rustls` 0.23.43 and does not treat prereleases as routine
updates.

The lockfile was then refreshed to the newest Rust-1.97-compatible transitive graph, updating 18
packages including AWS-LC, Tokio macros, wasm-bindgen, `memchr`, and `zerocopy`. Two newer
transitive patch releases remain outside their parents' resolved constraints: `crypto-common`
through SQLx's SHA-2 0.10 line and `matchit` through Axum 0.8.9. Neither is overridden or duplicated
solely to force a newer number; parent updates will advance them after compatibility review.
The separate fuzz lockfile was also refreshed; it now includes Moka and its transitive graph because
the fuzz targets compile the current condition/model crates rather than an older dependency shape.

The same review confirmed Rust 1.97.1, Go 1.26.5, `cargo-audit` 0.22.2, `cargo-deny` 0.20.2,
Gitleaks 8.30.1, Syft 1.50.0, Actionlint 1.7.12, PostgreSQL 18.4, MySQL 8.4.10, and the locked
JavaScript SDK tree as current for their declared channels. GitHub Actions are immutable-SHA pinned;
Dependabot tracks their release tags and the Cargo/npm ecosystems.

## Load-bearing dependencies

| Capability | Selected crates | Rationale and boundary |
| --- | --- | --- |
| Async runtime | Tokio 1.53.1 | Explicit runtime features; bounded channels, tasks, timers, process and network I/O |
| HTTP/gRPC | Axum 0.8.9, Tonic 0.14.6, Tower 0.5.3 | Confined to transport/application crates; domain and engines remain protocol-neutral |
| TLS/crypto | rustls 0.23.43, tokio-rustls 0.26.4, AWS-LC through selected features | No native-tls/OpenSSL path; TLS and JWT use one reviewed provider family |
| SQL | SQLx 0.9.0 | Parameterized queries, explicit backend features, migrations, bounded Tokio pools |
| Protocol | Prost 0.14.4, prost-reflect 0.16.5, pbjson 0.9.0 | Deterministic generation from checksummed vendored protocol inputs |
| Validation | prost-validate 0.2.9, regex 1.13.1 | Project-owned exhaustive rule interpreter; regex is linear-time on untrusted values |
| CEL | cel-parser 0.10.1, Jiff 0.2.35 | Parsing is isolated behind project-owned bounded compiler/evaluator types |
| Caching | Moka 0.12.15 | Bounded weighted caches and coalesced misses behind project-owned cache identities |
| Authentication | jsonwebtoken 11.0.0, secrecy 0.10.3, subtle 2.6.1 | AWS-LC JWT verification, redacted secrets, constant-time key comparison |
| Telemetry | tracing 0.1.44, OpenTelemetry 0.32 | Structured bounded-cardinality signals; exporter code stays in application composition |
| Serialization/config | Serde 1.0.229, config 0.15.25 | Strongly typed YAML/env configuration and generated JSON compatibility |
| Errors/CLI | thiserror 2.0.20, anyhow 1.0.104, clap 4.6.6 | Typed library errors; application-only context; bounded command surface |
| Testing/benchmarks | proptest 1.11.0, Criterion 0.8.2, dhat 0.3.3 | Invariants, statistical latency/throughput, and release-only heap evidence |

Most requirements disable default features and enable only reviewed functionality. Workspace-level
requirements prevent crate-local version drift. `Cargo.lock` is committed because the server and
release tools are deployable applications.

Workspace crates are implementation boundaries, not independently supported crates.io products,
and are marked `publish = false`. The versioned distribution contract is the `openfga-server`
source tree and checksummed release archive. Publishing a library crate later requires an explicit
API/MSRV/semver design, crate-specific metadata and documentation, and a new release decision.

## Update policy

- Patch updates are expressed with `~` and are expected to preserve the reviewed minor line.
- Minor or major upgrades are deliberate changes, not automatic merges. They require API/feature,
  MSRV, license, advisory, unsafe/native-code, and transitive-graph review.
- Prerelease versions, Git dependencies, wildcard requirements, and unknown registries are rejected.
- New dependencies must own a clear capability that is not reasonably provided by the standard
  library or an existing dependency. Convenience alone is insufficient.
- Framework types stay behind project boundaries at storage, cache, CEL, authentication, telemetry,
  and transport seams.
- Pure Rust is preferred. Native code is limited to reviewed cryptography/database components and
  is never wrapped by project `unsafe` code.
- Security fixes can override the normal cadence and must include the affected behavior and rollout
  risk in the pull request.

Dependabot opens grouped weekly Cargo patch/minor updates, npm updates for the official SDK smoke,
and GitHub Action updates. It does not merge them. Each pull request is evaluated against this
policy and the compatibility pins in [compatibility.md](compatibility.md).

## Required verification

For any Cargo manifest or lockfile change, including the separate fuzz workspace:

```sh
make check
make clippy-strict
make audit
make deny
```

Run affected differential, backend, migration, or performance gates when an update touches parsing,
serialization, networking, TLS, storage, concurrency, caching, CEL, generated protocol code, or hot
paths. `cargo audit` must report no known vulnerabilities. `cargo deny check` enforces allowed
licenses, registry/source policy, wildcard denial, and visibility of duplicate versions.

For GitHub Actions, scanners, database service images, or release tooling:

1. Read the upstream release notes from the primary repository or image publisher.
2. Pin actions to the immutable commit for the reviewed release tag and keep the tag in a comment.
3. Pin database images by patch tag and manifest digest.
4. Validate workflow syntax and run the closest local Make target.
5. Preserve least-privilege permissions, finite timeouts, checksums, SBOMs, and provenance.

## Generated and vendored inputs

`vendors/openfga` and `vendors/openfga-api` are Git submodules pinned by commit. Protocol imports,
source commits, license locations, hashes, protoc version, and generated output checksum are locked
in `crates/openfga-proto/proto.lock.json`. `make check-proto` regenerates into a temporary directory
and requires a byte-for-byte match.

Vendored source is not silently upgraded by Dependabot. Changing an upstream pin requires an
inventory diff plus all affected protocol, semantic, migration, differential, security, and
performance evidence.
