# Survey: Rust ecosystem for an OpenFGA-compatible server

Status: Done · Owner: openfga-rs · Date: 2026-08-05 · Sources checked: official project repositories, docs.rs/crates.io metadata, Rust release announcements, and OpenFGA documentation

## Question

Which maintained Rust components best fit a safe, async, API-compatible OpenFGA server today, and where is the ecosystem insufficient for an unconditional dependency choice?

## Current baseline

Rust `1.97.1` is the current stable toolchain on 2026-08-05; the official release index lists 1.97.0 and 1.97.1 in July 2026. The workspace must use edition 2024 and pin `1.97.1` in `rust-toolchain.toml`. Source: [Rust release announcements](https://blog.rust-lang.org/releases/).

Versions below were resolved from crates.io on 2026-08-05. They are research pins, not permission to add every crate. Implementation must re-run the same maintenance/security check immediately before changing a manifest.

| Concern | Candidate/version | Decision |
| --- | --- | --- |
| Runtime | `tokio 1.53.1` | Adopt with explicit `rt-multi-thread`, `macros`, `net`, `signal`, `sync`, `time` features. |
| gRPC/protobuf | `tonic 0.14.6`, `tonic-prost 0.14.6`, `prost 0.14.4` | Adopt; tonic is the production Tokio/Tower-native gRPC implementation and supports streaming, TLS, health, and reflection. [Official repository](https://github.com/grpc/grpc-rust). |
| HTTP/JSON | `axum 0.8.9`, `tower 0.5.3`, `tower-http 0.7.0`, `pbjson 0.9.0` | Adopt explicit OpenFGA HTTP routes and shared Tower middleware; generate protobuf-JSON-compatible Serde implementations from the pinned descriptor set rather than hand-maintaining a second wire schema. [Axum](https://github.com/tokio-rs/axum), [pbjson](https://github.com/influxdata/pbjson). |
| SQL | `sqlx 0.9.0` | Adopt with only `runtime-tokio`, `postgres`, `mysql`, `sqlite`, `migrate`, required type features; use checked queries. SQLx remains pure Rust and current 0.9 moved maintenance organization. [0.9 release](https://github.com/transact-rs/sqlx/discussions/4271). |
| Cache | `moka 0.12.15` | Adopt for bounded concurrent weighted/TTL caches; isolate behind project traits. [Official repository](https://github.com/moka-rs/moka). |
| Parsing | `winnow 1.0.4` | Adopt for tuple/user/reference and continuation-token grammars; zero-copy where ownership permits. [Official repository](https://github.com/winnow-rs/winnow). |
| Validation | `validator 0.21.0` plus domain constructors | Use `validator` for wire structs, but put semantic guarantees in private-field newtypes. [Official repository](https://github.com/Keats/validator). |
| Telemetry | `tracing 0.1.44`, `tracing-subscriber 0.3.23`, `opentelemetry 0.32.0`, `opentelemetry-otlp 0.32.0` | Adopt with structured redaction; metrics may use the OTel metrics SDK or Prometheus exporter through one facade. [Official OTel Rust repository](https://github.com/open-telemetry/opentelemetry-rust). |
| TLS/secrets | stable `rustls 0.23.x` with `aws-lc-rs`; `secrecy 0.10.3` | Adopt stable rustls only—not the 0.24 development release—and redacting secret types. [rustls](https://github.com/rustls/rustls), [secrecy](https://github.com/iqlusioninc/crates/tree/main/secrecy). |
| Configuration | `config 0.15.25`, `serde_yaml`-compatible YAML provider | Adopt behind `ServerConfig`; YAML file + environment overrides; validate once into domain config. [Official repository](https://github.com/rust-cli/config-rs). |
| Tests/benchmarks | `rstest 0.26.1`, `proptest 1.11.0`, `testcontainers 0.27.3`, `criterion 0.8.2` | Adopt only in dev-dependencies and only where each adds signal. |
| CEL | `cel-parser 0.10.1`; project evaluator | Use the pure-Rust parser behind project types and implement the bounded evaluator in project code. Phase 0 rejected `cel-interpreter 0.10.0`; see the [conformance spike](./spike-cel-openfga-conformance.md). [Official repository](https://github.com/cel-rust/cel-rust). |

## Transport choice

Tonic and Axum share Tokio, Hyper, and Tower, so one middleware policy can cover request IDs, tracing, authentication, timeouts, concurrency, load shedding, and body limits. Tonic's released branch documents gRPC streaming, rustls TLS, health, and reflection, while Axum 0.8 is the stable API line. A hand-written HTTP façade is preferable to a generic runtime transcoder when exact OpenFGA error bodies and route quirks need compatibility: generated protobuf messages remain the single wire model, and route adapters remain thin.

`tonic-prost-build` invokes `protoc` in common workflows. Builds do not depend on an ambient tool version: Phase 0 pinned API commit `f153694b…`, uses checksum-verified `protoc 31.1`, checks in generated Rust, and requires byte-for-byte regeneration. See the [protocol spike](./spike-openfga-proto-generation.md).

## Storage choice

SQLx 0.9 supplies async PostgreSQL/MySQL/SQLite drivers, migrations, transactions, pools, and compile-time query checking without an ORM abstraction that obscures index-critical queries. OpenFGA's hot paths are explicit query shapes, so repository code should use backend-specific checked SQL where syntax/query plans diverge and share row mapping/transaction policy at a higher layer.

An embedded in-memory backend should use immutable compiled models plus actor-owned maps/indexes. `DashMap` is not automatically appropriate: multi-index tuple writes and changelog append require a transaction boundary, which is easier to reason about in one actor. SQL backends remain naturally pool-concurrent.

## CEL gap analysis

OpenFGA conditions are typed CEL Boolean expressions with scalar, duration, timestamp, bytes, list, map, any, and custom IP address values. OpenFGA tracks actual evaluation cost, caps it (default 100), supports partial/unknown evaluation, makes tuple context override request context, and returns an error for missing required parameters. See [OpenFGA conditions](https://openfga.dev/docs/modeling/conditions) and [CEL specification](https://github.com/cel-expr/cel-spec).

Phase 0 proved that `cel-interpreter` offers useful baseline value semantics but fails the required static typing, OpenFGA IP address, deterministic cost, prompt cancellation, and panic-freedom gates. Therefore:

1. Define a project-owned `ConditionCompiler`/`CompiledCondition` boundary.
2. Test the adapter against CEL conformance cases and vendored OpenFGA condition tests.
3. Implement OpenFGA types/functions and a deterministic project cost meter in a project-owned evaluator over validated project IR lowered from the parser AST.
4. Reject model publication if compilation or Boolean output typing fails.
5. Do not use the C++ FFI implementation by default: it conflicts with the repository's no-unsafe rule and increases build/supply-chain complexity.

The compatibility gate is resolved: `cel-interpreter` is retained only as historical research evidence and is no longer a dependency; production depends on `cel-parser` and project-owned evaluation.

## Authentication and authorization

Use rustls with the `aws-lc-rs` provider at transport boundaries. OIDC validation needs issuer/audience/subject checks, algorithm allowlisting, bounded discovery/JWKS responses, SSRF-safe issuer/JWKS URLs, background refresh, key rotation, and stale-key policy. `jsonwebtoken` may decode/verify tokens, but discovery and refresh remain project-owned actor behavior. Preshared keys use `secrecy` and constant-time comparison. Unlike upstream's optional unauthenticated mode, production configuration must require an authentication mode; an explicit loopback-only development mode may disable it.

OpenFGA's own access-control-store behavior should be a separate authorization policy layer after authentication. This avoids conflating whether a token is valid with whether that caller may operate on a store.

## Concurrency and state ownership

- Request fan-out: `tokio::task::JoinSet`, `CancellationToken`, and bounded semaphores. All tasks are joined.
- Actor lifecycle: supervised Tokio tasks with bounded `mpsc` command queues for cache invalidation, JWKS refresh, and optional adaptive planning.
- Hot immutable config/model handles: `ArcSwap` only when measurements justify it; a cache returning `Arc<CompiledModel>` is sufficient initially.
- In-memory datastore: one actor owns tuple indexes so forward/reverse index and changelog updates are atomic without mutexes.
- SQL datastore: pools own connection concurrency; per-request and server-wide semaphores prevent pool starvation.

Native async trait methods are appropriate for statically dispatched internal traits. Storage plugins need `Arc<dyn Trait>` and therefore use `async-trait`; each module documents that object-safety exception as required by project policy.

## Dependency policy

1. Every dependency is declared once in `[workspace.dependencies]` with explicit features and a compatible patch policy.
2. `default-features = false` unless defaults were reviewed and intentionally selected.
3. No dependency is adopted solely because it appears in this survey; the implementing phase records license, maintenance, RUSTSEC, transitive native code, and MSRV checks.
4. Dependency/lock changes run `cargo audit` and `cargo deny check`.
5. Public project traits isolate CEL, cache, clock, ID generation, authentication, telemetry, and storage from third-party concrete types.

## Recommendation

Build on Tokio + Tonic/Prost + Axum/Tower + SQLx + Moka + tracing/OpenTelemetry. Use Winnow and domain newtypes for parsing and validation. Keep CEL behind the project contract and use the selected parser plus bounded project evaluator. This combination is idiomatic, mostly pure Rust, compatible with the repository's safety rules, and does not force authorization semantics into framework types.
