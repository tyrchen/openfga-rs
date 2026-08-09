# Developer experience and repository design

Status: Proposed · Applies to all phases

## Toolchain and code policy

The workspace pins Rust `1.97.1` in `rust-toolchain.toml`, uses edition 2024/resolver 3, and centralizes dependencies with reviewed explicit features in `[workspace.dependencies]`. Every crate root includes `#![forbid(unsafe_code)]` and warns for `rust_2024_compatibility`, `missing_docs`, and `missing_debug_implementations`. Public items have useful docs, errors, and runnable examples.

Production code has no `unwrap`, `expect`, `todo`, unchecked indexing on hostile data, or dead-code suppressions. Libraries use domain `thiserror`; the binary uses `anyhow` only for assembly/context. Imports, naming, serde, async, testing, and documentation follow the repository `AGENTS.md`; these rules are binding acceptance criteria, not suggestions.

## Automation

Discoverable Makefile targets own repeatable workflows:

- `make build`, `test`, `fmt`, `clippy`, and aggregate `check`;
- `make proto`, `check-proto`, and compatibility fixture generation;
- `make migrate`, backend integration tests, conformance, differential, fuzz, and benchmark gates;
- `make audit`, `deny`, documentation/link checking, and agent-file synchronization.

New automation is a Makefile target invoking stable tools; project-specific shell scripts are not added. Commands are noninteractive and CI/local equivalent. Generated outputs have source pins and reproducibility checks.

## Dependency governance

Before each manifest change, verify current versions and usage from primary sources; record feature rationale, license, maintenance, MSRV, RUSTSEC, native/unsafe/transitive surface, and alternatives. Use `default-features = false` where reviewed. Dependency changes run build/test/nightly fmt/clippy, `cargo audit`, and `cargo deny check`.

Framework types stay behind project contracts at load-bearing seams: storage, CEL, cache, auth, clock/ID, and telemetry. Avoid abstraction around ordinary value types where it adds no substitution value.

## Testing ergonomics

Unit tests live with code and use `test_should_...` names. `rstest` handles meaningful tables, `proptest` invariants, `wiremock` OIDC/JWKS, and testcontainers real SQL. Fixtures derived from upstream record commit/path/license and are transformed mechanically where possible. Slow suites are tagged/ignored for explicit CI stages, never silently skipped.

No test-only unsafe or production panic shortcuts. Deterministic clocks, IDs, randomness, and cancellation hooks replace sleeps. Failure messages state model/seed/backend without exposing secret contexts.

## Documentation and contribution flow

Architecture/spec changes update `specs/index.md` and the decision log. Research belongs under `docs/research` and is source-pinned. Public behavior has doc examples and operator docs. Pull requests state upstream baseline, semantic risks, tests run, performance evidence when relevant, and migration/security impact.

Project contributions are Apache-2.0. Source and binary distributions retain `LICENSE`, project and
upstream `NOTICE` attribution, and all separately vendored license material. Dependency-license
policy is distinct from the project license and remains enforced over the complete resolved graph.

## Acceptance criteria

- A clean checkout builds and regenerates documented artifacts with pinned tools.
- Full Rust gates pass for code/API/manifest changes; documentation-only changes run link/artifact checks rather than mechanical Rust builds.
- No duplicated dependency versions/features are introduced without justification.
- Contributor instructions lead to the same conformance and backend tests used in CI.
