# Normative requirement traceability

This matrix covers every literal RFC 2119 `MUST` in the specification set at the Phase 5 baseline.
The linked controls are automated tests or inspectable release gates; changing a normative
requirement requires updating this matrix in the same change.

| Requirement | Control |
| --- | --- |
| PRD: name the exact upstream baseline and claim only tested compatibility | [`compatibility.md`](../compatibility.md), `verify-go-pin`, protocol lock, differential reports |
| PRD: provide OpenFGA API compatibility, safe semantics, durable backends, secure operation and reproducible evidence | `make phase5-release-gate`; CI Rust/backend/release jobs; Phase 0–5 evidence reports |
| PRD: AuthZEN must reuse the core but must not weaken or delay OpenFGA v1 | AuthZEN is explicitly outside the GA matrix and remains Phase 6; no AuthZEN transport exists in the Phase 5 binary |
| Domain: validate all wire values before domain/service/compiler/storage use | `openfga-transport` conversion and golden/error suites; domain private-field constructors and arbitrary-input properties |
| Domain parser: consume all input, reject controls/ambiguity, return typed bounded errors, and round-trip canonical values | `openfga-domain` reference/identifier property tests and fuzz targets |
| CEL: match the pinned supported type/helper/error semantics and reject unsupported extensions at compile time | `make cel-spike`; pinned Go conformance cases and Rust condition conformance suite |
| Model: hash iteration must not affect diagnostics, fingerprints, serialization or plans | deterministic compiler tests, model baseline, clean `check-corpus-differential` reproduction |
| Storage: tuple deletes/writes and matching change events must be one transaction | reusable storage contract plus mutation-stage fault suites on memory and all SQL backends |
| List queries: candidate generation must be conservative and independently bounded | generated-set/ListObjects properties, upstream enumeration differential, query-plan and cancellation suites |
| Verification: every normative MUST maps to a test or inspectable release control | This document plus `make check-docs`; Phase 5 independent spec/code review |

The global implementation constraints in `AGENTS.md` are additionally enforced by workspace
lints, `make check`, `make clippy-strict`, crate-root `forbid(unsafe_code)`, dependency policy, and
review. The detailed threat-to-test mapping is in the [threat model](../security/threat-model.md).
