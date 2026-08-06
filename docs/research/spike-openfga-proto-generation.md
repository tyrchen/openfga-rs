# Spike: deterministic OpenFGA protocol generation

Status: Accepted · Baseline: OpenFGA `4e4f79ed841513dfd61746a75ef473f6198299f7`

## Decision

Generate and check in Tonic/Prost artifacts from the exact OpenFGA API revision used by the
vendored Go baseline. A dedicated Rust code-generation package verifies every input and the
platform-specific `protoc` binary before emitting code. `make check-proto` regenerates into a
temporary directory and requires a byte-for-byte match.

The source/tool pins are:

| Input | Pin | Integrity evidence |
| --- | --- | --- |
| OpenFGA server | `4e4f79ed841513dfd61746a75ef473f6198299f7` | `vendors/openfga` gitlink |
| OpenFGA API | `f153694bfc20f7be303e33cabe72b668596c5a06` | Git archive SHA-256 `1c6139e0…9b34`; per-input SHA-256 checks |
| API dependencies | exact BSR commits in upstream `buf.lock` | `buf.lock` SHA-256 `18331710…9961`; path-independent vendored aggregate `8afaacc6…7d28` |
| `protoc` | `31.1`, distributed by `protoc-bin-vendored 3.2.0` | platform binary SHA-256 allowlist in `proto.lock.json` |
| Tonic / Prost / pbjson | `0.14.6` / `0.14.4` / `0.9.0` | `Cargo.lock` registry checksums |

The complete unabridged hashes are in
[`crates/openfga-proto/proto.lock.json`](../../crates/openfga-proto/proto.lock.json).
The generated aggregate hashes each artifact's relative filename, a NUL separator, and its bytes
in the generator's fixed order. It is therefore independent of the checkout or temporary output
path while detecting both renames and content changes.

The three imported BSR modules are recorded by module commit and digest. Their source repositories,
reviewed license-source commits, local license paths, and license SHA-256 values are also locked.
The generator validates the API git archive, every API input, the upstream `buf.lock` entries, the
vendored import aggregate, each license, the active `protoc` binary, and generated artifacts before
it succeeds; changing a descriptive lock field without matching evidence is therefore a failure.

## Generation and ownership

```text
vendors/openfga-api @ f153694b
  ├─ openfga/v1/*.proto ───────────────┐
  ├─ docs/openapiv2/*.swagger.json ─┐  │
  └─ buf.lock ──▶ vendored imports  │  │
                                      │  ▼
protoc-bin-vendored 3.2.0 ──verify──▶ openfga-proto-codegen
                                      │  ├─ openfga.v1.rs
descriptor set ──▶ pbjson-build ──────┤  ├─ openfga.v1.serde.rs
upstream Swagger ──route extraction───┘  ├─ openfga_descriptor.bin
                                         └─ route_metadata.rs
                                                   │
                                                   ▼
                                           openfga-proto crate
```

The generated crate owns wire messages, protobuf-JSON-compatible Serde implementations, Tonic
clients/servers, the descriptor set, and the 18 OpenFGA v1 HTTP route templates. AuthZEN paths in
the merged Swagger document are deliberately excluded until Phase 6. Generated source is not
hand-edited. Missing upstream proto comments are the sole reason the generated module locally
allows `missing_docs`; project-owned public items remain documented.

## HTTP route proof

Route metadata is derived from the API revision's generated Swagger rather than duplicated from
memory. The test `test_should_generate_every_openfga_v1_http_route` verifies the route count,
the Check mapping, and the AuthZEN exclusion. The descriptor-set test proves that protocol
descriptors are emitted alongside Rust code.

## SDK smoke

`make differential-smoke` starts the exact vendored Go source, then uses the official JavaScript
SDK `@openfga/sdk 0.9.6` to create, read, and delete a store. The SDK pins a vulnerable Axios
release, so the smoke lockfile overrides it with Axios `1.19.0`; the smoke verifies compatibility
and runs `npm audit --audit-level=moderate`, which reports zero vulnerabilities.

The smoke deliberately targets only loopback HTTP, validates returned store identifiers, and
does not print the created identifier. It proves that the pinned protocol remains consumable by
an official SDK before Rust implements the full transport.

## Reproduction

```bash
make proto
make check-proto
cargo test -p openfga-proto
make differential-smoke
```

Observed on 2026-08-05: regeneration had no diff, both protocol tests passed, normalized Go/Rust
health observations matched, the SDK create/get/delete sequence passed, and npm reported zero
known vulnerabilities.

## Dependency review

- Tonic and Prost are actively maintained, permissively licensed, and below the pinned Rust MSRV.
- `protoc-bin-vendored` is build tooling only. It avoids host tool drift at the cost of carrying
  platform binaries; every supported binary is checksum-verified before execution.
- Protocol imports are committed from the exact BSR commits in the API `buf.lock`, so normal
  generation performs no network lookup.
- Generated framework types remain confined to `openfga-proto` and future transport conversion;
  they do not enter domain or engine contracts.
