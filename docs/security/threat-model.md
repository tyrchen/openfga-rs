# GA threat model

## Assets and trust boundaries

The protected assets are authorization decisions, tenant/store isolation, models and tuples,
credential and token confidentiality, changelog integrity, and finite service capacity. Untrusted
inputs cross six boundaries: HTTP/gRPC clients, authentication authorities, operator
configuration/secrets, SQL datastores and replica state, cache/changelog state, and build/release
inputs. A deny is never fabricated from an internal failure; failures remain typed errors, while
identity and operation/store authorization fail closed.

## Threat-to-control closure

| Threat | Preventive control | Verification/evidence |
| --- | --- | --- |
| Authentication downgrade or public plaintext | Production requires TLS and rejects disabled authentication; both listeners share one atomically reloaded rustls/aws-lc identity | `config::tests::test_should_reject_unknown_and_insecure_public_configuration`; runtime TLS reload test |
| OIDC algorithm confusion, malicious discovery/JWKS, SSRF or DNS rebinding | Asymmetric algorithm allowlist; HTTPS/host/IP allowlists; pinned resolved address; redirect, size, time and stale-key bounds | `openfga-auth` OIDC SSRF, oversize, algorithm-confusion, claim and rotation tests |
| Preshared-key disclosure or timing oracle | Secret wrappers, redacting `Debug`, bounded header, multiple active keys and constant-time comparison | `authenticate` redaction, header and every-active-key tests |
| Cross-store IDOR or existence disclosure | Authentication precedes admission/validation; every operation is checked against principal/action/store policy; denials share a public error | policy fail-closed tests and dual-transport authorization-order tests |
| Continuation-token tamper, replay or cross-query use | HMAC-authenticated, versioned, expiring, scope-bound tokens with rotating key IDs and constant-time verification | token tamper/scope/replay/expiry and rotation property tests; transport exact-scope test |
| Parser, model, CEL or enumeration resource bomb | Boundary newtypes plus byte/count/depth/cost/query/dispatch/result/concurrency limits; iterative graph work; cancellation and deadlines | domain arbitrary-input properties, model/CEL conformance, evaluator cancellation, generated enumeration suites |
| SQL injection or cross-tenant query | Static SQL with bound SQLx parameters; validated domain newtypes before storage; store ID included in every tenant query | shared storage contracts, hostile boundary transport tests, backend query-plan suites |
| Partial tuple mutation or missing invalidation signal | Tuple and changelog writes share one transaction; injected faults cover every mutation stage | memory/PostgreSQL/MySQL/SQLite contract and mutation-fault suites |
| Stale cache becomes an incorrect high-consistency answer | Higher consistency bypasses mutable caches and routes to primary; gaps, lag, overflow, failure and restart disable/flush conservatively | cache fault/model tests and Phase 4 cross-process consistency evidence |
| Slow client, cancellation storm or task leak | Bounded admission/channels/semaphores; owned structured tasks; request guards cancel storage work; bounded drain | transport disconnect tests, runtime in-flight shutdown test, Phase 4 soak |
| Secret/log injection | Structured tracing with bounded labels; sensitive headers/query tokens omitted; domain and config `Debug` redact hostile values | debug redaction tests and HTTP span tests |
| Malicious dependency or release substitution | Exact tool/dependency pins, checksum-verified scanners, advisory/license policy, full-history secret scan, CycloneDX/SPDX SBOM, SHA-256 manifests and GitHub OIDC/Sigstore attestations | `make audit deny secret-scan release-artifacts`; tag-only release artifact matrix |

## Residual risk and operating assumptions

- SQLite serializes work through one connection and relies on filesystem durability and access
  controls supplied by the operator. Use PostgreSQL or MySQL for multi-instance production.
- Database credentials, TLS keys, token keys, backup encryption and OIDC allowlists remain operator
  responsibilities. The server consumes environment references and never prints the referenced
  values.
- Dependency crates contain reviewed transitive unsafe/FFI, notably SQLite. Every project crate
  forbids unsafe code; `cargo audit`, `cargo deny`, SBOM review and lockfile review control the
  remaining supply-chain risk.
- Rate limits identify the direct TCP peer. A proxy must enforce original-client limits because
  forwarded client-IP headers are deliberately untrusted.
- The upstream migration tool accepts only the pinned OpenFGA SQLite layout and requires a new,
  empty destination. Any other upstream schema needs a separately reviewed converter.

## Release response

Any unexplained authorization mismatch, secret finding, advisory-policy failure, schema checksum
mismatch, failed restore, or invalid provenance blocks release. Preserve redacted logs and artifacts,
rotate possibly exposed credentials, and follow the failure, authentication, migration, and
backup/restore runbooks. Security controls may not be bypassed to restore availability.
