# Security design

Status: Proposed · Applies to all components

## Trust boundaries and security properties

All HTTP/gRPC bytes, headers, tokens, model/tuple/context content, configuration, environment variables, datastore rows, replica state, OIDC discovery/JWKS, and continuation tokens are untrusted. Store isolation, authorization correctness, secret confidentiality, bounded resource use, and audit integrity are required properties.

The system fails closed for identity and store authorization, but reports evaluator/storage/resource errors rather than converting them to deny. An outage cannot become an allow; a cache outage falls back to authoritative evaluation.

## Authentication and service authorization

Production supports:

- **OIDC:** allowlisted HTTPS issuer, audience and authorized-party checks as configured, subject extraction, algorithm allowlist, clock-skew bounds, token byte cap, and signature/key validation. Discovery/JWKS fetching is SSRF hardened: HTTPS only, allowlisted host policy, resolved IP rejection for loopback/private/link-local ranges, response/body/redirect/time limits, and pinned resolution per connection. A supervised actor refreshes keys, supports overlapping rotation, and applies a documented stale-key grace policy.
- **Preshared keys:** at least 256 bits of entropy, held in `SecretString`/`SecretBox`, hashed or constant-time compared with `subtle`, multiple active keys for rotation, and never accepted in query parameters.
- **Disabled:** accepted only in explicit development mode when every listener is loopback; startup otherwise fails.

Authentication yields a validated `Principal`. A separate policy authorizes every operation and store, including store creation/listing/deletion and model/tuple administration. The access-control store cannot recursively depend on an unchecked request path; bootstrap/break-glass policy is explicit, minimal, and audited. Responses do not reveal whether a forbidden store exists.

## Transport and secrets

Public listeners require TLS using rustls with `aws-lc-rs`; certificate/key reload is validated and atomic. Plaintext is restricted to loopback or explicitly trusted sidecar configuration. Sensitive headers and contexts are redacted before tracing. Secret-bearing structures implement manual redacting `Debug` and tests assert absence from logs/errors/panics.

Continuation tokens are versioned and authenticated with a rotating key set. Signing uses a standard MAC/AEAD primitive and constant-time verification; keys never double as authentication credentials. Token payloads contain cursors and fingerprints, not secret tuple/context data.

## Input and resource controls

- Transport caps header/message/body/decompressed bytes before deserialization.
- Every string has a byte cap and grammar/charset allowlist where structured.
- Every collection and nested context has count, depth, and aggregate-byte caps; unknown fields are denied where project-owned.
- Every numeric input has an explicit permitted range.
- Model graph nodes, CEL AST/evaluation cost, recursion depth, evaluator dispatches, datastore queries/items, concurrent tasks, list candidates/results, pagination bytes, and response bytes are bounded.
- Endpoint and principal/IP rate limits protect unauthenticated, administrative, write, Check, and enumeration traffic separately.
- All external I/O has timeouts. Decompression is streaming with a hard expanded-byte limit.

SQL uses bound SQLx parameters only. DynamoDB uses only project-built key/condition expressions with SDK attribute placeholders; request data never becomes expression source or a table/index name. No user input reaches a shell. File paths for configuration/certificates/migrations are operator-configured, canonicalized, and constrained where applicable. URL fetching follows the OIDC SSRF policy. A DynamoDB endpoint override is development-only loopback HTTP; production relies on AWS endpoint resolution. Rust regex is allowed only with input/pattern size limits; user-supplied regex is not a product feature.

The DynamoDB runtime uses workload identity/default credential providers and an exact-table least-privilege IAM policy. Long-lived AWS credentials are not YAML fields. A separate provisioning role owns create/migration/PITR/KMS/deletion-protection operations. Runtime IAM omits `Scan`, table deletion, backup/restore, and wildcard resources. Customer-managed KMS keys, table names, Regions, ARNs, account IDs, physical keys, transaction tokens, tuple payloads, and SDK errors follow the secret/redaction policy in [`17-dynamodb-storage-design.md` § 9](17-dynamodb-storage-design.md#9-security-and-operational-boundary).

## Supply chain and memory safety

Every crate has `#![forbid(unsafe_code)]`; unsafe dependencies are minimized and audited, with no project wrapper claiming to make arbitrary FFI safe. CI runs dependency license/advisory policy and secret scanning. Builds pin toolchain/protocol generation and emit an SBOM/provenance for releases.

No panic is reachable from hostile input. Checked/saturating arithmetic is explicit for sizes, costs, offsets, and limits. Fuzz/property tests cover parsers, deserialization, CEL, tokens, model compiler, and evaluator graph generation.

## Threat-driven verification

Tests cover cross-store IDOR, authentication downgrade, algorithm confusion, key rotation, JWKS rebinding/oversize, timing-resistant preshared checks, token tamper/replay, SQL and DynamoDB-expression injection payloads, AWS endpoint SSRF, IAM overgrant, credential/error leakage, path traversal config, log forging/control characters, model bombs, CEL bombs, tuple/list floods, slowloris/slow consumer, cancellation storms, replica/eventual-read staleness, cache poisoning, and changelog loss.

## Acceptance criteria

- A documented threat model maps each trust boundary/abuse case to control and automated test before GA.
- Production cannot start in an unauthenticated or publicly plaintext state by accidental default.
- Cross-store operations fail without existence disclosure and have audit events.
- Fuzz/resource tests remain within declared memory/task/time ceilings and never panic.
- Release dependency audit, deny policy, secret scan, SBOM, and redaction suites pass.
