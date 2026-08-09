# Authentication and service-authorization runbook

Every OpenFGA API request is authenticated before JSON/protobuf decoding and then authorized for an
exact action and store before storage lookup. HTTP and gRPC share the same policy. HTTP `/healthz`
and `/readyz` and the standard gRPC health service are intentionally credential-free for
orchestration; isolate them at the network boundary.

The development profile additionally exposes credential-free `GET /capacityz` diagnostics on the
loopback-only listener for the bounded-load harness. Production does not register this route;
operators use the exported OpenTelemetry metrics instead.

## Modes

| Mode | Use | Startup behavior |
| --- | --- | --- |
| `disabled` | Local development only | Rejected unless the profile is `development` and both listeners are loopback |
| `preshared` | Static service-to-service credentials and bootstrap operations | Loads and hashes every referenced key; missing/invalid secrets fail startup |
| `oidc` | JWT access tokens from an allowlisted issuer | Fetches and validates discovery/JWKS before binding listeners; failure stops startup |

Exactly one mode is active. Settings for another mode are rejected rather than silently ignored.
Credentials are accepted only as a single `Authorization: Bearer ...` header/metadata value, never
in a query parameter.

## Preshared keys

```yaml
auth:
  mode: preshared
  preshared:
    keys:
      - id: orders-reader-v1
        keyEnv: OPENFGA_ORDERS_READER_V1
  authorization:
    bindings:
      - principal: orders-reader-v1
        actions: [read, check, batchCheck]
        stores: [01ARZ3NDEKTSV4RRFFQ69G5FAV]
```

- Configure 1–32 active keys. Identity labels and environment references must be unique, and every
  policy principal must name an active key identity.
- Key material must be 32–256 ASCII-graphic bytes. Generate it from a CSPRNG through the platform
  secret manager; length alone does not prove entropy.
- The runtime hashes keys and performs exact constant-time comparisons across every active key.
- The effective configuration and `Debug` surfaces contain references/counts, not key material.

Client example:

```text
Authorization: Bearer <preshared-secret>
```

### Rotate a preshared key

1. Create a new random secret under a new environment reference and a new identity label.
2. Add the new key and duplicate only the required policy grants for the new identity.
3. Validate configuration, restart/canary the server, and prove that both old and new credentials
   work only for their intended stores/actions.
4. Move clients to the new credential and observe authentication failures until the old credential
   has no use.
5. Remove the old key and its policy bindings, restart, verify the old credential returns the same
   generic 401 as any invalid credential, then revoke/delete it in the secret manager.

Do not reuse the continuation-token key as a preshared credential.

## OIDC

```yaml
auth:
  mode: oidc
  oidc:
    issuer: https://identity.example.com/tenant
    audiences: [openfga]
    authorizedParties: [openfga-client]
    algorithms: [RS256]
    allowedHosts: [identity-keys.example.com]
    maximumTokenBytes: 8192
    maximumDocumentBytes: 262144
    fetchTimeoutMs: 5000
    clockSkewSeconds: 30
    refreshIntervalSeconds: 3600
    staleKeyGraceSeconds: 86400
  authorization:
    bindings:
      - principal: deployment-subject
        actions: [read, check]
        stores: [01ARZ3NDEKTSV4RRFFQ69G5FAV]
```

The `principal` is the validated token `sub`. Configure stable, non-secret subjects; do not key
authorization to mutable display names.

OIDC requirements and limits:

- The issuer is an exact credential-free HTTPS URL, has no query/fragment/trailing slash, and uses
  a DNS name rather than an IP literal.
- The issuer host is automatically allowed. `allowedHosts` adds exact DNS hosts for a separate
  `jwks_uri`; do not add broad parent domains or wildcard-like values.
- At least one audience is required. `authorizedParties` is optional; when nonempty, `azp` must
  match one configured value.
- Supported asymmetric algorithms are `ES256`, `ES384`, `RS256`, `RS384`, `RS512`, `PS256`,
  `PS384`, `PS512`, and `EdDSA`. Keep the allowlist to what the issuer actually publishes. HMAC and
  token-supplied key URLs/keys are rejected.
- Discovery and JWKS requests use HTTPS only, no proxy or redirects, bounded time/body/key counts,
  exact host allowlisting, public-IP DNS validation, and connection-local pinned resolution.
- JWT validation requires a bounded `kid`, signature, `exp`, exact `iss`, accepted `aud`, and valid
  `sub`; it enforces `nbf` when present and bounds future `iat` by clock skew.

The discovery protocol and issuer/JWKS metadata are defined by
[OpenID Connect Discovery 1.0](https://openid.net/specs/openid-connect-discovery-1_0.html).

### JWKS rotation and outage

- Keep old and new signing keys published together for at least the maximum token lifetime plus
  deployment skew. The actor atomically replaces a verified key set on refresh.
- A token with an unknown `kid` requests a rate-limited refresh; normal scheduled refresh continues
  at `refreshIntervalSeconds`.
- Refresh failure retains the last verified keys only until `staleKeyGraceSeconds` after the last
  success. Readiness then fails and authentication returns a generic unavailable response; it never
  silently accepts unverifiable tokens.
- Initial discovery/JWKS failure prevents listeners from opening. Investigate issuer reachability,
  DNS answers, certificates, allowlisted hosts, discovery issuer equality, algorithms, duplicate
  `kid` values, and document size. Do not bypass TLS/SSRF checks to recover.

## Authorization policy

Bindings are additive and default deny. `stores: ["*"]` grants the listed actions across stores and
is mandatory for system actions. A wildcard cannot be combined with explicit store IDs.

| Class | Actions |
| --- | --- |
| System | `createStore`, `listStores` |
| Store administration | `getStore`, `updateStore`, `deleteStore` |
| Models/assertions | `readAuthorizationModels`, `writeAuthorizationModel`, `readAssertions`, `writeAssertions` |
| Tuples/changes | `read`, `write`, `readChanges` |
| Evaluation/enumeration | `check`, `batchCheck`, `expand`, `listObjects`, `streamedListObjects`, `listUsers` |

Grant the smallest finite action/store set. Reserve wildcard/system grants for explicit bootstrap or
break-glass identities and audit every configuration change. A denied existing store and a denied
missing store both return the same forbidden response; authorization runs before existence lookup.

## Response and audit semantics

| Condition | HTTP | gRPC |
| --- | --- | --- |
| Missing, duplicate, malformed, expired, or invalid credentials | `401` plus `WWW-Authenticate: Bearer` | `Unauthenticated` |
| Authenticated but action/store denied | `403` | `PermissionDenied` |
| OIDC correctness state stale/unavailable | `503` | `Unavailable` |

Bodies and statuses are generic. Denial audit events include principal kind, action, resource type,
and outcome, but not credential material, principal ID, store ID, tuple, object, or subject IDs.
