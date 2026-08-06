# Phase 0 differential harness report

Status: Passing foundation smoke · Date: 2026-08-05

## Scope

This report proves the Phase 0 harness lifecycle and mismatch reporting before the Rust server has
authorization semantics. It does not claim endpoint or feature parity. Phase 1 extends the same
harness with Check cases; later phases add every delivered API operation.

The harness builds the vendored Go server at commit
`4e4f79ed841513dfd61746a75ef473f6198299f7` with checksum-verified Go `1.26.5`, starts it with the
in-memory datastore, starts the Rust Phase 0 probe, waits on both loopback health endpoints, runs
the Rust normalizer/comparator, then runs the official JavaScript SDK smoke against Go. A trap
terminates and waits for both processes on success, error, or interruption.

## Normalization contract

The health comparator preserves HTTP status and maps only a case-insensitive JSON `status` value
of `SERVING` to the Boolean `serving: true`. It does not discard any other compared field. Bodies
are capped at 4 KiB, URLs are capped at 1 KiB, and only credential-free loopback IP-literal HTTP
URLs are accepted. Redirects are disabled, so a validated origin cannot escape that boundary. The
probe itself caps request bodies at 1 KiB, concurrent requests at 16, and request execution at five
seconds. Mismatches name the exact normalized field and both values without including response
bodies or identifiers.

Observed report:

```json
{
  "go": {
    "httpStatus": 200,
    "serving": true
  },
  "rust": {
    "httpStatus": 200,
    "serving": true
  },
  "mismatches": []
}
```

The official `@openfga/sdk 0.9.6` create/get/delete store sequence then returned `status: pass`, and
`npm audit --audit-level=moderate` reported zero vulnerabilities with the reviewed Axios override.

## Reproduction

```bash
make differential-smoke
```

The target bootstraps Go only when absent, verifies the archive checksum before extraction, builds
from the vendored source with `GOTOOLCHAIN=local` and `-mod=readonly`, and writes tools only under
the ignored `.tools` directory.
