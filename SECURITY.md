# Security policy

Authorization software is security-sensitive. Please report suspected vulnerabilities privately and
give maintainers a reasonable opportunity to investigate before public disclosure.

## Supported versions

The project currently supports the latest released `0.1.x` line and the default branch. Older
commits, forks, locally modified builds, unpinned OpenFGA/API revisions, and configurations outside
the [compatibility matrix](docs/compatibility.md) are not maintained security branches.

## Reporting a vulnerability

Use the repository's **Security → Report a vulnerability** flow to open a private report. If private
vulnerability reporting is unavailable, open a public issue containing only a request for a private
maintainer contact; do not include exploit details, secrets, affected tenant/store identifiers, or
production data.

Include when possible:

- affected commit/version and deployment profile;
- impact and required attacker access;
- minimal reproduction or failing test using synthetic data;
- whether the issue affects confidentiality, decision integrity, availability, authentication,
  authorization, token scope, storage consistency, migration, or supply chain;
- suggested mitigation and whether coordinated disclosure has a deadline.

Maintainers will acknowledge a complete private report, reproduce and classify it, coordinate a fix
and advisory, and credit the reporter unless anonymity is requested. Response times are best-effort;
do not interpret silence as permission to publish secrets or active exploitation instructions.

## Disclosure and fixes

Security fixes must include regression evidence, affected-version analysis, operator mitigation,
and updates to the [threat model](docs/security/threat-model.md) or relevant runbook. Releases use
checksummed artifacts, CycloneDX/SPDX SBOMs, and GitHub build/SBOM attestations. Verify downloaded
artifacts as described in [docs/releasing.md](docs/releasing.md).

Operational incidents that do not reveal a new vulnerability belong in the
[failure-response runbook](docs/operations/failure-response-runbook.md). General feature requests
and hardening ideas can use ordinary issues as long as they do not disclose an exploitable path.
