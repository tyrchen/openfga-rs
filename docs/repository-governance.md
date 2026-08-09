# GitHub repository governance

Source-controlled workflows cannot enforce every GitHub repository setting. Apply and periodically
audit this checklist before treating a public fork as an official release source.

## Default branch

- Set the default branch to the branch targeted by `.github/workflows/build.yml` (`master` unless
  the workflow and documentation are changed together).
- Protect it from force pushes and deletion.
- Require pull requests, resolved review conversations, and at least one approval from someone other
  than the last committer for security-, release-, or compatibility-sensitive changes.
- Require the Rust, dependency-review, compatibility, PostgreSQL, MySQL, and SQLite CI checks that
  apply to the pull request. Do not permit an administrator bypass for ordinary releases.
- Dismiss stale approvals when code changes and require a linear, attributable history.

## Actions and releases

- Keep the default `GITHUB_TOKEN` permission read-only. Grant write, OIDC, attestation, and artifact
  permissions only on the tag jobs that need them, as the checked-in workflow does.
- Allow only actions pinned to a full commit SHA. `make check-docs check-actions` enforces this in
  source; Dependabot updates the reviewed SHA and release comment together.
- Require approval for workflows first run by outside contributors and do not expose release or
  database secrets to forked pull requests.
- Protect `v*` tags from mutation and limit release creation to maintainers. Never reuse a version.
- Keep GitHub artifact attestations and immutable releases enabled. Verify tag artifacts using
  [releasing.md](releasing.md) before promotion to another distribution channel.

## Security features

- Enable the dependency graph, Dependabot alerts, Dependabot security updates, secret scanning, and
  push protection where the hosting plan supports them.
- Enable private vulnerability reporting so [SECURITY.md](../SECURITY.md) has a concrete confidential
  channel. Assign at least two maintainers who can receive security notifications.
- Enable GitHub's immutable release setting and retain audit logs for branch, tag, environment,
  secret, and Actions-policy changes.
- Review CodeQL or another Rust-capable static analysis service before enabling it; do not claim
  language coverage that the selected analyzer does not provide.

## Maintainers and community

- Publish current maintainer identities and a private conduct/security contact through repository
  owner profiles or organization settings.
- Apply least-privilege roles and require strong authentication for release-capable maintainers.
- Keep issue forms, pull-request template, contribution guide, code of conduct, security policy,
  license, NOTICE, compatibility matrix, and changelog visible in the repository community profile.
- Record changes to licensing, compatibility scope, release authority, or governance in the
  key-decisions log and changelog.

## Periodic audit

At least before each release, verify protected-branch requirements match current job names, every
required maintainer still has appropriate access, private reporting works, Dependabot is active,
tag rules cover the next version, and no environment or Actions secret is available to untrusted
pull-request code. Repository settings are part of the release evidence even though they are not
stored in Git.
