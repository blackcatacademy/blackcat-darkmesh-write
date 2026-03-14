# Repo setup checklist

For maintainers configuring this repository in GitHub.

- **Visibility & license**: keep the repo private/proprietary; reference `LICENSE` in the repo description.
- **Default branch**: `main`; create `develop` for integration work.
- **Branch protection**:
  - Require PRs to target `develop` or `main`.
  - Require status check `Static checks` (from `CI` workflow) before merge.
  - Require at least one approval.
- **Issue templates**: enable the bundled templates; disable blank issues.
- **Security**: link to `SECURITY.md`; ensure security advisories are enabled; direct reporters to `blackcatacademy@protonmail.com`.
- **Dependabot**: enable alerts and the weekly GitHub Actions update from `.github/dependabot.yml`.
- **Actions permissions**: restrict to GitHub-hosted runners; disable self-hosted unless needed for AO testnets.
