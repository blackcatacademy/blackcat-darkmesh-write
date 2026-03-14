# Contributing

This repository is **proprietary**. External contributions are accepted only with prior written permission from Black Cat Academy s. r. o.

## Working model
- Branches: `main` (releasable), `develop` (integration), `feature/*`, `adr/*`, `release/*`.
- Keep changes scoped: one command or contract per PR when possible.
- Command contracts and schemas are treated as public API; prefer additive changes over breaking ones.

## Prerequisites
- `lua5.4` (or `luac`) for AO command code.
- `python3` for schema validation.

## Pre-flight checks
Run before opening a PR:

```bash
scripts/verify/preflight.sh
```

This validates JSON schemas and runs Lua syntax checks.

## Pull requests
- Describe the change and affected command area (page, route, catalog, profile, permissions).
- Update docs and fixtures when changing contracts or schemas.
- Add or update tests once the harness is in place (contract, conflict, security).

## Reporting issues
- Non-security bugs and feature requests: GitHub Issues using the provided templates.
- Security issues: follow `SECURITY.md` and report privately.
