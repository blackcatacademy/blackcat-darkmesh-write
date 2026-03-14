# Verify scripts

This directory holds verification and smoke-check helpers for the write layer.

Current tools:

- `preflight.sh` — validates JSON schemas and checks Lua sources for syntax errors (`lua5.4` or `luac` required).

Usage:

```bash
scripts/verify/preflight.sh
```

Run this locally before opening a PR to catch obvious issues early.
