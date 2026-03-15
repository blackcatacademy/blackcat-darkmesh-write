# blackcat-darkmesh-write

AO-native command layer for Blackcat Darkmesh. This repository hosts the write-side AO processes that enforce idempotent, authorized, and auditable changes to the canonical state maintained in `blackcat-darkmesh-ao`. No separate server-side authority exists; any bridge or admin client is only a transport adapter.

## Scope
- In scope: AO command processes, handlers, idempotency registry, audit/event emission, publish workflow (draft → review → publish → rollback), validators and schemas, minimal adapters, deploy/verify scripts, fixtures, CI workflows.
- Out of scope: read/state model (lives in `blackcat-darkmesh-ao`), gateway rendering, frontend assets, secrets or signing keys.

## Architecture Snapshot
- Role: command-first AO process set that owns write semantics, conflict detection, and append-only audit; delegates state materialization to `blackcat-darkmesh-ao`.
- Pipeline: command envelope → validation (schema + policy) → idempotency / anti-replay → handler → audit + event → downstream AO state update.
- Identity & auth: signed commands or capability tokens; gateway is never an implicit authority.
- Idempotence: `requestId` registry and optimistic `expectedVersion` guards to prevent duplicate writes.
- Audit: append-only log with correlation to requestId and actor; deterministic status codes.

## Repository Layout (blueprint)
```
docs/              # command contracts, flows, failure modes, ADRs, runbooks
ao/                # AO command process and shared libs
  write/           # command handlers, routing
  shared/          # auth, idempotency, validation, audit
schemas/           # JSON schemas for command envelopes and actions
scripts/           # deploy | verify
fixtures/          # sample command envelopes and expected outcomes
tests/             # contract, conflict, and security tests
scripts/bridge/    # stub forwarder from write outbox to -ao
.github/workflows/ # CI entrypoint
```

## Minimal Command Envelope
- Required tags: `Action`, `Request-Id`, `Actor`, `Tenant`, `Expected-Version`, `Nonce`, `Signature-Ref`, `Timestamp`.
- Core handlers (initial set): `SaveDraftPage`, `PublishPageVersion`, `UpsertRoute`, `UpsertProduct`, `AssignRole`, `UpsertProfile`, `GrantEntitlement`.
- Conflict strategy: reject on missing/expired nonce, replayed `Request-Id`, or mismatched `Expected-Version`; return prior result when replayed.

## Development
- Prereqs: `lua5.4` (or `luac`) and `python3`.
- Static checks: `scripts/verify/preflight.sh` (JSON schema validation + Lua syntax).
- Contract smoke tests: `LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" lua5.4 scripts/verify/contracts.lua` (or set `RUN_CONTRACTS=1` to run during preflight).
- Conflict/security smoke tests: `LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" lua5.4 scripts/verify/conflicts.lua` (or `RUN_CONFLICTS=1`).
- Branches: `main` (releasable), `develop` (integration), `feature/*`, `adr/*`, `release/*`.
- Message contracts and schemas are public API; prefer additive changes over breaking ones.

## Env toggles (write process)
- `WRITE_REQUIRE_SIGNATURE=1` — reject commands without `signatureRef`.
- `WRITE_REQUIRE_NONCE=1` — reject commands without nonce and block replay.
- `WRITE_NONCE_TTL_SECONDS` (default 300) and `WRITE_NONCE_MAX` (default 2048) — nonce cache sizing.
- `WRITE_ALLOW_ANON=1` — allow missing actor/tenant (off by default).

## Bridge (stub)
- `scripts/bridge/forward_outbox.lua` reads the in-memory outbox (`write._storage_outbox()`) and logs events you would forward to `blackcat-darkmesh-ao`. Replace `forward_event` with signed POST to AO endpoint (registry/site process) in production.

## Security Guard Rails
- No secrets or raw keys in AO state, manifests, or adapters.
- Gateways act only as clients; write process re-validates auth and policy.
- All comments and docs remain in English.

## License
Blackcat Darkmesh Write Proprietary License (see `LICENSE`). External contributions require written permission from Black Cat Academy s. r. o.
