# Failure Modes & Guard Rails

- **Replay / duplicate submissions**: Prevented by `Request-Id` registry and deterministic replay response.
- **Concurrent updates**: Guarded by `Expected-Version`; conflicts return a stable conflict code.
- **Unauthorized action**: Policy layer rejects when actor role/tenant/capability is insufficient.
- **Stale timestamps / nonce**: Commands outside allowed clock skew or with used nonce are rejected.
- **Oversized or malformed payloads**: Schema + size limits enforced before handler execution.
- **Partial success**: Command either records audit + emitted events or fails atomically; no silent partial updates.
