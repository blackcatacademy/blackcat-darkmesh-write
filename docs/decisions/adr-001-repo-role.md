# ADR-001: Role of blackcat-darkmesh-write

- **Context**: Clarify whether write is an external server or an AO-native component.
- **Decision**: blackcat-darkmesh-write is an AO command process (or set of processes) that enforces write semantics, idempotency, and audit. No separate server-side authority is allowed.
- **Consequences**:
  - Canonical data remains in `blackcat-darkmesh-ao`.
  - Any bridge or gateway acts only as a client/adapter; it is not a source of truth.
  - Command contracts must be stable and treated as public API.
