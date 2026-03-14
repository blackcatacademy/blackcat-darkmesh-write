# ADR-002: Idempotency Strategy

- **Problem**: Avoid duplicate writes when commands are retried by clients or multiple gateways.
- **Decision**: Use a `Request-Id` registry with deterministic replay plus optimistic `Expected-Version` checks for mutable entities.
- **Rationale**: Simple, auditable, and compatible with multi-gateway submissions without central coordination.
- **Implications**:
  - Command handlers must be pure/idempotent given the same validated input.
  - Responses for replayed requests must return the original outcome.
  - Registry retention and eviction rules will be defined per process limits.
