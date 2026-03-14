# Command Pipeline

1) **Envelope intake**  
   Accept signed command with tags: `Action`, `Request-Id`, `Actor`, `Tenant`, `Expected-Version`, `Nonce`, `Signature-Ref`, `Timestamp`.

2) **Validation**  
   - Schema validation (JSON schemas in `schemas/`).  
   - Policy checks (role, tenant scope, capability, action allowlist).  
   - Temporal checks (timestamp drift, nonce freshness).

3) **Idempotency / anti-replay**  
   - Look up `Request-Id` registry; if seen, return recorded outcome.  
   - Enforce optimistic concurrency via `Expected-Version` when targeting mutable entities.

4) **Command execution**  
   - Route to specific handler (page, route, catalog, profile, permission).  
   - Produce deterministic status and any emitted events.

5) **Audit + events**  
   - Append audit record with request, actor, decision, and hash of payload.  
   - Emit domain event toward `blackcat-darkmesh-ao` processes for state materialization.

6) **Response**  
   - Return status + correlation IDs; never return secrets or unvalidated payloads.
