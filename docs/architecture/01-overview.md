# Architecture Overview

blackcat-darkmesh-write is an AO-native command layer. It enforces write semantics (validation, idempotency, audit) and pushes state changes into `blackcat-darkmesh-ao`, which remains the canonical data holder.

Goals:
- Centralize command validation and authorization in AO, not in gateway servers.
- Eliminate duplicate writes via request-level idempotency.
- Preserve an append-only audit trail correlated to each command.
- Keep the read model unchanged; write only issues deterministic events to the `-ao` state processes.

Non-goals:
- Hosting gateway rendering or frontend assets.
- Storing raw secrets or signing keys in AO state.
- Becoming an alternative database; canonical state stays in `-ao`.
