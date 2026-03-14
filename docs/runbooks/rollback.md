# Rollback Runbook (placeholder)

- Disable new command intake if possible.
- Revert to last known good process version or configuration snapshot.
- Replay audit log to confirm stopped-at point; ensure idempotency registry survives rollback window.
- Re-run smoke fixtures to confirm stable outcomes.
