# Retry & Incident Response (placeholder)

- Identify failed commands via audit log (filter by status and action).
- For retryable failures, clients may resubmit with the same `Request-Id`; write process must return the original outcome or succeed without duplication.
- For non-retryable policy or schema failures, respond with deterministic error codes; do not mutate state.
- Track correlation IDs across gateway/admin adapters to aid postmortem.
