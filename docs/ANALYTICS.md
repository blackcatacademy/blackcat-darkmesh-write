# Analytics / Risk stubs

- `ao.shared.analytics.event(type, payload)` returns a timestamped event table; integrate into outbox if desired.
- TODO: emit events for order_created, payment_status_changed, shipment_updated into a dedicated analytics stream.
- TODO: add risk signals (ip_hash/device_id) and simple rule engine.
