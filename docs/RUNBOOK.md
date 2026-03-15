
## Checksum daemon (write)
Example systemd unit:
```
[Unit]
Description=Write checksum monitor
After=network.target

[Service]
WorkingDirectory=/opt/blackcat-darkmesh-write
Environment=CHECKSUM_INTERVAL_SEC=300
Environment=WRITE_WAL_PATH=/var/log/ao/write-wal.ndjson
Environment=AO_QUEUE_PATH=/var/lib/ao/outbox-queue.ndjson
ExecStart=/opt/blackcat-darkmesh-write/scripts/verify/checksum_daemon.sh
Restart=always

[Install]
WantedBy=multi-user.target
```

## 3‑DS / SCA callback (PaymentReturn)
- Frontend/resolver should POST `PaymentReturn` to write AO after the user finishes the provider challenge/redirect:
  - provider: stripe|paypal|gopay
  - paymentId: internal or provider id
  - status: provider callback status
  - payload: raw provider payload (used for status mapping)
  - redirectUrl: optional return URL (Stripe confirm uses it)
- Write maps provider payload to internal status and reuses ConfirmPayment when needed.
- On success, a `PaymentStatusChanged` outbox event is emitted; downstream AO/resolver should update UI.

## Carrier label/track hooks
- `CreateShippingLabel` uses `CARRIER_LABEL_URL` env to build label link; fallback stub URL otherwise.
- `UpdateShipmentTracking` can emit `trackingUrl` if `CARRIER_TRACK_URL` is set.
- A stub generator is available: `scripts/bridge/carrier_label_stub.lua <shipmentId> [carrier] [service]` (prints label URL and tracking info).

## Notifications
- Use `scripts/worker/notify_worker.lua` to deliver/preview outbox events.
- Set `NOTIFY_EMAIL_WEBHOOK` / `NOTIFY_SMS_WEBHOOK` for HTTP delivery; otherwise messages print to stdout.
- `NOTIFY_DRY_RUN=1` keeps queue entries (no ACK); default ACKs delivered entries.

## Key rotation SOP (ed25519)
- Rotate every 90 days or on incident.
- Generate new keypair; install pubkey at `WRITE_SIG_PUBLIC`; record `sha256sum` in vault.
- Restart write services; run health + signature tests; retire old key after validation.
