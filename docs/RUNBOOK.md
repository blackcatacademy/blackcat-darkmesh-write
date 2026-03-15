
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
- Optional SMTP: set `NOTIFY_SMTP_SENDMAIL` (path to sendmail), `NOTIFY_SMTP_FROM`, `NOTIFY_SMTP_TO` to send via local MTA.

## OTP / passwordless login
- Issue OTP: `action=IssueOtp` (role/admin/support only). TTL default 300s (`OTP_TTL_SECONDS`, min 30, max 3600). Response: `code`, `expiresAt`.
- Deliver OTP: `lua scripts/cli/send_otp.lua <code> <email|phone>` (uses same NOTIFY/Twilio/SendGrid envs as notify_worker; prints fallback).
- Exchange OTP for JWT: `action=ExchangeOtp` with the code. JWT TTL default 900s (`OTP_JWT_TTL_SECONDS`) and uses `WRITE_JWT_HS_SECRET`.
- Claims in JWT: `sub`, `tenant`, `role`, `exp`, `nonce`, `jti`. Write/AO už JWT ověřují; nastav `WRITE_REQUIRE_JWT=1` v produkci, pokud chceš čistě token-only.

## Key rotation SOP (ed25519)
- Rotate every 90 days or on incident.
- Generate new keypair; install pubkey at `WRITE_SIG_PUBLIC`; record `sha256sum` in vault.
- Restart write services; run health + signature tests; retire old key after validation.
