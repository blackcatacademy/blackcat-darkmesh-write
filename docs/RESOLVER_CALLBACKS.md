# Resolver callback guide (3‑DS / payment return)

Typical flow:
1) Client/FE redirects user to provider (Stripe/PayPal) for SCA/approval.
2) Provider redirects back to your resolver/edge with payload (or you fetch it via provider API).
3) Resolver POSTs to write AO `PaymentReturn` action to finalize state.

Minimal JSON to POST (example):
```json
{
  "provider": "stripe",
  "paymentId": "pi_123",
  "status": "requires_action",
  "payload": { "status": "succeeded" },
  "redirectUrl": "https://yourapp/return?pid=pi_123"
}
```

Curl to local write AO bridge:
```sh
cat payload.json | lua scripts/bridge/payment_return_handler.lua
```

Notes:
- `PaymentReturn` will map provider payload to internal status and reuse ConfirmPayment for Stripe/PayPal when needed.
- Outbox emits `PaymentStatusChanged`; downstream AO/resolver should refresh order/payment UI.
- For real deployments, expose an HTTP handler that forwards the JSON above to write AO.
- Disputes/chargebacks: Stripe `charge.dispute.*` and PayPal `CUSTOMER.DISPUTE.*` webhooks send `provider`=`stripe|paypal`, `eventType`=`...dispute...`, `paymentId` (intent/capture id) → write maps to `paymentStatus=disputed` and emits PaymentStatusChanged. AO then marks the order as `disputed`.

# Trusted resolvers (trust manifest)
- Manifest (unsigned) contains `resolvers` entries `{ id, pubkey, endpoint, validFrom, validTo, status }`.
- Signature (HMAC-SHA256) is added via `scripts/cli/trust_manifest_sign.lua` and uploaded to Arweave.
- Ops record txId in AO registry via `UpdateTrustResolvers` (role admin/registry-admin) and/or set `TRUST_MANIFEST_TX` in env.
- Resolver on startup fetches manifest from Arweave, verifies HMAC (`TRUST_MANIFEST_HMAC`), and uses only active/valid entries.
