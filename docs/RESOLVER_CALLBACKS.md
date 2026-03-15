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
