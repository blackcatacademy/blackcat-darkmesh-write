-- Minimal PayPal helper stubs (no live API). For bridging provider neutrality.

local crypto = require("ao.shared.crypto")

local PayPal = {}

function PayPal.create_payment(args)
  local pid = "pp_" .. (args.orderId or tostring(os.time()))
  local approve_url = (os.getenv("PAYPAL_APPROVAL_URL") or "https://www.sandbox.paypal.com/checkoutnow?token=") .. pid
  return pid, approve_url, "requires_capture"
end

function PayPal.capture(order_id)
  return true
end

function PayPal.void(order_id, reason)
  return true
end

function PayPal.refund(order_id, amount)
  return true
end

-- Basic webhook verification via HMAC if configured (not PayPal official RSA flow)
function PayPal.verify_webhook(body, sig_header, secret)
  if not (body and sig_header and secret) then return false end
  local expected = crypto.hmac_sha256_hex(body, secret)
  return expected == sig_header
end

return PayPal
