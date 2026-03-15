-- Lightweight Stripe helper (no live HTTP calls). Focused on webhook verify and local status mapping.

local crypto = require("ao.shared.crypto")

local Stripe = {}

-- Create payment intent stub: return providerPaymentId, checkout url, state
function Stripe.create_payment(args)
  local pid = "pi_" .. (args.orderId or tostring(os.time()))
  local checkout_url = (os.getenv("STRIPE_CHECKOUT_URL") or "https://checkout.stripe.com/pay/") .. pid
  return pid, checkout_url, "requires_capture"
end

function Stripe.capture(intent_id)
  return true
end

function Stripe.void(intent_id, reason)
  return true
end

function Stripe.refund(intent_id, amount)
  return true
end

local function parse_sig_header(sig_header)
  local out = {}
  for part in string.gmatch(sig_header or "", "([^,]+)") do
    local k, v = part:match("^%s*(%w+)=([^,]+)$")
    if k and v then out[k] = v end
  end
  return out
end

-- Verify Stripe webhook signature (t=timestamp,v1=signature)
function Stripe.verify_webhook(body, sig_header, secret, tolerance_sec)
  if not (body and sig_header and secret) then return false end
  local parts = parse_sig_header(sig_header)
  if not (parts.t and parts.v1) then return false end
  local signed_payload = parts.t .. "." .. body
  local expected = crypto.hmac_sha256_hex(signed_payload, secret)
  if not expected then return false end
  if expected ~= parts.v1 then return false end
  local now = os.time()
  local ts = tonumber(parts.t)
  local tol = tolerance_sec or 300
  if ts and math.abs(now - ts) > tol then return false end
  return true
end

return Stripe
