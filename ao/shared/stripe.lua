-- Stripe helper with live API when STRIPE_API_KEY is present; otherwise stub.

local crypto = require("ao.shared.crypto")
local ok_json, cjson = pcall(require, "cjson.safe")

local Stripe = {}

local function api_request(method, path, form)
  local key = os.getenv("STRIPE_API_KEY")
  if not key then return nil, "no_api_key" end
  local url = "https://api.stripe.com/v1" .. path
  local args = {}
  if form then
    for k, v in pairs(form) do
      table.insert(args, string.format("-d '%s=%s'", k, tostring(v)))
    end
  end
  local cmd = string.format("curl -sS -X %s %s -u %q %s", method, url, key .. ":", table.concat(args, " "))
  local fh = io.popen(cmd)
  if not fh then return nil, "curl_failed" end
  local body = fh:read("*a")
  fh:close()
  if not ok_json or not body then return nil, "json_missing" end
  local decoded = cjson.decode(body)
  if not decoded then return nil, "decode_failed" end
  return decoded
end

-- Create payment intent (live or stub)
function Stripe.create_payment(args)
  local key = os.getenv("STRIPE_API_KEY")
  if key then
    local amount_cents = math.floor((args.amount or 0) * 100)
    local resp, err = api_request("POST", "/payment_intents", {
      amount = amount_cents,
      currency = (args.currency or "usd"):lower(),
      description = args.description or ("Order " .. (args.orderId or "")),
      "metadata[orderId]" = args.orderId,
      capture_method = "manual",
      automatic_payment_methods__enabled = "true",
      return_url = args.returnUrl,
      payment_method = args.paymentMethodToken,
      confirm = args.paymentMethodToken and "true" or nil,
      off_session = args.paymentMethodToken and "true" or nil,
      setup_future_usage = args.saveForFuture and "off_session" or nil,
    })
    if not resp then return nil, nil, "pending" end
    local status_map = {
      requires_capture = "requires_capture",
      requires_action = "requires_capture",
      processing = "pending",
      succeeded = "captured",
    }
    local status = status_map[resp.status] or "pending"
    local next_action_url = resp.next_action and (resp.next_action.redirect_to_url and resp.next_action.redirect_to_url.url)
    return resp.id, next_action_url, status
  end
  -- fallback stub
  local pid = "pi_" .. (args.orderId or tostring(os.time()))
  local checkout_url = (os.getenv("STRIPE_CHECKOUT_URL") or "https://checkout.stripe.com/pay/") .. pid
  return pid, checkout_url, "requires_capture"
end

function Stripe.capture(intent_id)
  local key = os.getenv("STRIPE_API_KEY")
  if key then
    local resp = api_request("POST", "/payment_intents/" .. intent_id .. "/capture", {})
    return resp ~= nil, resp and resp.error and resp.error.message
  end
  return true
end

function Stripe.confirm(intent_id, return_url)
  local key = os.getenv("STRIPE_API_KEY")
  if key then
    local resp = api_request("POST", "/payment_intents/" .. intent_id .. "/confirm", {
      return_url = return_url,
    })
    return resp ~= nil, resp and resp.error and resp.error.message, resp
  end
  return true
end

function Stripe.status_from_payload(payload)
  if not payload then return "pending" end
  local status = payload.status or payload.intent_status
  local map = {
    requires_action = "requires_capture",
    requires_capture = "requires_capture",
    succeeded = "captured",
    processing = "pending",
    canceled = "voided",
    payment_failed = "failed",
  }
  return map[status] or "pending"
end

function Stripe.void(intent_id, reason)
  local key = os.getenv("STRIPE_API_KEY")
  if key then
    local resp = api_request("POST", "/payment_intents/" .. intent_id .. "/cancel", { cancellation_reason = reason or "requested_by_customer" })
    return resp ~= nil, resp and resp.error and resp.error.message
  end
  return true
end

function Stripe.refund(intent_id, amount)
  local key = os.getenv("STRIPE_API_KEY")
  if key then
    local params = { payment_intent = intent_id }
    if amount then params.amount = math.floor(amount * 100) end
    local resp = api_request("POST", "/refunds", params)
    return resp ~= nil, resp and resp.error and resp.error.message
  end
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
  local tol = tolerance_sec or tonumber(os.getenv("STRIPE_WEBHOOK_TOLERANCE") or "300")
  if ts and math.abs(now - ts) > tol then return false end
  return true
end

return Stripe
