-- PayPal helper with live API when credentials are present; otherwise stub.

local crypto = require("ao.shared.crypto")
local ok_json, cjson = pcall(require, "cjson.safe")

local PayPal = {}

local function api_token()
  local cid = os.getenv("PAYPAL_CLIENT_ID")
  local secret = os.getenv("PAYPAL_CLIENT_SECRET")
  if not (cid and secret) then return nil end
  local base = os.getenv("PAYPAL_API_BASE") or "https://api-m.sandbox.paypal.com"
  local cmd = string.format("curl -sS -u %q:%q -d 'grant_type=client_credentials' %s/v1/oauth2/token", cid, secret, base)
  local fh = io.popen(cmd)
  if not fh then return nil end
  local body = fh:read("*a")
  fh:close()
  if not ok_json then return nil end
  local decoded = cjson.decode(body)
  return decoded and decoded.access_token
end

local function api_request(method, path, payload)
  local token = api_token()
  if not token then return nil, "no_token" end
  local base = os.getenv("PAYPAL_API_BASE") or "https://api-m.sandbox.paypal.com"
  local url = base .. path
  local data_arg = ""
  if payload then
    if not ok_json then return nil, "json_missing" end
    data_arg = string.format("-d '%s'", cjson.encode(payload))
  end
  local cmd = string.format("curl -sS -X %s %s -H 'Authorization: Bearer %s' -H 'Content-Type: application/json' %s", method, url, token, data_arg)
  local fh = io.popen(cmd)
  if not fh then return nil, "curl_failed" end
  local body = fh:read("*a")
  fh:close()
  if not ok_json then return nil, "json_missing" end
  local decoded = cjson.decode(body)
  return decoded, decoded and decoded.message
end

function PayPal.create_payment(args)
  local cid = os.getenv("PAYPAL_CLIENT_ID")
  local secret = os.getenv("PAYPAL_CLIENT_SECRET")
  if cid and secret then
    local payload = {
      intent = "CAPTURE",
      purchase_units = {
        {
          reference_id = args.orderId,
          amount = {
            currency_code = string.upper(args.currency or "USD"),
            value = tostring(args.amount or 0),
          },
        },
      },
      application_context = {
        return_url = args.returnUrl,
        cancel_url = args.returnUrl,
      },
    }
    local resp = api_request("POST", "/v2/checkout/orders", payload)
    if resp and resp.id then
      local approve
      if resp.links then
        for _, l in ipairs(resp.links) do
          if l.rel == "approve" then approve = l.href end
        end
      end
      return resp.id, approve, "requires_capture"
    end
  end
  -- fallback stub
  local pid = "pp_" .. (args.orderId or tostring(os.time()))
  local approve_url = (os.getenv("PAYPAL_APPROVAL_URL") or "https://www.sandbox.paypal.com/checkoutnow?token=") .. pid
  return pid, approve_url, "requires_capture"
end

function PayPal.capture(order_id)
  local cid = os.getenv("PAYPAL_CLIENT_ID")
  local secret = os.getenv("PAYPAL_CLIENT_SECRET")
  if cid and secret then
    local resp, err = api_request("POST", "/v2/checkout/orders/" .. order_id .. "/capture", {})
    return resp ~= nil, err
  end
  return true
end

function PayPal.void(order_id, reason)
  -- PayPal "void" is effectively to cancel an order before capture
  local cid = os.getenv("PAYPAL_CLIENT_ID")
  local secret = os.getenv("PAYPAL_CLIENT_SECRET")
  if cid and secret then
    local resp, err = api_request("POST", "/v2/checkout/orders/" .. order_id .. "/cancel", { reason = reason })
    return resp ~= nil, err
  end
  return true
end

function PayPal.refund(order_id, amount)
  -- if order_id is a capture id, refund against it
  local cid = os.getenv("PAYPAL_CLIENT_ID")
  local secret = os.getenv("PAYPAL_CLIENT_SECRET")
  if cid and secret then
    local payload = {}
    if amount then
      payload.amount = {
        value = tostring(amount),
        currency_code = "USD",
      }
    end
    local resp, err = api_request("POST", "/v2/payments/captures/" .. order_id .. "/refund", payload)
    return resp ~= nil, err
  end
  return true
end

-- Basic webhook verification via HMAC if configured (not PayPal official RSA flow)
function PayPal.verify_webhook(body, sig_header, secret)
  if not (body and sig_header and secret) then return false end
  local expected = crypto.hmac_sha256_hex(body, secret)
  return expected == sig_header
end

function PayPal.status_from_payload(payload)
  local event = payload and payload.event_type
  local map = {
    ["PAYMENT.CAPTURE.COMPLETED"] = "captured",
    ["PAYMENT.CAPTURE.DENIED"] = "failed",
    ["PAYMENT.CAPTURE.REFUNDED"] = "refunded",
    ["PAYMENT.CAPTURE.REVERSED"] = "voided",
    ["CHECKOUT.ORDER.APPROVED"] = "requires_capture",
  }
  return map[event] or "pending"
end

return PayPal
