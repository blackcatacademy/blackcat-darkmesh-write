-- Minimal GoPay REST bridge (payments V3 + BackOffice capture/refund/void).
-- Uses curl; keeps scope small and optional.
-- Env:
--  GOPAY_CLIENT_ID
--  GOPAY_CLIENT_SECRET
--  GOPAY_API_BASE (default https://gate.gopay.com/api)
--  GOPAY_SANDBOX=1 to use https://gw.sandbox.gopay.com/api
--  GOPAY_TIMEOUT=10

local M = {}
local crypto_ok, crypto = pcall(require, "ao.shared.crypto")

local function api_base()
  if os.getenv("GOPAY_SANDBOX") == "1" then
    return "https://gw.sandbox.gopay.com/api"
  end
  return os.getenv("GOPAY_API_BASE") or "https://gate.gopay.com/api"
end

local function curl_json(method, path, body)
  local cid = os.getenv("GOPAY_CLIENT_ID")
  local secret = os.getenv("GOPAY_CLIENT_SECRET")
  if not cid or not secret then
    return nil, "missing_credentials"
  end
  local payload = body and (string.format("--data-binary @- <<'EOF'\n%s\nEOF", body)) or ""
  local cmd = string.format(
    "curl -s -w '\\n%%{http_code}' -X %s -u %q:%q -H 'Content-Type: application/json' --max-time %s %s%s",
    method,
    cid,
    secret,
    os.getenv("GOPAY_TIMEOUT") or "10",
    api_base(),
    path
  )
  local pipe = io.popen(cmd, "w")
  if not pipe then return nil, "curl_failed" end
  if body then pipe:write(body) end
  local resp = pipe:read("*a") or ""
  pipe:close()
  local http_code = tonumber(resp:match("\n(%d+)%s*$") or "0")
  local json = resp:gsub("\n%d+%s*$", "")
  return json, http_code
end

function M.create_payment(opts)
  -- opts: orderId, amount, currency, returnUrl, description
  local amt = tonumber(opts.amount or 0) or 0
  local payload = string.format([[{
    "payer": { "allowed_payment_instruments": ["PAYMENT_CARD"] },
    "amount": %d,
    "currency": "%s",
    "order_number": "%s",
    "callback": { "return_url": "%s" },
    "targets": [{ "type": "ACCOUNT", "goid": %s }],
    "order_description": "%s"
  }]],
    math.floor(amt * 100),
    opts.currency or "EUR",
    opts.orderId or "",
    opts.returnUrl or "",
    os.getenv("GOPAY_MERCHANT_ID") or "0",
    opts.description or "order"
  )
  local body, code = curl_json("POST", "/payments/payment", payload)
  code = tonumber(code) or 0
  if code == 0 then return nil, "curl_failed" end
  if code >= 300 then return nil, "gopay_create_failed", code end
  local ok, decoded = pcall(require("cjson").decode, body or "")
  if not ok or type(decoded) ~= "table" then
    return nil, "gopay_bad_json", code
  end
  local payment_id = decoded.id or decoded.payment_id
  local gw = decoded.gw_url
  local state = decoded.state
  return payment_id, gw, state
end

function M.capture(payment_id)
  local _, code = curl_json("POST", "/payments/payment/" .. payment_id .. "/capture", "{}")
  if not code or code >= 300 then return false, "gopay_capture_failed", code end
  return true
end

function M.refund(payment_id, amount)
  local payload = string.format([[{ "amount": %d }]], math.floor((amount or 0) * 100))
  local _, code = curl_json("POST", "/payments/payment/" .. payment_id .. "/refund", payload)
  if not code or code >= 300 then return false, "gopay_refund_failed", code end
  return true
end

function M.void(payment_id, reason)
  local payload = string.format([[{ "reason": "%s" }]], reason or "voided")
  local _, code = curl_json("POST", "/payments/payment/" .. payment_id .. "/void", payload)
  if not code or code >= 300 then return false, "gopay_void_failed", code end
  return true
end

-- Verify GoPay webhook signature (HMAC SHA256 hex) if crypto is available.
function M.verify_signature(body, signature_header, secret)
  if not (crypto_ok and crypto.hmac_sha256_hex) then return false, "crypto_missing" end
  if not body or not signature_header or not secret then return false, "missing_inputs" end
  local calc = crypto.hmac_sha256_hex(body, secret)
  return calc and calc:lower() == tostring(signature_header):lower()
end

function M.verify_basic(auth_header)
  if not auth_header then return false end
  local prefix = "Basic "
  if not auth_header:find(prefix) then return false end
  local b64 = auth_header:sub(#prefix + 1)
  local decoded
  if crypto_ok and crypto.base64_decode then
    decoded = crypto.base64_decode(b64)
  else
    local pipe = io.popen("printf %s " .. b64 .. " | base64 -d 2>/dev/null", "r")
    decoded = pipe and pipe:read("*a")
    if pipe then pipe:close() end
  end
  return decoded
end

return M
