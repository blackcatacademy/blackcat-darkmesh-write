#!/usr/bin/env lua
-- Simple notification worker: reads outbox NDJSON and posts email/SMS/webhook notifications.
-- Retry/backoff is delegated to supervisor (nonzero exit on failure keeps item in queue).

local json = require("cjson.safe")
local http = io.popen

local QUEUE_PATH = os.getenv("NOTIFY_QUEUE_PATH") or os.getenv("WRITE_OUTBOX_PATH")
local WEBHOOK_URL = os.getenv("NOTIFY_WEBHOOK_URL")
local EMAIL_WEBHOOK = os.getenv("NOTIFY_EMAIL_WEBHOOK") -- e.g. SendGrid/Tinybird proxy
local SMS_WEBHOOK = os.getenv("NOTIFY_SMS_WEBHOOK")     -- e.g. Twilio webhook wrapper
local METRICS = os.getenv("NOTIFY_METRICS_PATH")

local function send_webhook(url, payload)
  if not url or url == "" then return true end
  local body = json.encode(payload)
  local cmd = string.format("curl -sS -X POST -H 'Content-Type: application/json' --data '%s' %s", body, url)
  local p = http(cmd)
  if not p then return false end
  local out = p:read("*a") or ""
  p:close()
  return true, out
end

local MAX_RETRIES = tonumber(os.getenv("NOTIFY_MAX_RETRIES") or "3")
local BACKOFF_MS = tonumber(os.getenv("NOTIFY_BACKOFF_MS") or "250")

local function with_retry(fn)
  local attempt = 0
  while attempt <= MAX_RETRIES do
    local ok, err = fn()
    if ok then return true end
    attempt = attempt + 1
    local sleep_ms = BACKOFF_MS * attempt
    os.execute(string.format("sleep %.3f", sleep_ms / 1000))
  end
  return false, "notify_retry_exceeded"
end

local function process_event(ev)
  if ev.type == "OrderCreated" or ev.type == "PaymentStatusChanged" or ev.type == "OrderStatusUpdated" or ev.type == "ReturnUpdated" or ev.type == "ShipmentUpdated" then
    -- generic webhook
    local ok = select(1, with_retry(function() return send_webhook(WEBHOOK_URL, ev) end))
    if not ok then return false, "webhook_failed" end
  end
  if ev.type == "OrderCreated" and EMAIL_WEBHOOK then
    local ok = select(1, with_retry(function() return send_webhook(EMAIL_WEBHOOK, { template = "order_created", payload = ev }) end))
    if not ok then return false, "email_failed" end
  end
  if ev.type == "PaymentStatusChanged" and SMS_WEBHOOK then
    local ok = select(1, with_retry(function() return send_webhook(SMS_WEBHOOK, { template = "payment_status", payload = ev }) end))
    if not ok then return false, "sms_failed" end
  end
  return true
end

local function main()
  if not QUEUE_PATH or QUEUE_PATH == "" then
    io.stderr:write("NOTIFY_QUEUE_PATH or WRITE_OUTBOX_PATH not set\n")
    os.exit(1)
  end
  local f = io.open(QUEUE_PATH, "r")
  if not f then
    io.stderr:write("cannot open queue: " .. QUEUE_PATH .. "\n")
    os.exit(1)
  end
  local lines = f:lines()
  local processed, failed = 0, 0
  for line in lines do
    local obj = json.decode(line)
    if obj and obj.event then
      local ok, err = process_event(obj.event)
      if ok then processed = processed + 1 else failed = failed + 1 end
    end
  end
  f:close()
  io.stdout:write(string.format("notifications processed=%d failed=%d\n", processed, failed))
  if METRICS and METRICS ~= "" then
    local mf = io.open(METRICS, "w")
    if mf then
      mf:write(string.format("notify_processed %d\nnotify_failed %d\n", processed, failed))
      mf:close()
    end
  end
  if failed > 0 then os.exit(1) end
end

main()
