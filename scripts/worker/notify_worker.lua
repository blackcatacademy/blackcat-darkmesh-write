#!/usr/bin/env lua
-- Simple notification worker: reads outbox NDJSON and posts email/SMS/webhook notifications.
-- Retry/backoff is delegated to supervisor (nonzero exit on failure keeps item in queue).

local json = require("cjson.safe")
local http = io.popen

local QUEUE_PATH = os.getenv("NOTIFY_QUEUE_PATH") or os.getenv("WRITE_OUTBOX_PATH")
local WEBHOOK_URL = os.getenv("NOTIFY_WEBHOOK_URL")
local EMAIL_WEBHOOK = os.getenv("NOTIFY_EMAIL_WEBHOOK") -- e.g. SendGrid/Tinybird proxy
local SMS_WEBHOOK = os.getenv("NOTIFY_SMS_WEBHOOK")     -- e.g. Twilio webhook wrapper

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

local function process_event(ev)
  if ev.type == "OrderCreated" or ev.type == "PaymentStatusChanged" or ev.type == "OrderStatusUpdated" or ev.type == "ReturnUpdated" or ev.type == "ShipmentUpdated" then
    -- generic webhook
    local ok = select(1, send_webhook(WEBHOOK_URL, ev))
    if not ok then return false, "webhook_failed" end
  end
  if ev.type == "OrderCreated" and EMAIL_WEBHOOK then
    local ok = select(1, send_webhook(EMAIL_WEBHOOK, { template = "order_created", payload = ev }))
    if not ok then return false, "email_failed" end
  end
  if ev.type == "PaymentStatusChanged" and SMS_WEBHOOK then
    local ok = select(1, send_webhook(SMS_WEBHOOK, { template = "payment_status", payload = ev }))
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
  if failed > 0 then os.exit(1) end
end

main()
