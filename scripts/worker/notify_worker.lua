-- Notification worker with basic retry/backoff.
-- Reads outbox_queue but does not delete; intended as sidecar preview or to be adapted to real email/SMS provider.

local storage = require("ao.shared.storage")
local ok_luv, uv = pcall(require, "luv")

local backoff_ms = tonumber(os.getenv("NOTIFY_BACKOFF_MS") or "1000")
local retry_limit = tonumber(os.getenv("NOTIFY_MAX_RETRIES") or "5")
local dry_run = os.getenv("NOTIFY_DRY_RUN") == "1"

local prom = os.getenv("PROM_FORMAT") == "1"

local templates = {
  OrderCreated = function(ev) return string.format("Order %s created, total %.2f %s", ev.orderId or "?", ev.totalAmount or 0, ev.currency or "") end,
  PaymentCaptured = function(ev) return string.format("Payment %s captured for order %s", ev.paymentId or "?", ev.orderId or "?") end,
  ShipmentUpdated = function(ev) return string.format("Shipment %s is %s", ev.shipmentId or "?", ev.status or "updated") end,
  ReturnUpdated = function(ev) return string.format("Return %s is %s", ev.returnId or "?", ev.status or "updated") end,
}

local function send_email(text)
  local hook = os.getenv("NOTIFY_EMAIL_WEBHOOK")
  if hook and hook ~= "" then
    os.execute(string.format("curl -sS -X POST -H 'Content-Type: text/plain' --data %q %q >/dev/null", text, hook))
    return true
  end
  local smtp = os.getenv("NOTIFY_SMTP_SENDMAIL")
  if smtp and smtp ~= "" then
    -- naive sendmail pipe, expects NOTIFY_SMTP_TO and NOTIFY_SMTP_FROM
    local to = os.getenv("NOTIFY_SMTP_TO") or ""
    local from = os.getenv("NOTIFY_SMTP_FROM") or ""
    if to ~= "" and from ~= "" then
      local cmd = string.format("printf 'From: %s\\nTo: %s\\nSubject: AO Notification\\n\\n%s\\n' | %s", from, to, text:gsub("'", "'\\''"), smtp)
      os.execute(cmd .. " >/dev/null 2>&1")
      return true
    end
  else
    print("EMAIL " .. text)
  end
end

local function send_sms(text)
  local hook = os.getenv("NOTIFY_SMS_WEBHOOK")
  if hook and hook ~= "" then
    os.execute(string.format("curl -sS -X POST -H 'Content-Type: text/plain' --data %q %q >/dev/null", text, hook))
  else
    print("SMS " .. text)
  end
end

local function deliver(ev)
    local tmpl = templates[ev.type]
    if tmpl then
      local text = tmpl(ev)
      send_email(text)
      send_sms(text)
      return true
    end
    return false
end

local function run_once()
  local now = os.time()
  local q = storage.get("outbox_queue") or {}
  local keep = {}
  local sent, failed = 0, 0
  local retry_q = storage.get("notify_dlq") or {}
  for _, entry in ipairs(q) do
    -- honor scheduling
    if entry.nextAttempt and entry.nextAttempt > now then
      table.insert(keep, entry)
    else
      local ev = entry.event or {}
      local ok = deliver(ev)
      if ok then
        sent = sent + 1
        if dry_run then table.insert(keep, entry) end -- keep entry if dry-run
      else
        failed = failed + 1
        entry.attempts = (entry.attempts or 0) + 1
        if entry.attempts < retry_limit then
          entry.nextAttempt = now + math.ceil(backoff_ms / 1000)
          table.insert(retry_q, entry)
        end
      end
    end
  end
  if not dry_run then
    storage.put("outbox_queue", keep)
  end
  storage.put("notify_dlq", retry_q)
  if prom then
    print(string.format("notify_sent %d\nnotify_failed %d\nnotify_dlq %d", sent, failed, #retry_q))
  else
    print(string.format("notify sent=%d failed=%d dlq=%d queue_remaining=%d", sent, failed, #retry_q, #keep))
  end
end

if ok_luv then
  local t = uv.new_timer()
  t:start(0, backoff_ms, run_once)
  uv.run()
else
  run_once()
end
