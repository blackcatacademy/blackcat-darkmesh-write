-- Notification worker with basic retry/backoff.
-- Reads outbox_queue but does not delete; intended as sidecar preview or to be adapted to real email/SMS provider.

local storage = require("ao.shared.storage")
local ok_luv, uv = pcall(require, "luv")

local backoff_ms = tonumber(os.getenv("NOTIFY_BACKOFF_MS") or "1000")
local retry_limit = tonumber(os.getenv("NOTIFY_MAX_RETRIES") or "5")
local prom = os.getenv("PROM_FORMAT") == "1"

local templates = {
  OrderCreated = function(ev) return string.format("Email: Order %s created, total %.2f %s", ev.orderId or "?", ev.totalAmount or 0, ev.currency or "") end,
  PaymentCaptured = function(ev) return string.format("Email: Payment %s captured for order %s", ev.paymentId or "?", ev.orderId or "?") end,
  ShipmentUpdated = function(ev) return string.format("Email: Shipment %s is %s", ev.shipmentId or "?", ev.status or "updated") end,
  ReturnUpdated = function(ev) return string.format("Email: Return %s is %s", ev.returnId or "?", ev.status or "updated") end,
}

local function deliver(ev)
  local tmpl = templates[ev.type]
  if tmpl then
    print(tmpl(ev))
    return true
  end
  return false
end

local function run_once()
  local q = storage.get("outbox_queue") or {}
  local sent, failed = 0, 0
  local retry_q = storage.get("notify_dlq") or {}
  for _, entry in ipairs(q) do
    local ev = entry.event or {}
    local ok = deliver(ev)
    if ok then
      sent = sent + 1
    else
      failed = failed + 1
      entry.attempts = (entry.attempts or 0) + 1
      if entry.attempts < retry_limit then
        entry.nextAttempt = os.time() + math.ceil(backoff_ms / 1000)
        table.insert(retry_q, entry)
      end
    end
  end
  storage.put("notify_dlq", retry_q)
  if prom then
    print(string.format("notify_sent %d\nnotify_failed %d\nnotify_dlq %d", sent, failed, #retry_q))
  else
    print(string.format("notify sent=%d failed=%d dlq=%d", sent, failed, #retry_q))
  end
end

if ok_luv then
  local t = uv.new_timer()
  t:start(0, backoff_ms, run_once)
  uv.run()
else
  run_once()
end
