-- Simple notification worker: reads outbox_queue and logs email/SMS intents.
-- It does NOT consume/remove items; meant as a sidecar for preview/testing.

local storage = require("ao.shared.storage")
local ok_json, cjson = pcall(require, "cjson.safe")

local templates = {
  OrderCreated = function(ev) return string.format("Email: Order %s created, total %.2f %s", ev.orderId or "?", ev.totalAmount or 0, ev.currency or "") end,
  PaymentCaptured = function(ev) return string.format("Email: Payment %s captured for order %s", ev.paymentId or "?", ev.orderId or "?") end,
  ShipmentUpdated = function(ev) return string.format("Email: Shipment %s is %s", ev.shipmentId or "?", ev.status or "updated") end,
  ReturnUpdated = function(ev) return string.format("Email: Return %s is %s", ev.returnId or "?", ev.status or "updated") end,
}

local function run_once()
  local q = storage.get("outbox_queue") or {}
  for _, entry in ipairs(q) do
    local ev = entry.event or {}
    local tmpl = templates[ev.type]
    if tmpl then
      print(tmpl(ev))
    end
  end
end

run_once()
