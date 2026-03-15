-- Lightweight outbox daemon with retry/backoff.
-- Requires luv; falls back to single-run if luv missing.

local bridge = require("ao.shared.bridge")
local storage = require("ao.shared.storage")
local ok_luv, uv = pcall(require, "luv")
local retry_limit = tonumber(os.getenv("RETRY_LIMIT") or "5")
local backoff_ms = tonumber(os.getenv("OUTBOX_BACKOFF_MS") or "500")
local prom = os.getenv("PROM_FORMAT") == "1"

if OUTBOX_PATH then storage.load(OUTBOX_PATH) end

local function flush_queue()
  local dlq = storage.all("outbox_dlq")
  local keep = {}
  for _, entry in ipairs(dlq) do
    entry.retries = (entry.retries or 0) + 1
    local ok = entry.event and bridge.forward_event(entry.event)
    if not ok and entry.retries < retry_limit then
      table.insert(keep, entry)
    end
  end
  storage.put("outbox_dlq", keep)
  if OUTBOX_PATH then storage.persist(OUTBOX_PATH) end
  if prom then
    print(string.format("outbox_dlq_remaining %d", #keep))
  else
    print("retried " .. #dlq .. " remaining " .. #keep)
  end
end

if ok_luv then
  local timer = uv.new_timer()
  timer:start(0, backoff_ms, function()
    flush_queue()
  end)
  uv.run()
else
  flush_queue()
end
