-- Simple retry worker for outbox DLQ.
-- Env:
--  OUTBOX_PATH (optional) to load persisted storage snapshot.
--  RETRY_LIMIT (default 5)

local storage = require("ao.shared.storage")
local bridge = require("ao.shared.bridge")
local cjson_ok, cjson = pcall(require, "cjson")

local retry_limit = tonumber(os.getenv("RETRY_LIMIT") or "5")

if OUTBOX_PATH then
  storage.load(OUTBOX_PATH)
end

local dlq = storage.all("outbox_dlq")

local kept = {}
for _, entry in ipairs(dlq) do
  entry.retries = (entry.retries or 0) + 1
  local ok = false
  if entry.event then
    ok = bridge.forward_event(entry.event)
  end
  if not ok and entry.retries < retry_limit then
    table.insert(kept, entry)
  end
end

storage.put("outbox_dlq", kept)

if OUTBOX_PATH then
  storage.persist(OUTBOX_PATH)
end

if cjson_ok then
  print(cjson.encode({ retried = #dlq, remaining = #kept }))
else
  print("retried " .. #dlq .. " remaining " .. #kept)
end
