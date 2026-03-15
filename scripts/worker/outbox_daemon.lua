-- Lightweight outbox daemon with retry/backoff.
-- Requires luv; falls back to single-run if luv missing.

local bridge = require("ao.shared.bridge")
local storage = require("ao.shared.storage")
local ok_luv, uv = pcall(require, "luv")

local OUTBOX_PATH = os.getenv("WRITE_OUTBOX_PATH")
local retry_limit = tonumber(os.getenv("OUTBOX_RETRY_LIMIT") or os.getenv("RETRY_LIMIT") or "5")
local backoff_ms = tonumber(os.getenv("OUTBOX_BACKOFF_MS") or "500")
local prom = os.getenv("PROM_FORMAT") == "1"

if OUTBOX_PATH then storage.load(OUTBOX_PATH) end

local function next_backoff(attempts)
  local base = math.max(1, backoff_ms / 1000)
  return math.min(60, base * math.pow(2, attempts - 1))
end

local function flush_queue()
  local now = os.time()
  local q = storage.get("outbox_queue") or {}
  local keep = {}
  local sent = 0
  for _, entry in ipairs(q) do
    if entry.nextAttempt == nil or entry.nextAttempt <= now then
      local ok = entry.event and bridge.forward_event(entry.event)
      if ok then
        sent = sent + 1
      else
        entry.attempts = (entry.attempts or 0) + 1
        if entry.attempts >= retry_limit then
          local dlq = storage.get("outbox_dlq") or {}
          entry.failedAt = now
          table.insert(dlq, entry)
          storage.put("outbox_dlq", dlq)
        else
          entry.nextAttempt = now + next_backoff(entry.attempts)
          table.insert(keep, entry)
        end
      end
    else
      table.insert(keep, entry)
    end
  end
  storage.put("outbox_queue", keep)
  if OUTBOX_PATH then storage.persist(OUTBOX_PATH) end

  -- retry DLQ too
  local dlq = storage.get("outbox_dlq") or {}
  local dlq_keep = {}
  for _, entry in ipairs(dlq) do
    entry.retries = (entry.retries or 0) + 1
    local ok = entry.event and bridge.forward_event(entry.event)
    if not ok and entry.retries < retry_limit then
      table.insert(dlq_keep, entry)
    end
  end
  storage.put("outbox_dlq", dlq_keep)
  if OUTBOX_PATH then storage.persist(OUTBOX_PATH) end

  if prom then
    print(string.format("outbox_sent %d\noutbox_queue_pending %d\noutbox_dlq_size %d", sent, #keep, #dlq_keep))
  else
    print(string.format("sent=%d queue=%d dlq=%d", sent, #keep, #dlq_keep))
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
