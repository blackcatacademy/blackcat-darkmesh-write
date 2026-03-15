#!/usr/bin/env lua
-- File queue forwarder: reads persisted outbox and WAL, appends to queue, retries HTTP delivery.

local storage = require("ao.shared.storage")
local bridge = require("ao.shared.bridge")
local cjson = require("cjson")
local queue_path = os.getenv("AO_QUEUE_PATH") or "dev/outbox-queue.ndjson"
local log_path = os.getenv("AO_QUEUE_LOG_PATH") or "dev/queue-log.ndjson"
local max_retries = tonumber(os.getenv("AO_QUEUE_MAX_RETRIES") or "5")
local outbox_path = os.getenv("WRITE_OUTBOX_PATH")
local outbox_hmac_secret = os.getenv("OUTBOX_HMAC_SECRET")
local crypto = require("ao.shared.crypto")

local function ensure_dir(path)
  local dir = path:match("(.+)/[^/]+$")
  if dir then os.execute(string.format('mkdir -p "%s"', dir)) end
end

local function load_queue()
  local entries = {}
  local f = io.open(queue_path, "r")
  if not f then return entries end
  for line in f:lines() do
    local ok, val = pcall(cjson.decode, line)
    if ok and val then table.insert(entries, val) end
  end
  f:close()
  return entries
end

local function sha256_str(str)
  local tmp = os.tmpname()
  local f = io.open(tmp, "w"); if not f then return nil end
  f:write(str); f:close()
  local p = io.popen("sha256sum " .. tmp .. " 2>/dev/null")
  local out = p and p:read("*a") or ""
  if p then p:close() end
  os.remove(tmp)
  return out:match("^(%w+)")
end

local function append_log(entry)
  ensure_dir(log_path)
  local f = io.open(log_path, "a")
  if not f then return end
  f:write(cjson.encode(entry))
  f:write("\n")
  f:close()
end

local function save_queue(entries)
  ensure_dir(queue_path)
  local f = assert(io.open(queue_path, "w"))
  for _, ev in ipairs(entries) do
    f:write(cjson.encode(ev))
    f:write("\n")
  end
  f:close()
end

-- Seed queue from persisted outbox (if provided)
if outbox_path then
  storage.load(outbox_path)
end
local outbox = storage.all("outbox")

local queue = load_queue()
for _, ev in ipairs(outbox) do
  table.insert(queue, ev)
end

local delivered = {}
local remaining = {}
for _, ev in ipairs(queue) do
  ev.attempts = (ev.attempts or 0) + 1
  local req_hash = sha256_str(cjson.encode(ev))
  if outbox_hmac_secret and ev.hmac then
    local msg = (ev.siteId or "") .. "|" .. (ev.pageId or ev.orderId or "") .. "|" .. (ev.versionId or ev.amount or "")
    local expected = crypto.hmac_sha256_hex(msg, outbox_hmac_secret)
    if expected and expected:lower() ~= tostring(ev.hmac):lower() then
      append_log({ ts = os.date("!%Y-%m-%dT%H:%M:%SZ"), requestId = ev.requestId, status = "hmac_mismatch" })
      io.stderr:write(string.format("hmac mismatch for requestId=%s\n", tostring(ev.requestId)))
      goto continue
    end
  end
  local ok, status, resp_hash = bridge.forward_event(ev)
  append_log({
    ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
    requestId = ev.requestId,
    action = ev.type,
    attempt = ev.attempts,
    ok = ok,
    status = status,
    reqHash = req_hash,
    respHash = resp_hash,
  })
  if ok then
    table.insert(delivered, ev)
  else
    if ev.attempts < max_retries then
      table.insert(remaining, ev)
    else
      io.stderr:write(string.format("dropping after %d attempts requestId=%s\n", ev.attempts, tostring(ev.requestId)))
    end
    io.stderr:write(string.format("deliver failed (%s) for requestId=%s\n", tostring(status), tostring(ev.requestId)))
  end
  ::continue::
end

save_queue(remaining)
print(string.format("[queue] delivered=%d pending=%d", #delivered, #remaining))
