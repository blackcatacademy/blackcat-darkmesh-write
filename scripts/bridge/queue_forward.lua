#!/usr/bin/env lua
-- File queue forwarder: reads persisted outbox and WAL, appends to queue, retries HTTP delivery.

local storage = require("ao.shared.storage")
local bridge = require("ao.shared.bridge")
local cjson = require("cjson")
local queue_path = os.getenv("AO_QUEUE_PATH") or "dev/outbox-queue.ndjson"
local outbox_path = os.getenv("WRITE_OUTBOX_PATH")

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
  local ok, err = bridge.forward_event(ev)
  if ok then
    table.insert(delivered, ev)
  else
    table.insert(remaining, ev)
    io.stderr:write(string.format("deliver failed (%s) for requestId=%s\n", tostring(err), tostring(ev.requestId)))
  end
end

save_queue(remaining)
print(string.format("[queue] delivered=%d pending=%d", #delivered, #remaining))
