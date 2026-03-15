-- Export current write outbox to NDJSON for inspection or downstream piping.

local write = require("ao.write.process")
local storage = require("ao.shared.storage")

local out = arg[1] or "dev/outbox.ndjson"

local function ensure_dir(path)
  local dir = path:match("(.+)/[^/]+$")
  if dir then os.execute(string.format('mkdir -p "%s"', dir)) end
end

local function json_encode(tbl)
  local ok, cjson = pcall(require, "cjson")
  if ok then return cjson.encode(tbl) end
  local function enc(v)
    local t = type(v)
    if t == "nil" then return "null" end
    if t == "boolean" then return v and "true" or "false" end
    if t == "number" then return tostring(v) end
    if t == "string" then return string.format("%q", v) end
    if t == "table" then
      local is_array = (#v > 0)
      if is_array then
        local parts = {}
        for _, item in ipairs(v) do table.insert(parts, enc(item)) end
        return "[" .. table.concat(parts, ",") .. "]"
      else
        local parts = {}
        for k, val in pairs(v) do table.insert(parts, string.format("%q:%s", k, enc(val))) end
        return "{" .. table.concat(parts, ",") .. "}"
      end
    end
    return "\"<unsupported>\""
  end
  return enc(tbl)
end

-- Ensure we have the latest outbox mirrored to storage
local _ = write._outbox() -- triggers any pending updates
local events = storage.all("outbox")

ensure_dir(out)
local f = assert(io.open(out, "w"))
for _, ev in ipairs(events) do
  f:write(json_encode(ev))
  f:write("\n")
end
f:close()

print(string.format("[export] wrote %d events to %s", #events, out))
