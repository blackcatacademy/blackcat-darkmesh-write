-- Minimal in-memory storage abstraction (can be swapped with AO state ops).

local Storage = {}
local kv = {}

function Storage.put(key, value)
  kv[key] = value
  return true
end

function Storage.get(key)
  return kv[key]
end

function Storage.append(list_key, value)
  kv[list_key] = kv[list_key] or {}
  table.insert(kv[list_key], value)
  return true
end

function Storage.all(list_key)
  return kv[list_key] or {}
end

-- Optional persistence helpers (no-op if cjson missing)
function Storage.persist(path)
  local ok, cjson = pcall(require, "cjson")
  if not ok then return false, "cjson_missing" end
  local f = io.open(path, "w")
  if not f then return false, "open_failed" end
  f:write(cjson.encode(kv))
  f:close()
  return true
end

function Storage.load(path)
  local f = io.open(path, "r")
  if not f then return false end
  local content = f:read("*a")
  f:close()
  local ok, cjson = pcall(require, "cjson")
  if not ok then return false, "cjson_missing" end
  local decoded = cjson.decode(content)
  if type(decoded) == "table" then
    for k, v in pairs(decoded) do
      kv[k] = v
    end
    return true
  end
  return false
end

return Storage
