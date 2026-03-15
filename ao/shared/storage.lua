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

return Storage
