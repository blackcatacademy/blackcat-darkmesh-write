-- Idempotency registry with optional persistence via Storage.

local storage = require("ao.shared.storage")
local Idem = {}
local store = {}

function Idem.lookup(request_id)
  return store[request_id]
end

function Idem.record(request_id, outcome)
  store[request_id] = outcome
  return true
end

function Idem.persist(path)
  storage.put("idempotency", store)
  return storage.persist(path)
end

function Idem.load(path)
  local ok = storage.load(path)
  if ok then
    local persisted = storage.get("idempotency")
    if type(persisted) == "table" then store = persisted end
  end
end

return Idem
