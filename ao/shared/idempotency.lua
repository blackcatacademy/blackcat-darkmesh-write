-- Simple in-memory idempotency registry (AO runtime would persist).

local Idem = {}
local store = {}

-- Returns stored result if requestId exists, otherwise nil.
function Idem.lookup(request_id)
  return store[request_id]
end

-- Persist outcome for a requestId.
function Idem.record(request_id, outcome)
  store[request_id] = outcome
  return true
end

return Idem
