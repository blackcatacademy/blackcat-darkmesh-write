-- Idempotency registry helpers (placeholder).

local Idem = {}

-- Returns stored result if requestId exists, otherwise nil.
function Idem.lookup(request_id)
  -- TODO: connect to persistent registry in AO state
  return nil
end

-- Persist outcome for a requestId.
function Idem.record(request_id, outcome)
  -- TODO: write append-only record
  return true
end

return Idem
