-- Append-only audit helpers (in-memory for now).

local Audit = {}
local records = {}

function Audit.append(record)
  table.insert(records, record)
  return true
end

function Audit.all()
  return records
end

return Audit
