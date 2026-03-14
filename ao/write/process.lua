-- Entry point for the write command AO process.
-- TODO: wire handlers once contracts are finalized.

local M = {}

-- route(command) should validate and dispatch to specific handlers.
function M.route(command)
  -- placeholder: replace with actual routing logic
  return { status = "NOT_IMPLEMENTED", requestId = command.requestId }
end

return M
