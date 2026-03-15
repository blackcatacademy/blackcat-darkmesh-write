-- Schema validation helpers.

local schema = require("ao.shared.schema")

local Validation = {}

function Validation.validate_envelope(command)
  return schema.validate_envelope(command)
end

function Validation.validate_action(action, payload)
  return schema.validate_action(action, payload)
end

return Validation
