-- Schema and size validation stubs.

local Validation = {}

function Validation.validate_envelope(command)
  -- TODO: integrate JSON schema validation for command envelopes
  return true
end

function Validation.validate_action(action, payload)
  -- TODO: integrate action-specific schema validation
  return true
end

return Validation
