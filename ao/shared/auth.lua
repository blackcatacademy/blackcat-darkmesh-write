-- Auth and capability checks (placeholder).

local Auth = {}

function Auth.verify_signature(command)
  -- TODO: implement signature / capability token verification
  return true
end

function Auth.check_policy(command, policy)
  -- TODO: enforce role, tenant, and action allowlist
  return true
end

return Auth
