-- Minimal auth and policy checks; fail-closed if requested.

local Auth = {}

local REQUIRE_SIG = os.getenv("WRITE_REQUIRE_SIGNATURE") == "1"
local ALLOW_ANON = os.getenv("WRITE_ALLOW_ANON") == "1"

function Auth.verify_signature(command)
  if command.signatureRef and #tostring(command.signatureRef) > 0 then
    return true
  end
  if REQUIRE_SIG then
    return false, "missing_signature"
  end
  return true
end

function Auth.check_policy(command, policy)
  -- Basic allow/deny: tenant and actor must be present unless explicitly allowed.
  if not ALLOW_ANON then
    if not command.actor or command.actor == "" then
      return false, "missing_actor"
    end
    if not command.tenant or command.tenant == "" then
      return false, "missing_tenant"
    end
  end
  if policy and policy.allowed_roles then
    local role = command.role or command.ActorRole
    local ok = false
    for _, r in ipairs(policy.allowed_roles) do
      if r == role then ok = true end
    end
    if not ok then return false, "forbidden" end
  end
  return true
end

return Auth
