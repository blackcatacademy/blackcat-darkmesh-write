-- Minimal auth and policy checks; fail-closed if requested.

local Auth = {}

local function env_bool(name)
  if _G[name] == "1" then return true end
  return os.getenv(name) == "1"
end

local overrides = { require_sig = nil, require_nonce = nil, allow_anon = nil }

function Auth._set_flags(opts)
  overrides.require_sig = opts.require_sig
  overrides.require_nonce = opts.require_nonce
  overrides.allow_anon = opts.allow_anon
end

local function flag(name, override_key)
  if overrides[override_key] ~= nil then return overrides[override_key] end
  return env_bool(name)
end

local NONCE_TTL = tonumber(os.getenv("WRITE_NONCE_TTL_SECONDS") or "300")
local NONCE_MAX = tonumber(os.getenv("WRITE_NONCE_MAX") or "2048")
local nonce_store = {}

local function prune_nonces()
  local now = os.time()
  local count = 0
  for k, exp in pairs(nonce_store) do
    if exp < now then
      nonce_store[k] = nil
    else
      count = count + 1
    end
  end
  if count > NONCE_MAX then
    -- drop the oldest
    local oldest_key, oldest_val
    for k, v in pairs(nonce_store) do
      if not oldest_val or v < oldest_val then
        oldest_val = v; oldest_key = k
      end
    end
    if oldest_key then nonce_store[oldest_key] = nil end
  end
end

function Auth.require_nonce(command)
  prune_nonces()
  local nonce = command.nonce
  if not nonce or nonce == "" then
    if flag("WRITE_REQUIRE_NONCE", "require_nonce") then
      return false, "missing_nonce"
    end
    return true
  end
  if nonce_store[nonce] then
    return false, "replay_nonce"
  end
  nonce_store[nonce] = os.time() + NONCE_TTL
  prune_nonces()
  return true
end

function Auth.verify_signature(command)
  if command.signatureRef and #tostring(command.signatureRef) > 0 then
    return true
  end
  if flag("WRITE_REQUIRE_SIGNATURE", "require_sig") then
    return false, "missing_signature"
  end
  return true
end

function Auth.check_policy(command, policy)
  -- Basic allow/deny: tenant and actor must be present unless explicitly allowed.
  if not flag("WRITE_ALLOW_ANON", "allow_anon") then
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
