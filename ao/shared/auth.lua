-- Minimal auth and policy checks; fail-closed if requested.

local Auth = {}
local crypto = require("ao.shared.crypto")

local function env_bool(name)
  if _G[name] == "1" then return true end
  return os.getenv(name) == "1"
end

local jwt_ok, jwt = pcall(require, "ao.shared.jwt")

local overrides = { require_sig = nil, require_nonce = nil, allow_anon = nil }
local rl_override = { window = nil, max = nil }
local role_policy = {
  SaveDraftPage      = { "editor", "admin" },
  PublishPageVersion = { "publisher", "admin" },
  UpsertRoute        = { "editor", "admin" },
  DeleteRoute        = { "editor", "admin" },
  UpsertProduct      = { "catalog-admin", "admin" },
  UpsertInventory    = { "inventory-admin", "admin" },
  UpsertPriceRule    = { "pricing-admin", "admin" },
  UpsertCustomer     = { "support", "admin" },
  UpsertOrderStatus  = { "support", "admin" },
  IssueRefund        = { "support", "admin" },
  GrantRole          = { "admin" },
  GrantEntitlement   = { "access-admin", "admin" },
  RevokeEntitlement  = { "access-admin", "admin" },
  UpsertProfile      = { "editor", "admin" },
}

function Auth._set_flags(opts)
  overrides.require_sig = opts.require_sig
  overrides.require_nonce = opts.require_nonce
  overrides.allow_anon = opts.allow_anon
end

function Auth._set_role_policy(map)
  role_policy = map
end

function Auth._set_rate_limits(window, max)
  rl_override.window = window
  rl_override.max = max
end

local function flag(name, override_key)
  if overrides[override_key] ~= nil then return overrides[override_key] end
  return env_bool(name)
end

local NONCE_TTL = tonumber(os.getenv("WRITE_NONCE_TTL_SECONDS") or "300")
local NONCE_MAX = tonumber(os.getenv("WRITE_NONCE_MAX") or "2048")
local nonce_store = {}
local RL_WINDOW = tonumber(os.getenv("WRITE_RL_WINDOW_SECONDS") or "60")
local RL_MAX = tonumber(os.getenv("WRITE_RL_MAX_REQUESTS") or "200")
local rate_store = {}
local function getenv(name)
  return os.getenv(name) or _G[name]
end

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

-- Optional JWT (HS256) consumption:
--  WRITE_JWT_HS_SECRET=... (shared secret)
--  WRITE_REQUIRE_JWT=1 to fail-closed if token missing/invalid.
local function extract_bearer(command)
  if command.jwt then return command.jwt end
  if command.JWT then return command.JWT end
  if command.token then return command.token end
  local authz = command.Authorization or command.authorization or command.auth
  if authz and type(authz) == "string" then
    return (authz:gsub("^%s*[Bb]earer%s+", ""))
  end
end

function Auth.consume_jwt(command)
  local secret = getenv("WRITE_JWT_HS_SECRET")
  if not secret or secret == "" then return true end
  if not jwt_ok then return false, "jwt_module_missing" end
  local token = extract_bearer(command)
  if (not token or token == "") and env_bool("WRITE_REQUIRE_JWT") then
    return false, "missing_jwt"
  elseif not token or token == "" then
    return true
  end
  local ok, claims = jwt.verify_hs256(token, secret)
  if not ok then return false, claims or "jwt_invalid" end
  if claims.exp and os.time() > claims.exp then
    return false, "jwt_expired"
  end
  command.actor = command.actor or claims.sub
  command.tenant = command.tenant or claims.tenant
  command.role = command.role or claims.role
  command.nonce = command.nonce or claims.nonce
  command.jwt_claims = claims
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

-- Optional: detached signature check (ed25519 | ecdsa | HMAC) when env set.
-- Env:
--  WRITE_SIG_TYPE=ed25519|ecdsa|hmac
--  WRITE_SIG_PUBLIC=/path/to/pubkey (ed25519/ecdsa PEM)
--  WRITE_SIG_SECRET=... (hmac)
function Auth.verify_detached(message, signature_hex)
  local sig_type = getenv("WRITE_SIG_TYPE") or "none"
  if sig_type == "ed25519" then
    local pub = getenv("WRITE_SIG_PUBLIC")
    if not pub then return false, "missing_public_key" end
    return crypto.verify_ed25519(message, signature_hex, pub)
  elseif sig_type == "ecdsa" then
    local pub = getenv("WRITE_SIG_PUBLIC")
    if not pub then return false, "missing_public_key" end
    return crypto.verify_ecdsa_sha256(message, signature_hex, pub)
  elseif sig_type == "hmac" then
    local secret = getenv("WRITE_SIG_SECRET")
    if not secret then return false, "missing_secret" end
    return crypto.verify_hmac_sha256(message, secret, signature_hex)
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

function Auth.check_role_for_action(command)
  local rp = role_policy
  local env = os.getenv("WRITE_ROLE_POLICY")
  if env and env ~= "" then
    local ok, cjson = pcall(require, "cjson")
    if ok then
      local decoded = cjson.decode(env)
      if type(decoded) == "table" then rp = decoded end
    end
  end
  local allowed = rp and rp[command.action or command.Action]
  if not allowed then return true end
  local role = command.role or command.ActorRole
  if not role then return false, "missing_role" end
  for _, r in ipairs(allowed) do
    if r == role then return true end
  end
  return false, "forbidden_role"
end

function Auth.check_rate_limit(command)
  local now = os.time()
  local key = (command.tenant or "global") .. ":" .. (command.actor or "anon")
  local window = rl_override.window or RL_WINDOW
  local max = rl_override.max or RL_MAX
  local bucket = rate_store[key] or { count = 0, reset = now + window }
  if now > bucket.reset then
    bucket.count = 0
    bucket.reset = now + window
  end
  bucket.count = bucket.count + 1
  rate_store[key] = bucket
  if bucket.count > max then
    return false, "rate_limited"
  end
  return true
end

return Auth
