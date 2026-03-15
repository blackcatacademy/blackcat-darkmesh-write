-- Minimal auth and policy checks; fail-closed if requested.

local Auth = {}
local crypto = require("ao.shared.crypto")

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
local RL_WINDOW = tonumber(os.getenv("WRITE_RL_WINDOW_SECONDS") or "60")
local RL_MAX = tonumber(os.getenv("WRITE_RL_MAX_REQUESTS") or "200")
local rate_store = {}

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

-- Optional: detached signature check (ed25519 or HMAC) when env set.
-- Env:
--  WRITE_SIG_TYPE=ed25519|hmac
--  WRITE_SIG_PUBLIC=/path/to/pubkey (ed25519)
--  WRITE_SIG_SECRET=... (hmac)
function Auth.verify_detached(message, signature_hex)
  local sig_type = os.getenv("WRITE_SIG_TYPE") or "none"
  if sig_type == "ed25519" then
    local pub = os.getenv("WRITE_SIG_PUBLIC")
    if not pub then return false, "missing_public_key" end
    return crypto.verify_ed25519(message, signature_hex, pub)
  elseif sig_type == "hmac" then
    local secret = os.getenv("WRITE_SIG_SECRET")
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

function Auth.check_rate_limit(command)
  local now = os.time()
  local key = (command.tenant or "global") .. ":" .. (command.actor or "anon")
  local bucket = rate_store[key] or { count = 0, reset = now + RL_WINDOW }
  if now > bucket.reset then
    bucket.count = 0
    bucket.reset = now + RL_WINDOW
  end
  bucket.count = bucket.count + 1
  rate_store[key] = bucket
  if bucket.count > RL_MAX then
    return false, "rate_limited"
  end
  return true
end

return Auth
