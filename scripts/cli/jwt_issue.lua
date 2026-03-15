-- Simple JWT HS256 issuer for admin/tenant tokens
-- Usage:
--   LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" \
--   WRITE_JWT_HS_SECRET=changeme lua scripts/cli/jwt_issue.lua tenant_id role ttl_seconds subject

local secret = os.getenv("WRITE_JWT_HS_SECRET")
if not secret or secret == "" then
  io.stderr:write("WRITE_JWT_HS_SECRET not set\n")
  os.exit(1)
end

local tenant = arg[1] or "tenant-1"
local role = arg[2] or "admin"
local ttl = tonumber(arg[3] or "600")
local sub = arg[4] or "admin"

local ok_json, cjson = pcall(require, "cjson.safe")
local ok_mime, mime = pcall(require, "mime")
local ok_crypto, crypto = pcall(require, "ao.shared.crypto")

if not (ok_json and ok_mime and ok_crypto) then
  io.stderr:write("missing deps: cjson/mime/ao.shared.crypto\n")
  os.exit(1)
end

local function b64url(x)
  return (mime.b64(x) or ""):gsub("+", "-"):gsub("/", "_"):gsub("=", "")
end

local header = b64url(cjson.encode({ alg = "HS256", typ = "JWT" }))
local now = os.time()
local payload_tbl = {
  iss = "blackcat-write",
  sub = sub,
  tenant = tenant,
  role = role,
  iat = now,
  exp = now + ttl,
  nonce = "n-" .. tostring(math.random(1, 1e9)),
  jti = "j-" .. tostring(math.random(1, 1e9)),
}
local payload = b64url(cjson.encode(payload_tbl))
local signing = header .. "." .. payload
local sig_hex = crypto.hmac_sha256_hex(signing, secret)
local sig = sig_hex:gsub("%x%x", function(x) return string.char(tonumber(x, 16)) end)
local token = signing .. "." .. b64url(sig)
print(token)
