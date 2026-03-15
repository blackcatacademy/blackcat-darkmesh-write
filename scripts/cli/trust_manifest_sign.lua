-- Sign a resolver trust manifest (HMAC-SHA256) for publishing to Arweave.
-- Usage:
--   TRUST_MANIFEST_HMAC=secret lua scripts/cli/trust_manifest_sign.lua manifest.json > manifest.signed.json
--
-- manifest.json example:
-- {
--   "version": 1,
--   "resolvers": [
--     { "id": "resolver-1", "pubkey": "base64/hex", "endpoint": "https://...", "validFrom": 0, "validTo": 0, "status": "active" }
--   ]
-- }

local secret = os.getenv("TRUST_MANIFEST_HMAC")
if not secret or secret == "" then
  io.stderr:write("TRUST_MANIFEST_HMAC not set\n")
  os.exit(1)
end

local path = arg[1]
if not path then
  io.stderr:write("usage: lua trust_manifest_sign.lua manifest.json\n")
  os.exit(1)
end

local ok_json, cjson = pcall(require, "cjson.safe")
local ok_mime, mime = pcall(require, "mime")
local ok_crypto, crypto = pcall(require, "ao.shared.crypto")
if not (ok_json and ok_mime and ok_crypto) then
  io.stderr:write("missing deps: cjson.safe, mime, ao.shared.crypto\n")
  os.exit(1)
end

local f = assert(io.open(path, "r"))
local raw = f:read("*a")
f:close()
local manifest, err = cjson.decode(raw)
if not manifest then
  io.stderr:write("decode failed: " .. tostring(err) .. "\n")
  os.exit(1)
end

local payload = cjson.encode(manifest)
local sig_hex = crypto.hmac_sha256_hex(payload, secret)
if not sig_hex then
  io.stderr:write("hmac failed: crypto backend missing\n")
  os.exit(1)
end

local signed = {
  manifest = manifest,
  signature = sig_hex,
  signed_at = os.time(),
  signer = os.getenv("TRUST_MANIFEST_SIGNER") or "unknown",
}

print(cjson.encode(signed))
