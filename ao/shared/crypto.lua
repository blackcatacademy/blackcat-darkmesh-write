-- Basic signature verification stubs (ed25519/hmac where available).

local Crypto = {}

local function has(mod)
  local ok, m = pcall(require, mod)
  if ok then return m end
  return nil
end

local openssl = has("openssl")
local sodium = has("sodium")

function Crypto.verify_ed25519(message, signature_hex, pubkey_path)
  if sodium and sodium.crypto_sign_verify_detached then
    local pub = assert(io.open(pubkey_path, "rb")):read("*a")
    local sig = sodium.from_hex(signature_hex)
    if not sig then return false, "bad_hex" end
    local ok = sodium.crypto_sign_verify_detached(sig, message, pub)
    return ok, ok and nil or "bad_signature"
  end
  if openssl and openssl.pkey and openssl.hex then
    local pem = assert(io.open(pubkey_path, "r")):read("*a")
    local pkey = openssl.pkey.read(pem, true, "public")
    local raw = openssl.hex(signature_hex)
    local ok = pkey:verify(raw, message, "NONE")
    return ok, ok and nil or "bad_signature"
  end
  return false, "ed25519_not_available"
end

function Crypto.verify_hmac_sha256(message, secret, signature_hex)
  if openssl and openssl.hmac then
    local raw = openssl.hmac.digest("sha256", message, secret, true)
    local hex = (openssl.hex and openssl.hex(raw)) or raw:gsub(".", function(c) return string.format("%02x", string.byte(c)) end)
    return hex:lower() == tostring(signature_hex):lower(), nil
  end
  if sodium and sodium.crypto_auth then
    local tag = sodium.crypto_auth(message, secret)
    local hex = sodium.to_hex(tag)
    return hex:lower() == tostring(signature_hex):lower(), nil
  end
  return false, "hmac_not_available"
end

return Crypto
