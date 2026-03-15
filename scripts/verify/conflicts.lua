-- Conflict and security edge cases for write router.

local function assert_eq(a, b, label)
  if a ~= b then error(string.format("%s expected %s, got %s", label, tostring(b), tostring(a))) end
end

local function assert_status(resp, status, label)
  assert_eq(resp.status, status, label .. " status")
end

local function with_req(cmd)
  cmd.requestId = cmd.requestId or string.format("rid-%015d", math.random(1, 1e9))
  cmd.timestamp = cmd.timestamp or "2026-03-15T00:00:00Z"
  cmd.nonce = cmd.nonce or string.format("nonce-%017d", math.random(1, 1e9))
  cmd.signatureRef = cmd.signatureRef or string.format("sigref-%017d", math.random(1, 1e9))
  cmd.actor = cmd.actor or "actor-1"
  cmd.tenant = cmd.tenant or "tenant-1"
  cmd.role = cmd.role or "admin"
  return cmd
end

-- require signature / nonce
do
  local auth = require("ao.shared.auth")
  auth._set_flags({ require_sig = true, require_nonce = true })
  package.loaded["ao.write.process"] = nil
  local write = require("ao.write.process")
  local resp = write.route({
    action = "SaveDraftPage",
    requestId = "rid-missing-00001",
    actor = "actor-1",
    tenant = "tenant-1",
    timestamp = "2026-03-15T00:00:00Z",
    payload = { siteId = "s1", pageId = "home", locale = "en", blocks = {} },
    signatureRef = nil,
    nonce = nil,
  })
  assert_status(resp, "ERROR", "missing signature/nonce")
  auth._set_flags({ require_sig = nil, require_nonce = nil })
end

-- nonce replay
do
  local write = require("ao.write.process")
  local first_cmd = with_req({
    action = "SaveDraftPage",
    requestId = "rid-nonce-1",
    nonce = "nonce-replay",
    role = "editor",
    payload = { siteId = "s1", pageId = "about", locale = "en", blocks = {} },
  })
  local first = write.route(first_cmd)
  assert_status(first, "OK", "first nonce")
  local second = write.route(with_req({
    action = "SaveDraftPage",
    requestId = "rid-nonce-2",
    nonce = "nonce-replay",
    role = "editor",
    payload = { siteId = "s1", pageId = "about", locale = "en", blocks = {} },
  }))
  assert_status(second, "ERROR", "replay nonce")
end

-- forbidden role
do
  local write = require("ao.write.process")
  local resp = write.route(with_req({
    action = "GrantRole",
    role = "viewer",
    payload = { tenant = "t1", subject = "u1", role = "editor" },
  }))
  assert_status(resp, "ERROR", "forbidden role")
end

-- missing role for action that requires one
do
  local write = require("ao.write.process")
  local cmd = with_req({
    action = "UpsertInventory",
    role = nil,
    payload = { siteId = "s1", sku = "sku-rl-1", quantity = 3, location = "wh-x" },
  })
  cmd.role = nil
  local resp = write.route(cmd)
  assert_status(resp, "ERROR", "missing role")
end

-- bad signature (hmac mismatch)
do
  _G.WRITE_SIG_TYPE = "hmac"
  os.setenv = os.setenv or function() end
  os.setenv("WRITE_SIG_TYPE", "hmac")
  os.setenv("WRITE_SIG_SECRET", "secret1")
  package.loaded["ao.shared.auth"] = nil
  package.loaded["ao.write.process"] = nil
  local write = require("ao.write.process")
  local resp = write.route(with_req({
    action = "SaveDraftPage",
    signature = "deadbeef",
    payload = { siteId = "s-bad", pageId = "p", locale = "en", blocks = {} },
  }))
  assert_status(resp, "ERROR", "bad signature")
  os.setenv("WRITE_SIG_TYPE", nil)
  os.setenv("WRITE_SIG_SECRET", nil)
  _G.WRITE_SIG_TYPE = nil
  package.loaded["ao.shared.auth"] = nil
  package.loaded["ao.write.process"] = nil
end

-- bad ed25519 signature
do
  local pub = [[-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAgN7aX6ixkmKuuNHYsYvwdivEafgAvFp8Z64KbjsggqU=
-----END PUBLIC KEY-----]]
  local pub_path = os.tmpname() .. "-ed25519.pub"
  local f = assert(io.open(pub_path, "w")); f:write(pub); f:close()
  os.setenv = os.setenv or function() end
  os.setenv("WRITE_SIG_TYPE", "ed25519")
  os.setenv("WRITE_SIG_PUBLIC", pub_path)
  _G.WRITE_SIG_TYPE = "ed25519"
  _G.WRITE_SIG_PUBLIC = pub_path
  package.loaded["ao.shared.auth"] = nil
  package.loaded["ao.write.process"] = nil
  local write = require("ao.write.process")
  local resp = write.route(with_req({
    action = "SaveDraftPage",
    signature = "deadbeef",
    payload = { siteId = "s-ed", pageId = "p-ed", locale = "en", blocks = {} },
  }))
  assert_status(resp, "ERROR", "bad ed25519 signature")
  os.setenv("WRITE_SIG_TYPE", nil)
  os.setenv("WRITE_SIG_PUBLIC", nil)
  _G.WRITE_SIG_TYPE = nil
  _G.WRITE_SIG_PUBLIC = nil
  package.loaded["ao.shared.auth"] = nil
  package.loaded["ao.write.process"] = nil
  os.remove(pub_path)
end

-- bad ecdsa signature
do
  local pub = [[-----BEGIN PUBLIC KEY-----
MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAE6wLdystD5nq2WEYLRh3SeDsICoZ6irM+
tL6vhfVqlhK/SXxPxq8np5xpoE2mR7BncpsbR9f7DmqDveoxu48UUw==
-----END PUBLIC KEY-----]]
  local pub_path = os.tmpname() .. "-ecdsa.pub"
  local f = assert(io.open(pub_path, "w")); f:write(pub); f:close()
  os.setenv = os.setenv or function() end
  os.setenv("WRITE_SIG_TYPE", "ecdsa")
  os.setenv("WRITE_SIG_PUBLIC", pub_path)
  _G.WRITE_SIG_TYPE = "ecdsa"
  _G.WRITE_SIG_PUBLIC = pub_path
  package.loaded["ao.shared.auth"] = nil
  package.loaded["ao.write.process"] = nil
  local write = require("ao.write.process")
  local resp = write.route(with_req({
    action = "SaveDraftPage",
    signature = "cafebabe",
    payload = { siteId = "s-ec", pageId = "p-ec", locale = "en", blocks = {} },
  }))
  assert_status(resp, "ERROR", "bad ecdsa signature")
  os.setenv("WRITE_SIG_TYPE", nil)
  os.setenv("WRITE_SIG_PUBLIC", nil)
  _G.WRITE_SIG_TYPE = nil
  _G.WRITE_SIG_PUBLIC = nil
  package.loaded["ao.shared.auth"] = nil
  package.loaded["ao.write.process"] = nil
  os.remove(pub_path)
end

-- missing actor/tenant
do
  local write = require("ao.write.process")
  local resp = write.route({
    action = "SaveDraftPage",
    requestId = "rid-missing-actor",
    timestamp = "2026-03-15T00:00:00Z",
    nonce = "nonce-xxx",
    signatureRef = "sigref-xxx",
    payload = { siteId = "s1", pageId = "p1", locale = "en", blocks = {} },
  })
  assert_status(resp, "ERROR", "missing actor/tenant")
end

-- replay id across different actions should return first response
do
  local write = require("ao.write.process")
  local rid = "rid-cross-action"
  local first = write.route(with_req({
    action = "SaveDraftPage",
    requestId = rid,
    role = "editor",
    payload = { siteId = "s1", pageId = "p1", locale = "en", blocks = {} },
  }))
  assert_status(first, "OK", "first action ok")
  local second = write.route(with_req({
    action = "PublishPageVersion",
    requestId = rid,
    role = "publisher",
    payload = { siteId = "s1", pageId = "p1", versionId = "v1", manifestTx = "tx1" },
  }))
  assert_status(second, "OK", "idempotent replay returns stored response")
  assert_eq(second.payload and second.payload.draftKey, nil, "replayed payload from first")
end

-- repeated same action with same requestId should always return identical payload
do
  local write = require("ao.write.process")
  local rid = "rid-same-action"
  local cmd = with_req({
    action = "SaveDraftPage",
    requestId = rid,
    role = "editor",
    payload = { siteId = "s2", pageId = "p2", locale = "en", blocks = {} },
  })
  local r1 = write.route(cmd)
  local r2 = write.route(cmd)
  assert_status(r1, "OK", "first save ok")
  assert_eq(r1.payload.draftKey, r2.payload.draftKey, "idempotent payload same")
end

-- negative role tests for protected actions
do
  local write = require("ao.write.process")
  local function forbid(action, role, payload)
    local resp = write.route(with_req({ action = action, role = role, payload = payload }))
    assert_status(resp, "ERROR", action .. " forbidden for role " .. role)
  end
  forbid("PublishPageVersion", "editor", { siteId = "s1", pageId = "p1", versionId = "v2", manifestTx = "tx2" })
  forbid("UpsertRoute", "viewer", { siteId = "s1", path = "/x", target = "t" })
  forbid("UpsertProduct", "viewer", { siteId = "s1", sku = "sku-x", payload = {} })
  forbid("UpsertInventory", "viewer", { siteId = "s1", sku = "sku-y", quantity = 1, location = "wh1" })
  forbid("UpsertPriceRule", "viewer", { siteId = "s1", ruleId = "r1", formula = "x", active = true })
  forbid("UpsertCustomer", "viewer", { tenant = "t1", customerId = "c1", profile = {} })
  forbid("UpsertOrderStatus", "viewer", { orderId = "o1", status = "paid" })
  forbid("IssueRefund", "viewer", { orderId = "o1", amount = 1 })
  forbid("GrantRole", "viewer", { tenant = "t1", subject = "u1", role = "editor" })
  forbid("GrantEntitlement", "viewer", { subject = "u1", asset = "a1", policy = "p1" })
  forbid("RevokeEntitlement", "viewer", { subject = "u1", asset = "a1" })
  forbid("CreateWebhook", "viewer", { tenant = "t1", url = "https://x", events = { "PublishPageVersion" } })
  forbid("UpsertProfile", "viewer", { subject = "u1", profile = {} })
end

-- rate limit breach
do
  package.loaded["ao.shared.auth"] = nil
  local auth = require("ao.shared.auth")
  auth._set_rate_limits(60, 1)
  package.loaded["ao.write.process"] = nil
  local write = require("ao.write.process")
  local r1 = write.route(with_req({ action = "SaveDraftPage", payload = { siteId = "r1", pageId = "p1", locale = "en", blocks = {} } }))
  assert_status(r1, "OK", "rl first ok")
  local r2 = write.route(with_req({ action = "SaveDraftPage", payload = { siteId = "r1", pageId = "p2", locale = "en", blocks = {} } }))
  assert_status(r2, "ERROR", "rl second blocked")
  auth._set_rate_limits(nil, nil)
  package.loaded["ao.shared.auth"] = nil
  package.loaded["ao.write.process"] = nil
end

print("conflict tests passed")
