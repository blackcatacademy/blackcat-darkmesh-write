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
