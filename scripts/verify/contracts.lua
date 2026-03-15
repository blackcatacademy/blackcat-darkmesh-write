-- Contract smoke tests for write process.

local function assert_eq(a, b, label)
  if a ~= b then error(string.format("%s expected %s, got %s", label, tostring(b), tostring(a))) end
end

local function assert_status(resp, status, label)
  assert_eq(resp.status, status, label .. " status")
end

local function with_req(cmd)
  cmd.requestId = cmd.requestId or string.format("rid-%012d", math.random(1, 1e9))
  cmd.timestamp = cmd.timestamp or "2026-03-15T00:00:00Z"
  cmd.nonce = cmd.nonce or "nonce-123456"
  cmd.signatureRef = cmd.signatureRef or "sigref-123456"
  cmd.actor = cmd.actor or "actor-1"
  cmd.tenant = cmd.tenant or "tenant-1"
  return cmd
end

local write = require("ao.write.process")

-- Happy path SaveDraftPage
do
  local resp = write.route(with_req({
    action = "SaveDraftPage",
    payload = { siteId = "s1", pageId = "home", locale = "en", blocks = {} },
  }))
  assert_status(resp, "OK", "save draft")
end

-- Idempotency: same requestId returns same payload
do
  local req = with_req({
    action = "UpsertRoute",
    requestId = "rid-route",
    payload = { siteId = "s1", path = "/", target = "home" },
  })
  local r1 = write.route(req)
  local r2 = write.route(req)
  assert_eq(r1.payload.path, r2.payload.path, "idempotent route")
end

-- Version conflict
do
  write.route(with_req({
    action = "PublishPageVersion",
    payload = { siteId = "s2", pageId = "home", versionId = "v1", manifestTx = "tx1" },
  }))
  local conflict = write.route(with_req({
    action = "PublishPageVersion",
    payload = { siteId = "s2", pageId = "home", versionId = "v2", manifestTx = "tx2" },
    expectedVersion = "old",
  }))
  assert_status(conflict, "ERROR", "version conflict")
  assert_eq(conflict.code, "VERSION_CONFLICT", "conflict code")
end

-- Unknown action
do
  local resp = write.route(with_req({ action = "Nope", payload = {} }))
  assert_status(resp, "ERROR", "unknown action")
end

-- Envelope validation failure
do
  local resp = write.route({ action = "SaveDraftPage" }) -- missing fields
  assert_status(resp, "ERROR", "bad envelope")
end

print("write contract tests passed")
