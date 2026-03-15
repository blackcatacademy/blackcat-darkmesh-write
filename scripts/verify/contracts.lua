-- Contract smoke tests for write process.

local function assert_eq(a, b, label)
  if a ~= b then error(string.format("%s expected %s, got %s", label, tostring(b), tostring(a))) end
end

local function assert_status(resp, status, label)
  assert_eq(resp.status, status, label .. " status")
end

local function with_req(cmd)
  cmd.requestId = cmd.requestId or string.format("rid-%017d", math.random(1, 1e9))
  cmd.timestamp = cmd.timestamp or "2026-03-15T00:00:00Z"
  cmd.nonce = cmd.nonce or string.format("nonce-%017d", math.random(1, 1e9))
  cmd.signatureRef = cmd.signatureRef or string.format("sigref-%017d", math.random(1, 1e9))
  cmd.actor = cmd.actor or "actor-1"
  cmd.tenant = cmd.tenant or "tenant-1"
  cmd.role = cmd.role or "admin"
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
    requestId = "rid-route-123456",
    nonce = "nonce-route-123456",
    signatureRef = "sigref-route-123456",
    payload = { siteId = "s1", path = "/", target = "home" },
  })
  local r1 = write.route(req)
  local r2 = write.route(req)
  assert_status(r1, "OK", "idempotent route first")
  assert_status(r2, "OK", "idempotent route second")
  assert_eq(r1.payload.path, r2.payload.path, "idempotent route")
end

-- Version conflict
do
  local first = write.route(with_req({
    action = "PublishPageVersion",
    requestId = "rid-pub-0001",
    nonce = "nonce-pub-0001",
    signatureRef = "sigref-pub-0001",
    payload = { siteId = "s2", pageId = "home", versionId = "v1", manifestTx = "tx1234567890" },
  }))
  assert_status(first, "OK", "publish v1")
  local conflict = write.route(with_req({
    action = "PublishPageVersion",
    requestId = "rid-pub-0002",
    nonce = "nonce-pub-0002",
    signatureRef = "sigref-pub-0002",
    payload = { siteId = "s2", pageId = "home", versionId = "v2", manifestTx = "tx2234567890" },
    expectedVersion = "old",
  }))
  assert_status(conflict, "ERROR", "version conflict")
  assert_eq(conflict.code, "VERSION_CONFLICT", "conflict code")

  local outbox = write._outbox()
  assert(outbox and #outbox >= 1, "outbox should have publish event")
  assert_eq(outbox[#outbox].manifestTx, "tx1234567890", "outbox manifest matches")
end

-- New actions: inventory, price rule, revoke entitlement, grant role
do
  local inv = write.route(with_req({
    action = "UpsertInventory",
    requestId = "rid-inv-0001",
    nonce = "nonce-inv-0001",
    signatureRef = "sigref-inv-0001",
    payload = { siteId = "s3", sku = "sku-1", quantity = 10, location = "wh1" },
  }))
  assert_status(inv, "OK", "inventory upsert")
  local price = write.route(with_req({
    action = "UpsertPriceRule",
    requestId = "rid-price-0001",
    nonce = "nonce-price-0001",
    signatureRef = "sigref-price-0001",
    payload = { siteId = "s3", ruleId = "rule-1", formula = "price*0.9", active = true, currency = "USD", vatRate = 0.2 },
  }))
  assert_status(price, "OK", "price rule upsert")
  local grant = write.route(with_req({
    action = "GrantRole",
    requestId = "rid-grant-0001",
    nonce = "nonce-grant-0001",
    signatureRef = "sigref-grant-0001",
    payload = { tenant = "t1", subject = "user1", role = "editor" },
  }))
  assert_status(grant, "OK", "grant role")
  local revoke = write.route(with_req({
    action = "RevokeEntitlement",
    requestId = "rid-revoke-0001",
    nonce = "nonce-revoke-0001",
    signatureRef = "sigref-revoke-0001",
    payload = { subject = "subj1", asset = "asset1" },
  }))
  assert_status(revoke, "OK", "revoke entitlement")
  local customer = write.route(with_req({
    action = "UpsertCustomer",
    role = "support",
    payload = { tenant = "t1", customerId = "c1", profile = { email = "a@b.com" } },
  }))
  assert_status(customer, "OK", "upsert customer")
  local order = write.route(with_req({
    action = "UpsertOrderStatus",
    role = "support",
    payload = { orderId = "o1", status = "paid", reason = "paid test" },
  }))
  assert_status(order, "OK", "order status")
  local refund = write.route(with_req({
    action = "IssueRefund",
    role = "support",
    payload = { orderId = "o1", amount = 10.5, currency = "USD", vatRate = 0.2 },
  }))
  assert_status(refund, "OK", "issue refund")
  local webhook = write.route(with_req({
    action = "CreateWebhook",
    payload = { tenant = "t1", url = "https://example.com/hook", events = { "order.created" } },
  }))
  assert_status(webhook, "OK", "create webhook")

  local pay = write.route(with_req({
    action = "CreatePaymentIntent",
    payload = { orderId = "o1", amount = 10.5, currency = "USD", provider = "gopay" },
  }))
  assert_status(pay, "OK", "create payment intent")
  local cap = write.route(with_req({
    action = "CapturePayment",
    payload = { paymentId = pay.payload.paymentId },
  }))
  assert_status(cap, "OK", "capture payment")
  -- Coupon apply/remove
  local coup_apply = write.route(with_req({
    action = "ApplyCoupon",
    payload = { orderId = "o1", code = "WELCOME10" },
  }))
  -- coupon store is stubbed empty, expect error
  assert_status(coup_apply, "ERROR", "apply coupon should fail without seed")

  local ship = write.route(with_req({
    action = "UpsertShipmentStatus",
    payload = { shipmentId = "ship1", status = "shipped", tracking = "TRK", carrier = "DHL", orderId = "o1" },
  }))
  assert_status(ship, "OK", "shipment status")
  local ret = write.route(with_req({
    action = "UpsertReturnStatus",
    payload = { returnId = "ret1", status = "approved", reason = "size", orderId = "o1" },
  }))
  assert_status(ret, "OK", "return status")

  -- Provider webhook updates payment status
  local webhook = write.route(with_req({
    action = "ProviderWebhook",
    payload = { provider = "gopay", paymentId = pay.payload.providerPaymentId or pay.payload.paymentId, status = "PAID" },
  }))
  assert_status(webhook, "OK", "provider webhook")
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
