-- Contract smoke tests for write process.

local function assert_eq(a, b, label)
  if a ~= b then error(string.format("%s expected %s, got %s", label, tostring(b), tostring(a))) end
end

local function assert_status(resp, status, label)
  assert_eq(resp.status, status, label .. " status")
end

-- Enable dev-mode JWT fallback
_G.RUN_CONTRACTS = "1"

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

-- Stripe dispute webhook propagates to payments/orders
do
  local pay = write.route(with_req({
    action = "CreatePaymentIntent",
    payload = { orderId = "o-dispute", amount = 50, currency = "USD", provider = "stripe" },
  }))
  assert_status(pay, "OK", "create stripe payment")
  local wh = write.route(with_req({
    action = "ProviderWebhook",
    payload = { provider = "stripe", paymentId = pay.payload.paymentId, eventType = "charge.dispute.created", status = "disputed", reason = "fraud" },
  }))
  assert_status(wh, "OK", "stripe dispute webhook")
  local st = write._state()
  assert(st.payment_disputes[pay.payload.paymentId], "dispute recorded")
  assert(st.payment_disputes[pay.payload.paymentId].status == "disputed", "dispute status set")
  assert(st.orders["o-dispute"].status == "disputed", "order marked disputed")
end

-- Cart / pricing / order creation
do
  write.route(with_req({
    action = "AddShippingRate",
    payload = { siteId = "s-cart", country = "CZ", price = 5, currency = "CZK", carrier = "PPL" },
  }))
  write.route(with_req({
    action = "AddTaxRate",
    payload = { siteId = "s-cart", country = "CZ", rate = 0.21, category = "books" },
  }))
  local cart_add = write.route(with_req({
    action = "CartAddItem",
    payload = {
      cartId = "cart-1",
      siteId = "s-cart",
      sku = "sku-book-1",
      qty = 2,
      price = 100,
      currency = "CZK",
      weight = 0.5,
      categoryId = "books",
    },
  }))
  assert_status(cart_add, "OK", "cart add")
  local cart_get = write.route(with_req({
    action = "CartGet",
    payload = { cartId = "cart-1" },
  }))
  assert_status(cart_get, "OK", "cart get")
  assert_eq(#(cart_get.payload.cart.items), 1, "cart items count")
  local price = write.route(with_req({
    action = "CartPrice",
    payload = { cartId = "cart-1", address = { country = "CZ" } },
  }))
  assert_status(price, "OK", "cart price")
  assert(price.payload.totals.total > 0, "cart total computed")
  local create = write.route(with_req({
    action = "CreateOrder",
    payload = { cartId = "cart-1", customerId = "cust-1", siteId = "s-cart", currency = "CZK", address = { country = "CZ" } },
  }))
  assert_status(create, "OK", "create order")
  local create_again = write.route(with_req({
    action = "CreateOrder",
    requestId = "rid-create-idem",
    payload = { cartId = "cart-1", customerId = "cust-1", siteId = "s-cart", currency = "CZK", address = { country = "CZ" } },
  }))
  assert_status(create_again, "OK", "create order second")
end

-- Coupons enforcement: scope, redemptions, expiry
do
  -- seed coupon applicable to sku-1 with single redemption and expiry in future
  local now = os.time()
  local up = write.route(with_req({
    action = "UpsertCoupon",
    payload = {
      code = "ONEUSE10",
      type = "percent",
      value = 10,
      currency = "USD",
      maxRedemptions = 1,
      redeemByCustomer = 1,
      maxDiscount = 8,
      maxStack = 2,
      startsAt = now - 100,
      expiresAt = now + 1000,
      applies_to = { "sku-1" },
    },
  }))
  assert_status(up, "OK", "upsert coupon")

  -- build order via cart + create order
  local cart = write.route(with_req({
    action = "CartAddItem",
    payload = { cartId = "cart1", siteId = "s6", sku = "sku-1", qty = 1, price = 100, currency = "USD" },
  }))
  assert_status(cart, "OK", "cart add")
  local order = write.route(with_req({
    action = "CreateOrder",
    payload = { cartId = "cart1", customerId = "cust1", siteId = "s6", currency = "USD" },
  }))
  assert_status(order, "OK", "order create")

  local apply_ok = write.route(with_req({
    action = "ApplyCoupon",
    payload = { orderId = order.payload.orderId, code = "ONEUSE10" },
  }))
  assert_status(apply_ok, "OK", "coupon apply first time")
  assert(apply_ok.payload.totalAmount, "totalAmount present after coupon")

  -- second order should hit maxRedemptions
  local cart2 = write.route(with_req({
    action = "CartAddItem",
    payload = { cartId = "cart2", siteId = "s6", sku = "sku-1", qty = 1, price = 50, currency = "USD" },
  }))
  assert_status(cart2, "OK", "cart2 add")
  local order2 = write.route(with_req({
    action = "CreateOrder",
    payload = { cartId = "cart2", customerId = "cust2", siteId = "s6", currency = "USD" },
  }))
  assert_status(order2, "OK", "order2 create")
  local apply_fail = write.route(with_req({
    action = "ApplyCoupon",
    payload = { orderId = order2.payload.orderId, code = "ONEUSE10" },
  }))
  assert_status(apply_fail, "ERROR", "coupon exhausted")
  assert_eq(apply_fail.code, "INVALID_INPUT", "exhausted code")

  -- per-customer redemption limit hit (redeemByCustomer=1)
  local cart3 = write.route(with_req({
    action = "CartAddItem",
    payload = { cartId = "cart3", siteId = "s6", sku = "sku-1", qty = 1, price = 50, currency = "USD" },
  }))
  assert_status(cart3, "OK", "cart3 add")
  local order3 = write.route(with_req({
    action = "CreateOrder",
    payload = { cartId = "cart3", customerId = "cust1", siteId = "s6", currency = "USD" },
  }))
  assert_status(order3, "OK", "order3 create")
  local apply_customer_fail = write.route(with_req({
    action = "ApplyCoupon",
    payload = { orderId = order3.payload.orderId, code = "ONEUSE10" },
  }))
  assert_status(apply_customer_fail, "ERROR", "coupon customer exhausted")

  -- stacking: non-stackable blocks second coupon
  local up2 = write.route(with_req({
    action = "UpsertCoupon",
    payload = {
      code = "STACKABLE",
      type = "fixed",
      value = 5,
      currency = "USD",
      stackable = true,
    },
  }))
  assert_status(up2, "OK", "upsert stackable")
  local apply_stack = write.route(with_req({
    action = "ApplyCoupon",
    payload = { orderId = order.payload.orderId, code = "STACKABLE" },
  }))
  assert_status(apply_stack, "ERROR", "non-stackable existing blocks")
  assert_eq(apply_stack.code, "INVALID_STATE", "non-stackable code")

  -- stackable coupons allowed when both are stackable
  local up_stack = write.route(with_req({
    action = "UpsertCoupon",
    payload = { code = "STACK1", type = "fixed", value = 5, currency = "USD", stackable = true },
  }))
  assert_status(up_stack, "OK", "upsert stack1")
  local up_stack2 = write.route(with_req({
    action = "UpsertCoupon",
    payload = { code = "STACK2", type = "fixed", value = 3, currency = "USD", stackable = true },
  }))
  assert_status(up_stack2, "OK", "upsert stack2")
  local cart4 = write.route(with_req({
    action = "CartAddItem",
    payload = { cartId = "cart4", siteId = "s6", sku = "sku-1", qty = 1, price = 50, currency = "USD" },
  }))
  assert_status(cart4, "OK", "cart4 add")
  local order4 = write.route(with_req({
    action = "CreateOrder",
    payload = { cartId = "cart4", customerId = "cust4", siteId = "s6", currency = "USD" },
  }))
  assert_status(order4, "OK", "order4 create")
  local apply_stack1 = write.route(with_req({
    action = "ApplyCoupon",
    payload = { orderId = order4.payload.orderId, code = "STACK1" },
  }))
  assert_status(apply_stack1, "OK", "apply stack1")
  local apply_stack2 = write.route(with_req({
    action = "ApplyCoupon",
    payload = { orderId = order4.payload.orderId, code = "STACK2" },
  }))
  assert_status(apply_stack2, "OK", "apply stack2")

  -- expiry enforcement
  local up_expired = write.route(with_req({
    action = "UpsertCoupon",
    payload = { code = "EXPIRED", type = "fixed", value = 5, currency = "USD", expiresAt = now - 10, startsAt = now - 100 },
  }))
  assert_status(up_expired, "OK", "upsert expired")
  local apply_expired = write.route(with_req({
    action = "ApplyCoupon",
    payload = { orderId = order4.payload.orderId, code = "EXPIRED" },
  }))
  assert_status(apply_expired, "ERROR", "expired coupon rejected")

  -- OTP flow: Issue and exchange
  local otp_issue = write.route(with_req({
    action = "IssueOtp",
    payload = { sub = "user-otp", tenant = "tenant-otp", role = "support", ttl = 120 },
  }))
  assert_status(otp_issue, "OK", "otp issue")
  local code = otp_issue.payload.code
  local otp_exchange = write.route(with_req({
    action = "ExchangeOtp",
    payload = { code = code },
  }))
  assert_status(otp_exchange, "OK", "otp exchange")
  assert(otp_exchange.payload.token and #otp_exchange.payload.token > 10, "otp token present")

  -- Sessions
  local sess = write.route(with_req({
    action = "IssueSession",
    payload = { sub = "user-1", tenant = "tenant-1", role = "editor", ttl = 120 },
  }))
  assert_status(sess, "OK", "issue session")
  assert(sess.payload.token and #sess.payload.token > 10, "session token present")
  local rev = write.route(with_req({
    action = "RevokeSession",
    payload = { sessionId = sess.payload.sessionId },
  }))
  assert_status(rev, "OK", "revoke session")

  -- Subscriptions
  local sub = write.route(with_req({
    action = "CreateSubscription",
    payload = { subscriptionId = "sub-1", customerId = "cust-sub", planId = "plan-basic", status = "active" },
  }))
  assert_status(sub, "OK", "create subscription")
  local up = write.route(with_req({
    action = "UpdateSubscriptionStatus",
    payload = { subscriptionId = "sub-1", status = "past_due" },
  }))
  assert_status(up, "OK", "update subscription")

  -- scope enforcement: coupon applies only to sku-2
  local scoped = write.route(with_req({
    action = "UpsertCoupon",
    payload = {
      code = "SKU2ONLY",
      type = "fixed",
      value = 5,
      currency = "USD",
      applies_to = { "sku-2" },
    },
  }))
  assert_status(scoped, "OK", "upsert scoped coupon")
  local apply_scope_fail = write.route(with_req({
    action = "ApplyCoupon",
    payload = { orderId = order.payload.orderId, code = "SKU2ONLY" },
  }))
  assert_status(apply_scope_fail, "ERROR", "scope mismatch")
  assert_eq(apply_scope_fail.code, "INVALID_STATE", "scope mismatch code")

  -- CreateOrder should reject expired coupon when passed directly
  local expired = write.route(with_req({
    action = "UpsertCoupon",
    payload = {
      code = "EXPIRED",
      type = "fixed",
      value = 5,
      currency = "USD",
      expiresAt = now - 10,
    },
  }))
  assert_status(expired, "OK", "upsert expired coupon")
  local cart5 = write.route(with_req({
    action = "CartAddItem",
    payload = { cartId = "cart5", siteId = "s6", sku = "sku-1", qty = 1, price = 30, currency = "USD" },
  }))
  assert_status(cart5, "OK", "cart5 add")
  local order_fail = write.route(with_req({
    action = "CreateOrder",
    payload = { cartId = "cart5", customerId = "cust5", siteId = "s6", currency = "USD", coupon = "EXPIRED" },
  }))
  assert_status(order_fail, "ERROR", "expired coupon should fail in CreateOrder")
end

-- Category-scoped coupon enforcement
do
  local _ = write.route(with_req({
    action = "UpsertCoupon",
    payload = {
      code = "BOOKS10",
      type = "percent",
      value = 10,
      currency = "USD",
      applies_to_categories = { "books" },
      expiresAt = os.time() + 3600,
    },
  }))
  local cart_bad = write.route(with_req({
    action = "CartAddItem",
    payload = { cartId = "cart6", siteId = "s7", sku = "sku-ebook", qty = 1, price = 20, currency = "USD", categoryId = "ebooks" },
  }))
  assert_status(cart_bad, "OK", "cart6 add")
  local order_fail = write.route(with_req({
    action = "CreateOrder",
    payload = { cartId = "cart6", customerId = "cust6", siteId = "s7", currency = "USD", coupon = "BOOKS10" },
  }))
  assert_status(order_fail, "ERROR", "category coupon should block non-matching category")

  local cart_ok = write.route(with_req({
    action = "CartAddItem",
    payload = { cartId = "cart7", siteId = "s7", sku = "sku-book", qty = 1, price = 20, currency = "USD", categoryId = "books" },
  }))
  assert_status(cart_ok, "OK", "cart7 add")
  local order_ok = write.route(with_req({
    action = "CreateOrder",
    payload = { cartId = "cart7", customerId = "cust7", siteId = "s7", currency = "USD", coupon = "BOOKS10" },
  }))
  assert_status(order_ok, "OK", "category coupon should pass when category matches")
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
