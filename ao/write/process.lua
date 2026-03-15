-- Entry point for the write command AO process.

local validation = require("ao.shared.validation")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")
local audit = require("ao.shared.audit")
local storage = require("ao.shared.storage")
local bridge = require("ao.shared.bridge")
local crypto = require("ao.shared.crypto")
local jwt = require("ao.shared.jwt")
local gopay_ok, gopay = pcall(require, "ao.shared.gopay")
local stripe_ok, stripe = pcall(require, "ao.shared.stripe")
local paypal_ok, paypal = pcall(require, "ao.shared.paypal")
local tax = require("ao.shared.tax")
local ok_mime, mime = pcall(require, "mime")
local ok_json, cjson = pcall(require, "cjson.safe")

local function enqueue_event(ev)
  local q = storage.get("outbox_queue") or {}
  table.insert(q, { event = ev, status = "pending", attempts = 0, nextAttempt = os.time() })
  storage.put("outbox_queue", q)
  if os.getenv("WRITE_OUTBOX_PATH") then storage.persist(os.getenv("WRITE_OUTBOX_PATH")) end
end
-- legacy helper used by older code paths; now routes everything to the durable queue
local function send_event(ev)
  enqueue_event(ev)
end
local OUTBOX_PATH = os.getenv("WRITE_OUTBOX_PATH")
local WAL_PATH = os.getenv("WRITE_WAL_PATH")
local OUTBOX_HMAC_SECRET = os.getenv("OUTBOX_HMAC_SECRET")
local CART_STORE_PATH = os.getenv("WRITE_CART_STORE_PATH")
local RATE_STORE_PATH = os.getenv("WRITE_RATE_STORE_PATH")

local M = {}

local function sha256_str(str)
  local tmp = os.tmpname()
  local f = io.open(tmp, "w"); if not f then return nil end
  f:write(str); f:close()
  local p = io.popen("sha256sum " .. tmp .. " 2>/dev/null")
  local out = p and p:read("*a") or ""
  if p then p:close() end
  os.remove(tmp)
  return out:match("^(%w+)")
end

-- simple in-memory state; AO runtime would persist
local state = {
  drafts = {},        -- key: siteId:pageId -> payload
  versions = {},      -- siteId -> versionId
  routes = {},        -- siteId -> map[path] = target
  products = {},      -- siteId -> map[sku] = payload
  roles = {},         -- tenant -> subject -> role
  profiles = {},      -- subject -> profile
  entitlements = {},  -- subject -> list of {asset, policy}
  inventory = {},     -- siteId -> sku -> entry
  price_rules = {},   -- siteId -> ruleId -> entry
  customers = {},     -- tenant -> customerId -> profile
  orders = {},        -- orderId -> full order payload
  coupons = {},       -- code -> { type, value, currency, minOrder, expiresAt }
  webhooks = {},      -- tenant -> list of endpoints
  payments = {},      -- paymentId -> {orderId, amount, currency, provider, status}
  shipments = {},     -- shipmentId -> {status, tracking, carrier}
  returns = {},       -- returnId -> {status, reason}
  dlq = {},           -- dead-letter for outbox
  inventory_reservations = {}, -- orderId -> { siteId=..., items = { {sku, qty} } }
  carts = {},         -- cartId -> { siteId, currency, items = { {sku, qty, price, currency, productId, title} } }
  coupon_redemptions = {}, -- code -> count
  shipping_rates = {}, -- siteId -> list of {country, region, minWeight, maxWeight, price, currency, carrier, service}
  tax_rates = {},     -- siteId -> list of {country, region, rate, category}
  otps = {},          -- code_hash -> { sub, tenant, role, exp }
  otp_rate = {},      -- key -> { count, reset }
  payment_tokens = {}, -- customerId -> provider -> token
  payment_disputes = {}, -- paymentId -> { status, reason, evidence }
  sessions = {},      -- sessionId -> { sub, tenant, role, exp, device }
  subscriptions = {}, -- subscriptionId -> { customerId, planId, status, meta }
}

-- load persisted carts if available
do
  if CART_STORE_PATH then
    storage.load(CART_STORE_PATH)
    local persisted = storage.get("carts")
    if persisted then state.carts = persisted end
  end
  if RATE_STORE_PATH then
    storage.load(RATE_STORE_PATH)
    local sh = storage.get("shipping_rates")
    if sh then state.shipping_rates = sh end
    local tx = storage.get("tax_rates")
    if tx then state.tax_rates = tx end
  end
end
local outbox = {}      -- emitted events for downstream (-ao bridge)

local function ok(req_id, payload)
  return { status = "OK", requestId = req_id, payload = payload or {} }
end

local function err(req_id, code, msg, details)
  return { status = "ERROR", code = code, message = msg, requestId = req_id, details = details }
end

local handlers = {}
local role_policy = {
  ProviderShippingWebhook = { "support", "admin", "catalog-admin" },
  AddDisputeEvidence = { "support", "admin" },
}

local function b64url(x)
  return (mime.b64(x) or ""):gsub("+", "-"):gsub("/", "_"):gsub("=", "")
end

local function otp_hash(code)
  local salt = os.getenv("OTP_HMAC_SECRET")
  if not salt or salt == "" then return code end -- fallback (plain)
  return crypto.hmac_sha256_hex(code, salt) or code
end

local function new_session_id()
  return string.format("sess_%d_%06d", os.time(), math.random(0, 999999))
end

local function set_payment_status(pid, new_status, provider_status, req_id)
  local p = state.payments[pid]
  if not p then return end
  p.status = new_status or p.status
  p.updatedAt = os.time()
  local ev = {
    type = "PaymentStatusChanged",
    paymentId = pid,
    status = p.status,
    providerStatus = provider_status,
    requestId = req_id,
  }
  enqueue_event(ev)
  if p.orderId and state.orders[p.orderId] then
    local map = {
      captured = "paid",
      refunded = "refunded",
      voided = "cancelled",
      disputed = "disputed",
      failed = "payment_failed",
      pending = state.orders[p.orderId].status,
    }
    local new_order_status = map[p.status]
    if new_order_status then
      state.orders[p.orderId].status = new_order_status
      enqueue_event({
        type = "OrderStatusUpdated",
        orderId = p.orderId,
        status = new_order_status,
        requestId = req_id,
      })
    end
  end
end

function handlers.AddDisputeEvidence(cmd)
  local payment = state.payments[cmd.payload.paymentId]
  if not payment then return err(cmd.requestId, "NOT_FOUND", "payment not found") end
  local pd = state.payment_disputes[cmd.payload.paymentId] or { status = payment.status, reason = payment.reason }
  pd.evidence = cmd.payload.evidence or pd.evidence
  if cmd.payload.status then pd.status = cmd.payload.status end
  if cmd.payload.reason then pd.reason = cmd.payload.reason end
  state.payment_disputes[cmd.payload.paymentId] = pd
  if pd.status then set_payment_status(cmd.payload.paymentId, pd.status, "dispute_evidence", cmd.requestId) end
  enqueue_event({
    type = "PaymentDisputeEvidence",
    paymentId = cmd.payload.paymentId,
    provider = cmd.payload.provider,
    status = pd.status,
    reason = pd.reason,
    requestId = cmd.requestId,
  })
  return ok(cmd.requestId, { paymentId = cmd.payload.paymentId, status = pd.status })
end

local function otp_rate_key(sub, tenant)
  return (tenant or "tenant") .. ":" .. (sub or "user")
end

local function check_otp_rate(sub, tenant)
  local window = tonumber(os.getenv("OTP_RATE_WINDOW") or "60")
  local max = tonumber(os.getenv("OTP_RATE_MAX") or "5")
  local key = otp_rate_key(sub, tenant)
  local bucket = state.otp_rate[key] or { count = 0, reset = os.time() + window }
  if os.time() > bucket.reset then
    bucket.count = 0
    bucket.reset = os.time() + window
  end
  bucket.count = bucket.count + 1
  state.otp_rate[key] = bucket
  if bucket.count > max then
    return false, "otp_rate_limited"
  end
  return true
end

local function issue_jwt(sub, tenant, role, ttl)
  local secret = os.getenv("WRITE_JWT_HS_SECRET")
  local dev_mode = (_G.RUN_CONTRACTS == "1") or (os.getenv("RUN_CONTRACTS") == "1") or (os.getenv("CI") == "true") or (os.getenv("ALLOW_DEV_JWT") == "1")
  if (not secret or secret == "") and dev_mode then
    secret = "dev-otp-secret"
  end
  if not secret or secret == "" then
    return nil, "jwt_secret_missing"
  end
  -- sodium crypto_auth expects 32-byte key; pad in dev mode
  if #secret < 32 and dev_mode then
    secret = secret .. string.rep("0", 32 - #secret)
  end
  if not (ok_mime and ok_json) then
    return nil, "jwt_deps_missing"
  end
  local now = os.time()
  local header = b64url(cjson.encode({ alg = "HS256", typ = "JWT" }))
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
  if not sig_hex then return nil, "jwt_sign_failed" end
  local sig = sig_hex:gsub("%x%x", function(x) return string.char(tonumber(x, 16)) end)
  local token = signing .. "." .. b64url(sig)
  return token
end

function handlers.SaveDraftPage(cmd)
  local key = (cmd.payload.siteId or "") .. ":" .. (cmd.payload.pageId or "")
  state.drafts[key] = {
    locale = cmd.payload.locale,
    blocks = cmd.payload.blocks,
    updatedAt = cmd.timestamp,
  }
  return ok(cmd.requestId, { draftKey = key })
end

function handlers.PublishPageVersion(cmd)
  local siteId = cmd.payload.siteId
  if cmd.expectedVersion and state.versions[siteId] and state.versions[siteId] ~= cmd.expectedVersion then
    return err(cmd.requestId, "VERSION_CONFLICT", "expectedVersion mismatch", { current = state.versions[siteId] })
  end
  state.versions[siteId] = cmd.payload.versionId
  local ev = {
    type = "PublishPageVersion",
    siteId = siteId,
    pageId = cmd.payload.pageId,
    versionId = cmd.payload.versionId,
    manifestTx = cmd.payload.manifestTx,
    requestId = cmd.requestId,
  }
  if OUTBOX_HMAC_SECRET then
    local msg = (cmd.payload.siteId or "") .. "|" .. (cmd.payload.pageId or "") .. "|" .. (cmd.payload.versionId or "")
    ev.hmac = crypto.hmac_sha256_hex(msg, OUTBOX_HMAC_SECRET)
  end
  enqueue_event(ev)
  table.insert(outbox, ev) -- keep in-memory outbox for tests/introspection
  return ok(cmd.requestId, { version = cmd.payload.versionId, manifestTx = cmd.payload.manifestTx })
end

function handlers.UpsertRoute(cmd)
  local siteId = cmd.payload.siteId
  state.routes[siteId] = state.routes[siteId] or {}
  state.routes[siteId][cmd.payload.path] = cmd.payload.target
  return ok(cmd.requestId, { path = cmd.payload.path })
end

function handlers.DeleteRoute(cmd)
  local siteId = cmd.payload.siteId
  if state.routes[siteId] then
    state.routes[siteId][cmd.payload.path] = nil
  end
  return ok(cmd.requestId, { deleted = cmd.payload.path })
end

function handlers.UpsertProduct(cmd)
  local siteId = cmd.payload.siteId
  state.products[siteId] = state.products[siteId] or {}
  state.products[siteId][cmd.payload.sku] = cmd.payload.payload
  return ok(cmd.requestId, { sku = cmd.payload.sku })
end

function handlers.AssignRole(cmd)
  local tenant = cmd.payload.tenant
  state.roles[tenant] = state.roles[tenant] or {}
  state.roles[tenant][cmd.payload.subject] = cmd.payload.role
  return ok(cmd.requestId, { subject = cmd.payload.subject, role = cmd.payload.role })
end

function handlers.UpsertProfile(cmd)
  state.profiles[cmd.payload.subject] = cmd.payload.profile
  return ok(cmd.requestId, { subject = cmd.payload.subject })
end

function handlers.UpsertCoupon(cmd)
  state.coupons[cmd.payload.code] = {
    type = cmd.payload.type,
    value = cmd.payload.value,
    currency = cmd.payload.currency,
    minOrder = cmd.payload.minOrder,
    maxRedemptions = cmd.payload.maxRedemptions,
    startsAt = cmd.payload.startsAt,
    expiresAt = cmd.payload.expiresAt,
    applies_to = cmd.payload.applies_to,
    is_active = cmd.payload.is_active ~= false,
    stackable = cmd.payload.stackable == true,
  }
  return ok(cmd.requestId, { code = cmd.payload.code })
end

function handlers.GrantEntitlement(cmd)
  local subj = cmd.payload.subject
  state.entitlements[subj] = state.entitlements[subj] or {}
  table.insert(state.entitlements[subj], { asset = cmd.payload.asset, policy = cmd.payload.policy })
  return ok(cmd.requestId, { subject = subj, asset = cmd.payload.asset })
end

function handlers.RevokeEntitlement(cmd)
  local subj = cmd.payload.subject
  local list = state.entitlements[subj] or {}
  local kept = {}
  for _, e in ipairs(list) do
    if e.asset ~= cmd.payload.asset then table.insert(kept, e) end
  end
  state.entitlements[subj] = kept
  return ok(cmd.requestId, { subject = subj, asset = cmd.payload.asset, revoked = true })
end

function handlers.UpsertInventory(cmd)
  local site = cmd.payload.siteId
  state.inventory[site] = state.inventory[site] or {}
  state.inventory[site][cmd.payload.sku] = {
    quantity = cmd.payload.quantity,
    location = cmd.payload.location,
    updatedAt = cmd.timestamp,
  }
  return ok(cmd.requestId, { sku = cmd.payload.sku, quantity = cmd.payload.quantity })
end

function handlers.UpsertPriceRule(cmd)
  local site = cmd.payload.siteId
  state.price_rules[site] = state.price_rules[site] or {}
  state.price_rules[site][cmd.payload.ruleId] = {
    formula = cmd.payload.formula,
    active = cmd.payload.active ~= false,
    currency = cmd.payload.currency,
    vatRate = cmd.payload.vatRate,
  }
  return ok(cmd.requestId, { ruleId = cmd.payload.ruleId, currency = cmd.payload.currency, vatRate = cmd.payload.vatRate })
end

function handlers.GrantRole(cmd)
  local tenant = cmd.payload.tenant or cmd.tenant
  state.roles[tenant] = state.roles[tenant] or {}
  state.roles[tenant][cmd.payload.subject] = cmd.payload.role
  return ok(cmd.requestId, { tenant = tenant, subject = cmd.payload.subject, role = cmd.payload.role })
end

function handlers.UpsertCustomer(cmd)
  local tenant = cmd.payload.tenant
  state.customers[tenant] = state.customers[tenant] or {}
  state.customers[tenant][cmd.payload.customerId] = cmd.payload.profile
  return ok(cmd.requestId, { customerId = cmd.payload.customerId })
end

function handlers.CreateSubscription(cmd)
  state.subscriptions[cmd.payload.subscriptionId] = {
    customerId = cmd.payload.customerId,
    planId = cmd.payload.planId,
    status = cmd.payload.status or "active",
    meta = cmd.payload.meta,
    createdAt = cmd.timestamp,
  }
  enqueue_event({
    type = "SubscriptionCreated",
    subscriptionId = cmd.payload.subscriptionId,
    customerId = cmd.payload.customerId,
    planId = cmd.payload.planId,
    status = cmd.payload.status or "active",
    requestId = cmd.requestId,
  })
  return ok(cmd.requestId, { subscriptionId = cmd.payload.subscriptionId, status = state.subscriptions[cmd.payload.subscriptionId].status })
end

function handlers.UpdateSubscriptionStatus(cmd)
  local sub = state.subscriptions[cmd.payload.subscriptionId]
  if not sub then return err(cmd.requestId, "NOT_FOUND", "subscription not found") end
  sub.status = cmd.payload.status
  sub.updatedAt = cmd.timestamp
  enqueue_event({
    type = "SubscriptionStatusUpdated",
    subscriptionId = cmd.payload.subscriptionId,
    status = sub.status,
    requestId = cmd.requestId,
  })
  return ok(cmd.requestId, { subscriptionId = cmd.payload.subscriptionId, status = sub.status })
end

function handlers.UpsertOrderStatus(cmd)
  state.orders[cmd.payload.orderId] = state.orders[cmd.payload.orderId] or { items = {} }
  state.orders[cmd.payload.orderId].status = cmd.payload.status
  state.orders[cmd.payload.orderId].reason = cmd.payload.reason
  state.orders[cmd.payload.orderId].totalAmount = cmd.payload.totalAmount or state.orders[cmd.payload.orderId].totalAmount
  state.orders[cmd.payload.orderId].currency = cmd.payload.currency or state.orders[cmd.payload.orderId].currency
  state.orders[cmd.payload.orderId].vatRate = cmd.payload.vatRate or state.orders[cmd.payload.orderId].vatRate
  state.orders[cmd.payload.orderId].updatedAt = cmd.timestamp
  return ok(cmd.requestId, {
    orderId = cmd.payload.orderId,
    status = cmd.payload.status,
    totalAmount = cmd.payload.totalAmount,
    currency = cmd.payload.currency,
    vatRate = cmd.payload.vatRate,
  })
end

function handlers.IssueRefund(cmd)
  local payment = state.payments[cmd.payload.orderId]
  if payment and payment.provider == "gopay" and payment.providerPaymentId then
    if gopay_ok then
      gopay.refund(payment.providerPaymentId, cmd.payload.amount)
    else
      return err(cmd.requestId, "PROVIDER_ERROR", "gopay module unavailable")
    end
  elseif payment and payment.provider == "stripe" and payment.providerPaymentId then
    if stripe_ok then
      local ok, perr = stripe.refund(payment.providerPaymentId, cmd.payload.amount)
      if not ok then return err(cmd.requestId, "PROVIDER_ERROR", perr or "stripe refund failed") end
    end
  end
  local ev = {
    type = "IssueRefund",
    orderId = cmd.payload.orderId,
    amount = cmd.payload.amount,
    currency = cmd.payload.currency,
    vatRate = cmd.payload.vatRate,
    requestId = cmd.requestId,
  }
  if OUTBOX_HMAC_SECRET then
    local msg = (cmd.payload.orderId or "") .. "|" .. tostring(cmd.payload.amount or "") .. "|" .. (cmd.payload.currency or "")
    ev.hmac = crypto.hmac_sha256_hex(msg, OUTBOX_HMAC_SECRET)
  end
  enqueue_event(ev)
  return ok(cmd.requestId, { orderId = cmd.payload.orderId, amount = cmd.payload.amount, currency = cmd.payload.currency, vatRate = cmd.payload.vatRate })
end

-- Coupon helpers (very simplified)
local function is_coupon_valid(code, order)
  local c = state.coupons[code]
  if not c then return false, "unknown_coupon" end
  local now = os.time()
  if c.startsAt and now < c.startsAt then return false, "not_started" end
  if c.expiresAt and now > c.expiresAt then return false, "expired" end
  if c.is_active == false then return false, "inactive" end
  if c.currency and order.currency and c.currency ~= order.currency then return false, "currency_mismatch" end
  if c.minOrder and order.totalAmount and order.totalAmount < c.minOrder then return false, "min_order_not_met" end
  if c.maxRedemptions and (state.coupon_redemptions[code] or 0) >= c.maxRedemptions then return false, "coupon_exhausted" end
  if c.redeemByCustomer and order.customerId then
    local per_customer = state.coupon_redemptions_customer[code] and state.coupon_redemptions_customer[code][order.customerId] or 0
    if c.redeemByCustomer > 0 and per_customer >= c.redeemByCustomer then return false, "coupon_customer_exhausted" end
  end
  if c.maxStack and order.coupons and #order.coupons >= c.maxStack then
    return false, "coupon_stack_limit"
  end
  if c.applies_to and type(c.applies_to) == "table" and order.items then
    local sku_allowed = {}
    for _, sku in ipairs(c.applies_to) do sku_allowed[sku] = true end
    local ok_any = false
    for _, it in ipairs(order.items) do
      if sku_allowed[it.sku] then ok_any = true break end
    end
    if not ok_any then return false, "coupon_not_applicable" end
  end
  if c.applies_to_categories and type(c.applies_to_categories) == "table" and order.items then
    local cat_allowed = {}
    for _, cat in ipairs(c.applies_to_categories) do cat_allowed[cat] = true end
    local ok_any = false
    for _, it in ipairs(order.items) do
      if it.categoryId and cat_allowed[it.categoryId] then ok_any = true break end
    end
    if not ok_any then return false, "coupon_not_applicable" end
  end
  return true
end

function handlers.ApplyCoupon(cmd)
  local order = state.orders[cmd.payload.orderId]
  if not order then return err(cmd.requestId, "NOT_FOUND", "order not found") end
  order.totalAmount = order.totalAmount or (order.totals and order.totals.total)
  if not order.totalAmount then return err(cmd.requestId, "NOT_FOUND", "order missing total") end

  order.coupons = order.coupons or {}
  if #order.coupons > 0 then
    -- stacking only if both existing and new coupon are stackable
    local existing_codes = order.coupons
    local any_non_stackable = false
    for _, code in ipairs(existing_codes) do
      if state.coupons[code] and state.coupons[code].stackable == false then any_non_stackable = true end
    end
    local new_c = state.coupons[cmd.payload.code]
    if any_non_stackable or (new_c and new_c.stackable == false) then
      return err(cmd.requestId, "INVALID_STATE", "coupon_not_stackable")
    end
  end

  local ok_coupon, reason = is_coupon_valid(cmd.payload.code, order)
  if not ok_coupon then
    return err(cmd.requestId, "INVALID_INPUT", reason)
  end
  local c = state.coupons[cmd.payload.code]
  local discount = 0
  if c.type == "percent" then
    discount = order.totalAmount * (c.value or 0) / 100
  else
    discount = c.value or 0
  end
  if c.maxDiscount and discount > c.maxDiscount then discount = c.maxDiscount end
  local new_total = math.max(0, order.totalAmount - discount)
  order.totalAmount = tax.round(new_total, os.getenv("CURRENCY_ROUND_MODE") or "half-up", 2)
  table.insert(order.coupons, cmd.payload.code)
  order.coupon = order.coupons[1] -- legacy
  state.coupon_redemptions[cmd.payload.code] = (state.coupon_redemptions[cmd.payload.code] or 0) + 1
  local ev = { type = "CouponApplied", orderId = cmd.payload.orderId, code = cmd.payload.code, discount = discount, requestId = cmd.requestId }
  enqueue_event(ev)
  return ok(cmd.requestId, { orderId = cmd.payload.orderId, totalAmount = order.totalAmount, code = cmd.payload.code, coupons = order.coupons })
end

function handlers.RemoveCoupon(cmd)
  local order = state.orders[cmd.payload.orderId]
  if not order then return err(cmd.requestId, "NOT_FOUND", "order not found") end
  order.coupons = order.coupons or {}
  local keep = {}
  for _, code in ipairs(order.coupons) do
    if code ~= cmd.payload.code then table.insert(keep, code) end
  end
  order.coupons = keep
  order.coupon = keep[1]
  local ev = { type = "CouponRemoved", orderId = cmd.payload.orderId, code = cmd.payload.code, requestId = cmd.requestId }
  enqueue_event(ev)
  return ok(cmd.requestId, { orderId = cmd.payload.orderId })
end

-- OTP issuance and exchange for short-lived JWT
function handlers.IssueOtp(cmd)
  local ttl = tonumber(cmd.payload.ttl) or tonumber(os.getenv("OTP_TTL_SECONDS") or "300")
  if ttl < 30 then ttl = 30 end
  if ttl > 3600 then ttl = 3600 end
  local ok_rate, rate_err = check_otp_rate(cmd.payload.sub, cmd.payload.tenant)
  if not ok_rate then
    return err(cmd.requestId, "RATE_LIMITED", rate_err)
  end
  local code = string.format("%06d", math.random(0, 999999))
  local exp = os.time() + ttl
  state.otps[otp_hash(code)] = {
    sub = cmd.payload.sub,
    tenant = cmd.payload.tenant,
    role = cmd.payload.role or "user",
    exp = exp,
  }
  return ok(cmd.requestId, { code = code, expiresAt = exp })
end

function handlers.ExchangeOtp(cmd)
  local code = cmd.payload.code and cmd.payload.code:gsub("%s+", "")
  local entry = code and state.otps[otp_hash(code)]
  if not entry then return err(cmd.requestId, "NOT_FOUND", "otp_not_found") end
  if os.time() > entry.exp then
    state.otps[otp_hash(code)] = nil
    return err(cmd.requestId, "UNAUTHORIZED", "otp_expired")
  end
  state.otps[otp_hash(code)] = nil -- one-time
  local ttl = tonumber(os.getenv("OTP_JWT_TTL_SECONDS") or "900")
  local token, terr = issue_jwt(entry.sub, entry.tenant, entry.role, ttl)
  if not token then
    return err(cmd.requestId, "SERVER_ERROR", terr or "jwt_failed")
  end
  return ok(cmd.requestId, { token = token, exp = os.time() + ttl, role = entry.role, tenant = entry.tenant, sub = entry.sub })
end

-- Session issuance (short-lived JWT) and revocation
function handlers.IssueSession(cmd)
  local ttl = tonumber(cmd.payload.ttl) or tonumber(os.getenv("SESSION_TTL_SECONDS") or "900")
  if ttl < 60 then ttl = 60 end
  if ttl > 86400 then ttl = 86400 end
  local sub = cmd.payload.sub or cmd.actor
  local tenant = cmd.payload.tenant or cmd.tenant
  local role = cmd.payload.role or cmd.role or "user"
  local token, terr = issue_jwt(sub, tenant, role, ttl)
  if not token then
    return err(cmd.requestId, "SERVER_ERROR", terr or "jwt_failed")
  end
  local sid = new_session_id()
  state.sessions[sid] = { sub = sub, tenant = tenant, role = role, exp = os.time() + ttl, device = cmd.payload.deviceToken }
  return ok(cmd.requestId, { sessionId = sid, token = token, exp = os.time() + ttl })
end

function handlers.RevokeSession(cmd)
  if not cmd.payload.sessionId then
    return err(cmd.requestId, "INVALID_INPUT", "sessionId required")
  end
  state.sessions[cmd.payload.sessionId] = nil
  return ok(cmd.requestId, { revoked = cmd.payload.sessionId })
end

-- Cart & Order creation
local compute_totals

local function assert_currency(cart_currency, item_currency)
  if item_currency and cart_currency and item_currency ~= cart_currency then
    return false, "currency_mismatch"
  end
  return true
end

function handlers.CartAddItem(cmd)
  local cart = state.carts[cmd.payload.cartId] or { siteId = cmd.payload.siteId, currency = cmd.payload.currency, items = {} }
  local ok_cur, cur_err = assert_currency(cart.currency, cmd.payload.currency)
  if not ok_cur then return err(cmd.requestId, "INVALID_INPUT", cur_err) end
  cart.currency = cart.currency or cmd.payload.currency
  -- replace if same sku
  local updated = false
  for _, it in ipairs(cart.items) do
    if it.sku == cmd.payload.sku then
      it.qty = cmd.payload.qty
      it.price = cmd.payload.price
      it.title = cmd.payload.title or it.title
      it.weight = cmd.payload.weight or it.weight
      it.dimensions = cmd.payload.dimensions or it.dimensions
      updated = true
    end
  end
  if not updated then
    table.insert(cart.items, {
      sku = cmd.payload.sku,
      productId = cmd.payload.productId,
      qty = cmd.payload.qty,
      price = cmd.payload.price,
      currency = cmd.payload.currency,
      title = cmd.payload.title,
      variant = cmd.payload.variant,
      weight = cmd.payload.weight,
      dimensions = cmd.payload.dimensions,
      categoryId = cmd.payload.categoryId,
    })
  end
  state.carts[cmd.payload.cartId] = cart
  storage.put("carts", state.carts)
  if CART_STORE_PATH then storage.persist(CART_STORE_PATH) end
  return ok(cmd.requestId, { cartId = cmd.payload.cartId, items = #cart.items })
end

function handlers.CartGet(cmd)
  local cart = state.carts[cmd.payload.cartId]
  if not cart then return err(cmd.requestId, "NOT_FOUND", "cart not found") end
  return ok(cmd.requestId, { cart = cart })
end

function handlers.CartPrice(cmd)
  local cart = state.carts[cmd.payload.cartId]
  if not cart or #(cart.items or {}) == 0 then
    return err(cmd.requestId, "NOT_FOUND", "cart empty or missing")
  end
  local vatRate = cmd.payload.vatRate or tonumber(os.getenv("TAX_RATE_DEFAULT") or "0")
  local totals, total_err = compute_totals(cart, cmd.payload.coupon, vatRate, cmd.payload.shipping, cmd.payload.address)
  if not totals then
    return err(cmd.requestId, "INVALID_INPUT", total_err)
  end
  return ok(cmd.requestId, { cartId = cmd.payload.cartId, totals = totals })
end

function handlers.CartRemoveItem(cmd)
  local cart = state.carts[cmd.payload.cartId]
  if not cart then return err(cmd.requestId, "NOT_FOUND", "cart not found") end
  local keep = {}
  for _, it in ipairs(cart.items) do
    if it.sku ~= cmd.payload.sku then table.insert(keep, it) end
  end
  cart.items = keep
  state.carts[cmd.payload.cartId] = cart
  storage.put("carts", state.carts)
  if CART_STORE_PATH then storage.persist(CART_STORE_PATH) end
  return ok(cmd.requestId, { cartId = cmd.payload.cartId, items = #cart.items })
end

function compute_totals(cart, coupon_code, vatRate, shipping, address)
  local subtotal = 0
  local total_weight = 0
  for _, it in ipairs(cart.items or {}) do
    subtotal = subtotal + (it.price or 0) * (it.qty or 1)
    total_weight = total_weight + (it.weight or 0) * (it.qty or 1)
  end
  local discount = 0
  if coupon_code then
    local dummy_order = {
      totalAmount = subtotal,
      currency = cart.currency,
      items = cart.items,
    }
    local ok_coupon, reason = is_coupon_valid(coupon_code, dummy_order)
    if not ok_coupon then
      return nil, reason
    end
    local c = state.coupons[coupon_code]
    if c then
      if c.type == "percent" then discount = subtotal * (c.value or 0) / 100 else discount = c.value or 0 end
      if c.maxRedemptions and (state.coupon_redemptions[coupon_code] or 0) >= c.maxRedemptions then
        return nil, "coupon_exhausted"
      end
    end
  end
  local net = math.max(0, subtotal - discount)
  local shipping_fee = shipping or tonumber(os.getenv("SHIPPING_FLAT_FEE") or "0") or 0
  -- try lookup rate table if no explicit shipping provided
  if shipping == nil then
    local rates = state.shipping_rates[cart.siteId or "default"] or {}
    local country = address and address.country and address.country:upper()
    local region = address and address.region
    local best_price
    for _, r in ipairs(rates) do
      local country_match = (not r.country) or (country and r.country == country)
      local region_match = (not r.region) or (region and r.region == region)
      local currency_match = (not r.currency) or (r.currency == cart.currency)
      local fits_weight = (not r.minWeight or total_weight >= r.minWeight) and (not r.maxWeight or total_weight <= r.maxWeight)
      if country_match and region_match and currency_match and fits_weight then
        if not best_price or (r.price or 0) < best_price then
          best_price = r.price or 0
          shipping_fee = r.price or shipping_fee
        end
      end
    end
  end
  local vat = vatRate and net * vatRate or 0
  -- per-item tax if table is available
  local site = cart.siteId or "default"
  local rates = state.tax_rates[site] or {}
  local country = address and address.country and address.country:upper()
  local region = address and address.region
  local function match_rate(cat)
    for _, r in ipairs(rates) do
      local country_match = (not r.country) or (country and r.country == country)
      local region_match = (not r.region) or (region and r.region == region)
      local cat_match = (not r.category) or (cat and r.category == cat)
      if country_match and region_match and cat_match then
        return r.rate
      end
    end
  end
  local vat_total = 0
  for _, it in ipairs(cart.items or {}) do
    local rate = match_rate(it.categoryId) or vatRate or tonumber(os.getenv("TAX_RATE_DEFAULT") or "0")
    vat_total = vat_total + ((it.price or 0) * (it.qty or 1) * (rate or 0))
  end
  vat = tax.round(vat_total, os.getenv("CURRENCY_ROUND_MODE") or "half-up", 2)
  local total = tax.round(net + vat + shipping_fee, os.getenv("CURRENCY_ROUND_MODE") or "half-up", 2)
  return {
    subtotal = tax.round(subtotal, os.getenv("CURRENCY_ROUND_MODE") or "half-up", 2),
    discount = tax.round(discount, os.getenv("CURRENCY_ROUND_MODE") or "half-up", 2),
    vat = tax.round(vat, os.getenv("CURRENCY_ROUND_MODE") or "half-up", 2),
    shipping = shipping_fee,
    total = total,
  }
end

function handlers.CreateOrder(cmd)
  local cart = state.carts[cmd.payload.cartId]
  if not cart or #(cart.items or {}) == 0 then
    return err(cmd.requestId, "NOT_FOUND", "cart empty or missing")
  end
  local existing_order_id = cmd.payload.orderId or ("ord_" .. tostring(cmd.payload.cartId))
  if state.orders[existing_order_id] then
    return ok(cmd.requestId, { orderId = existing_order_id, totalAmount = state.orders[existing_order_id].totals and state.orders[existing_order_id].totals.total, currency = state.orders[existing_order_id].currency })
  end
  -- derive vatRate from tax table if not provided
  local vatRate = cmd.payload.vatRate
  if not vatRate then
    local site = cart.siteId or "default"
    local rates = state.tax_rates[site] or {}
    for _, r in ipairs(rates) do
      local country_match = (not r.country) or (cmd.payload.address and r.country == string.upper(cmd.payload.address.country or ""))
      local region_match = (not r.region) or (cmd.payload.address and r.region == cmd.payload.address.region)
      if country_match and region_match then
        vatRate = r.rate
        break
      end
    end
  end
  vatRate = vatRate or tonumber(os.getenv("TAX_RATE_DEFAULT") or "0")
  local totals, total_err = compute_totals(cart, cmd.payload.coupon, vatRate, cmd.payload.shipping, cmd.payload.address)
  if not totals then
    return err(cmd.requestId, "INVALID_INPUT", total_err)
  end
  local orderId = existing_order_id
  state.orders[orderId] = {
    siteId = cmd.payload.siteId or cart.siteId,
    customerId = cmd.payload.customerId,
    currency = cart.currency,
    items = cart.items,
    status = "pending",
    totals = totals,
    coupon = cmd.payload.coupon, -- legacy
    coupons = cmd.payload.coupon and { cmd.payload.coupon } or {},
    vatRate = vatRate,
    shipping = totals.shipping,
    address = cmd.payload.address,
    createdAt = cmd.timestamp,
  }
  if cmd.payload.coupon then
    state.coupon_redemptions[cmd.payload.coupon] = (state.coupon_redemptions[cmd.payload.coupon] or 0) + 1
  end
  local ev = {
    type = "OrderCreated",
    orderId = orderId,
    siteId = state.orders[orderId].siteId,
    customerId = cmd.payload.customerId,
    currency = cart.currency,
    totalAmount = totals.total,
    requestId = cmd.requestId,
  }
  enqueue_event(ev)
  return ok(cmd.requestId, { orderId = orderId, totalAmount = totals.total, currency = cart.currency })
end

function handlers.AddShippingRate(cmd)
  local site = cmd.payload.siteId or "default"
  state.shipping_rates[site] = state.shipping_rates[site] or {}
  table.insert(state.shipping_rates[site], {
    country = (cmd.payload.country or ""):upper(),
    region = cmd.payload.region,
    minWeight = cmd.payload.minWeight,
    maxWeight = cmd.payload.maxWeight,
    price = cmd.payload.price,
    currency = cmd.payload.currency,
    carrier = cmd.payload.carrier,
    service = cmd.payload.service,
  })
  storage.put("shipping_rates", state.shipping_rates)
  if RATE_STORE_PATH then storage.persist(RATE_STORE_PATH) end
  return ok(cmd.requestId, { siteId = site, rates = #state.shipping_rates[site] })
end

function handlers.AddTaxRate(cmd)
  local site = cmd.payload.siteId or "default"
  state.tax_rates[site] = state.tax_rates[site] or {}
  table.insert(state.tax_rates[site], {
    country = (cmd.payload.country or ""):upper(),
    region = cmd.payload.region,
    rate = cmd.payload.rate,
    category = cmd.payload.category,
  })
  storage.put("tax_rates", state.tax_rates)
  if RATE_STORE_PATH then storage.persist(RATE_STORE_PATH) end
  return ok(cmd.requestId, { siteId = site, rates = #state.tax_rates[site] })
end

function handlers.ValidateAddress(cmd)
  -- Stub: basic presence checks; real implementation would call provider API
  if not cmd.payload.country or #cmd.payload.country < 2 then
    return err(cmd.requestId, "INVALID_INPUT", "country_required")
  end
  return ok(cmd.requestId, { valid = true, normalized = {
    country = cmd.payload.country:upper(),
    region = cmd.payload.region,
    city = cmd.payload.city,
    postal = cmd.payload.postal,
    line1 = cmd.payload.line1,
    line2 = cmd.payload.line2,
  }})
end

function handlers.GetShippingQuote(cmd)
  local cart = state.carts[cmd.payload.cartId]
  if not cart then return err(cmd.requestId, "NOT_FOUND", "cart not found") end
  local total_weight = 0
  for _, it in ipairs(cart.items or {}) do
    total_weight = total_weight + (it.weight or 0) * (it.qty or 1)
  end
  local site = cart.siteId or "default"
  local rates = state.shipping_rates[site] or {}
  local selected
  for _, r in ipairs(rates) do
    local country_match = (not r.country) or r.country == string.upper(cmd.payload.country)
    local region_match = (not r.region) or (cmd.payload.region and r.region == cmd.payload.region)
    local fits_weight = (not r.minWeight or total_weight >= r.minWeight) and (not r.maxWeight or total_weight <= r.maxWeight)
    if country_match and region_match and fits_weight then
      selected = r
      break
    end
  end
  if not selected then
    return err(cmd.requestId, "NOT_FOUND", "no rate")
  end
  return ok(cmd.requestId, { price = selected.price, currency = selected.currency, carrier = selected.carrier, service = selected.service })
end

function handlers.CreatePaymentIntent(cmd)
  local provider = cmd.payload.provider or os.getenv("PAYMENT_PROVIDER") or "manual"
  local pid = string.format("pay_%s", cmd.payload.orderId)
  local providerPaymentId, gatewayUrl
  local status = "requires_capture"
  if provider == "gopay" then
    if gopay_ok then
      local pid_out, gw, state = gopay.create_payment({
        orderId = cmd.payload.orderId,
        amount = cmd.payload.amount,
        currency = cmd.payload.currency,
        returnUrl = cmd.payload.returnUrl,
        description = cmd.payload.description,
        paymentMethodToken = cmd.payload.paymentMethodToken,
      })
      providerPaymentId, gatewayUrl = pid_out, gw
      if state == "CREATED" or state == "AUTHORIZED" then
        status = "requires_capture"
      elseif state == "PAID" then
        status = "captured"
      else
        status = "pending"
      end
    else
      status = "requires_capture"
    end
  elseif provider == "stripe" then
    if stripe_ok then
      if cmd.payload.customerId and not cmd.payload.paymentMethodToken then
        local token = state.payment_tokens[cmd.payload.customerId] and state.payment_tokens[cmd.payload.customerId].stripe
        if token then cmd.payload.paymentMethodToken = token end
      end
      providerPaymentId, gatewayUrl, status = stripe.create_payment({
        orderId = cmd.payload.orderId,
        amount = cmd.payload.amount,
        currency = cmd.payload.currency,
        returnUrl = cmd.payload.returnUrl,
        description = cmd.payload.description,
        metadata = cmd.payload.providerMetadata,
        paymentMethodToken = cmd.payload.paymentMethodToken,
        saveForFuture = cmd.payload.saveForFuture,
      })
    end
  elseif provider == "paypal" then
    if paypal_ok then
      if cmd.payload.customerId and not cmd.payload.paymentMethodToken then
        local token = state.payment_tokens[cmd.payload.customerId] and state.payment_tokens[cmd.payload.customerId].paypal
        if token then cmd.payload.paymentMethodToken = token end
      end
      providerPaymentId, gatewayUrl, status = paypal.create_payment({
        orderId = cmd.payload.orderId,
        amount = cmd.payload.amount,
        currency = cmd.payload.currency,
        returnUrl = cmd.payload.returnUrl,
        description = cmd.payload.description,
        metadata = cmd.payload.providerMetadata,
        paymentMethodToken = cmd.payload.paymentMethodToken,
      })
    end
  end
  state.payments[pid] = {
    orderId = cmd.payload.orderId,
    amount = cmd.payload.amount,
    currency = cmd.payload.currency,
    provider = provider,
    status = status,
    risk = (os.getenv("PAYMENT_RISK_REQUIRED") == "1") and "review" or "pass",
    returnUrl = cmd.payload.returnUrl,
    description = cmd.payload.description,
    providerUrl = (provider == "gopay" and (os.getenv("GOPAY_GATEWAY_URL") or "https://gw.gopay.com")) or nil,
    providerPaymentId = providerPaymentId,
    gatewayUrl = gatewayUrl,
    tokenized = cmd.payload.paymentMethodToken ~= nil,
  }
  if cmd.payload.customerId and cmd.payload.paymentMethodToken then
    state.payment_tokens[cmd.payload.customerId] = state.payment_tokens[cmd.payload.customerId] or {}
    state.payment_tokens[cmd.payload.customerId][provider] = cmd.payload.paymentMethodToken
  end
  local ev = {
    type = "PaymentIntentCreated",
    paymentId = pid,
    orderId = cmd.payload.orderId,
    amount = cmd.payload.amount,
    currency = cmd.payload.currency,
    provider = provider,
    risk = state.payments[pid].risk,
    providerUrl = state.payments[pid].providerUrl,
    providerPaymentId = providerPaymentId,
    gatewayUrl = gatewayUrl,
    requestId = cmd.requestId,
  }
  if OUTBOX_HMAC_SECRET then
    local msg = table.concat({ pid, cmd.payload.orderId, tostring(cmd.payload.amount or ""), cmd.payload.currency or "" }, "|")
    ev.hmac = crypto.hmac_sha256_hex(msg, OUTBOX_HMAC_SECRET)
  end
  enqueue_event(ev)
  return ok(cmd.requestId, { paymentId = pid, provider = provider, status = status, providerPaymentId = providerPaymentId, gatewayUrl = gatewayUrl })
end

function handlers.CapturePayment(cmd)
  local payment = state.payments[cmd.payload.paymentId]
  if not payment then
    return err(cmd.requestId, "NOT_FOUND", "payment not found")
  end
  if payment.status ~= "requires_capture" then
    -- allow capture for pending/authorized/pending-provider
    local allowed = { requires_capture = true, pending = true }
    if not allowed[payment.status] then
      return err(cmd.requestId, "INVALID_STATE", "payment not capturable", { status = payment.status })
    end
  end
  if payment.provider == "gopay" and payment.providerPaymentId then
    if gopay_ok then
      local ok, perr = gopay.capture(payment.providerPaymentId)
      if not ok then return err(cmd.requestId, "PROVIDER_ERROR", perr) end
    else
      return err(cmd.requestId, "PROVIDER_ERROR", "gopay module unavailable")
    end
  elseif payment.provider == "stripe" and payment.providerPaymentId then
    if stripe_ok then
      local ok, perr = stripe.capture(payment.providerPaymentId)
      if not ok then return err(cmd.requestId, "PROVIDER_ERROR", perr) end
    end
  elseif payment.provider == "paypal" and payment.providerPaymentId then
    if paypal_ok then
      local ok, perr = paypal.capture(payment.providerPaymentId)
      if not ok then return err(cmd.requestId, "PROVIDER_ERROR", perr) end
    end
  end
  set_payment_status(cmd.payload.paymentId, "captured", "captured", cmd.requestId)
  return ok(cmd.requestId, { paymentId = cmd.payload.paymentId, status = "captured" })
end

function handlers.ConfirmPayment(cmd)
  local payment = state.payments[cmd.payload.paymentId]
  if not payment then return err(cmd.requestId, "NOT_FOUND", "payment not found") end
  if payment.provider == "stripe" and payment.providerPaymentId then
    if stripe_ok then
      local ok, perr, resp = stripe.confirm(payment.providerPaymentId, cmd.payload.returnUrl)
      if not ok then return err(cmd.requestId, "PROVIDER_ERROR", perr) end
      -- If Stripe still requires action, keep status; else mark captured
      local status = (resp and resp.status) or "requires_capture"
      if status == "requires_action" or status == "processing" then
        payment.status = "requires_capture"
      elseif status == "succeeded" then
        payment.status = "captured"
      end
    end
  end
  if payment.provider == "paypal" and payment.providerPaymentId then
    if paypal_ok then
      local ok, perr = paypal.capture(payment.providerPaymentId)
      if not ok then return err(cmd.requestId, "PROVIDER_ERROR", perr) end
      payment.status = "captured"
    end
  end
  set_payment_status(cmd.payload.paymentId, payment.status, payment.status, cmd.requestId)
  return ok(cmd.requestId, { paymentId = cmd.payload.paymentId, status = payment.status })
end

-- PaymentReturn: invoked after 3-DS/SCA or redirect back
function handlers.PaymentReturn(cmd)
  local payment = state.payments[cmd.payload.paymentId]
  if not payment then return err(cmd.requestId, "NOT_FOUND", "payment not found") end
  local status = "pending"
  if cmd.payload.provider == "stripe" then
    status = stripe_ok and stripe.status_from_payload(cmd.payload.payload) or "pending"
    -- fallback paymentId from payload
    if not payment.providerPaymentId and cmd.payload.payload and cmd.payload.payload.payment_intent then
      payment.providerPaymentId = cmd.payload.payload.payment_intent
    end
    if status == "requires_capture" then
      handlers.ConfirmPayment({ payload = { paymentId = cmd.payload.paymentId, provider = "stripe", returnUrl = cmd.payload.redirectUrl }, requestId = cmd.requestId })
      status = payment.status or status
    end
    if payment.providerPaymentId and stripe_ok then
      local live_status = stripe.retrieve_status(payment.providerPaymentId)
      if live_status then
        status = stripe.status_from_payload({ status = live_status })
      end
    end
  elseif cmd.payload.provider == "paypal" then
    status = paypal_ok and paypal.status_from_payload(cmd.payload.payload) or "pending"
    if not payment.providerPaymentId and cmd.payload.payload and cmd.payload.payload.resource and cmd.payload.payload.resource.id then
      payment.providerPaymentId = cmd.payload.payload.resource.id
    end
    if status == "requires_capture" then
      handlers.ConfirmPayment({ payload = { paymentId = cmd.payload.paymentId, provider = "paypal" }, requestId = cmd.requestId })
      status = payment.status or status
    end
  end
  payment.status = status or payment.status
  set_payment_status(cmd.payload.paymentId, payment.status, payment.status, cmd.requestId)
  return ok(cmd.requestId, { paymentId = cmd.payload.paymentId, status = payment.status })
end

-- RefreshPaymentStatus: fetch latest status from provider and sync order/payment states
function handlers.RefreshPaymentStatus(cmd)
  local payment = state.payments[cmd.payload.paymentId]
  if not payment then return err(cmd.requestId, "NOT_FOUND", "payment not found") end
  local provider = cmd.payload.provider or payment.provider
  local new_status = payment.status
  if provider == "stripe" and payment.providerPaymentId and stripe_ok then
    local live = stripe.retrieve_status(payment.providerPaymentId)
    if live then new_status = stripe.status_from_payload({ status = live }) end
  elseif provider == "paypal" and payment.providerPaymentId and paypal_ok then
    local live = paypal.retrieve_status and paypal.retrieve_status(payment.providerPaymentId)
    if live then new_status = live end
  elseif provider == "gopay" and payment.providerPaymentId and gopay_ok then
    local live = gopay.status and gopay.status(payment.providerPaymentId)
    if live then new_status = live.status or live end
  end
  if new_status and new_status ~= payment.status then
    set_payment_status(cmd.payload.paymentId, new_status, "refresh", cmd.requestId)
  end
  return ok(cmd.requestId, { paymentId = cmd.payload.paymentId, status = state.payments[cmd.payload.paymentId].status })
end

function handlers.VoidPayment(cmd)
  local payment = state.payments[cmd.payload.paymentId]
  if not payment then
    return err(cmd.requestId, "NOT_FOUND", "payment not found")
  end
  if payment.provider == "gopay" and payment.providerPaymentId then
    if gopay_ok then
      local ok, perr = gopay.void(payment.providerPaymentId, cmd.payload.reason)
      if not ok then return err(cmd.requestId, "PROVIDER_ERROR", perr) end
    else
      return err(cmd.requestId, "PROVIDER_ERROR", "gopay module unavailable")
    end
  elseif payment.provider == "stripe" and payment.providerPaymentId then
    if stripe_ok then
      local ok, perr = stripe.void(payment.providerPaymentId, cmd.payload.reason)
      if not ok then return err(cmd.requestId, "PROVIDER_ERROR", perr) end
    end
  elseif payment.provider == "paypal" and payment.providerPaymentId then
    if paypal_ok then
      local ok, perr = paypal.void(payment.providerPaymentId, cmd.payload.reason)
      if not ok then return err(cmd.requestId, "PROVIDER_ERROR", perr) end
    end
  end
  payment.status = "voided"
  payment.voidedAt = cmd.timestamp
  local ev = {
    type = "PaymentVoided",
    paymentId = cmd.payload.paymentId,
    orderId = payment.orderId,
    requestId = cmd.requestId,
  }
  enqueue_event(ev)
  return ok(cmd.requestId, { paymentId = cmd.payload.paymentId, status = "voided" })
end

function handlers.UpsertShipmentStatus(cmd)
  state.shipments[cmd.payload.shipmentId] = {
    status = cmd.payload.status,
    tracking = cmd.payload.tracking,
    carrier = cmd.payload.carrier,
    orderId = cmd.payload.orderId,
    eta = cmd.payload.eta,
    updatedAt = cmd.timestamp,
  }
  -- release reservations when shipped/delivered
  if cmd.payload.status == "shipped" or cmd.payload.status == "delivered" then
    local res = state.inventory_reservations[cmd.payload.orderId]
    if res and res.items then
      for _, item in ipairs(res.items) do
        state.inventory[res.siteId] = state.inventory[res.siteId] or {}
        local inv = state.inventory[res.siteId][item.sku] or { quantity = 0 }
        inv.quantity = math.max(0, inv.quantity - (item.qty or 0))
        state.inventory[res.siteId][item.sku] = inv
      end
      res.released = true
    end
  end
  local ev = {
    type = "ShipmentUpdated",
    shipmentId = cmd.payload.shipmentId,
    orderId = cmd.payload.orderId,
    status = cmd.payload.status,
    tracking = cmd.payload.tracking,
    carrier = cmd.payload.carrier,
    requestId = cmd.requestId,
  }
  enqueue_event(ev)
  return ok(cmd.requestId, { shipmentId = cmd.payload.shipmentId, status = cmd.payload.status })
end

function handlers.CreateShippingLabel(cmd)
  state.shipments[cmd.payload.shipmentId] = state.shipments[cmd.payload.shipmentId] or {}
  local label_url
  local base = os.getenv("CARRIER_LABEL_URL")
  if base then
    label_url = string.format("%s/%s.pdf", base, cmd.payload.shipmentId)
  else
    label_url = string.format("https://labels.example/label/%s.pdf", cmd.payload.shipmentId)
  end
  state.shipments[cmd.payload.shipmentId].labelUrl = label_url
  state.shipments[cmd.payload.shipmentId].carrier = cmd.payload.carrier
  state.shipments[cmd.payload.shipmentId].service = cmd.payload.service
  state.shipments[cmd.payload.shipmentId].orderId = cmd.payload.orderId
  enqueue_event({
    type = "ShippingLabelCreated",
    shipmentId = cmd.payload.shipmentId,
    carrier = cmd.payload.carrier,
    service = cmd.payload.service,
    labelUrl = label_url,
    orderId = cmd.payload.orderId,
  })
  return ok(cmd.requestId, { shipmentId = cmd.payload.shipmentId, labelUrl = label_url })
end

function handlers.UpdateShipmentTracking(cmd)
  state.shipments[cmd.payload.shipmentId] = state.shipments[cmd.payload.shipmentId] or {}
  state.shipments[cmd.payload.shipmentId].tracking = cmd.payload.tracking
  state.shipments[cmd.payload.shipmentId].carrier = cmd.payload.carrier or state.shipments[cmd.payload.shipmentId].carrier
  state.shipments[cmd.payload.shipmentId].eta = cmd.payload.eta or state.shipments[cmd.payload.shipmentId].eta
  if os.getenv("CARRIER_TRACK_URL") and cmd.payload.tracking then
    state.shipments[cmd.payload.shipmentId].trackingUrl = string.format("%s/%s", os.getenv("CARRIER_TRACK_URL"), cmd.payload.tracking)
  end
  enqueue_event({
    type = "ShipmentTrackingUpdated",
    shipmentId = cmd.payload.shipmentId,
    tracking = cmd.payload.tracking,
    carrier = state.shipments[cmd.payload.shipmentId].carrier,
    eta = state.shipments[cmd.payload.shipmentId].eta,
    trackingUrl = state.shipments[cmd.payload.shipmentId].trackingUrl,
    orderId = cmd.payload.orderId,
  })
  return ok(cmd.requestId, { shipmentId = cmd.payload.shipmentId, tracking = cmd.payload.tracking })
end

function handlers.UpsertReturnStatus(cmd)
  state.returns[cmd.payload.returnId] = {
    status = cmd.payload.status,
    reason = cmd.payload.reason,
    orderId = cmd.payload.orderId,
    updatedAt = cmd.timestamp,
  }
  -- restock on approved/refunded returns
  if cmd.payload.status == "approved" or cmd.payload.status == "refunded" then
    local res = state.inventory_reservations[cmd.payload.orderId]
    if res and res.items then
      for _, item in ipairs(res.items) do
        state.inventory[res.siteId] = state.inventory[res.siteId] or {}
        local inv = state.inventory[res.siteId][item.sku] or { quantity = 0 }
        inv.quantity = inv.quantity + (item.qty or 0)
        state.inventory[res.siteId][item.sku] = inv
      end
    end
  end
  local ev = {
    type = "ReturnUpdated",
    returnId = cmd.payload.returnId,
    orderId = cmd.payload.orderId,
    status = cmd.payload.status,
    reason = cmd.payload.reason,
    requestId = cmd.requestId,
  }
  enqueue_event(ev)
  return ok(cmd.requestId, { returnId = cmd.payload.returnId, status = cmd.payload.status })
end

function handlers.ProviderWebhook(cmd)
  if cmd.payload.provider == "gopay" then
    -- optional signature/basic verification if configured
    local secret = os.getenv("GOPAY_WEBHOOK_SECRET")
    if secret and cmd.payload.raw and cmd.payload.raw.body then
      local sig = cmd.payload.raw.headers and (cmd.payload.raw.headers["X-GoPay-Signature"] or cmd.payload.raw.headers["GoPay-Signature"])
      if not sig then return err(cmd.requestId, "UNAUTHORIZED", "missing_signature") end
      local ok_sig = gopay_ok and gopay.verify_signature and gopay.verify_signature(cmd.payload.raw.body, sig, secret)
      if not ok_sig then return err(cmd.requestId, "UNAUTHORIZED", "signature_invalid") end
    end
    if os.getenv("GOPAY_WEBHOOK_BASIC") == "1" and cmd.payload.raw and cmd.payload.raw.headers then
      local auth = cmd.payload.raw.headers["Authorization"]
      local decoded = gopay_ok and gopay.verify_basic and gopay.verify_basic(auth)
      if not decoded then return err(cmd.requestId, "UNAUTHORIZED", "basic_invalid") end
      local expected = (os.getenv("GOPAY_CLIENT_ID") or "") .. ":" .. (os.getenv("GOPAY_CLIENT_SECRET") or "")
      if decoded ~= expected then return err(cmd.requestId, "UNAUTHORIZED", "basic_mismatch") end
    end

    -- risk signal
    if cmd.payload.raw and cmd.payload.raw.risk then
      local thresh = tonumber(os.getenv("GOPAY_RISK_THRESHOLD") or "70")
      if tonumber(cmd.payload.raw.risk) and tonumber(cmd.payload.raw.risk) >= thresh then
        cmd.payload.status = "RISK"
      end
    end
    for pid, p in pairs(state.payments) do
      if p.providerPaymentId == cmd.payload.paymentId or pid == cmd.payload.paymentId then
  local status_map = {
    PAID = "captured",
    CHARGED = "captured",
    AUTHORIZED = "requires_capture",
    CREATED = "pending",
    CANCELED = "voided",
    REFUNDED = "refunded",
    PARTIALLY_REFUNDED = "refunded",
    RISK = "risk_review",
    DISPUTED = "disputed",
  }
        p.status = status_map[cmd.payload.status] or string.lower(cmd.payload.status)
        p.updatedAt = cmd.timestamp
        if p.orderId then
          state.orders[p.orderId] = state.orders[p.orderId] or {}
          local order_status_map = {
            captured = "paid",
            requires_capture = "pending",
            voided = "cancelled",
            refunded = "refunded",
            pending = "pending",
            risk_review = "risk_review",
          }
          state.orders[p.orderId].status = order_status_map[p.status] or state.orders[p.orderId].status
        end
        local ev = {
          type = "PaymentStatusChanged",
          paymentId = pid,
          providerStatus = cmd.payload.status,
          status = p.status,
          requestId = cmd.requestId,
        }
        enqueue_event(ev)
        if p.orderId and state.orders[p.orderId] and state.orders[p.orderId].status then
          local oev = {
            type = "OrderStatusUpdated",
            orderId = p.orderId,
            status = state.orders[p.orderId].status,
            requestId = cmd.requestId,
          }
          enqueue_event(oev)
        end
        return ok(cmd.requestId, { paymentId = pid, status = p.status })
      end
    end
  end
  if cmd.payload.provider == "stripe" then
    local secret = os.getenv("STRIPE_WEBHOOK_SECRET")
    if secret and cmd.payload.raw and cmd.payload.raw.body then
      local sig = cmd.payload.raw.headers and cmd.payload.raw.headers["Stripe-Signature"]
      local ok_sig = stripe_ok and stripe.verify_webhook(cmd.payload.raw.body, sig, secret, tonumber(os.getenv("STRIPE_WEBHOOK_TOLERANCE") or "300"))
      if not ok_sig then return err(cmd.requestId, "UNAUTHORIZED", "signature_invalid") end
    end
    local status_map = {
      ["payment_intent.succeeded"] = "captured",
      ["payment_intent.payment_failed"] = "failed",
      ["payment_intent.canceled"] = "voided",
      ["charge.refunded"] = "refunded",
      ["charge.refund.updated"] = "refunded",
      ["payment_intent.processing"] = "pending",
      ["payment_intent.requires_action"] = "requires_capture",
      ["charge.dispute.created"] = "disputed",
      ["charge.dispute.closed"] = "captured",
      ["charge.dispute.funds_withdrawn"] = "disputed",
      ["charge.dispute.funds_reinstated"] = "captured",
      ["charge.dispute.accepted"] = "disputed",
      ["charge.dispute.expired"] = "disputed",
      ["charge.dispute.escalated"] = "disputed",
    }
    local new_status = status_map[cmd.payload.eventType] or "pending"
    for pid, p in pairs(state.payments) do
      if p.providerPaymentId == cmd.payload.paymentId or pid == cmd.payload.paymentId then
        if cmd.payload.eventType:match("dispute") then
          state.payment_disputes[pid] = state.payment_disputes[pid] or {}
          state.payment_disputes[pid].status = new_status
          state.payment_disputes[pid].reason = cmd.payload.reason
          state.payment_disputes[pid].evidence = cmd.payload.evidence or state.payment_disputes[pid].evidence
        end
        set_payment_status(pid, new_status, cmd.payload.eventType, cmd.requestId)
        return ok(cmd.requestId, { paymentId = pid, status = new_status })
      end
    end
    return err(cmd.requestId, "NOT_FOUND", "payment not tracked")
  end
  if cmd.payload.provider == "paypal" then
    local secret = os.getenv("PAYPAL_WEBHOOK_SECRET")
    local strict = os.getenv("PAYPAL_WEBHOOK_STRICT") == "1"
    if (secret or strict) and cmd.payload.raw and cmd.payload.raw.body then
      local headers = cmd.payload.raw.headers or {}
      local sig = headers["PayPal-Transmission-Sig"] or headers["PP-Signature"]
      if strict and not sig then return err(cmd.requestId, "UNAUTHORIZED", "missing_signature") end
      local ok_sig = false
      if paypal_ok then
        if sig and secret then
          ok_sig = paypal.verify_webhook(cmd.payload.raw.body, sig, secret)
        end
        if not ok_sig then
          local remote_ok = select(1, paypal.verify_webhook_remote(cmd.payload.raw.body, headers))
          ok_sig = remote_ok or ok_sig
        end
      end
      if strict and not ok_sig then return err(cmd.requestId, "UNAUTHORIZED", "signature_invalid") end
    end
    local status_map = {
      ["PAYMENT.CAPTURE.COMPLETED"] = "captured",
      ["PAYMENT.CAPTURE.DENIED"] = "failed",
      ["PAYMENT.CAPTURE.REFUNDED"] = "refunded",
      ["PAYMENT.CAPTURE.REVERSED"] = "voided",
      ["CHECKOUT.ORDER.APPROVED"] = "requires_capture",
      ["PAYMENT.CAPTURE.PENDING"] = "pending",
      ["CUSTOMER.DISPUTE.CREATED"] = "disputed",
      ["CUSTOMER.DISPUTE.UPDATED"] = "disputed",
      ["CUSTOMER.DISPUTE.RESOLVED"] = "captured",
      ["CUSTOMER.DISPUTE.EXPIRED"] = "disputed",
      ["CUSTOMER.DISPUTE.ESCALATED"] = "disputed",
    }
    local new_status = status_map[cmd.payload.eventType] or "pending"
    for pid, p in pairs(state.payments) do
      if p.providerPaymentId == cmd.payload.paymentId or pid == cmd.payload.paymentId then
        if cmd.payload.eventType:match("DISPUTE") then
          state.payment_disputes[pid] = state.payment_disputes[pid] or {}
          state.payment_disputes[pid].status = new_status
          state.payment_disputes[pid].reason = cmd.payload.reason
          state.payment_disputes[pid].evidence = cmd.payload.evidence or state.payment_disputes[pid].evidence
        end
        set_payment_status(pid, new_status, cmd.payload.eventType, cmd.requestId)
        return ok(cmd.requestId, { paymentId = pid, status = new_status })
      end
    end
    return err(cmd.requestId, "NOT_FOUND", "payment not tracked")
  end
  return err(cmd.requestId, "NOT_FOUND", "payment not tracked")
end

function handlers.ProviderShippingWebhook(cmd)
  local status = string.lower(cmd.payload.status or "")
  state.shipments[cmd.payload.shipmentId] = state.shipments[cmd.payload.shipmentId] or {}
  local sh = state.shipments[cmd.payload.shipmentId]
  sh.orderId = cmd.payload.orderId or sh.orderId
  sh.status = status ~= "" and status or (sh.status or "pending")
  sh.tracking = cmd.payload.tracking or sh.tracking
  sh.carrier = cmd.payload.carrier or sh.carrier
  sh.labelUrl = cmd.payload.labelUrl or sh.labelUrl
  sh.eta = cmd.payload.eta or sh.eta
  enqueue_event({
    type = "ShipmentUpdated",
    shipmentId = cmd.payload.shipmentId,
    orderId = sh.orderId,
    status = sh.status,
    tracking = sh.tracking,
    carrier = sh.carrier,
    labelUrl = sh.labelUrl,
    requestId = cmd.requestId,
  })
  if sh.orderId then
    enqueue_event({
      type = "OrderStatusUpdated",
      orderId = sh.orderId,
      status = sh.status,
      requestId = cmd.requestId,
    })
  end
  return ok(cmd.requestId, { shipmentId = cmd.payload.shipmentId, status = sh.status })
end

function handlers.CreateWebhook(cmd)
  local tenant = cmd.payload.tenant
  state.webhooks[tenant] = state.webhooks[tenant] or {}
  table.insert(state.webhooks[tenant], { url = cmd.payload.url, events = cmd.payload.events })
  return ok(cmd.requestId, { url = cmd.payload.url })
end

-- route(command) validates and dispatches.
function M.route(command)
  -- idempotency first: if we have it, return stored response.
  local stored = idem.lookup(command.requestId or command["Request-Id"])
  if stored then return stored end

  local ok_jwt, jwt_err = auth.consume_jwt(command)
  if not ok_jwt then
    return err(command.requestId, "UNAUTHORIZED", jwt_err or "jwt_failed")
  end

  local ok_env, env_errs = validation.validate_envelope(command)
  if not ok_env then
    return err(command.requestId, "INVALID_INPUT", "Envelope validation failed", env_errs)
  end

  local ok_nonce, nonce_err = auth.require_nonce(command)
  if not ok_nonce then
    return err(command.requestId, "UNAUTHORIZED", nonce_err or "nonce failed")
  end

  local ok_sig, sig_err = auth.verify_signature(command)
  if not ok_sig then
    return err(command.requestId, "UNAUTHORIZED", sig_err or "signature failed")
  end
  if command.signature and (command.action or command.Action) then
    local message = (command.action or command.Action) .. "|" .. (command.tenant or "") .. "|" .. (command.requestId or command["Request-Id"] or "")
    local ok_det, det_err = auth.verify_detached(message, command.signature)
    if not ok_det then
      return err(command.requestId, "UNAUTHORIZED", det_err or "detached signature failed")
    end
  end

  local ok_policy, pol_err = auth.check_policy(command, nil)
  if not ok_policy then
    return err(command.requestId, "FORBIDDEN", pol_err or "policy denied")
  end
  local ok_role, role_err = auth.check_role_for_action(command)
  if not ok_role then
    return err(command.requestId, "FORBIDDEN", role_err or "role denied")
  end
  local ok_rl, rl_err = auth.check_rate_limit(command)
  if not ok_rl then
    return err(command.requestId, "RATE_LIMITED", rl_err)
  end

  local ok_act, act_errs = validation.validate_action(command.action, command.payload)
  if not ok_act then
    return err(command.requestId, "INVALID_INPUT", "Action payload invalid", act_errs)
  end

  local handler = handlers[command.action]
  if not handler then
    return err(command.requestId, "UNKNOWN_ACTION", "Handler not found")
  end

  local response = handler(command)
  idem.record(command.requestId, response)
  audit.append({ action = command.action, requestId = command.requestId, status = response.status, actor = command.actor, tenant = command.tenant })
  if WAL_PATH then
    local ok, cjson = pcall(require, "cjson")
    if ok then
      local req_json = cjson.encode(command)
      local resp_json = cjson.encode(response)
      local f = io.open(WAL_PATH, "a")
      if f then
        f:write(cjson.encode({
          ts = os.date("!%Y-%m-%dT%H:%M:%SZ"),
          req = command.requestId,
          action = command.action,
          status = response.status,
          reqHash = sha256_str(req_json),
          respHash = sha256_str(resp_json),
        }))
        f:write("\n")
        f:close()
      end
    end
  end
  return response
end

function M._state()
  return state
end

function M._outbox()
  return outbox
end

function M._storage_outbox()
  return storage.all("outbox")
end

return M
