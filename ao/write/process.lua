-- Entry point for the write command AO process.

local validation = require("ao.shared.validation")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")
local audit = require("ao.shared.audit")
local storage = require("ao.shared.storage")
local bridge = require("ao.shared.bridge")
local crypto = require("ao.shared.crypto")
local gopay_ok, gopay = pcall(require, "ao.shared.gopay")
local stripe_ok, stripe = pcall(require, "ao.shared.stripe")
local paypal_ok, paypal = pcall(require, "ao.shared.paypal")
local tax = require("ao.shared.tax")

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
}
local outbox = {}      -- emitted events for downstream (-ao bridge)

local function ok(req_id, payload)
  return { status = "OK", requestId = req_id, payload = payload or {} }
end

local function err(req_id, code, msg, details)
  return { status = "ERROR", code = code, message = msg, requestId = req_id, details = details }
end

local handlers = {}

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

-- Cart & Order creation

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
    })
  end
  state.carts[cmd.payload.cartId] = cart
  return ok(cmd.requestId, { cartId = cmd.payload.cartId, items = #cart.items })
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
  return ok(cmd.requestId, { cartId = cmd.payload.cartId, items = #cart.items })
end

local function compute_totals(cart, coupon_code, vatRate, shipping, address)
  local subtotal = 0
  local total_weight = 0
  for _, it in ipairs(cart.items or {}) do
    subtotal = subtotal + (it.price or 0) * (it.qty or 1)
    total_weight = total_weight + (it.weight or 0) * (it.qty or 1)
  end
  local discount = 0
  if coupon_code then
    local dummy_order = { totalAmount = subtotal, currency = cart.currency }
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
  local orderId = "ord_" .. (cmd.payload.cartId or tostring(os.time()))
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
      providerPaymentId, gatewayUrl, status = stripe.create_payment({
        orderId = cmd.payload.orderId,
        amount = cmd.payload.amount,
        currency = cmd.payload.currency,
        returnUrl = cmd.payload.returnUrl,
        description = cmd.payload.description,
        metadata = cmd.payload.providerMetadata,
      })
    end
  elseif provider == "paypal" then
    if paypal_ok then
      providerPaymentId, gatewayUrl, status = paypal.create_payment({
        orderId = cmd.payload.orderId,
        amount = cmd.payload.amount,
        currency = cmd.payload.currency,
        returnUrl = cmd.payload.returnUrl,
        description = cmd.payload.description,
        metadata = cmd.payload.providerMetadata,
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
  }
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
  payment.status = "captured"
  payment.capturedAt = cmd.timestamp
  local ev = {
    type = "PaymentCaptured",
    paymentId = cmd.payload.paymentId,
    orderId = payment.orderId,
    amount = payment.amount,
    currency = payment.currency,
    requestId = cmd.requestId,
  }
  if OUTBOX_HMAC_SECRET then
    local msg = table.concat({ cmd.payload.paymentId, payment.orderId, tostring(payment.amount or ""), payment.currency or "" }, "|")
    ev.hmac = crypto.hmac_sha256_hex(msg, OUTBOX_HMAC_SECRET)
  end
  enqueue_event(ev)
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
  enqueue_event({
    type = "PaymentStatusChanged",
    paymentId = cmd.payload.paymentId,
    status = payment.status,
    requestId = cmd.requestId,
  })
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
  enqueue_event({
    type = "PaymentStatusChanged",
    paymentId = cmd.payload.paymentId,
    status = payment.status,
    requestId = cmd.requestId,
  })
  return ok(cmd.requestId, { paymentId = cmd.payload.paymentId, status = payment.status })
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
    }
    local new_status = status_map[cmd.payload.eventType] or "pending"
    for pid, p in pairs(state.payments) do
      if p.providerPaymentId == cmd.payload.paymentId or pid == cmd.payload.paymentId then
        p.status = new_status
        p.updatedAt = cmd.timestamp
        local ev = {
          type = "PaymentStatusChanged",
          paymentId = pid,
          providerStatus = cmd.payload.eventType,
          status = p.status,
          requestId = cmd.requestId,
        }
        enqueue_event(ev)
        if p.orderId and state.orders[p.orderId] then
          state.orders[p.orderId].status = (new_status == "captured") and "paid" or state.orders[p.orderId].status
          enqueue_event({
            type = "OrderStatusUpdated",
            orderId = p.orderId,
            status = state.orders[p.orderId].status,
            requestId = cmd.requestId,
          })
        end
        return ok(cmd.requestId, { paymentId = pid, status = p.status })
      end
    end
    return err(cmd.requestId, "NOT_FOUND", "payment not tracked")
  end
  if cmd.payload.provider == "paypal" then
    local secret = os.getenv("PAYPAL_WEBHOOK_SECRET")
    local strict = os.getenv("PAYPAL_WEBHOOK_STRICT") == "1"
    if (secret or strict) and cmd.payload.raw and cmd.payload.raw.body then
      local sig = cmd.payload.raw.headers and (cmd.payload.raw.headers["PayPal-Transmission-Sig"] or cmd.payload.raw.headers["PP-Signature"])
      if strict and not sig then return err(cmd.requestId, "UNAUTHORIZED", "missing_signature") end
      if sig then
        local ok_sig = paypal_ok and paypal.verify_webhook(cmd.payload.raw.body, sig, secret)
        if strict and not ok_sig then return err(cmd.requestId, "UNAUTHORIZED", "signature_invalid") end
      end
    end
    local status_map = {
      ["PAYMENT.CAPTURE.COMPLETED"] = "captured",
      ["PAYMENT.CAPTURE.DENIED"] = "failed",
      ["PAYMENT.CAPTURE.REFUNDED"] = "refunded",
      ["PAYMENT.CAPTURE.REVERSED"] = "voided",
      ["CHECKOUT.ORDER.APPROVED"] = "requires_capture",
    }
    local new_status = status_map[cmd.payload.eventType] or "pending"
    for pid, p in pairs(state.payments) do
      if p.providerPaymentId == cmd.payload.paymentId or pid == cmd.payload.paymentId then
        p.status = new_status
        p.updatedAt = cmd.timestamp
        enqueue_event({
          type = "PaymentStatusChanged",
          paymentId = pid,
          providerStatus = cmd.payload.eventType,
          status = p.status,
          requestId = cmd.requestId,
        })
        return ok(cmd.requestId, { paymentId = pid, status = p.status })
      end
    end
    return err(cmd.requestId, "NOT_FOUND", "payment not tracked")
  end
  return err(cmd.requestId, "NOT_FOUND", "payment not tracked")
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
