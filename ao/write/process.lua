-- Entry point for the write command AO process.

local validation = require("ao.shared.validation")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")
local audit = require("ao.shared.audit")
local storage = require("ao.shared.storage")
local bridge = require("ao.shared.bridge")
local crypto = require("ao.shared.crypto")
local gopay_ok, gopay = pcall(require, "ao.shared.gopay")
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
  orders = {},        -- orderId -> status, reason
  coupons = {},       -- code -> { type, value, currency, minOrder, expiresAt }
  webhooks = {},      -- tenant -> list of endpoints
  payments = {},      -- paymentId -> {orderId, amount, currency, provider, status}
  shipments = {},     -- shipmentId -> {status, tracking, carrier}
  returns = {},       -- returnId -> {status, reason}
  dlq = {},           -- dead-letter for outbox
  inventory_reservations = {}, -- orderId -> { siteId=..., items = { {sku, qty} } }
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
  state.orders[cmd.payload.orderId] = {
    status = cmd.payload.status,
    reason = cmd.payload.reason,
    totalAmount = cmd.payload.totalAmount,
    currency = cmd.payload.currency,
    vatRate = cmd.payload.vatRate,
    updatedAt = cmd.timestamp,
  }
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
  if c.expiresAt and c.expiresAt < os.time() then return false, "expired" end
  if c.currency and order.currency and c.currency ~= order.currency then return false, "currency_mismatch" end
  if c.minOrder and order.totalAmount and order.totalAmount < c.minOrder then return false, "min_order_not_met" end
  return true
end

function handlers.ApplyCoupon(cmd)
  local order = state.orders[cmd.payload.orderId]
  if not order or not order.totalAmount then
    return err(cmd.requestId, "NOT_FOUND", "order not found or no total")
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
  order.coupon = cmd.payload.code
  local ev = { type = "CouponApplied", orderId = cmd.payload.orderId, code = cmd.payload.code, discount = discount, requestId = cmd.requestId }
  enqueue_event(ev)
  return ok(cmd.requestId, { orderId = cmd.payload.orderId, totalAmount = order.totalAmount, code = cmd.payload.code })
end

function handlers.RemoveCoupon(cmd)
  local order = state.orders[cmd.payload.orderId]
  if not order then return err(cmd.requestId, "NOT_FOUND", "order not found") end
  order.coupon = nil
  local ev = { type = "CouponRemoved", orderId = cmd.payload.orderId, requestId = cmd.requestId }
  enqueue_event(ev)
  return ok(cmd.requestId, { orderId = cmd.payload.orderId })
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
