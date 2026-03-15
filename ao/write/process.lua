-- Entry point for the write command AO process.

local validation = require("ao.shared.validation")
local auth = require("ao.shared.auth")
local idem = require("ao.shared.idempotency")
local audit = require("ao.shared.audit")
local storage = require("ao.shared.storage")

local M = {}

-- simple in-memory state; AO runtime would persist
local state = {
  drafts = {},        -- key: siteId:pageId -> payload
  versions = {},      -- siteId -> versionId
  routes = {},        -- siteId -> map[path] = target
  products = {},      -- siteId -> map[sku] = payload
  roles = {},         -- tenant -> subject -> role
  profiles = {},      -- subject -> profile
  entitlements = {},  -- subject -> list of {asset, policy}
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
  table.insert(outbox, {
    type = "PublishPageVersion",
    siteId = siteId,
    pageId = cmd.payload.pageId,
    versionId = cmd.payload.versionId,
    manifestTx = cmd.payload.manifestTx,
    requestId = cmd.requestId,
  })
  storage.append("outbox", outbox[#outbox])
  return ok(cmd.requestId, { version = cmd.payload.versionId, manifestTx = cmd.payload.manifestTx })
end

function handlers.UpsertRoute(cmd)
  local siteId = cmd.payload.siteId
  state.routes[siteId] = state.routes[siteId] or {}
  state.routes[siteId][cmd.payload.path] = cmd.payload.target
  return ok(cmd.requestId, { path = cmd.payload.path })
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
