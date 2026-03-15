#!/usr/bin/env lua
-- Minimal helper to forward PaymentReturn callbacks to the write AO process.
-- Usage: cat payload.json | lua scripts/bridge/payment_return_handler.lua

local cjson = require("cjson.safe")
local write = require("ao.write.process")

local body = io.read("*a")
local payload, err = cjson.decode(body or "")
if not payload then
  io.stderr:write("failed to decode JSON: " .. tostring(err) .. "\n")
  os.exit(1)
end

local cmd = {
  action = "PaymentReturn",
  payload = payload,
  requestId = payload.requestId or ("return-" .. tostring(os.time())),
  nonce = payload.nonce or ("nonce-" .. tostring(os.time())),
  signatureRef = payload.signatureRef or ("sigref-" .. tostring(os.time())),
  actor = payload.actor or "resolver",
  tenant = payload.tenant or "tenant-1",
  role = payload.role or "system",
}

local resp = write.route(cmd)
io.stdout:write(cjson.encode(resp) .. "\n")
