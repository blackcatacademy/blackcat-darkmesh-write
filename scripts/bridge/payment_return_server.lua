#!/usr/bin/env lua
-- Minimal HTTP server to accept PaymentReturn callbacks and forward to write AO.
-- Requires luasocket. Intended for local/dev; not production-grade.
-- Usage: lua scripts/bridge/payment_return_server.lua [port]

local socket_ok, http = pcall(require, "socket.http")
local ltn12 = require("ltn12")
local json_ok, cjson = pcall(require, "cjson.safe")
if not socket_ok then
  io.stderr:write("luasocket not installed; payment_return_server skipped\n")
  os.exit(1)
end

local port = tonumber(arg[1] or "8088")
local write = require("ao.write.process")
local socket = require("socket")

local function read_request(client)
  client:settimeout(2)
  local line = client:receive("*l")
  if not line then return nil end
  local method, path = line:match("^(%u+)%s+([^%s]+)")
  local headers = {}
  while true do
    local l = client:receive("*l")
    if not l or l == "" then break end
    local k, v = l:match("^([^:]+):%s*(.*)")
    if k and v then headers[k:lower()] = v end
  end
  local length = tonumber(headers["content-length"] or "0") or 0
  local body = ""
  if length > 0 then
    body = client:receive(length) or ""
  end
  return method, path, headers, body
end

local function send_response(client, code, body)
  body = body or ""
  local resp = table.concat({
    string.format("HTTP/1.1 %d OK", code),
    "Content-Type: application/json",
    "Connection: close",
    string.format("Content-Length: %d", #body),
    "",
    body,
  }, "\r\n")
  client:send(resp)
end

local function handle(client)
  local method, path, headers, body = read_request(client)
  if not method then return end
  if method ~= "POST" or path ~= "/payment-return" then
    send_response(client, 404, '{"error":"not found"}')
    return
  end
  if not json_ok then
    send_response(client, 500, '{"error":"cjson not available"}')
    return
  end
  local payload, err = cjson.decode(body or "")
  if not payload then
    send_response(client, 400, string.format('{"error":"bad json","detail":"%s"}', err or "decode"))
    return
  end
  local resp = write.route({
    action = "PaymentReturn",
    payload = payload,
    requestId = payload.requestId or ("return-" .. tostring(os.time())),
    nonce = payload.nonce or ("nonce-" .. tostring(os.time())),
    signatureRef = payload.signatureRef or ("sigref-" .. tostring(os.time())),
    actor = payload.actor or "resolver",
    tenant = payload.tenant or "tenant-1",
    role = payload.role or "system",
  })
  send_response(client, 200, cjson.encode(resp))
end

local server = assert(socket.bind("*", port))
print(string.format("PaymentReturn server listening on :%d", port))
while true do
  local client = server:accept()
  if client then
    pcall(handle, client)
    client:close()
  end
end
