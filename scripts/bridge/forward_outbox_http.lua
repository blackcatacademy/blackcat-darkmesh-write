#!/usr/bin/env lua
-- Forward outbox events to -ao HTTP endpoint (mock-safe).
-- Env:
--   AO_ENDPOINT=https://... (required)
--   AO_API_KEY=... (optional)
--   AO_SITE_ID=... (optional tag)
--   DRY_RUN=1 to log only

local write = require("ao.write.process")
local storage = require("ao.shared.storage")

local endpoint = os.getenv("AO_ENDPOINT")
if not endpoint or endpoint == "" then
  io.stderr:write("AO_ENDPOINT is required\n")
  os.exit(1)
end
local api_key = os.getenv("AO_API_KEY")
local site_id = os.getenv("AO_SITE_ID")
local dry_run = os.getenv("DRY_RUN") == "1"

local function shell_escape(s)
  return string.format("'%s'", s:gsub("'", "'\"'\"'"))
end

local function http_post(json_body)
  local headers = "-H \"Content-Type: application/json\""
  if api_key and api_key ~= "" then
    headers = headers .. " -H \"Authorization: Bearer " .. api_key .. "\""
  end
  local cmd = string.format("printf %%s %s | curl -s -o /tmp/ao-forward.log -w \"%%{http_code}\" %s -X POST %s --data-binary @-",
    shell_escape(json_body),
    headers,
    shell_escape(endpoint)
  )
  local p = io.popen(cmd, "r")
  if not p then return nil, "curl_failed" end
  local status = p:read("*a")
  p:close()
  return tonumber(status), nil
end

-- refresh outbox mirror
write._outbox()
local events = storage.all("outbox")

local ok, cjson = pcall(require, "cjson")
if not ok then
  io.stderr:write("cjson required for forward_outbox_http\n")
  os.exit(1)
end

local sent, failed = 0, 0
for _, ev in ipairs(events) do
  if site_id and not ev.siteId then ev.siteId = site_id end
  local body = cjson.encode(ev)
  if dry_run then
    print("[dry-run] would POST: " .. body)
    sent = sent + 1
  else
    local status, err = http_post(body)
    if status and status < 300 then
      sent = sent + 1
    else
      failed = failed + 1
      io.stderr:write(string.format("failed (%s): %s\n", tostring(status or err), body))
    end
  end
end

print(string.format("[forward-http] sent=%d failed=%d", sent, failed))
