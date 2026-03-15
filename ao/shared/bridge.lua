-- Bridge utilities to send events to AO (HTTP mock).

local Bridge = {}

local cjson_ok, cjson = pcall(require, "cjson")
local endpoint = os.getenv("AO_ENDPOINT")
local api_key = os.getenv("AO_API_KEY")
local dry_run = os.getenv("DRY_RUN") == "1"

local function shell_escape(s)
  return string.format("'%s'", s:gsub("'", "'\"'\"'"))
end

local function post(body)
  if dry_run or not endpoint then
    return true, 200
  end
  local headers = "-H \"Content-Type: application/json\""
  if api_key and api_key ~= "" then
    headers = headers .. " -H \"Authorization: Bearer " .. api_key .. "\""
  end
  local cmd = string.format("printf %%s %s | curl -s -o /tmp/ao-bridge.log -w \"%%{http_code}\" %s -X POST %s --data-binary @-",
    shell_escape(body),
    headers,
    shell_escape(endpoint)
  )
  local p = io.popen(cmd, "r")
  if not p then return false, "curl_failed" end
  local status = p:read("*a")
  p:close()
  status = tonumber(status)
  return status and status < 300, status
end

function Bridge.forward_event(ev)
  if not cjson_ok then return false, "cjson_missing" end
  local body = cjson.encode(ev)
  return post(body)
end

return Bridge
