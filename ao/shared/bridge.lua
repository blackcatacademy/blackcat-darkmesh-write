-- Bridge utilities to send events to AO (HTTP mock).

local Bridge = {}

local cjson_ok, cjson = pcall(require, "cjson")
local endpoint = os.getenv("AO_ENDPOINT")
local api_key = os.getenv("AO_API_KEY")
local dry_run = os.getenv("DRY_RUN") == "1" or (os.getenv("AO_BRIDGE_MODE") == "mock")
local retries = tonumber(os.getenv("AO_BRIDGE_RETRIES") or "3")
local backoff_ms = tonumber(os.getenv("AO_BRIDGE_BACKOFF_MS") or "200")

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

local function sleep(ms)
  os.execute(string.format("sleep %.3f", ms / 1000))
end

function Bridge.forward_event(ev)
  if not cjson_ok then return false, "cjson_missing" end
  local body = cjson.encode(ev)
  for attempt = 1, retries do
    local ok, status = post(body)
    if ok then return true end
    if attempt < retries then
      local jitter = math.random() * 0.5 + 0.75
      sleep(backoff_ms * jitter)
    else
      return false, status
    end
  end
end

Bridge._post = post

return Bridge
