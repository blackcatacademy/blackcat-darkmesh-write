-- Bridge utilities to send events to AO (HTTP mock).

local Bridge = {}

local cjson_ok, cjson = pcall(require, "cjson")
local endpoint = os.getenv("AO_ENDPOINT")
local api_key = os.getenv("AO_API_KEY")
local bridge_mode = os.getenv("AO_BRIDGE_MODE") or (os.getenv("DRY_RUN") == "1" and "mock") or "http"
local dry_run = bridge_mode == "mock"
local retries = tonumber(os.getenv("AO_BRIDGE_RETRIES") or "3")
local backoff_ms = tonumber(os.getenv("AO_BRIDGE_BACKOFF_MS") or "200")

local function shell_escape(s)
  return string.format("'%s'", s:gsub("'", "'\"'\"'"))
end

local function read_file(path)
  local f = io.open(path, "r")
  if not f then return nil end
  local content = f:read("*a")
  f:close()
  return content
end

local function sha256_file(path)
  local pipe = io.popen("sha256sum " .. path .. " 2>/dev/null")
  if not pipe then return nil end
  local out = pipe:read("*a") or ""
  pipe:close()
  return out:match("^(%w+)")
end

local function sha256_str(str)
  local tmp = os.tmpname()
  local f = io.open(tmp, "w"); if not f then return nil end
  f:write(str); f:close()
  local h = sha256_file(tmp)
  os.remove(tmp)
  return h
end

local function post(body)
  if dry_run or not endpoint then
    return true, 200, body
  end
  local headers = "-H \"Content-Type: application/json\""
  if api_key and api_key ~= "" then
    headers = headers .. " -H \"Authorization: Bearer " .. api_key .. "\""
  end
  local tmp = os.tmpname()
  local cmd = string.format("printf %%s %s | curl -s -o %s -w \"%%{http_code}\" %s -X POST %s --data-binary @-",
    shell_escape(body),
    tmp,
    headers,
    shell_escape(endpoint)
  )
  local p = io.popen(cmd, "r")
  if not p then return false, "curl_failed" end
  local status = p:read("*a")
  p:close()
  status = tonumber(status)
  local resp_body = read_file(tmp) or ""
  os.remove(tmp)
  return status and status < 300, status, resp_body
end

local function sleep(ms)
  os.execute(string.format("sleep %.3f", ms / 1000))
end

function Bridge.forward_event(ev)
  if bridge_mode == "off" then return true end
  if not cjson_ok then return false, "cjson_missing" end
  local body = cjson.encode(ev)
  local expected_hash = ev.expectedResponseHash or os.getenv("AO_EXPECT_RESPONSE_HASH")
  for attempt = 1, retries do
    local ok, status, resp_body = post(body)
    if ok then
      if expected_hash then
        if resp_body and #resp_body > 0 then
          local hash = sha256_str(resp_body)
          if hash and hash:lower() ~= expected_hash:lower() then
            ok = false
            status = "response_hash_mismatch"
          end
        end
      end
      if ok then return true, status end
    end
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
