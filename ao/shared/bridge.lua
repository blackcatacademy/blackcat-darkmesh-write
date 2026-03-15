-- Bridge utilities to send events to AO (HTTP mock).

local Bridge = {}

local cjson_ok, cjson = pcall(require, "cjson")
local endpoint = os.getenv("AO_ENDPOINT")
local api_key = os.getenv("AO_API_KEY")
local resolver_id = os.getenv("AO_RESOLVER_ID")
local flags_file = os.getenv("AO_FLAGS_PATH") or os.getenv("AUTH_RESOLVER_FLAGS_FILE")
local bridge_mode = os.getenv("AO_BRIDGE_MODE") or (os.getenv("DRY_RUN") == "1" and "mock") or "http"
local dry_run = bridge_mode == "mock"
local retries = tonumber(os.getenv("AO_BRIDGE_RETRIES") or "3")
local backoff_ms = tonumber(os.getenv("AO_BRIDGE_BACKOFF_MS") or "200")
local flags_cache = {}

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

local function load_flags()
  if not flags_file or flags_file == "" or not cjson_ok then return end
  local f = io.open(flags_file, "r")
  if not f then return end
  local tmp = {}
  for line in f:lines() do
    local ok, obj = pcall(cjson.decode, line)
    if ok and obj and obj.resolverId and obj.flag then
      tmp[obj.resolverId] = obj
    end
  end
  f:close()
  flags_cache = tmp
end

local function check_resolver()
  if not resolver_id then return true end
  load_flags()
  local entry = flags_cache[resolver_id]
  if not entry then return true end
  if entry.flag == "blocked" then
    return false, "resolver_blocked"
  elseif entry.flag == "suspicious" then
    -- allow only read-only events? Here we conservatively block non-read events (write bridge emits writes)
    return false, "resolver_suspicious_blocked_for_writes"
  end
  return true
end

function Bridge.forward_event(ev)
  if bridge_mode == "off" then return true end
  if not cjson_ok then return false, "cjson_missing" end
  local ok_flag, flag_err = check_resolver()
  if not ok_flag then return false, flag_err end
  if resolver_id then ev.resolverId = resolver_id end
  local body = cjson.encode(ev)
  local expected_hash = ev.expectedResponseHash or os.getenv("AO_EXPECT_RESPONSE_HASH")
  for attempt = 1, retries do
    local ok, status, resp_body = post(body)
    local resp_hash
    if resp_body and #resp_body > 0 then
      resp_hash = sha256_str(resp_body)
    end
    if ok then
      if expected_hash then
        if resp_hash and resp_hash:lower() ~= expected_hash:lower() then
          ok = false
          status = "response_hash_mismatch"
        end
      end
      if ok then return true, status, resp_hash end
    end
    if attempt < retries then
      local jitter = math.random() * 0.5 + 0.75
      sleep(backoff_ms * jitter)
    else
      return false, status, resp_hash
    end
  end
end

Bridge._post = post

return Bridge
