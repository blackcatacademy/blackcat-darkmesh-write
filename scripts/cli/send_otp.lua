-- Send OTP code via webhook/SendGrid/Twilio or print to stdout.
-- Usage:
--   lua scripts/cli/send_otp.lua <code> <target>
-- target can be email or phone number.

local code = arg[1]
local target = arg[2]
if not code or not target then
  io.stderr:write("usage: lua send_otp.lua <code> <target>\n")
  os.exit(1)
end

local function is_email(s) return s:find("@") end
local function send_email(text, to)
  local hook = os.getenv("NOTIFY_EMAIL_WEBHOOK")
  if hook and hook ~= "" then
    os.execute(string.format("curl -sS -X POST -H 'Content-Type: text/plain' --data %q %q >/dev/null", text, hook))
    return true
  end
  local sg_key = os.getenv("SENDGRID_API_KEY")
  local sg_from = os.getenv("SENDGRID_FROM")
  if sg_key and sg_from then
    local cmd = string.format([[curl -sS -X POST https://api.sendgrid.com/v3/mail/send \
-H "Authorization: Bearer %s" -H "Content-Type: application/json" \
-d '{"personalizations":[{"to":[{"email":"%s"}]}],"from":{"email":"%s"},"subject":"Your OTP","content":[{"type":"text/plain","value":%q}]}' >/dev/null]], sg_key, to, sg_from, text)
    os.execute(cmd)
    return true
  end
  return false
end

local function send_sms(text, to)
  local hook = os.getenv("NOTIFY_SMS_WEBHOOK")
  if hook and hook ~= "" then
    os.execute(string.format("curl -sS -X POST -H 'Content-Type: text/plain' --data %q %q >/dev/null", text, hook))
    return true
  end
  local sid = os.getenv("TWILIO_SID")
  local token = os.getenv("TWILIO_TOKEN")
  local from = os.getenv("TWILIO_FROM")
  if sid and token and from then
    local cmd = string.format([[curl -sS -X POST https://api.twilio.com/2010-04-01/Accounts/%s/Messages.json \
-u %s:%s -d From=%q -d To=%q -d Body=%q >/dev/null]], sid, sid, token, from, to, text)
    os.execute(cmd)
    return true
  end
  return false
end

local message = string.format("Your login code is: %s", code)

local ok
if is_email(target) then
  ok = send_email(message, target)
else
  ok = send_sms(message, target)
end

if not ok then
  print(string.format("[OTP] %s -> %s", target, message))
end
