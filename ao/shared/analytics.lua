-- Minimal analytics event helper.
local Analytics = {}

function Analytics.event(event_type, payload)
  return {
    type = event_type,
    ts = os.time(),
    payload = payload,
  }
end

return Analytics
