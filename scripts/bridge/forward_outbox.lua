-- Bridge stub: forward write outbox events to AO (mock).
-- In production, this would post to blackcat-darkmesh-ao registry/site process.

local write = require("ao.write.process")

local function forward_event(ev)
  -- Placeholder: just log to stdout; in real bridge, sign and POST to -ao endpoint.
  print(string.format("[bridge] forward %s site=%s version=%s manifest=%s", ev.type or "?", ev.siteId or "-", ev.versionId or "-", ev.manifestTx or "-"))
end

local outbox = write._storage_outbox()
for _, ev in ipairs(outbox) do
  forward_event(ev)
end

print(string.format("[bridge] forwarded %d events", #outbox))
