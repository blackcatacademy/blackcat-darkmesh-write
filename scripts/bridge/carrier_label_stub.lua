#!/usr/bin/env lua
-- Stub carrier label generator.
-- Usage: lua scripts/bridge/carrier_label_stub.lua <shipmentId> [carrier] [service]

local shipmentId = arg[1] or ("ship-" .. os.time())
local carrier = arg[2] or "stub-carrier"
local service = arg[3] or "ground"

local label_url = string.format("https://labels.example/%s/%s/%s.pdf", carrier, service, shipmentId)
local tracking = string.format("%s-%s-%s", carrier, service, shipmentId)
local tracking_url = string.format("https://track.example/%s", tracking)

print(label_url)
io.stderr:write(string.format("carrier=%s service=%s tracking=%s url=%s\n", carrier, service, tracking, tracking_url))
