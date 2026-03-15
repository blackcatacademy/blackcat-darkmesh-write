#!/usr/bin/env lua
-- Export persisted shipping/tax rates to NDJSON files (for AO ingestion).
local storage = require("ao.shared.storage")
local json_ok, cjson = pcall(require, "cjson")
if not json_ok then
  io.stderr:write("cjson missing\n")
  os.exit(1)
end

local rate_store = os.getenv("WRITE_RATE_STORE_PATH") or arg[1] or "dev/rates.json"
local ship_out = os.getenv("AO_SHIPPING_RATES_PATH") or arg[2] or "dev/shipping.ndjson"
local tax_out = os.getenv("AO_TAX_RATES_PATH") or arg[3] or "dev/tax.ndjson"

local ok, err = storage.load(rate_store)
if not ok then
  io.stderr:write("failed to load rate store: " .. tostring(err or rate_store) .. "\n")
  os.exit(1)
end

local shipping = storage.get("shipping_rates") or {}
local tax = storage.get("tax_rates") or {}

local function write_ndjson(path, tbl)
  local f = assert(io.open(path, "w"))
  for site, list in pairs(tbl) do
    for _, row in ipairs(list) do
      row.siteId = row.siteId or site
      f:write(cjson.encode(row))
      f:write("\n")
    end
  end
  f:close()
end

write_ndjson(ship_out, shipping)
write_ndjson(tax_out, tax)
print(string.format("exported shipping=%d tax=%d", #shipping, #tax))
