-- Dependency check for write process
local deps = {
  { name = "cjson", mod = "cjson" },
  { name = "dkjson", mod = "dkjson", optional = true },
  { name = "luv", mod = "luv" },
  { name = "lsqlite3", mod = "lsqlite3" },
  { name = "openssl", mod = "openssl" },
  { name = "sodium", mod = "sodium" },
}

local ok_all = true
for _, d in ipairs(deps) do
  local ok = pcall(require, d.mod)
  if ok then
    io.stdout:write(string.format("%s: ok\n", d.name))
  else
    local status = d.optional and "missing (optional)" or "missing"
    io.stdout:write(string.format("%s: %s\n", d.name, status))
    if not d.optional then ok_all = false end
  end
end

if not ok_all then os.exit(1) end

-- fail-closed if signatures are required but sodium/openssl unavailable
if os.getenv("WRITE_REQUIRE_SIGNATURE") == "1" then
  local ok_sodium = pcall(require, "sodium")
  local ok_ossl = pcall(require, "openssl")
  if not (ok_sodium or ok_ossl) then
    io.stderr:write("signature required but sodium/openssl missing\n")
    os.exit(1)
  end
end
