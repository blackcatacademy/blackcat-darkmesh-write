#!/usr/bin/env lua
-- Run all fixture commands and optionally compare with expected outputs.

local function load_json_module()
  local ok, mod = pcall(require, "cjson")
  if ok then return mod end
  ok, mod = pcall(require, "dkjson")
  if ok then
    return {
      decode = function(str) return mod.decode(str) end,
      encode = function(tbl) return mod.encode(tbl) end,
    }
  end
  return nil
end

local cjson = load_json_module()
if not cjson then
  io.stderr:write("cjson or dkjson required for batch_run\n")
  os.exit(1)
end
local lfs_ok, lfs = pcall(require, "lfs")
if not lfs_ok then
  io.stderr:write("lua-filesystem (lfs) required for batch_run\n")
  os.exit(1)
end
local write = require("ao.write.process")

local function read_file(path)
  local f = io.open(path, "r")
  if not f then return nil end
  local s = f:read("*a"); f:close(); return s
end

local function run_fixture(path)
  local body = read_file(path)
  if not body then return false, "read_failed" end
  local cmd = cjson.decode(body)
  local resp = write.route(cmd)
  local expected_path = path .. ".expected.json"
  local expected_str = read_file(expected_path)
  if expected_str then
    local expected = cjson.decode(expected_str)
    if cjson.encode(resp) ~= cjson.encode(expected) then
      return false, "mismatch"
    end
  end
  return true
end

local fixtures_dir = "fixtures"
local passed, failed = 0, 0
for file in lfs.dir(fixtures_dir) do
  if file:match("%.json$") and not file:match("%.expected%.json$") then
    local ok, err = run_fixture(fixtures_dir .. "/" .. file)
    if ok then
      passed = passed + 1
      print("[ok] " .. file)
    else
      failed = failed + 1
      print(string.format("[fail] %s (%s)", file, err))
    end
  end
end

print(string.format("batch run: passed=%d failed=%d", passed, failed))
os.exit(failed == 0 and 0 or 1)
