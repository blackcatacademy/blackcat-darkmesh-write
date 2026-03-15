#!/usr/bin/env lua
-- Run a write command locally against the in-memory router.
-- Usage: lua scripts/cli/run_command.lua path/to/command.json

local path = arg[1]
if not path then
  io.stderr:write("Usage: lua scripts/cli/run_command.lua <command.json>\n")
  os.exit(1)
end

local f = assert(io.open(path, "r"))
local content = f:read("*a")
f:close()

local ok, cjson = pcall(require, "cjson")
if not ok then
  io.stderr:write("cjson is required for this tool\n")
  os.exit(1)
end

local cmd = cjson.decode(content)
local write = require("ao.write.process")

local resp = write.route(cmd)
print(cjson.encode(resp))
