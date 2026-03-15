-- Health snapshot for write process

local cjson = require("cjson")
local lfs_ok, lfs = pcall(require, "lfs")
local function sha256_file(path)
  local p = io.popen("sha256sum " .. path .. " 2>/dev/null")
  if not p then return nil end
  local out = p:read("*a") or ""
  p:close()
  return out:match("^(%w+)")
end

local function file_size(path)
  if not lfs_ok then return "n/a" end
  local attr = lfs.attributes(path)
  return attr and attr.size or 0
end

local function print_line(k, v)
  io.stdout:write(string.format("%s: %s\n", k, tostring(v)))
end

local wal = os.getenv("WRITE_WAL_PATH") or ""
local outbox = os.getenv("WRITE_OUTBOX_PATH") or ""
local queue = os.getenv("AO_QUEUE_PATH") or ""
local queue_log = os.getenv("AO_QUEUE_LOG_PATH") or ""

print_line("deps.cjson", cjson and "yes" or "no")
print_line("deps.luv", pcall(require, "luv") and "yes" or "no")
print_line("deps.luaossl", pcall(require, "openssl") and "yes" or "no")
print_line("deps.sodium", pcall(require, "sodium") and "yes" or "no")
print_line("deps.lsqlite3", pcall(require, "lsqlite3") and "yes" or "no")

if wal ~= "" then
  print_line("wal.path", wal)
  print_line("wal.size", file_size(wal))
  print_line("wal.sha256", sha256_file(wal) or "n/a")
end
if outbox ~= "" then
  print_line("outbox.path", outbox)
  print_line("outbox.size", file_size(outbox))
  print_line("outbox.sha256", sha256_file(outbox) or "n/a")
end
if queue ~= "" then
  print_line("queue.path", queue)
  print_line("queue.size", file_size(queue))
  print_line("queue.sha256", sha256_file(queue) or "n/a")
end
if queue_log ~= "" then
  print_line("queue_log.path", queue_log)
  print_line("queue_log.size", file_size(queue_log))
end

print_line("health", "ok")
