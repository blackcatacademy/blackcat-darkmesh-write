#!/usr/bin/env bash
# Lightweight preflight checks for the write repo.
# - validates JSON schemas are well-formed
# - ensures Lua sources have no syntax errors

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
export ROOT_DIR

echo "[verify] JSON schemas"
python3 - <<'PY'
import json
import os
from pathlib import Path

root = Path(os.environ["ROOT_DIR"])
schemas = sorted((root / "schemas").glob("*.json"))
if not schemas:
    raise SystemExit("No schemas found under schemas/")

for path in schemas:
    with path.open("r", encoding="utf-8") as f:
        json.load(f)
    print(f"  ✓ {path.relative_to(root)}")
PY

echo "[verify] Lua syntax"

lua_runner=()
if command -v luac >/dev/null 2>&1; then
  lua_runner=(luac -p)
elif command -v lua5.4 >/dev/null 2>&1; then
  lua_runner=(lua5.4 -e "assert(loadfile(arg[1]))")
elif command -v lua >/dev/null 2>&1; then
  lua_runner=(lua -e "assert(loadfile(arg[1]))")
fi

if [ ${#lua_runner[@]} -eq 0 ]; then
  echo "Lua interpreter/compiler not found. Install lua5.4 (or luac) to run syntax checks." >&2
  exit 1
fi

find "$ROOT_DIR/ao" -name '*.lua' -print -exec "${lua_runner[@]}" {} \;

echo "[verify] done"

# optional contract smoke tests
if command -v lua5.4 >/dev/null 2>&1; then
  if [ "${RUN_CONTRACTS:-1}" -eq 1 ]; then
    echo "[verify] contract smoke tests"
    LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" lua5.4 "$ROOT_DIR/scripts/verify/contracts.lua"
  fi
  if [ "${RUN_CONFLICTS:-1}" -eq 1 ]; then
    echo "[verify] conflict/security tests"
    LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" lua5.4 "$ROOT_DIR/scripts/verify/conflicts.lua"
  fi
  if [ -n "${WRITE_IDEM_PATH:-}" ]; then
    echo "[verify] idempotency persistence"
    LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" lua5.4 - <<'LUA'
local idem = require("ao.shared.idempotency")
local write = require("ao.write.process")
local tmp = os.getenv("WRITE_IDEM_PATH")
local cmd = { action = "SaveDraftPage", requestId = "rid-persist-1", actor = "a", tenant = "t", role = "admin", timestamp = "2026-03-15T00:00:00Z", nonce = "nonce-persist-1", signatureRef = "sigref-persist-1", payload = { siteId = "s", pageId = "p", locale = "en", blocks = {} } }
write.route(cmd)
package.loaded["ao.write.process"] = nil
package.loaded["ao.shared.idempotency"] = nil
local idem2 = require("ao.shared.idempotency")
local resp = idem2.lookup("rid-persist-1")
if not resp or resp.status ~= "OK" then error("persisted idempotency missing") end
os.remove(tmp)
LUA
  fi
  if [ -n "${WRITE_OUTBOX_PATH:-}" ]; then
    echo "[verify] outbox persistence"
    LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" lua5.4 - <<'LUA'
local write = require("ao.write.process")
local tmp = os.getenv("WRITE_OUTBOX_PATH")
write.route({ action = "PublishPageVersion", requestId = "rid-outbox-1", actor = "a", tenant = "t", role = "publisher", timestamp = "2026-03-15T00:00:00Z", nonce = "nonce-outbox-1", signatureRef = "sigref-outbox-1", payload = { siteId = "s", pageId = "p", versionId = "v1", manifestTx = "tx1" } })
package.loaded["ao.write.process"] = nil
local storage = require("ao.shared.storage")
storage.load(tmp)
local events = storage.all("outbox")
if #events == 0 then error("outbox persistence missing") end
os.remove(tmp)
LUA
  fi
fi
