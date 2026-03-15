# Contract tests

Run locally:
```
LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" lua5.4 scripts/verify/contracts.lua
```

The script checks envelope validation, idempotency replay, version conflict, and unknown-action handling for the write router.
