#!/usr/bin/env bash
# Periodically run checksum_alert
set -euo pipefail
INTERVAL=${CHECKSUM_INTERVAL_SEC:-300}
if [ "$INTERVAL" -le 0 ]; then
  echo "CHECKSUM_INTERVAL_SEC must be >0" >&2
  exit 1
fi
while true; do
  RUN_CHECKSUM_ALERT=1 \
  WRITE_WAL_PATH=${WRITE_WAL_PATH:-dev/write-wal.ndjson} \
  WRITE_OUTBOX_PATH=${WRITE_OUTBOX_PATH:-dev/outbox-queue.ndjson} \
  LUA_PATH="?.lua;?/init.lua;ao/?.lua;ao/?/init.lua" \
    ./scripts/verify/checksum_alert.sh || true
  sleep "$INTERVAL"
done
