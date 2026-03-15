#!/usr/bin/env bash
set -euo pipefail
WAL=${WRITE_WAL_PATH:-}
QUEUE=${WRITE_OUTBOX_PATH:-${AO_QUEUE_PATH:-}}
WAL_MAX=${WRITE_WAL_MAX_BYTES:-5242880}
QUEUE_MAX=${WRITE_OUTBOX_MAX_BYTES:-${AO_QUEUE_MAX_BYTES:-2097152}}
status=0
check(){
  local path=$1
  local max=$2
  local label=$3
  if [ -z "$path" ] || [ ! -f "$path" ]; then
    echo "$label: skip"
    return
  fi
  local sz=$(stat -c%s "$path")
  local sha=$(sha256sum "$path" | awk '{print $1}')
  echo "$label: size=$sz hash=$sha"
  if [ "$max" -gt 0 ] && [ "$sz" -gt "$max" ]; then
    echo "$label: size_exceeded" >&2
    status=2
  fi
}
check "$WAL" "$WAL_MAX" "wal"
check "$QUEUE" "$QUEUE_MAX" "queue"
exit $status
