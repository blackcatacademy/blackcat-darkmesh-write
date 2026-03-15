#!/usr/bin/env bash
set -euo pipefail
FILE="$1"
TXID="$2"
if [ -z "${FILE:-}" ] || [ -z "${TXID:-}" ]; then
  echo "usage: $0 <local-file> <txid>" >&2
  exit 1
fi
if [ ! -f "$FILE" ]; then
  echo "file not found: $FILE" >&2
  exit 1
fi
LOCAL_HASH=$(sha256sum "$FILE" | awk '{print $1}')
REMOTE_HASH=$(curl -sL "https://arweave.net/${TXID}" | sha256sum | awk '{print $1}')
if [ "$LOCAL_HASH" = "$REMOTE_HASH" ]; then
  echo "hash match: $LOCAL_HASH"
  exit 0
else
  echo "hash mismatch! local=$LOCAL_HASH remote=$REMOTE_HASH" >&2
  exit 2
fi
