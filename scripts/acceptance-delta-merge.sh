#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 8 ]]; then
  echo "Usage: $0 <database> <table> <rpc-host> <rpc-port> <metadata-host> <metadata-port> <target-path> <checkpoint-location> [spark-submit args...]" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATABASE="$1"
TABLE="$2"
RPC_HOST="$3"
RPC_PORT="$4"
METADATA_HOST="$5"
METADATA_PORT="$6"
TARGET_PATH="$7"
CHECKPOINT_LOCATION="$8"
shift 8

"${ROOT_DIR}/scripts/run-delta-merge.sh" \
  --database "${DATABASE}" \
  --table "${TABLE}" \
  --rpc-host "${RPC_HOST}" \
  --rpc-port "${RPC_PORT}" \
  --metadata-host "${METADATA_HOST}" \
  --metadata-port "${METADATA_PORT}" \
  --target-path "${TARGET_PATH}" \
  --checkpoint-location "${CHECKPOINT_LOCATION}" \
  "$@"

"${ROOT_DIR}/scripts/check-delta-primary-key.sh" \
  "${METADATA_HOST}" \
  "${METADATA_PORT}" \
  "${DATABASE}" \
  "${TABLE}" \
  "${TARGET_PATH}"
