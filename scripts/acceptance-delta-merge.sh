#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 9 ]]; then
  echo "Usage: $0 <database> <table> <buckets> <rpc-host> <rpc-port> <metadata-host> <metadata-port> <target-path> <checkpoint-location> [spark-submit args...]" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATABASE="$1"
TABLE="$2"
BUCKETS="$3"
RPC_HOST="$4"
RPC_PORT="$5"
METADATA_HOST="$6"
METADATA_PORT="$7"
TARGET_PATH="$8"
CHECKPOINT_LOCATION="$9"
shift 9

"${ROOT_DIR}/scripts/run-delta-merge.sh" \
  --database "${DATABASE}" \
  --table "${TABLE}" \
  --buckets "${BUCKETS}" \
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
