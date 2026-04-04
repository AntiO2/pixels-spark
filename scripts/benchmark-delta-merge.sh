#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 10 ]]; then
  echo "Usage: $0 <runs> <database> <table> <buckets> <rpc-host> <rpc-port> <metadata-host> <metadata-port> <target-path> <checkpoint-base> [extra args...]" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNS="$1"
DATABASE="$2"
TABLE="$3"
BUCKETS="$4"
RPC_HOST="$5"
RPC_PORT="$6"
METADATA_HOST="$7"
METADATA_PORT="$8"
TARGET_PATH="$9"
CHECKPOINT_BASE="${10}"
shift 10

for ((i=1; i<=RUNS; i++)); do
  CHECKPOINT_LOCATION="${CHECKPOINT_BASE}/run-${i}"
  START_TS="$(date +%s)"
  echo "run=${i} start_ts=${START_TS}"
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
  END_TS="$(date +%s)"
  echo "run=${i} elapsed_seconds=$((END_TS - START_TS))"
done
