#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <table-name>" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TABLE_NAME="$1"
STATE_DIR="/tmp/hybench_sf10_cdc_state"
LOG_DIR="/tmp/hybench_sf10_cdc_logs"
DEFAULT_ENV_FILE="${ROOT_DIR}/scripts/hybench-sf10-cdc.env"

if [[ -n "${CDC_ENV_FILE:-}" ]]; then
  source "${CDC_ENV_FILE}"
elif [[ -f "${DEFAULT_ENV_FILE}" ]]; then
  source "${DEFAULT_ENV_FILE}"
fi

PID_FILE="${STATE_DIR}/${TABLE_NAME}.pid"
LOG_FILE="${LOG_DIR}/${TABLE_NAME}.log"
UNIT_NAME="pixels-cdc-${TABLE_NAME}"

if [[ -f "${PID_FILE}" ]]; then
  old_pid="$(cat "${PID_FILE}")"
  if [[ -n "${old_pid}" ]] && kill -0 "${old_pid}" 2>/dev/null; then
    echo "already-running pid=${old_pid}"
    exit 0
  fi
fi

mkdir -p "${STATE_DIR}" "${LOG_DIR}"

systemctl --user stop "${UNIT_NAME}.service" >/dev/null 2>&1 || true

systemd-run \
  --user \
  --unit "${UNIT_NAME}" \
  --same-dir \
  --collect \
  --property=WorkingDirectory="${ROOT_DIR}" \
  --property=StandardOutput=append:"${LOG_FILE}" \
  --property=StandardError=append:"${LOG_FILE}" \
  "${ROOT_DIR}/scripts/run-single-cdc-foreground.sh" "${TABLE_NAME}" >/dev/null

for _ in $(seq 1 20); do
  if [[ -f "${PID_FILE}" ]]; then
    pid="$(cat "${PID_FILE}")"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      echo "${pid}"
      exit 0
    fi
  fi
  sleep 1
done

echo "failed-to-start unit=${UNIT_NAME}" >&2
exit 1
