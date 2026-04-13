#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <table-name>" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TABLE_NAME="$1"
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${ROOT_DIR}/etc/pixels-spark.properties}"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"

STATE_DIR="${STATE_DIR:-$(pixels_get_property pixels.cdc.state-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_state)}"
LOG_DIR="${LOG_DIR:-$(pixels_get_property pixels.cdc.log-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_logs)}"
UNIT_PREFIX="${UNIT_PREFIX:-pixels-cdc}"

PID_FILE="${STATE_DIR}/${TABLE_NAME}.pid"
LOG_FILE="${LOG_DIR}/${TABLE_NAME}.log"
UNIT_NAME="${UNIT_PREFIX}-${TABLE_NAME}"

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
  --setenv=PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG}" \
  ${STATE_DIR:+--setenv=STATE_DIR="${STATE_DIR}"} \
  ${LOG_DIR:+--setenv=LOG_DIR="${LOG_DIR}"} \
  ${CKPT_ROOT:+--setenv=CKPT_ROOT="${CKPT_ROOT}"} \
  ${TARGET_ROOT:+--setenv=TARGET_ROOT="${TARGET_ROOT}"} \
  ${METRICS_DIR:+--setenv=METRICS_DIR="${METRICS_DIR}"} \
  ${RESOURCE_DIR:+--setenv=RESOURCE_DIR="${RESOURCE_DIR}"} \
  ${SPARK_EVENTS_DIR:+--setenv=SPARK_EVENTS_DIR="${SPARK_EVENTS_DIR}"} \
  ${DATABASE:+--setenv=DATABASE="${DATABASE}"} \
  ${RPC_HOST:+--setenv=RPC_HOST="${RPC_HOST}"} \
  ${RPC_PORT:+--setenv=RPC_PORT="${RPC_PORT}"} \
  ${METADATA_HOST:+--setenv=METADATA_HOST="${METADATA_HOST}"} \
  ${METADATA_PORT:+--setenv=METADATA_PORT="${METADATA_PORT}"} \
  ${SPARK_MASTER:+--setenv=SPARK_MASTER="${SPARK_MASTER}"} \
  ${TRIGGER_MODE:+--setenv=TRIGGER_MODE="${TRIGGER_MODE}"} \
  ${TRIGGER_INTERVAL:+--setenv=TRIGGER_INTERVAL="${TRIGGER_INTERVAL}"} \
  ${DELETE_MODE:+--setenv=DELETE_MODE="${DELETE_MODE}"} \
  ${MODE:+--setenv=MODE="${MODE}"} \
  ${CDC_BENCHMARK:+--setenv=CDC_BENCHMARK="${CDC_BENCHMARK}"} \
  ${NOOP_BUCKETS:+--setenv=NOOP_BUCKETS="${NOOP_BUCKETS}"} \
  ${UNIT_PREFIX:+--setenv=UNIT_PREFIX="${UNIT_PREFIX}"} \
  --property=WorkingDirectory="${ROOT_DIR}" \
  --property=StandardOutput=append:"${LOG_FILE}" \
  --property=StandardError=append:"${LOG_FILE}" \
  ${SINK_MODE:+--setenv=SINK_MODE="${SINK_MODE}"} \
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
