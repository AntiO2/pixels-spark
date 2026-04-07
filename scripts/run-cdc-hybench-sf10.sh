#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="/tmp/hybench_sf10_cdc_state"
LOG_DIR="/tmp/hybench_sf10_cdc_logs"
CKPT_ROOT="/tmp/hybench_sf10_cdc_ckpt"
DEFAULT_ENV_FILE="${ROOT_DIR}/scripts/hybench-sf10-cdc.env"

if [[ -n "${CDC_ENV_FILE:-}" ]]; then
  source "${CDC_ENV_FILE}"
elif [[ -f "${DEFAULT_ENV_FILE}" ]]; then
  source "${DEFAULT_ENV_FILE}"
fi

TARGET_ROOT="${TARGET_ROOT:-s3a://home-zinuo/deltalake/hybench_sf10}"
DATABASE="${DATABASE:-pixels_bench}"
BUCKETS="${BUCKETS:-0,1,2,3}"
RPC_HOST="${RPC_HOST:-127.0.0.1}"
RPC_PORT="${RPC_PORT:-9091}"
METADATA_HOST="${METADATA_HOST:-127.0.0.1}"
METADATA_PORT="${METADATA_PORT:-18888}"
SPARK_MASTER="${SPARK_MASTER:-local[2]}"
TRIGGER_MODE="${TRIGGER_MODE:-processing-time}"
TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-10 seconds}"
DELETE_MODE="${DELETE_MODE:-hard}"
TABLES=(
  customer
  company
  savingaccount
  checkingaccount
  transfer
  checking
  loanapps
  loantrans
)

mkdir -p "${STATE_DIR}" "${LOG_DIR}" "${CKPT_ROOT}" "/tmp/spark-events"

source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh
export JAVA_HOME="${JAVA17_HOME}"
export AWS_ACCESS_KEY_ID
AWS_ACCESS_KEY_ID="$(awk -F= '/aws_access_key_id/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_SECRET_ACCESS_KEY
AWS_SECRET_ACCESS_KEY="$(awk -F= '/aws_secret_access_key/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_REGION="${AWS_REGION:-us-east-2}"

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

is_port_open() {
  local host="$1"
  local port="$2"
  nc -z "${host}" "${port}" >/dev/null 2>&1
}

ensure_ready() {
  local host="$1"
  local port="$2"
  local name="$3"

  if ! is_port_open "${host}" "${port}"; then
    log "${name} is unavailable on ${host}:${port}"
    exit 1
  fi
}

start_table() {
  local table_name="$1"
  local pid_file="${STATE_DIR}/${table_name}.pid"

  if [[ -f "${pid_file}" ]] && kill -0 "$(cat "${pid_file}")" 2>/dev/null; then
    log "skip table=${table_name} reason=already-running pid=$(cat "${pid_file}")"
    return 0
  fi

  log "start table=${table_name} target=${TARGET_ROOT}/${table_name}"
  if [[ -n "${CDC_ENV_FILE:-}" ]]; then
    CDC_ENV_FILE="${CDC_ENV_FILE}" \
      "${ROOT_DIR}/scripts/start-single-cdc-job.sh" "${table_name}" >/dev/null
  elif [[ -f "${DEFAULT_ENV_FILE}" ]]; then
    CDC_ENV_FILE="${DEFAULT_ENV_FILE}" \
      "${ROOT_DIR}/scripts/start-single-cdc-job.sh" "${table_name}" >/dev/null
  else
    "${ROOT_DIR}/scripts/start-single-cdc-job.sh" "${table_name}" >/dev/null
  fi
  log "started table=${table_name} pid=$(cat "${pid_file}") log=${LOG_DIR}/${table_name}.log"
}

main() {
  ensure_ready "${RPC_HOST}" "${RPC_PORT}" "Pixels RPC"
  ensure_ready "${METADATA_HOST}" "${METADATA_PORT}" "Pixels metadata"

  local table_name
  for table_name in "${TABLES[@]}"; do
    start_table "${table_name}"
  done

  log "all table jobs submitted"
}

main "$@"
