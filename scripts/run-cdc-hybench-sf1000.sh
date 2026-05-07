#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${ROOT_DIR}/etc/pixels-spark.properties}"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"

PROFILE="sf1000"
UNIT_PREFIX="${UNIT_PREFIX:-pixels-cdc-${PROFILE}}"
STATE_DIR="${STATE_DIR:-$(pixels_get_property pixels.cdc.hybench.${PROFILE}.state-dir /home/ubuntu/disk1/tmp/hybench_sf1000_cdc_state)}"
LOG_DIR="${LOG_DIR:-$(pixels_get_property pixels.cdc.hybench.${PROFILE}.log-dir /home/ubuntu/disk1/tmp/hybench_sf1000_cdc_logs)}"
CKPT_ROOT="${CKPT_ROOT:-$(pixels_get_property pixels.cdc.hybench.${PROFILE}.checkpoint-root /home/ubuntu/disk1/tmp/hybench_sf1000_cdc_ckpt)}"
METRICS_DIR="${METRICS_DIR:-$(pixels_get_property pixels.cdc.hybench.${PROFILE}.metrics-dir /home/ubuntu/disk1/tmp/hybench_sf1000_cdc_metrics)}"
RESOURCE_DIR="${RESOURCE_DIR:-$(pixels_get_property pixels.cdc.hybench.${PROFILE}.resource-dir ${ROOT_DIR}/data/hybench/sf1000/resource)}"
SPARK_EVENTS_DIR="${SPARK_EVENTS_DIR:-$(pixels_get_property pixels.spark.event-log.dir /home/ubuntu/disk1/tmp/spark-events)}"
TARGET_ROOT="${TARGET_ROOT:-$(pixels_get_property pixels.cdc.hybench.${PROFILE}.target-root s3a://home-zinuo/deltalake/hybench_sf1000)}"
DATABASE="${DATABASE:-$(pixels_get_property pixels.cdc.hybench.${PROFILE}.database "$(pixels_get_property pixels.cdc.database pixels_bench)")}"
CDC_BENCHMARK="${CDC_BENCHMARK:-$(pixels_get_property pixels.cdc.hybench.${PROFILE}.benchmark hybench)}"
RPC_HOST="${RPC_HOST:-$(pixels_get_property pixels.spark.rpc.host 127.0.0.1)}"
RPC_PORT="${RPC_PORT:-$(pixels_get_property pixels.spark.rpc.port 9091)}"
METADATA_HOST="${METADATA_HOST:-$(pixels_get_property pixels.spark.metadata.host 127.0.0.1)}"
METADATA_PORT="${METADATA_PORT:-$(pixels_get_property pixels.spark.metadata.port 18888)}"
SPARK_MASTER="${SPARK_MASTER:-$(pixels_get_property pixels.spark.master local[2])}"
MODE="${MODE:-$(pixels_get_property pixels.spark.cdc.execution.mode "$(pixels_get_property pixels.spark.delta.mode polling)")}"
TRIGGER_MODE="${TRIGGER_MODE:-$(pixels_get_property pixels.spark.delta.trigger.mode processing-time)}"
TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-$(pixels_get_property pixels.spark.delta.trigger.interval 10 seconds)}"
DELETE_MODE="${DELETE_MODE:-$(pixels_get_property pixels.spark.delta.delete.mode hard)}"
SINK_MODE="${SINK_MODE:-$(pixels_get_property pixels.spark.sink.mode delta)}"
NOOP_BUCKETS="${NOOP_BUCKETS:-$(pixels_get_property pixels.spark.delta.noop-buckets "")}"
TABLES=()
pixels_split_csv_property "$(pixels_get_property pixels.cdc.hybench.${PROFILE}.tables "$(pixels_get_property pixels.cdc.tables customer,company,savingaccount,checkingaccount,transfer,checking,loanapps,loantrans)")" TABLES

mkdir -p "${STATE_DIR}" "${LOG_DIR}" "${CKPT_ROOT}" "${METRICS_DIR}" "${RESOURCE_DIR}" "${SPARK_EVENTS_DIR}"

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
  STATE_DIR="${STATE_DIR}" \
  LOG_DIR="${LOG_DIR}" \
  CKPT_ROOT="${CKPT_ROOT}" \
  TARGET_ROOT="${TARGET_ROOT}" \
  METRICS_DIR="${METRICS_DIR}" \
  RESOURCE_DIR="${RESOURCE_DIR}" \
  SPARK_EVENTS_DIR="${SPARK_EVENTS_DIR}" \
  DATABASE="${DATABASE}" \
  CDC_BENCHMARK="${CDC_BENCHMARK}" \
  RPC_HOST="${RPC_HOST}" \
  RPC_PORT="${RPC_PORT}" \
  METADATA_HOST="${METADATA_HOST}" \
  METADATA_PORT="${METADATA_PORT}" \
  SPARK_MASTER="${SPARK_MASTER}" \
  MODE="${MODE}" \
  TRIGGER_MODE="${TRIGGER_MODE}" \
  TRIGGER_INTERVAL="${TRIGGER_INTERVAL}" \
  DELETE_MODE="${DELETE_MODE}" \
  SINK_MODE="${SINK_MODE}" \
  NOOP_BUCKETS="${NOOP_BUCKETS}" \
  UNIT_PREFIX="${UNIT_PREFIX}" \
  "${ROOT_DIR}/scripts/start-single-cdc-job.sh" "${table_name}" >/dev/null
  log "started table=${table_name} pid=$(cat "${pid_file}") log=${LOG_DIR}/${table_name}.log"
}

main() {
  ensure_ready "${RPC_HOST}" "${RPC_PORT}" "Pixels RPC"
  ensure_ready "${METADATA_HOST}" "${METADATA_PORT}" "Pixels metadata"

  local table_name
  for table_name in "${TABLES[@]}"; do
    start_table "${table_name}"
  done

  log "all table jobs submitted profile=${PROFILE} unit_prefix=${UNIT_PREFIX}"
}

main "$@"
