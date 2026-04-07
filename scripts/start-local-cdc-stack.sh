#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPT_ROOT="/home/ubuntu/disk1/opt"
RUN_DIR="/tmp/hybench_sf10_cdc_state"
LOG_DIR="/tmp/hybench_sf10_cdc_logs"
METRICS_DIR="/tmp/hybench_sf10_cdc_metrics"
SPARK_EVENTS_DIR="/tmp/spark-events"
PIXELS_HOME_DEFAULT="${OPT_ROOT}/pixels"
PIXELS_SINK_HOME_DEFAULT="${OPT_ROOT}/pixels-sink"

mkdir -p "${RUN_DIR}" "${LOG_DIR}" "${METRICS_DIR}" "${SPARK_EVENTS_DIR}"

source "${OPT_ROOT}/conf/pixels-delta-env.sh"

export PIXELS_HOME="${PIXELS_HOME:-${PIXELS_HOME_DEFAULT}}"
export PIXELS_SINK_HOME="${PIXELS_SINK_HOME:-${PIXELS_SINK_HOME_DEFAULT}}"
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

wait_for_port() {
  local host="$1"
  local port="$2"
  local name="$3"
  local retries="${4:-60}"
  local delay="${5:-1}"
  local i

  for ((i=1; i<=retries; i++)); do
    if is_port_open "${host}" "${port}"; then
      log "${name} is ready on ${host}:${port}"
      return 0
    fi
    sleep "${delay}"
  done

  log "timed out waiting for ${name} on ${host}:${port}"
  return 1
}

start_hms() {
  if is_port_open 127.0.0.1 9083; then
    log "HMS already running"
    return 0
  fi

  log "starting HMS"
  "${OPT_ROOT}/run/start-metastore.sh"
  wait_for_port 127.0.0.1 9083 "HMS"
}

start_trino() {
  if is_port_open 127.0.0.1 8080; then
    log "Trino already running"
    return 0
  fi

  log "starting Trino"
  "${OPT_ROOT}/run/start-trino.sh"
  wait_for_port 127.0.0.1 8080 "Trino"
}

start_pixels_local() {
  if is_port_open 127.0.0.1 18888; then
    log "Pixels metadata already running"
  else
    log "starting local Pixels coordinator"
    export JAVA_HOME="${JAVA11_HOME}"
    "${PIXELS_HOME}/bin/start-daemon.sh" coordinator -daemon
    wait_for_port 127.0.0.1 18888 "Pixels metadata"
  fi

  if pgrep -f 'io\.pixelsdb\.pixels\.daemon\.PixelsWorker' >/dev/null 2>&1; then
    log "local Pixels worker already running"
  else
    log "starting local Pixels worker"
    export JAVA_HOME="${JAVA11_HOME}"
    "${PIXELS_HOME}/bin/start-daemon.sh" worker -daemon
  fi
}

start_optional_sink_rpc() {
  local config_path="${PIXELS_SINK_CONFIG:-${PIXELS_SINK_HOME}/target/classes/pixels-sink.local.properties}"
  local sink_jar="${PIXELS_SINK_JAR:-${PIXELS_SINK_HOME}/target/pixels-sink-0.2.0-SNAPSHOT-full.jar}"
  local sink_log="${LOG_DIR}/pixels-sink-rpc.log"

  if is_port_open 127.0.0.1 9091; then
    log "Pixels RPC already running"
    return 0
  fi

  if [[ "${START_PIXELS_SINK_RPC:-0}" != "1" ]]; then
    log "Pixels RPC is not running on 127.0.0.1:9091"
    log "set START_PIXELS_SINK_RPC=1 to attempt local sink startup"
    return 1
  fi

  if [[ ! -f "${sink_jar}" ]]; then
    log "cannot start Pixels RPC: missing sink jar ${sink_jar}"
    return 1
  fi

  if [[ ! -f "${config_path}" ]]; then
    log "cannot start Pixels RPC: missing sink config ${config_path}"
    return 1
  fi

  log "starting local Pixels sink RPC from ${config_path}"
  nohup "${JAVA17_HOME}/bin/java" -jar "${sink_jar}" -c "${config_path}" \
    >"${sink_log}" 2>&1 </dev/null &
  echo $! > "${RUN_DIR}/pixels-sink-rpc.pid"

  wait_for_port 127.0.0.1 9091 "Pixels RPC" 30 2
}

start_history_server() {
  if is_port_open 127.0.0.1 18080; then
    log "Spark History Server already running"
    return 0
  fi

  log "starting Spark History Server"
  export JAVA_HOME="${JAVA17_HOME}"
  export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=${SPARK_EVENTS_DIR}"
  "${SPARK_HOME}/sbin/start-history-server.sh"
  wait_for_port 127.0.0.1 18080 "Spark History Server"
}

ensure_jar() {
  if [[ -f "${ROOT_DIR}/target/pixels-spark-0.1.jar" ]]; then
    log "pixels-spark jar already exists"
    return 0
  fi

  log "building pixels-spark package"
  (
    cd "${ROOT_DIR}"
    ./scripts/build-package.sh
  )
}

main() {
  ensure_jar
  start_hms
  start_trino
  start_pixels_local
  start_optional_sink_rpc || true
  start_history_server

  log "stack startup finished"
  log "HMS: http://127.0.0.1:9083"
  log "Trino: http://127.0.0.1:8080"
  log "Spark History: http://127.0.0.1:18080"
  if is_port_open 127.0.0.1 18888; then
    log "Pixels metadata port 18888 is ready"
  fi
  if is_port_open 127.0.0.1 9091; then
    log "Pixels RPC port 9091 is ready"
  else
    log "Pixels RPC port 9091 is still unavailable"
  fi
}

main "$@"
