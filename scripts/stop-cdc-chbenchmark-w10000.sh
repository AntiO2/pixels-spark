#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${ROOT_DIR}/etc/pixels-spark.properties}"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"

PROFILE="w10000"
STATE_DIR="${STATE_DIR:-$(pixels_get_property pixels.cdc.chbenchmark.${PROFILE}.state-dir /home/ubuntu/disk1/tmp/chbenchmark_w10000_cdc_state)}"
UNIT_PREFIX="${UNIT_PREFIX:-pixels-cdc-chbenchmark-${PROFILE}}"
TABLES=()
pixels_split_csv_property "$(pixels_get_property pixels.cdc.chbenchmark.${PROFILE}.tables "$(pixels_get_property pixels.import.chbenchmark.tables warehouse,district,customer,history,neworder,order,orderline,item,stock,nation,supplier,region)")" TABLES

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

stop_table() {
  local table_name="$1"
  local unit_name="${UNIT_PREFIX}-${table_name}.service"
  local pid_file="${STATE_DIR}/${table_name}.pid"

  systemctl --user stop "${unit_name}" >/dev/null 2>&1 || true

  if [[ -f "${pid_file}" ]]; then
    local pid
    pid="$(cat "${pid_file}" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      log "waiting table=${table_name} pid=${pid}"
      for _ in $(seq 1 10); do
        if ! kill -0 "${pid}" 2>/dev/null; then
          break
        fi
        sleep 1
      done
    fi
    if [[ -n "${pid}" ]] && ! kill -0 "${pid}" 2>/dev/null; then
      rm -f "${pid_file}"
    fi
  fi

  log "stopped table=${table_name}"
}

main() {
  mkdir -p "${STATE_DIR}"
  local table_name
  for table_name in "${TABLES[@]}"; do
    stop_table "${table_name}"
  done
}

main "$@"
