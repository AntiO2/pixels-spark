#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${ROOT_DIR}/etc/pixels-spark.properties}"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"

STATE_DIR="${STATE_DIR:-$(pixels_get_property pixels.cdc.hybench.sf1000.state-dir /home/ubuntu/disk1/tmp/hybench_sf1000_cdc_state)}"
LOG_DIR="${LOG_DIR:-$(pixels_get_property pixels.cdc.hybench.sf1000.log-dir /home/ubuntu/disk1/tmp/hybench_sf1000_cdc_logs)}"
UNIT_PREFIX="${UNIT_PREFIX:-pixels-cdc-sf1000}"
TABLES=()
pixels_split_csv_property "$(pixels_get_property pixels.cdc.tables customer,company,savingaccount,checkingaccount,transfer,checking,loanapps,loantrans)" TABLES

find_live_pid() {
  local table_name="$1"
  local pid_file="${STATE_DIR}/${table_name}.pid"
  local pid=""

  if [[ -f "${pid_file}" ]]; then
    pid="$(cat "${pid_file}" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      printf '%s' "${pid}"
      return 0
    fi
  fi

  pid="$(
    ps -eo pid=,args= \
      | awk -v table="${table_name}" '
          index($0, "run-delta-merge.sh") > 0 &&
          index($0, "--table " table) > 0 { print $1; exit }
          index($0, "pixels-spark-0.1.jar") > 0 &&
          index($0, "--table " table) > 0 { print $1; exit }
        '
  )"

  if [[ -n "${pid}" ]]; then
    printf '%s' "${pid}" > "${pid_file}"
    printf '%s' "${pid}"
    return 0
  fi

  printf ''
}

main() {
  mkdir -p "${STATE_DIR}" "${LOG_DIR}"
  printf '%-18s %-10s %-8s %-8s %s\n' "table" "status" "pid" "unit" "log"

  local table_name pid status unit_state unit_name log_file
  for table_name in "${TABLES[@]}"; do
    pid="$(find_live_pid "${table_name}")"
    status="stopped"
    [[ -n "${pid}" ]] && status="running"

    unit_name="${UNIT_PREFIX}-${table_name}.service"
    unit_state="$(systemctl --user is-active "${unit_name}" 2>/dev/null || true)"
    log_file="${LOG_DIR}/${table_name}.log"

    printf '%-18s %-10s %-8s %-8s %s\n' \
      "${table_name}" \
      "${status}" \
      "${pid:--}" \
      "${unit_state:--}" \
      "${log_file}"
  done
}

main "$@"
