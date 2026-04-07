#!/usr/bin/env bash
set -euo pipefail

STATE_DIR="/tmp/hybench_sf10_cdc_state"
LOG_DIR="/tmp/hybench_sf10_cdc_logs"
METRICS_DIR="/tmp/hybench_sf10_cdc_metrics"
SYSTEM_CSV="${METRICS_DIR}/system.csv"
INTERVAL="${1:-5}"
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

mkdir -p "${STATE_DIR}" "${LOG_DIR}" "${METRICS_DIR}"

if [[ ! -f "${SYSTEM_CSV}" ]]; then
  echo "ts,load1,mem_used_mb,mem_avail_mb,disk_used_pct" > "${SYSTEM_CSV}"
fi

extract_log_line() {
  local table_name="$1"
  local log_file="${LOG_DIR}/${table_name}.log"
  if [[ ! -f "${log_file}" ]]; then
    printf ''
    return 0
  fi
  tail -n 200 "${log_file}" | grep -E 'StreamingQueryProgress|numInputRows|batchId|Exception|ERROR|WARN' | tail -n 1 || true
}

find_live_pid() {
  local table_name="$1"
  local pid_file="${STATE_DIR}/${table_name}.pid"
  local pid=""

  if [[ -f "${pid_file}" ]]; then
    pid="$(cat "${pid_file}")"
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

write_table_json() {
  local table_name="$1"
  local pid="$2"
  local status="$3"
  local cpu="$4"
  local rss_kb="$5"
  local etimes="$6"
  local log_excerpt="$7"
  local output="${METRICS_DIR}/${table_name}.json"
  local csv="${METRICS_DIR}/${table_name}.csv"
  local ts

  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ ! -f "${csv}" ]]; then
    echo "ts,pid,status,cpu,rss_kb,etimes" > "${csv}"
  fi
  printf '%s,%s,%s,%s,%s,%s\n' "${ts}" "${pid}" "${status}" "${cpu}" "${rss_kb}" "${etimes}" >> "${csv}"

  python3 - <<'PY' "${output}" "${table_name}" "${ts}" "${pid}" "${status}" "${cpu}" "${rss_kb}" "${etimes}" "${log_excerpt}"
import json
import sys

path, table, ts, pid, status, cpu, rss_kb, etimes, log_excerpt = sys.argv[1:]
payload = {
    "table": table,
    "ts": ts,
    "pid": int(pid) if pid.isdigit() else None,
    "status": status,
    "cpu": float(cpu) if cpu not in ("", "-") else None,
    "rss_kb": int(float(rss_kb)) if rss_kb not in ("", "-") else None,
    "etimes": int(float(etimes)) if etimes not in ("", "-") else None,
    "log_excerpt": log_excerpt,
}
with open(path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh)
PY
}

collect_system() {
  local load1 mem_used mem_avail disk_pct
  load1="$(awk '{print $1}' /proc/loadavg)"
  read -r _ total used free shared buff_cache avail < <(free -m | awk '/Mem:/ {print $1, $2, $3, $4, $5, $6, $7}')
  mem_used="${used}"
  mem_avail="${avail}"
  disk_pct="$(df -P /tmp | awk 'NR==2 {gsub(/%/, "", $5); print $5}')"
  printf '%s,%s,%s,%s,%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${load1}" "${mem_used}" "${mem_avail}" "${disk_pct}" >> "${SYSTEM_CSV}"
}

main() {
  while true; do
    collect_system

    local table_name pid pid_file status cpu rss_kb etimes ps_row log_excerpt
    for table_name in "${TABLES[@]}"; do
      pid_file="${STATE_DIR}/${table_name}.pid"
      pid=""
      status="stopped"
      cpu=""
      rss_kb=""
      etimes=""

      pid="$(find_live_pid "${table_name}")"

      if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
        status="running"
        ps_row="$(ps -p "${pid}" -o %cpu=,rss=,etimes= 2>/dev/null | awk 'NF {print $1, $2, $3}')"
        if [[ -n "${ps_row}" ]]; then
          read -r cpu rss_kb etimes <<< "${ps_row}"
        fi
      fi

      log_excerpt="$(extract_log_line "${table_name}")"
      write_table_json "${table_name}" "${pid}" "${status}" "${cpu}" "${rss_kb}" "${etimes}" "${log_excerpt}"
    done

    sleep "${INTERVAL}"
  done
}

main "$@"
