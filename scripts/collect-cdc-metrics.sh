#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"

STATE_DIR="$(pixels_get_property pixels.cdc.state-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_state)"
LOG_DIR="$(pixels_get_property pixels.cdc.log-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_logs)"
METRICS_DIR="$(pixels_get_property pixels.cdc.metrics-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_metrics)"
TMP_ROOT="$(pixels_get_property pixels.tmp.root /home/ubuntu/disk1/tmp)"
RESOURCE_DIR="$(pixels_get_property pixels.cdc.resource-dir "${ROOT_DIR}/data/hybench/sf10/resource")"
RESOURCE_FILE="$(pixels_get_property pixels.cdc.resource-file resource_cdc.csv)"
SYSTEM_CSV="${METRICS_DIR}/system.csv"
RESOURCE_CSV="${RESOURCE_DIR}/${RESOURCE_FILE}"
INTERVAL="${1:-5}"
CDC_TABLES_RAW="$(pixels_get_property pixels.cdc.tables customer,company,savingaccount,checkingaccount,transfer,checking,loanapps,loantrans)"
TABLES=()
pixels_split_csv_property "${CDC_TABLES_RAW}" TABLES

mkdir -p "${STATE_DIR}" "${LOG_DIR}" "${METRICS_DIR}" "${RESOURCE_DIR}"

if [[ ! -f "${SYSTEM_CSV}" ]]; then
  echo "ts,load1,mem_used_mb,mem_avail_mb,disk_used_pct" > "${SYSTEM_CSV}"
fi

if [[ ! -f "${RESOURCE_CSV}" ]]; then
  echo "time,cpu,jvm_heap,jvm_managed,jvm_direct,jvm_noheap" > "${RESOURCE_CSV}"
fi

format_mib_as_gib_or_mib() {
  local mib="$1"
  if [[ -z "${mib}" ]]; then
    printf '0 MiB'
    return 0
  fi

  awk -v mib="${mib}" '
    BEGIN {
      if (mib >= 1024) {
        printf "%.2f GiB", mib / 1024.0;
      } else {
        printf "%.0f MiB", mib;
      }
    }
  '
}

extract_xmx_mib() {
  local pid="$1"
  local cmdline
  cmdline="$(ps -ww -p "${pid}" -o args= 2>/dev/null || true)"
  if [[ "${cmdline}" =~ -Xmx([0-9]+)([gGmMkK]) ]]; then
    local value="${BASH_REMATCH[1]}"
    local unit="${BASH_REMATCH[2]}"
    case "${unit}" in
      g|G) echo $(( value * 1024 )) ;;
      m|M) echo "${value}" ;;
      k|K) echo $(( value / 1024 )) ;;
      *) echo 0 ;;
    esac
    return 0
  fi
  echo 0
}

extract_jvm_heap_and_noheap_mib() {
  local pid="$1"
  local heap_info heap_kb metaspace_kb class_kb
  heap_info="$(jcmd "${pid}" GC.heap_info 2>/dev/null || true)"
  heap_kb="$(printf '%s\n' "${heap_info}" | awk '/garbage-first heap/ {for (i=1; i<=NF; i++) if ($i == "used") {value=$(i+1); gsub(/[^0-9]/, "", value); print value; exit}}')"
  metaspace_kb="$(printf '%s\n' "${heap_info}" | awk '/Metaspace/ {for (i=1; i<=NF; i++) if ($i == "used") {value=$(i+1); gsub(/[^0-9]/, "", value); print value; exit}}')"
  class_kb="$(printf '%s\n' "${heap_info}" | awk '/class space/ {for (i=1; i<=NF; i++) if ($i == "used") {value=$(i+1); gsub(/[^0-9]/, "", value); print value; exit}}')"

  local heap_mib=$(( ${heap_kb:-0} / 1024 ))
  local noheap_mib=$(( (${metaspace_kb:-0} + ${class_kb:-0}) / 1024 ))
  printf '%s %s\n' "${heap_mib}" "${noheap_mib}"
}

collect_resource_csv() {
  local total_cpu="0"
  local total_heap_mib=0
  local total_managed_mib=0
  local total_direct_mib=0
  local total_noheap_mib=0
  local table_name pid ps_cpu heap_mib noheap_mib xmx_mib

  for table_name in "${TABLES[@]}"; do
    pid="$(find_live_pid "${table_name}")"
    if [[ -z "${pid}" ]] || ! kill -0 "${pid}" 2>/dev/null; then
      continue
    fi

    ps_cpu="$(ps -p "${pid}" -o %cpu= 2>/dev/null | awk 'NF {print $1; exit}')"
    total_cpu="$(awk -v a="${total_cpu}" -v b="${ps_cpu:-0}" 'BEGIN {printf "%.2f", a + b}')"

    read -r heap_mib noheap_mib <<< "$(extract_jvm_heap_and_noheap_mib "${pid}")"
    xmx_mib="$(extract_xmx_mib "${pid}")"

    total_heap_mib=$(( total_heap_mib + ${heap_mib:-0} ))
    total_noheap_mib=$(( total_noheap_mib + ${noheap_mib:-0} ))
    total_managed_mib=$(( total_managed_mib + ${xmx_mib:-0} ))
  done

  printf '%s,%s%%,%s,%s,%s,%s\n' \
    "$(date '+%Y/%-m/%-d %H:%M:%S')" \
    "${total_cpu}" \
    "$(format_mib_as_gib_or_mib "${total_heap_mib}")" \
    "$(format_mib_as_gib_or_mib "${total_managed_mib}")" \
    "$(format_mib_as_gib_or_mib "${total_direct_mib}")" \
    "$(format_mib_as_gib_or_mib "${total_noheap_mib}")" \
    >> "${RESOURCE_CSV}"
}

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
  disk_pct="$(df -P "${TMP_ROOT}" | awk 'NR==2 {gsub(/%/, "", $5); print $5}')"
  printf '%s,%s,%s,%s,%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${load1}" "${mem_used}" "${mem_avail}" "${disk_pct}" >> "${SYSTEM_CSV}"
}

main() {
  while true; do
    collect_system
    collect_resource_csv

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
