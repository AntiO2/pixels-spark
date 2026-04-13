#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"

TMP_ROOT="$(pixels_get_property pixels.tmp.root /home/ubuntu/disk1/tmp)"
PROFILE_RAW="${PROFILE:-hybench_sf10}"
PROFILE_NORMALIZED="$(
  printf '%s' "${PROFILE_RAW}" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
)"
STATE_DIR=""
LOG_DIR=""
METRICS_DIR=""
RESOURCE_DIR=""
RESOURCE_FILE="$(pixels_get_property pixels.cdc.resource-file resource_cdc.csv)"
INTERVAL="${1:-5}"
CDC_TABLES_RAW=""
NETWORK_INTERFACE="${PIXELS_CDC_NETWORK_INTERFACE:-$(pixels_get_property pixels.cdc.network-interface auto)}"
DISK_DEVICE="${PIXELS_CDC_DISK_DEVICE:-$(pixels_get_property pixels.cdc.disk-device auto)}"
TABLES=()

SYSTEM_CSV_HEADER="ts,load1,mem_used_mb,mem_avail_mb,disk_used_pct,net_rx_mbps,net_tx_mbps,disk_read_mbps,disk_write_mbps"
RESOURCE_CSV_HEADER="time,cpu,jvm_heap,jvm_managed,jvm_direct,jvm_noheap,net_rx_mbps,net_tx_mbps,disk_read_mbps,disk_write_mbps"
PREV_NET_RX_BYTES=0
PREV_NET_TX_BYTES=0
PREV_DISK_READ_SECTORS=0
PREV_DISK_WRITE_SECTORS=0
PRIMARY_IFACE=""
PRIMARY_DISK=""
DISK_SECTOR_BYTES=512
CURRENT_NET_RX_MBPS="0.00"
CURRENT_NET_TX_MBPS="0.00"
CURRENT_DISK_READ_MBPS="0.00"
CURRENT_DISK_WRITE_MBPS="0.00"

resolve_profile() {
  case "${PROFILE_NORMALIZED}" in
    hybench_sf10|hybench10|sf10)
      STATE_DIR="$(pixels_get_property pixels.cdc.hybench.sf10.state-dir "$(pixels_get_property pixels.cdc.state-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_state)")"
      LOG_DIR="$(pixels_get_property pixels.cdc.hybench.sf10.log-dir "$(pixels_get_property pixels.cdc.log-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_logs)")"
      METRICS_DIR="$(pixels_get_property pixels.cdc.hybench.sf10.metrics-dir "$(pixels_get_property pixels.cdc.metrics-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_metrics)")"
      RESOURCE_DIR="$(pixels_get_property pixels.cdc.hybench.sf10.resource-dir "$(pixels_get_property pixels.cdc.resource-dir "${ROOT_DIR}/data/hybench/sf10/resource")")"
      CDC_TABLES_RAW="$(pixels_get_property pixels.cdc.tables customer,company,savingaccount,checkingaccount,transfer,checking,loanapps,loantrans)"
      ;;
    hybench_sf1000|hybench1000|sf1000)
      STATE_DIR="$(pixels_get_property pixels.cdc.hybench.sf1000.state-dir /home/ubuntu/disk1/tmp/hybench_sf1000_cdc_state)"
      LOG_DIR="$(pixels_get_property pixels.cdc.hybench.sf1000.log-dir /home/ubuntu/disk1/tmp/hybench_sf1000_cdc_logs)"
      METRICS_DIR="$(pixels_get_property pixels.cdc.hybench.sf1000.metrics-dir /home/ubuntu/disk1/tmp/hybench_sf1000_cdc_metrics)"
      RESOURCE_DIR="$(pixels_get_property pixels.cdc.hybench.sf1000.resource-dir "${ROOT_DIR}/data/hybench/sf1000/resource")"
      CDC_TABLES_RAW="$(pixels_get_property pixels.cdc.tables customer,company,savingaccount,checkingaccount,transfer,checking,loanapps,loantrans)"
      ;;
    chbenchmark_wh10000|chbenchmark_w10000|chbenchmark10000|wh10000|w10000)
      STATE_DIR="$(pixels_get_property pixels.cdc.chbenchmark.w10000.state-dir /home/ubuntu/disk1/tmp/chbenchmark_w10000_cdc_state)"
      LOG_DIR="$(pixels_get_property pixels.cdc.chbenchmark.w10000.log-dir /home/ubuntu/disk1/tmp/chbenchmark_w10000_cdc_logs)"
      METRICS_DIR="$(pixels_get_property pixels.cdc.chbenchmark.w10000.metrics-dir /home/ubuntu/disk1/tmp/chbenchmark_w10000_cdc_metrics)"
      RESOURCE_DIR="$(pixels_get_property pixels.cdc.chbenchmark.w10000.resource-dir "${ROOT_DIR}/data/chbenchmark/w10000/resource")"
      CDC_TABLES_RAW="$(pixels_get_property pixels.import.chbenchmark.tables warehouse,district,customer,history,neworder,order,orderline,item,stock,nation,supplier,region)"
      ;;
    *)
      echo "Unsupported PROFILE=${PROFILE_RAW}. Use hybench_sf10, hybench_sf1000, or chbenchmark_w10000." >&2
      exit 1
      ;;
  esac
}

resolve_profile
pixels_split_csv_property "${CDC_TABLES_RAW}" TABLES
SYSTEM_CSV="${METRICS_DIR}/system.csv"
RESOURCE_CSV="${RESOURCE_DIR}/${RESOURCE_FILE}"

mkdir -p "${STATE_DIR}" "${LOG_DIR}" "${METRICS_DIR}" "${RESOURCE_DIR}"

ensure_csv_header() {
  local path="$1"
  local header="$2"
  if [[ ! -f "${path}" ]] || [[ "$(head -n 1 "${path}" 2>/dev/null || true)" != "${header}" ]]; then
    printf '%s\n' "${header}" > "${path}"
  fi
}

ensure_csv_header "${SYSTEM_CSV}" "${SYSTEM_CSV_HEADER}"
ensure_csv_header "${RESOURCE_CSV}" "${RESOURCE_CSV_HEADER}"

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

detect_primary_interface() {
  local route_iface
  route_iface="$(ip route show default 2>/dev/null | awk 'NR==1 {for (i=1; i<=NF; i++) if ($i == "dev") {print $(i+1); exit}}')"
  if [[ -n "${route_iface}" ]]; then
    printf '%s\n' "${route_iface}"
    return 0
  fi

  awk -F: '
    $1 !~ /lo/ {
      gsub(/^[ \t]+/, "", $1);
      print $1;
      exit
    }
  ' /proc/net/dev 2>/dev/null || true
}

detect_primary_disk() {
  local source_device base_device parent_device
  source_device="$(df -P "${TMP_ROOT}" 2>/dev/null | awk 'NR==2 {print $1}')"
  if [[ "${source_device}" == /dev/* ]]; then
    base_device="${source_device#/dev/}"
    parent_device="$(lsblk -no PKNAME "/dev/${base_device}" 2>/dev/null | awk 'NF {print $1; exit}')"
    if [[ -n "${parent_device}" ]]; then
      printf '%s\n' "${parent_device}"
      return 0
    fi
    printf '%s\n' "${base_device}"
    return 0
  fi

  awk '$3 == "disk" {print $1; exit}' /proc/diskstats 2>/dev/null || true
}

init_io_targets() {
  if [[ -z "${PRIMARY_IFACE}" ]]; then
    if [[ "${NETWORK_INTERFACE}" == "auto" ]]; then
      PRIMARY_IFACE="$(detect_primary_interface)"
    else
      PRIMARY_IFACE="${NETWORK_INTERFACE}"
    fi
  fi

  if [[ -z "${PRIMARY_DISK}" ]]; then
    if [[ "${DISK_DEVICE}" == "auto" ]]; then
      PRIMARY_DISK="$(detect_primary_disk)"
    else
      PRIMARY_DISK="${DISK_DEVICE#/dev/}"
    fi
  fi
}

read_network_bytes() {
  local iface="$1"
  if [[ -z "${iface}" ]]; then
    printf '0 0\n'
    return 0
  fi

  awk -v iface="${iface}" -F'[: ]+' '
    $2 == iface {
      print $3, $11;
      found = 1;
      exit
    }
    END {
      if (!found) print "0 0";
    }
  ' /proc/net/dev
}

read_disk_sectors() {
  local disk="$1"
  if [[ -z "${disk}" ]]; then
    printf '0 0\n'
    return 0
  fi

  awk -v disk="${disk}" '
    $3 == disk {
      print $6, $10;
      found = 1;
      exit
    }
    END {
      if (!found) print "0 0";
    }
  ' /proc/diskstats
}

format_rate_mbps() {
  local current="$1"
  local previous="$2"
  local interval="$3"
  awk -v current="${current:-0}" -v previous="${previous:-0}" -v interval="${interval:-1}" '
    BEGIN {
      if (interval <= 0) {
        printf "0.00";
      } else {
        delta = current - previous;
        if (delta < 0) delta = 0;
        printf "%.2f", (delta * 8.0) / (interval * 1000000.0);
      }
    }
  '
}

format_sector_rate_mbps() {
  local current="$1"
  local previous="$2"
  local interval="$3"
  local sector_bytes="$4"
  awk -v current="${current:-0}" -v previous="${previous:-0}" -v interval="${interval:-1}" -v sector_bytes="${sector_bytes:-512}" '
    BEGIN {
      if (interval <= 0) {
        printf "0.00";
      } else {
        delta = current - previous;
        if (delta < 0) delta = 0;
        bytes = delta * sector_bytes;
        printf "%.2f", (bytes * 8.0) / (interval * 1000000.0);
      }
    }
  '
}

sample_io_rates() {
  init_io_targets

  local net_rx_bytes net_tx_bytes disk_read_sectors disk_write_sectors
  read -r net_rx_bytes net_tx_bytes <<< "$(read_network_bytes "${PRIMARY_IFACE}")"
  read -r disk_read_sectors disk_write_sectors <<< "$(read_disk_sectors "${PRIMARY_DISK}")"

  CURRENT_NET_RX_MBPS="0.00"
  CURRENT_NET_TX_MBPS="0.00"
  CURRENT_DISK_READ_MBPS="0.00"
  CURRENT_DISK_WRITE_MBPS="0.00"

  if (( PREV_NET_RX_BYTES > 0 || PREV_NET_TX_BYTES > 0 || PREV_DISK_READ_SECTORS > 0 || PREV_DISK_WRITE_SECTORS > 0 )); then
    CURRENT_NET_RX_MBPS="$(format_rate_mbps "${net_rx_bytes}" "${PREV_NET_RX_BYTES}" "${INTERVAL}")"
    CURRENT_NET_TX_MBPS="$(format_rate_mbps "${net_tx_bytes}" "${PREV_NET_TX_BYTES}" "${INTERVAL}")"
    CURRENT_DISK_READ_MBPS="$(format_sector_rate_mbps "${disk_read_sectors}" "${PREV_DISK_READ_SECTORS}" "${INTERVAL}" "${DISK_SECTOR_BYTES}")"
    CURRENT_DISK_WRITE_MBPS="$(format_sector_rate_mbps "${disk_write_sectors}" "${PREV_DISK_WRITE_SECTORS}" "${INTERVAL}" "${DISK_SECTOR_BYTES}")"
  fi

  PREV_NET_RX_BYTES="${net_rx_bytes}"
  PREV_NET_TX_BYTES="${net_tx_bytes}"
  PREV_DISK_READ_SECTORS="${disk_read_sectors}"
  PREV_DISK_WRITE_SECTORS="${disk_write_sectors}"

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
  printf '%s,%s%%,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(date '+%Y/%-m/%-d %H:%M:%S')" \
    "${total_cpu}" \
    "$(format_mib_as_gib_or_mib "${total_heap_mib}")" \
    "$(format_mib_as_gib_or_mib "${total_managed_mib}")" \
    "$(format_mib_as_gib_or_mib "${total_direct_mib}")" \
    "$(format_mib_as_gib_or_mib "${total_noheap_mib}")" \
    "${CURRENT_NET_RX_MBPS}" \
    "${CURRENT_NET_TX_MBPS}" \
    "${CURRENT_DISK_READ_MBPS}" \
    "${CURRENT_DISK_WRITE_MBPS}" \
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
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "${load1}" \
    "${mem_used}" \
    "${mem_avail}" \
    "${disk_pct}" \
    "${CURRENT_NET_RX_MBPS}" \
    "${CURRENT_NET_TX_MBPS}" \
    "${CURRENT_DISK_READ_MBPS}" \
    "${CURRENT_DISK_WRITE_MBPS}" \
    >> "${SYSTEM_CSV}"
}

main() {
  while true; do
    sample_io_rates
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
