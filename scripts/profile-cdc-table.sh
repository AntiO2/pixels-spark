#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${ROOT_DIR}/etc/pixels-spark.properties}"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"

PROFILE_RAW="${PROFILE:-hybench_sf10}"
PROFILE_NORMALIZED="$(
  printf '%s' "${PROFILE_RAW}" \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
)"

TABLE_NAME="${1:-savingaccount}"
DURATION_SECONDS="${DURATION_SECONDS:-30}"
EVENT="${EVENT:-cpu}"
ASYNC_PROFILER_BIN="${ASYNC_PROFILER_BIN:-/home/ubuntu/async-profiler-4.1-linux-x64/bin/asprof}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/profiles}"
TIMESTAMP="$(date -u +%Y%m%d-%H%M%S)"
OUTPUT_FILE="${OUTPUT_DIR}/${TABLE_NAME}-${PROFILE_NORMALIZED}-${EVENT}-${TIMESTAMP}.html"
STATE_DIR=""
UNIT_PREFIX=""

resolve_profile() {
  case "${PROFILE_NORMALIZED}" in
    hybench_sf10|hybench10|sf10)
      STATE_DIR="$(pixels_get_property pixels.cdc.hybench.sf10.state-dir "$(pixels_get_property pixels.cdc.state-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_state)")"
      UNIT_PREFIX="pixels-cdc"
      ;;
    hybench_sf1000|hybench1000|sf1000)
      STATE_DIR="$(pixels_get_property pixels.cdc.hybench.sf1000.state-dir /home/ubuntu/disk1/tmp/hybench_sf1000_cdc_state)"
      UNIT_PREFIX="pixels-cdc-sf1000"
      ;;
    chbenchmark_wh10000|chbenchmark_w10000|chbenchmark10000|wh10000|w10000)
      STATE_DIR="$(pixels_get_property pixels.cdc.chbenchmark.w10000.state-dir /home/ubuntu/disk1/tmp/chbenchmark_w10000_cdc_state)"
      UNIT_PREFIX="pixels-cdc-chbenchmark-w10000"
      ;;
    *)
      echo "Unsupported PROFILE=${PROFILE_RAW}. Use hybench_sf10, hybench_sf1000, or chbenchmark_w10000." >&2
      exit 1
      ;;
  esac
}

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

resolve_profile

if [[ ! -x "${ASYNC_PROFILER_BIN}" ]]; then
  echo "async-profiler not found: ${ASYNC_PROFILER_BIN}" >&2
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"

PID="$(find_live_pid "${TABLE_NAME}")"
if [[ -z "${PID}" ]]; then
  echo "No running CDC process found for table=${TABLE_NAME} profile=${PROFILE_NORMALIZED} state_dir=${STATE_DIR}" >&2
  exit 1
fi

echo "profiling table=${TABLE_NAME} profile=${PROFILE_NORMALIZED} pid=${PID} event=${EVENT} duration=${DURATION_SECONDS}s"
"${ASYNC_PROFILER_BIN}" -d "${DURATION_SECONDS}" -e "${EVENT}" -f "${OUTPUT_FILE}" "${PID}"
echo "flamegraph=${OUTPUT_FILE}"
