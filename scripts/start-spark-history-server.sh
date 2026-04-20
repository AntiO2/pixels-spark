#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPT_ROOT="${OPT_ROOT:-/home/ubuntu/disk1/opt}"
PIXELS_DELTA_ENV="${PIXELS_DELTA_ENV:-${OPT_ROOT}/conf/pixels-delta-env.sh}"
SPARK_EVENTS_DIR="${SPARK_EVENTS_DIR:-/home/ubuntu/disk1/tmp/spark-events}"
SPARK_HISTORY_PORT="${SPARK_HISTORY_PORT:-18080}"

if [[ -f "${PIXELS_DELTA_ENV}" ]]; then
  # shellcheck disable=SC1091
  source "${PIXELS_DELTA_ENV}"
fi

if [[ -z "${SPARK_HOME:-}" ]]; then
  SPARK_HOME="${OPT_ROOT}/spark-3.5.6-bin-hadoop3"
fi

if [[ -n "${JAVA17_HOME:-}" ]]; then
  export JAVA_HOME="${JAVA17_HOME}"
fi

if [[ ! -x "${SPARK_HOME}/sbin/start-history-server.sh" ]]; then
  echo "Missing Spark history server launcher under ${SPARK_HOME}" >&2
  exit 1
fi

is_port_open() {
  local host="$1"
  local port="$2"
  nc -z "${host}" "${port}" >/dev/null 2>&1
}

mkdir -p "${SPARK_EVENTS_DIR}"
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file://${SPARK_EVENTS_DIR} -Dspark.history.ui.port=${SPARK_HISTORY_PORT}"

if is_port_open 127.0.0.1 "${SPARK_HISTORY_PORT}"; then
  echo "Spark History Server already running at http://127.0.0.1:${SPARK_HISTORY_PORT}"
  exit 0
fi

"${SPARK_HOME}/sbin/start-history-server.sh"

for _ in $(seq 1 30); do
  if is_port_open 127.0.0.1 "${SPARK_HISTORY_PORT}"; then
    echo "Spark History Server started at http://127.0.0.1:${SPARK_HISTORY_PORT}"
    exit 0
  fi
  sleep 1
done

echo "Spark History Server did not become ready on port ${SPARK_HISTORY_PORT}" >&2
exit 1
