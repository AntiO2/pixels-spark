#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${SPARK_SUBMIT_BIN:-}" ]]; then
  :
elif [[ -n "${SPARK_HOME:-}" ]]; then
  SPARK_SUBMIT_BIN="${SPARK_HOME}/bin/spark-submit"
else
  SPARK_SUBMIT_BIN="spark-submit"
fi
JAR_PATH="${ROOT_DIR}/target/pixels-spark-0.1.jar"

if ! command -v "${SPARK_SUBMIT_BIN}" >/dev/null 2>&1 && [[ ! -x "${SPARK_SUBMIT_BIN}" ]]; then
  echo "spark-submit not found. Set SPARK_HOME or SPARK_SUBMIT_BIN." >&2
  exit 1
fi

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "Missing shaded jar at ${JAR_PATH}. Run scripts/build-package.sh first." >&2
  exit 1
fi

exec "${SPARK_SUBMIT_BIN}" \
  --class io.pixelsdb.spark.app.PixelsDeltaMergeApp \
  "${JAR_PATH}" \
  "$@"
