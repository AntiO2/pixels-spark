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
EXTRA_SUBMIT_ARGS=()
LOCAL_SUBMIT_ARGS=()
LOCAL_JARS=()

if ! command -v "${SPARK_SUBMIT_BIN}" >/dev/null 2>&1 && [[ ! -x "${SPARK_SUBMIT_BIN}" ]]; then
  echo "spark-submit not found. Set SPARK_HOME or SPARK_SUBMIT_BIN." >&2
  exit 1
fi

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "Missing shaded jar at ${JAR_PATH}. Run scripts/build-package.sh first." >&2
  exit 1
fi

if [[ -n "${SPARK_SUBMIT_EXTRA_ARGS:-}" ]]; then
  # Keep this interface simple: whitespace-delimited spark-submit flags only.
  read -r -a EXTRA_SUBMIT_ARGS <<< "${SPARK_SUBMIT_EXTRA_ARGS}"
fi

JARS_ARG=""
for local_jar in "${LOCAL_JARS[@]}"; do
  if [[ -f "${local_jar}" ]]; then
    if [[ -n "${JARS_ARG}" ]]; then
      JARS_ARG+=","
    fi
    JARS_ARG+="${local_jar}"
  fi
done

if [[ -n "${JARS_ARG}" ]]; then
  LOCAL_SUBMIT_ARGS=(--jars "${JARS_ARG}")
fi

exec "${SPARK_SUBMIT_BIN}" \
  "${EXTRA_SUBMIT_ARGS[@]}" \
  "${LOCAL_SUBMIT_ARGS[@]}" \
  --class io.pixelsdb.spark.app.PixelsDeltaMergeApp \
  "${JAR_PATH}" \
  "$@"
