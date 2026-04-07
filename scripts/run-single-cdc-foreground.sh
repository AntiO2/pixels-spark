#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <table-name>" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TABLE_NAME="$1"
STATE_DIR="/tmp/hybench_sf10_cdc_state"
LOG_DIR="/tmp/hybench_sf10_cdc_logs"
CKPT_ROOT="/tmp/hybench_sf10_cdc_ckpt"
DEFAULT_ENV_FILE="${ROOT_DIR}/scripts/hybench-sf10-cdc.env"
DEFAULT_PIXELS_SPARK_CONFIG="${ROOT_DIR}/etc/pixels-spark.properties"

if [[ -n "${CDC_ENV_FILE:-}" ]]; then
  source "${CDC_ENV_FILE}"
elif [[ -f "${DEFAULT_ENV_FILE}" ]]; then
  source "${DEFAULT_ENV_FILE}"
fi

TARGET_ROOT="${TARGET_ROOT:-s3a://home-zinuo/deltalake/hybench_sf10}"
DATABASE="${DATABASE:-pixels_bench}"
BUCKETS="${BUCKETS:-0,1,2,3}"
RPC_HOST="${RPC_HOST:-127.0.0.1}"
RPC_PORT="${RPC_PORT:-9091}"
METADATA_HOST="${METADATA_HOST:-127.0.0.1}"
METADATA_PORT="${METADATA_PORT:-18888}"
SPARK_MASTER="${SPARK_MASTER:-local[2]}"
TRIGGER_MODE="${TRIGGER_MODE:-processing-time}"
TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-10 seconds}"
DELETE_MODE="${DELETE_MODE:-hard}"

mkdir -p "${STATE_DIR}" "${LOG_DIR}" "${CKPT_ROOT}" "/tmp/spark-events"

source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh
export JAVA_HOME="${JAVA17_HOME}"
export AWS_ACCESS_KEY_ID
AWS_ACCESS_KEY_ID="$(awk -F= '/aws_access_key_id/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_SECRET_ACCESS_KEY
AWS_SECRET_ACCESS_KEY="$(awk -F= '/aws_secret_access_key/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_REGION="${AWS_REGION:-us-east-2}"
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${DEFAULT_PIXELS_SPARK_CONFIG}}"

echo "$$" > "${STATE_DIR}/${TABLE_NAME}.pid"

submit_args="--conf spark.eventLog.enabled=true"
submit_args+=" --conf spark.eventLog.dir=file:///tmp/spark-events"
submit_args+=" --conf spark.sql.streaming.metricsEnabled=true"
submit_args+=" --conf spark.sql.shuffle.partitions=8"
submit_args+=" --conf spark.default.parallelism=8"
submit_args+=" --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
submit_args+=" --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
submit_args+=" --conf spark.hadoop.fs.s3a.endpoint=s3.us-east-2.amazonaws.com"
submit_args+=" --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true"
submit_args+=" --conf spark.hadoop.fs.s3a.path.style.access=false"

export SPARK_SUBMIT_EXTRA_ARGS="${submit_args}"

exec "${ROOT_DIR}/scripts/run-delta-merge.sh" \
  --spark-master "${SPARK_MASTER}" \
  --database "${DATABASE}" \
  --table "${TABLE_NAME}" \
  --buckets "${BUCKETS}" \
  --rpc-host "${RPC_HOST}" \
  --rpc-port "${RPC_PORT}" \
  --metadata-host "${METADATA_HOST}" \
  --metadata-port "${METADATA_PORT}" \
  --target-path "${TARGET_ROOT}/${TABLE_NAME}" \
  --checkpoint-location "${CKPT_ROOT}/${TABLE_NAME}" \
  --trigger-mode "${TRIGGER_MODE}" \
  --trigger-interval "${TRIGGER_INTERVAL}" \
  --delete-mode "${DELETE_MODE}" \
  --auto-create-table true
