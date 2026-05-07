#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <table-name>" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TABLE_NAME="$1"
DEFAULT_PIXELS_SPARK_CONFIG="${ROOT_DIR}/etc/pixels-spark.properties"
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${DEFAULT_PIXELS_SPARK_CONFIG}}"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"

STATE_DIR="${STATE_DIR:-$(pixels_get_property pixels.cdc.state-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_state)}"
LOG_DIR="${LOG_DIR:-$(pixels_get_property pixels.cdc.log-dir /home/ubuntu/disk1/tmp/hybench_sf10_cdc_logs)}"
CKPT_ROOT="${CKPT_ROOT:-$(pixels_get_property pixels.cdc.checkpoint-root /home/ubuntu/disk1/tmp/hybench_sf10_cdc_ckpt)}"
SPARK_EVENTS_DIR="${SPARK_EVENTS_DIR:-$(pixels_get_property pixels.spark.event-log.dir /home/ubuntu/disk1/tmp/spark-events)}"
TARGET_ROOT="${TARGET_ROOT:-$(pixels_get_property pixels.spark.delta.target.path s3a://home-zinuo/deltalake/hybench_sf10)}"
DATABASE="${DATABASE:-$(pixels_get_property pixels.cdc.database pixels_bench)}"
CDC_BENCHMARK="${CDC_BENCHMARK:-$(pixels_get_property pixels.cdc.benchmark hybench)}"
RPC_HOST="${RPC_HOST:-$(pixels_get_property pixels.spark.rpc.host 127.0.0.1)}"
RPC_PORT="${RPC_PORT:-$(pixels_get_property pixels.spark.rpc.port 9091)}"
METADATA_HOST="${METADATA_HOST:-$(pixels_get_property pixels.spark.metadata.host 127.0.0.1)}"
METADATA_PORT="${METADATA_PORT:-$(pixels_get_property pixels.spark.metadata.port 18888)}"
SPARK_MASTER="${SPARK_MASTER:-$(pixels_get_property pixels.spark.master local[2])}"
MODE="${MODE:-$(pixels_get_property pixels.spark.cdc.execution.mode "$(pixels_get_property pixels.spark.delta.mode polling)")}"
TRIGGER_MODE="${TRIGGER_MODE:-$(pixels_get_property pixels.spark.delta.trigger.mode processing-time)}"
TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-$(pixels_get_property pixels.spark.delta.trigger.interval 10 seconds)}"
DELETE_MODE="${DELETE_MODE:-$(pixels_get_property pixels.spark.delta.delete.mode hard)}"
SINK_MODE="${SINK_MODE:-$(pixels_get_property pixels.spark.sink.mode delta)}"
NOOP_BUCKETS="${NOOP_BUCKETS:-$(pixels_get_property pixels.spark.delta.noop-buckets "")}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-$(pixels_get_property pixels.spark.driver.memory 8g)}"
SPARK_LOCAL_PARALLELISM="${SPARK_LOCAL_PARALLELISM:-}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-$(pixels_get_property pixels.spark.shuffle.partitions "")}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-$(pixels_get_property pixels.spark.default.parallelism "")}"
SPARK_SQL_FILES_MAX_PARTITION_BYTES="${SPARK_SQL_FILES_MAX_PARTITION_BYTES:-$(pixels_get_property pixels.spark.sql.files.max-partition-bytes 268435456)}"
SPARK_STREAMING_METRICS_ENABLED="${SPARK_STREAMING_METRICS_ENABLED:-$(pixels_get_property pixels.spark.streaming.metrics.enabled true)}"

if [[ -z "${SPARK_LOCAL_PARALLELISM}" ]]; then
  SPARK_LOCAL_PARALLELISM="$(infer_local_parallelism "${SPARK_MASTER}")"
fi
if [[ -z "${SPARK_SHUFFLE_PARTITIONS}" ]]; then
  SPARK_SHUFFLE_PARTITIONS="${SPARK_LOCAL_PARALLELISM}"
fi
if [[ -z "${SPARK_DEFAULT_PARALLELISM}" ]]; then
  SPARK_DEFAULT_PARALLELISM="${SPARK_LOCAL_PARALLELISM}"
fi

mkdir -p "${STATE_DIR}" "${LOG_DIR}" "${CKPT_ROOT}" "${SPARK_EVENTS_DIR}"

source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh
export JAVA_HOME="${JAVA17_HOME}"
export PIXELS_HOME="${PIXELS_HOME:-/home/ubuntu/opt/pixels}"
export PIXELS_CONFIG="${PIXELS_CONFIG:-${PIXELS_HOME}/etc/pixels.properties}"
export AWS_ACCESS_KEY_ID
AWS_ACCESS_KEY_ID="$(trim "$(awk -F= '/aws_access_key_id/ {print $2}' /home/ubuntu/.aws/credentials)")"
export AWS_SECRET_ACCESS_KEY
AWS_SECRET_ACCESS_KEY="$(trim "$(awk -F= '/aws_secret_access_key/ {print $2}' /home/ubuntu/.aws/credentials)")"
export AWS_REGION="${AWS_REGION:-us-east-2}"

echo "$$" > "${STATE_DIR}/${TABLE_NAME}.pid"
cleanup() {
  rm -f "${STATE_DIR}/${TABLE_NAME}.pid"
}
trap cleanup EXIT

submit_args="--conf spark.eventLog.enabled=true"
submit_args+=" --driver-memory ${SPARK_DRIVER_MEMORY}"
submit_args+=" --conf spark.eventLog.dir=file://${SPARK_EVENTS_DIR}"
submit_args+=" --conf spark.sql.streaming.metricsEnabled=${SPARK_STREAMING_METRICS_ENABLED}"
submit_args+=" --conf spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}"
submit_args+=" --conf spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}"
submit_args+=" --conf spark.sql.files.maxPartitionBytes=${SPARK_SQL_FILES_MAX_PARTITION_BYTES}"
submit_args+=" --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
submit_args+=" --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
submit_args+=" --conf spark.hadoop.fs.s3a.endpoint=s3.us-east-2.amazonaws.com"
submit_args+=" --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true"
submit_args+=" --conf spark.hadoop.fs.s3a.path.style.access=false"

# Keep submit-time SQL extensions aligned with sink mode so parser/planner
# behavior is deterministic even when external Spark defaults are present.
if [[ "${SINK_MODE}" == "hudi" ]]; then
  submit_args+=" --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
  submit_args+=" --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
  submit_args+=" --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog"
elif [[ "${SINK_MODE}" == "delta" ]]; then
  submit_args+=" --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
  submit_args+=" --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
fi

export SPARK_SUBMIT_EXTRA_ARGS="${submit_args}"

exec "${ROOT_DIR}/scripts/run-delta-merge.sh" \
  --spark-master "${SPARK_MASTER}" \
  --mode "${MODE}" \
  --database "${DATABASE}" \
  --table "${TABLE_NAME}" \
  --benchmark "${CDC_BENCHMARK}" \
  --rpc-host "${RPC_HOST}" \
  --rpc-port "${RPC_PORT}" \
  --metadata-host "${METADATA_HOST}" \
  --metadata-port "${METADATA_PORT}" \
  --target-path "${TARGET_ROOT}/${TABLE_NAME}" \
  --checkpoint-location "${CKPT_ROOT}/${TABLE_NAME}" \
  --trigger-mode "${TRIGGER_MODE}" \
  --trigger-interval "${TRIGGER_INTERVAL}" \
  --sink-mode "${SINK_MODE}" \
  ${NOOP_BUCKETS:+--noop-buckets "${NOOP_BUCKETS}"} \
  --delete-mode "${DELETE_MODE}" \
  --auto-create-table true
