#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${ROOT_DIR}/etc/pixels-spark.properties}"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"

if [[ -n "${SPARK_SUBMIT_BIN:-}" ]]; then
  :
elif [[ -n "${SPARK_HOME:-}" ]]; then
  SPARK_SUBMIT_BIN="${SPARK_HOME}/bin/spark-submit"
else
  SPARK_SUBMIT_BIN="spark-submit"
fi

JAR_PATH="${ROOT_DIR}/target/pixels-spark-0.1.jar"
TARGET_ROOT="${TARGET_ROOT:-$(pixels_get_property pixels.cdc.chbenchmark.w10000.target-root "$(pixels_get_property pixels.import.chbenchmark.w10000.target-root s3a://home-zinuo/deltalake/chbenchmark_w10000)")}"
SPARK_MASTER="${SPARK_MASTER:-$(pixels_get_property pixels.spark.master local[2])}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-32g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-16g}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-64}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-16}"
SPARK_FILES_MAX_PARTITION_BYTES="${SPARK_FILES_MAX_PARTITION_BYTES:-268435456}"
SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS:-/home/ubuntu/disk1/tmp/spark-local}"
COMPACT_BEFORE_ZORDER="${COMPACT_BEFORE_ZORDER:-1}"
CHECKPOINT_AFTER_OPTIMIZE="${CHECKPOINT_AFTER_OPTIMIZE:-1}"
TABLES=("$@")

if ! command -v "${SPARK_SUBMIT_BIN}" >/dev/null 2>&1 && [[ ! -x "${SPARK_SUBMIT_BIN}" ]]; then
  echo "spark-submit not found. Set SPARK_HOME or SPARK_SUBMIT_BIN." >&2
  exit 1
fi

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "Missing shaded jar at ${JAR_PATH}. Run scripts/build-package.sh first." >&2
  exit 1
fi

source /home/ubuntu/disk1/opt/conf/pixels-delta-env.sh
export JAVA_HOME="${JAVA17_HOME}"
mkdir -p "${SPARK_LOCAL_DIRS}"
export SPARK_LOCAL_DIRS
export AWS_ACCESS_KEY_ID
AWS_ACCESS_KEY_ID="$(awk -F= '/aws_access_key_id/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_SECRET_ACCESS_KEY
AWS_SECRET_ACCESS_KEY="$(awk -F= '/aws_secret_access_key/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_REGION="${AWS_REGION:-us-east-2}"

declare -A ZORDER_COLUMNS=(
  [warehouse]="w_id"
  [district]="d_w_id,d_id"
  [customer]="c_w_id,c_d_id,c_id,c_last"
  [history]="h_c_w_id,h_c_d_id,h_c_id,h_w_id,h_d_id"
  [neworder]="no_w_id,no_d_id,no_o_id"
  [order]="o_w_id,o_d_id,o_id,o_c_id,o_carrier_id"
  [orderline]="ol_w_id,ol_d_id,ol_o_id,ol_i_id"
  [item]="i_id,i_im_id"
  [stock]="s_w_id,s_i_id,s_quantity,s_su_suppkey"
  [nation]="n_nationkey,n_regionkey"
  [supplier]="su_suppkey,su_nationkey"
  [region]="r_regionkey"
)

if [[ ${#TABLES[@]} -eq 0 ]]; then
  TABLES=(warehouse district customer history neworder order orderline item stock nation supplier region)
fi

for table_name in "${TABLES[@]}"; do
  if [[ -z "${ZORDER_COLUMNS[${table_name}]:-}" ]]; then
    echo "Unsupported table: ${table_name}" >&2
    exit 1
  fi

  delta_path="${TARGET_ROOT}/${table_name}"
  zorder_cols="${ZORDER_COLUMNS[${table_name}]}"

  echo "optimize table=${table_name} delta_path=${delta_path} zorder_by=${zorder_cols}"
  echo "spark_master=${SPARK_MASTER} driver_memory=${SPARK_DRIVER_MEMORY} executor_memory=${SPARK_EXECUTOR_MEMORY} shuffle_partitions=${SPARK_SQL_SHUFFLE_PARTITIONS} default_parallelism=${SPARK_DEFAULT_PARALLELISM} spark_local_dirs=${SPARK_LOCAL_DIRS}"
  "${SPARK_SUBMIT_BIN}" \
    --driver-memory "${SPARK_DRIVER_MEMORY}" \
    --executor-memory "${SPARK_EXECUTOR_MEMORY}" \
    --conf "spark.local.dir=${SPARK_LOCAL_DIRS}" \
    --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS}" \
    --conf "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}" \
    --conf "spark.sql.files.maxPartitionBytes=${SPARK_FILES_MAX_PARTITION_BYTES}" \
    --class io.pixelsdb.spark.app.PixelsDeltaOptimizeApp \
    "${JAR_PATH}" \
    "${delta_path}" \
    "${zorder_cols}" \
    "${SPARK_MASTER}" \
    "${COMPACT_BEFORE_ZORDER}" \
    "${CHECKPOINT_AFTER_OPTIMIZE}"
done
