#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 || $# -gt 5 ]]; then
  cat <<'EOF'
Usage:
  import-benchmark-csv-to-hudi.sh <csv-path> <target-path> <spark-master> <table-name> [benchmark]

Example:
  ./scripts/import-benchmark-csv-to-hudi.sh \
    /home/antio2/data/hybench_sf1x/savingAccount.csv \
    s3://home-zinuo/hudi/hybench_sf1x/savingAccount \
    'local[4]' \
    savingaccount \
    hybench
EOF
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSV_PATH="$1"
TARGET_PATH="$2"
SPARK_MASTER="$3"
TABLE_NAME="$4"
BENCHMARK="${5:-hybench}"

SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-$(command -v spark-submit)}"
HUDI_SPARK_BUNDLE="${HUDI_SPARK_BUNDLE:-org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0}"
HADOOP_AWS_PACKAGE="${HADOOP_AWS_PACKAGE:-org.apache.hadoop:hadoop-aws:3.3.4}"
AWS_JAVA_SDK_BUNDLE="${AWS_JAVA_SDK_BUNDLE:-com.amazonaws:aws-java-sdk-bundle:1.12.262}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-4g}"
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/tmp/pixels-spark-local}"
S3A_BUFFER_DIR="${S3A_BUFFER_DIR:-/tmp/pixels-s3a-buffer}"
S3_ENDPOINT="${PIXELS_SPARK_S3_ENDPOINT:-}"
S3_PATH_STYLE="${PIXELS_SPARK_S3_PATH_STYLE:-false}"
S3_SSL_ENABLED="${PIXELS_SPARK_S3_SSL_ENABLED:-true}"
AWS_REGION_VALUE="${AWS_REGION:-us-east-2}"

mkdir -p "$SPARK_LOCAL_DIR" "$S3A_BUFFER_DIR"

COMMON_CONF=(
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog
  --conf spark.local.dir="$SPARK_LOCAL_DIR"
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain
  --conf spark.hadoop.fs.s3a.buffer.dir="$S3A_BUFFER_DIR"
  --conf spark.hadoop.fs.s3a.path.style.access="$S3_PATH_STYLE"
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled="$S3_SSL_ENABLED"
  --conf spark.hadoop.fs.s3a.endpoint.region="$AWS_REGION_VALUE"
)

if [[ -n "$S3_ENDPOINT" ]]; then
  COMMON_CONF+=(--conf spark.hadoop.fs.s3a.endpoint="$S3_ENDPOINT")
fi

"$SPARK_SUBMIT_BIN" \
  --packages "${HUDI_SPARK_BUNDLE},${HADOOP_AWS_PACKAGE},${AWS_JAVA_SDK_BUNDLE}" \
  --class io.pixelsdb.spark.app.PixelsBenchmarkHudiImportApp \
  --master "$SPARK_MASTER" \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --executor-memory "$SPARK_EXECUTOR_MEMORY" \
  "${COMMON_CONF[@]}" \
  "$ROOT_DIR/target/pixels-spark-0.1.jar" \
  "$CSV_PATH" \
  "$TARGET_PATH" \
  "$SPARK_MASTER" \
  "$TABLE_NAME" \
  "$BENCHMARK"
