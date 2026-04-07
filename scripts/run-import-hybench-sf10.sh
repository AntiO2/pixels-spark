#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/ubuntu/disk1/projects/pixels-spark
OPT_ROOT=/home/ubuntu/disk1/opt
CSV_ROOT="${1:-/home/ubuntu/disk1/Data_10x}"
TARGET_ROOT="${2:-s3a://home-zinuo/deltalake/hybench_sf10}"
LOG_DIR="/tmp/hybench_sf10_import_logs"
STATE_DIR="/tmp/hybench_sf10_import_state"
PID_FILE="$STATE_DIR/import.pid"
TABLES=(
  customer
  company
  savingAccount
  checkingAccount
  transfer
  checking
  loanapps
  loantrans
)

mkdir -p "$LOG_DIR" "$STATE_DIR"

source "$OPT_ROOT/conf/pixels-delta-env.sh"
export JAVA_HOME="$JAVA17_HOME"
export AWS_ACCESS_KEY_ID
AWS_ACCESS_KEY_ID="$(awk -F= '/aws_access_key_id/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_SECRET_ACCESS_KEY
AWS_SECRET_ACCESS_KEY="$(awk -F= '/aws_secret_access_key/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_REGION=us-east-2
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-$ROOT/etc/pixels-spark.properties}"

echo $$ > "$PID_FILE"

for table_name in "${TABLES[@]}"; do
  if [[ -f "$STATE_DIR/${table_name}.done" ]]; then
    echo "skip table=$table_name reason=done-marker-exists"
    continue
  fi

  echo "start table=$table_name ts=$(date -u +%Y-%m-%dT%H:%M:%SZ) csv_root=$CSV_ROOT target_root=$TARGET_ROOT"
  "$SPARK_HOME/bin/spark-submit" \
    --master local[4] \
    --driver-memory 20g \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.sql.shuffle.partitions=32 \
    --conf spark.default.parallelism=32 \
    --conf spark.sql.files.maxPartitionBytes=268435456 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider \
    --conf spark.hadoop.fs.s3a.endpoint=s3.us-east-2.amazonaws.com \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true \
    --conf spark.hadoop.fs.s3a.path.style.access=false \
    "$ROOT/scripts/import-benchmark-csv-to-delta-s3.py" \
    "$CSV_ROOT" \
    "$TARGET_ROOT" \
    "$table_name" \
    2>&1 | tee "$LOG_DIR/${table_name}.log"

  touch "$STATE_DIR/${table_name}.done"
  echo "done table=$table_name ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
done

rm -f "$PID_FILE"
echo "all_done ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
