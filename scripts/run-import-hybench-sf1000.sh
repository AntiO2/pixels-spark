#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/ubuntu/disk1/projects/pixels-spark
OPT_ROOT=/home/ubuntu/disk1/opt
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-$ROOT/etc/pixels-spark.properties}"
source "$ROOT/scripts/lib/pixels-config.sh"

CSV_ROOT="${1:-$(pixels_get_property pixels.import.hybench.sf1000.csv-root /home/ubuntu/disk1/hybench_sf1000)}"
TARGET_ROOT="${2:-$(pixels_get_property pixels.import.hybench.sf1000.target-root s3a://home-zinuo/deltalake/hybench_sf1000)}"
LOG_DIR="${LOG_DIR:-$(pixels_get_property pixels.import.hybench.sf1000.log-dir /home/ubuntu/disk1/tmp/hybench_sf1000_logs)}"
STATE_DIR="${STATE_DIR:-$(pixels_get_property pixels.import.hybench.sf1000.state-dir /home/ubuntu/disk1/tmp/hybench_sf1000_state)}"
PID_FILE="$STATE_DIR/import.pid"
TABLES=()
pixels_split_csv_property "$(pixels_get_property pixels.import.tables customer,company,savingaccount,checkingaccount,transfer,checking,loanapps,loantrans)" TABLES
SPARK_MASTER="${SPARK_MASTER:-$(pixels_get_property pixels.spark.master local[4])}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-$(pixels_get_property pixels.import.spark.driver.memory 20g)}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-$(pixels_get_property pixels.import.spark.shuffle.partitions 32)}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-$(pixels_get_property pixels.import.spark.default.parallelism 32)}"
SPARK_SQL_FILES_MAX_PARTITION_BYTES="${SPARK_SQL_FILES_MAX_PARTITION_BYTES:-$(pixels_get_property pixels.import.spark.sql.files.max-partition-bytes 268435456)}"
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-$(pixels_get_property pixels.spark.local.dir /home/ubuntu/disk1/tmp/spark-local)}"
S3A_BUFFER_DIR="${S3A_BUFFER_DIR:-$(pixels_get_property pixels.spark.s3a.buffer.dir /home/ubuntu/disk1/tmp/s3a-buffer)}"

mkdir -p "$LOG_DIR" "$STATE_DIR" "$SPARK_LOCAL_DIR" "$S3A_BUFFER_DIR"

source "$OPT_ROOT/conf/pixels-delta-env.sh"
export JAVA_HOME="$JAVA17_HOME"
export AWS_ACCESS_KEY_ID
AWS_ACCESS_KEY_ID="$(awk -F= '/aws_access_key_id/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_SECRET_ACCESS_KEY
AWS_SECRET_ACCESS_KEY="$(awk -F= '/aws_secret_access_key/ {print $2}' /home/ubuntu/.aws/credentials)"
export AWS_REGION=us-east-2

echo $$ > "$PID_FILE"

for table_name in "${TABLES[@]}"; do
  if [[ -f "$STATE_DIR/${table_name}.done" ]]; then
    echo "skip table=$table_name reason=done-marker-exists"
    continue
  fi

  echo "start table=$table_name ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  "$SPARK_HOME/bin/spark-submit" \
    --class io.pixelsdb.spark.app.PixelsBenchmarkDeltaImportApp \
    --master "$SPARK_MASTER" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.sql.shuffle.partitions="$SPARK_SHUFFLE_PARTITIONS" \
    --conf spark.default.parallelism="$SPARK_DEFAULT_PARALLELISM" \
    --conf spark.sql.files.maxPartitionBytes="$SPARK_SQL_FILES_MAX_PARTITION_BYTES" \
    --conf spark.local.dir="$SPARK_LOCAL_DIR" \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.buffer.dir="$S3A_BUFFER_DIR" \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider \
    --conf spark.hadoop.fs.s3a.endpoint=s3.us-east-2.amazonaws.com \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true \
    --conf spark.hadoop.fs.s3a.path.style.access=false \
    "$ROOT/target/pixels-spark-0.1.jar" \
    "$CSV_ROOT" \
    "$TARGET_ROOT" \
    "$SPARK_MASTER" \
    "$table_name" \
    hybench \
    2>&1 | tee "$LOG_DIR/${table_name}.log"

  touch "$STATE_DIR/${table_name}.done"
  echo "done table=$table_name ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
done

rm -f "$PID_FILE"
echo "all_done ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
