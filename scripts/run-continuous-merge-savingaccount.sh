#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPT_ROOT="${OPT_ROOT:-/home/ubuntu/disk1/opt}"
PIXELS_DELTA_ENV="${PIXELS_DELTA_ENV:-${OPT_ROOT}/conf/pixels-delta-env.sh}"

if [[ -f "${PIXELS_DELTA_ENV}" ]]; then
  # shellcheck disable=SC1091
  source "${PIXELS_DELTA_ENV}"
fi

if [[ -n "${JAVA17_HOME:-}" ]]; then
  export JAVA_HOME="${JAVA17_HOME}"
fi

if [[ -n "${SPARK_SHELL_BIN:-}" ]]; then
  :
elif [[ -n "${SPARK_HOME:-}" ]]; then
  SPARK_SHELL_BIN="${SPARK_HOME}/bin/spark-shell"
elif [[ -x "${OPT_ROOT}/spark-3.5.6-bin-hadoop3/bin/spark-shell" ]]; then
  SPARK_SHELL_BIN="${OPT_ROOT}/spark-3.5.6-bin-hadoop3/bin/spark-shell"
else
  SPARK_SHELL_BIN="spark-shell"
fi

if ! command -v "${SPARK_SHELL_BIN}" >/dev/null 2>&1 && [[ ! -x "${SPARK_SHELL_BIN}" ]]; then
  echo "spark-shell not found. Set SPARK_HOME or SPARK_SHELL_BIN." >&2
  exit 1
fi

TARGET_PATH="${TARGET_PATH:-s3a://home-zinuo/deltalake/hybench_sf1000/savingaccount}"
SPARK_MASTER="${SPARK_MASTER:-local[16]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-32g}"
SPARK_APP_NAME="${SPARK_APP_NAME:-continuous-merge-savingaccount}"
SPARK_UI_PORT="${SPARK_UI_PORT:-4050}"
SPARK_EVENTS_DIR="${SPARK_EVENTS_DIR:-/home/ubuntu/disk1/tmp/spark-events}"
PID_FILE="${PID_FILE:-/tmp/continuous-merge-savingaccount.pid}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-/tmp/continuous-merge-savingaccount-ckpt}"
DERBY_HOME="${DERBY_HOME:-/tmp/derby-continuous-merge-savingaccount}"
SPARK_WAREHOUSE_DIR="${SPARK_WAREHOUSE_DIR:-/tmp/spark-warehouse-continuous-merge-savingaccount}"

BUCKET_ID="${BUCKET_ID:-63}"
BATCH_ROWS="${BATCH_ROWS:-2000}"
BALANCE_STEP="${BALANCE_STEP:-1.0}"
TRIGGER_INTERVAL_SECONDS="${TRIGGER_INTERVAL_SECONDS:-2}"
ROWS_PER_SECOND="${ROWS_PER_SECOND:-1}"

AWS_PROFILE="${AWS_PROFILE:-default}"
AWS_CREDENTIALS_FILE="${AWS_CREDENTIALS_FILE:-/home/ubuntu/.aws/credentials}"
AWS_CONFIG_FILE="${AWS_CONFIG_FILE:-/home/ubuntu/.aws/config}"
AWS_REGION="${AWS_REGION:-}"

load_aws_credentials() {
  local profile="$1"
  if [[ ! -f "${AWS_CREDENTIALS_FILE}" ]]; then
    echo "Missing AWS credentials file: ${AWS_CREDENTIALS_FILE}" >&2
    return 1
  fi

  local access_key
  local secret_key
  access_key="$(awk -v p="${profile}" '
    $0 ~ "^\\[" p "\\]$" {in_sec=1; next}
    /^\[/ {in_sec=0}
    in_sec && $1 ~ /^aws_access_key_id[[:space:]]*=/ {sub(/^[^=]*=[[:space:]]*/, "", $0); print $0; exit}
  ' "${AWS_CREDENTIALS_FILE}")"
  secret_key="$(awk -v p="${profile}" '
    $0 ~ "^\\[" p "\\]$" {in_sec=1; next}
    /^\[/ {in_sec=0}
    in_sec && $1 ~ /^aws_secret_access_key[[:space:]]*=/ {sub(/^[^=]*=[[:space:]]*/, "", $0); print $0; exit}
  ' "${AWS_CREDENTIALS_FILE}")"

  if [[ -z "${access_key}" || -z "${secret_key}" ]]; then
    echo "Unable to load AWS credentials for profile=${profile}" >&2
    return 1
  fi

  export AWS_ACCESS_KEY_ID="${access_key}"
  export AWS_SECRET_ACCESS_KEY="${secret_key}"
}

load_aws_region() {
  local profile="$1"
  if [[ -n "${AWS_REGION}" ]]; then
    export AWS_REGION
    return 0
  fi
  if [[ ! -f "${AWS_CONFIG_FILE}" ]]; then
    export AWS_REGION="us-east-2"
    return 0
  fi

  local section="${profile}"
  if [[ "${profile}" != "default" ]]; then
    section="profile ${profile}"
  fi
  local region
  region="$(awk -v p="${section}" '
    $0 ~ "^\\[" p "\\]$" {in_sec=1; next}
    /^\[/ {in_sec=0}
    in_sec && $1 ~ /^region[[:space:]]*=/ {sub(/^[^=]*=[[:space:]]*/, "", $0); print $0; exit}
  ' "${AWS_CONFIG_FILE}")"
  export AWS_REGION="${region:-us-east-2}"
}

load_aws_credentials "${AWS_PROFILE}"
load_aws_region "${AWS_PROFILE}"

mkdir -p "${SPARK_EVENTS_DIR}" "$(dirname "${PID_FILE}")" "${CHECKPOINT_DIR}" "${DERBY_HOME}" "${SPARK_WAREHOUSE_DIR}"

SCALA_FILE="$(mktemp /tmp/continuous-merge-savingaccount-XXXXXX.scala)"
cleanup() {
  rm -f "${SCALA_FILE}" "${PID_FILE}"
}
trap cleanup EXIT

cat > "${SCALA_FILE}" <<EOF
import org.apache.spark.sql.functions._
import scala.util.control.NonFatal

val targetPath = "${TARGET_PATH}"
val checkpointDir = "${CHECKPOINT_DIR}"
val bucketId = ${BUCKET_ID}
val batchRows = ${BATCH_ROWS}
val balanceStep = ${BALANCE_STEP}
val triggerInterval = "${TRIGGER_INTERVAL_SECONDS} seconds"
val rowsPerSecond = ${ROWS_PER_SECOND}

println(s"[continuous-merge-stream] target=\$targetPath bucket=\$bucketId batchRows=\$batchRows balanceStep=\$balanceStep trigger=\$triggerInterval")

val heartbeat = spark.readStream
  .format("rate")
  .option("rowsPerSecond", rowsPerSecond)
  .option("numPartitions", 1)
  .load()

val query = heartbeat.writeStream
  .queryName("continuous-merge-savingaccount")
  .option("checkpointLocation", checkpointDir)
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime(triggerInterval))
  .foreachBatch { (batchDf: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
    try {
      val hasRows = batchDf.limit(1).count() > 0
      if (hasRows) {
        val src = spark.read
          .format("delta")
          .load(targetPath)
          .where(col("_pixels_bucket_id") === lit(bucketId))
          .selectExpr(
            "accountID",
            "userID",
            s"CAST(balance + \$balanceStep AS FLOAT) AS balance",
            "Isblocked",
            "ts",
            "current_timestamp() AS freshness_ts",
            "_pixels_bucket_id")
          .limit(batchRows)
          .cache()

        val srcRows = src.count()
        if (srcRows > 0) {
          src.createOrReplaceTempView("merge_source")
          val metrics = spark.sql(
            s"""
               |MERGE INTO delta.\`\$targetPath\` t
               |USING merge_source s
               |ON t.accountID <=> s.accountID
               |AND t._pixels_bucket_id <=> s._pixels_bucket_id
               |WHEN MATCHED THEN UPDATE SET
               |  t.accountID = s.accountID,
               |  t.userID = s.userID,
               |  t.balance = s.balance,
               |  t.Isblocked = s.Isblocked,
               |  t.ts = s.ts,
               |  t.freshness_ts = s.freshness_ts,
               |  t._pixels_bucket_id = s._pixels_bucket_id
               |""".stripMargin)
          println(s"[continuous-merge-stream] batchId=\$batchId sourceRows=\$srcRows")
          metrics.show(false)
        } else {
          println(s"[continuous-merge-stream] batchId=\$batchId sourceRows=0 skip")
        }
        src.unpersist()
        ()
      }
    } catch {
      case NonFatal(e) =>
        println(s"[continuous-merge-stream] batchId=\$batchId failed: \${e.getMessage}")
        e.printStackTrace()
    }
  }
  .start()

query.awaitTermination()
EOF

echo "$$" > "${PID_FILE}"
echo "continuous-merge-stream pid-file=${PID_FILE}"
echo "Spark UI: http://127.0.0.1:${SPARK_UI_PORT}"

exec "${SPARK_SHELL_BIN}" \
  --master "${SPARK_MASTER}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --name "${SPARK_APP_NAME}" \
  --conf spark.ui.port="${SPARK_UI_PORT}" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir="file://${SPARK_EVENTS_DIR}" \
  --conf spark.sql.warehouse.dir="${SPARK_WAREHOUSE_DIR}" \
  --conf spark.driver.extraJavaOptions="-Dderby.system.home=${DERBY_HOME}" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider \
  --conf spark.hadoop.fs.s3a.endpoint=s3.us-east-2.amazonaws.com \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true \
  --conf spark.hadoop.fs.s3a.path.style.access=false \
  -i "${SCALA_FILE}"
