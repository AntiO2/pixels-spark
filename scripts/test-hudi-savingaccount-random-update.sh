#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPT_ROOT="${OPT_ROOT:-/home/ubuntu/disk1/opt}"
PIXELS_DELTA_ENV="${PIXELS_DELTA_ENV:-${OPT_ROOT}/conf/pixels-delta-env.sh}"

if [[ -f "${PIXELS_DELTA_ENV}" ]]; then
  source "${PIXELS_DELTA_ENV}"
fi

if [[ -n "${JAVA17_HOME:-}" ]]; then
  export JAVA_HOME="${JAVA17_HOME}"
fi

if [[ -n "${SPARK_SQL_BIN:-}" ]]; then
  :
elif [[ -n "${SPARK_HOME:-}" ]]; then
  SPARK_SQL_BIN="${SPARK_HOME}/bin/spark-sql"
elif [[ -x "${OPT_ROOT}/spark-3.5.6-bin-hadoop3/bin/spark-sql" ]]; then
  SPARK_SQL_BIN="${OPT_ROOT}/spark-3.5.6-bin-hadoop3/bin/spark-sql"
else
  SPARK_SQL_BIN="spark-sql"
fi

if ! command -v "${SPARK_SQL_BIN}" >/dev/null 2>&1 && [[ ! -x "${SPARK_SQL_BIN}" ]]; then
  echo "spark-sql not found. Set SPARK_HOME or SPARK_SQL_BIN." >&2
  exit 1
fi

HUDI_SPARK_BUNDLE="${HUDI_SPARK_BUNDLE:-org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0}"
HADOOP_AWS_PACKAGE="${HADOOP_AWS_PACKAGE:-org.apache.hadoop:hadoop-aws:3.3.4}"
AWS_JAVA_SDK_BUNDLE="${AWS_JAVA_SDK_BUNDLE:-com.amazonaws:aws-java-sdk-bundle:1.12.262}"
TABLE_PATH="${TABLE_PATH:-s3a://home-zinuo/hudi/hybench_sf1x/savingAccount}"
TABLE_NAME="${TABLE_NAME:-savingaccount_random_update_target}"
UPDATE_ROWS="${UPDATE_ROWS:-50000}"
RANDOM_SEED="${RANDOM_SEED:-20260421}"
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-8g}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-16}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-16}"
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/tmp/pixels-spark-local}"
S3A_BUFFER_DIR="${S3A_BUFFER_DIR:-/tmp/pixels-s3a-buffer}"
AWS_REGION_VALUE="${AWS_REGION:-us-east-2}"
S3_ENDPOINT="${S3_ENDPOINT:-s3.us-east-2.amazonaws.com}"
S3_PATH_STYLE="${S3_PATH_STYLE:-false}"
S3_SSL_ENABLED="${S3_SSL_ENABLED:-true}"
OUTPUT_LOG="${OUTPUT_LOG:-/tmp/test-hudi-savingaccount-random-update.log}"

mkdir -p "${SPARK_LOCAL_DIR}" "${S3A_BUFFER_DIR}"

SQL_FILE="$(mktemp /tmp/test-hudi-savingaccount-random-update-XXXXXX.sql)"
trap 'rm -f "${SQL_FILE}"' EXIT

cat > "${SQL_FILE}" <<EOF
SET hoodie.metadata.enable=true;
SET hoodie.metadata.record.index.enable=true;
SET hoodie.index.type=RECORD_INDEX;
SET spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS};

DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME}
USING hudi
LOCATION '${TABLE_PATH}';

CACHE TABLE ${TABLE_NAME};
SELECT COUNT(*) AS table_row_count_before FROM ${TABLE_NAME};

CREATE OR REPLACE TEMP VIEW savingaccount_updates AS
SELECT
  accountID,
  userID,
  CAST(balance + ((rand(${RANDOM_SEED} + 1) * 20.0) - 10.0) AS FLOAT) AS balance,
  CASE
    WHEN rand(${RANDOM_SEED} + 2) > 0.98 THEN 1 - Isblocked
    ELSE Isblocked
  END AS Isblocked,
  current_timestamp() AS ts,
  current_timestamp() AS freshness_ts
FROM ${TABLE_NAME}
ORDER BY rand(${RANDOM_SEED})
LIMIT ${UPDATE_ROWS};

CACHE TABLE savingaccount_updates;
SELECT COUNT(*) AS update_row_count FROM savingaccount_updates;

MERGE INTO ${TABLE_NAME} AS target
USING savingaccount_updates AS source
ON target.accountID = source.accountID
WHEN MATCHED THEN UPDATE SET
  target.userID = source.userID,
  target.balance = source.balance,
  target.Isblocked = source.Isblocked,
  target.ts = source.ts,
  target.freshness_ts = source.freshness_ts;

SELECT COUNT(*) AS table_row_count_after FROM ${TABLE_NAME};
SELECT MAX(freshness_ts) AS max_freshness_ts_after FROM ${TABLE_NAME};
EOF

start_ts="$(date +%s)"

set +e
"${SPARK_SQL_BIN}" \
  --master "${SPARK_MASTER}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --packages "${HUDI_SPARK_BUNDLE},${HADOOP_AWS_PACKAGE},${AWS_JAVA_SDK_BUNDLE}" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.sql.shuffle.partitions="${SPARK_SQL_SHUFFLE_PARTITIONS}" \
  --conf spark.default.parallelism="${SPARK_DEFAULT_PARALLELISM}" \
  --conf spark.local.dir="${SPARK_LOCAL_DIR}" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.hadoop.fs.s3a.buffer.dir="${S3A_BUFFER_DIR}" \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT}" \
  --conf spark.hadoop.fs.s3a.path.style.access="${S3_PATH_STYLE}" \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled="${S3_SSL_ENABLED}" \
  --conf spark.hadoop.fs.s3a.endpoint.region="${AWS_REGION_VALUE}" \
  -f "${SQL_FILE}" \
  2>&1 | tee "${OUTPUT_LOG}"
spark_status="${PIPESTATUS[0]}"
set -e

end_ts="$(date +%s)"
elapsed="$((end_ts - start_ts))"

echo "status=${spark_status} elapsed_seconds=${elapsed} update_rows=${UPDATE_ROWS} table_path=${TABLE_PATH} log=${OUTPUT_LOG}"

exit "${spark_status}"
