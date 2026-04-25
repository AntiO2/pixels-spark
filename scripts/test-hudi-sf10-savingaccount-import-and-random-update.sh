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

CSV_LINK_PATH="${CSV_LINK_PATH:-/home/ubuntu/disk1/hybench_sf10/savingAccount.csv}"
CSV_PATH="${CSV_PATH:-$(readlink -f "${CSV_LINK_PATH}")}"
TABLE_PATH="${TABLE_PATH:-s3a://home-zinuo/hudi/hybench_sf10/savingAccount}"
TABLE_NAME="${TABLE_NAME:-savingaccount_sf10_hudi_target}"
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-16g}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-64}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-64}"
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/tmp/pixels-spark-local}"
S3A_BUFFER_DIR="${S3A_BUFFER_DIR:-/tmp/pixels-s3a-buffer}"
AWS_REGION_VALUE="${AWS_REGION:-us-east-2}"
S3_ENDPOINT="${S3_ENDPOINT:-s3.us-east-2.amazonaws.com}"
S3_PATH_STYLE="${S3_PATH_STYLE:-false}"
S3_SSL_ENABLED="${S3_SSL_ENABLED:-true}"
HUDI_SPARK_BUNDLE="${HUDI_SPARK_BUNDLE:-org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0}"
HADOOP_AWS_PACKAGE="${HADOOP_AWS_PACKAGE:-org.apache.hadoop:hadoop-aws:3.3.4}"
AWS_JAVA_SDK_BUNDLE="${AWS_JAVA_SDK_BUNDLE:-com.amazonaws:aws-java-sdk-bundle:1.12.262}"
UPDATE_ROWS="${UPDATE_ROWS:-100000}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
BATCH_COUNT="${BATCH_COUNT:-100}"
RUN_UPDATE_TEST="${RUN_UPDATE_TEST:-false}"
RANDOM_SEED="${RANDOM_SEED:-20260421}"
ACCOUNT_ID_MIN="${ACCOUNT_ID_MIN:-1}"
ACCOUNT_ID_MAX="${ACCOUNT_ID_MAX:-3020000}"
OUTPUT_LOG="${OUTPUT_LOG:-/tmp/test-hudi-sf10-savingaccount-import-and-random-update.log}"
HUDI_TABLE_TYPE="${HUDI_TABLE_TYPE:-mor}"

mkdir -p "${SPARK_LOCAL_DIR}" "${S3A_BUFFER_DIR}"

if [[ ! -f "${CSV_PATH}" ]]; then
  echo "CSV file not found: ${CSV_PATH}" >&2
  exit 1
fi

IMPORT_SQL_FILE="$(mktemp /tmp/test-hudi-sf10-savingaccount-import-XXXXXX.sql)"
INDEX_SQL_FILE="$(mktemp /tmp/test-hudi-sf10-savingaccount-index-XXXXXX.sql)"
trap 'rm -f "${IMPORT_SQL_FILE}" "${INDEX_SQL_FILE}"' EXIT

cat > "${IMPORT_SQL_FILE}" <<EOF
SET spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS};
SET hoodie.metadata.enable=false;
SET hoodie.metadata.record.index.enable=false;
SET hoodie.index.type=SIMPLE;

DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME} (
  accountID INT,
  userID INT,
  balance FLOAT,
  Isblocked INT,
  ts TIMESTAMP,
  freshness_ts TIMESTAMP
)
USING hudi
TBLPROPERTIES (
  type = '${HUDI_TABLE_TYPE}',
  primaryKey = 'accountID',
  preCombineField = 'freshness_ts'
)
LOCATION '${TABLE_PATH}';

CREATE OR REPLACE TEMP VIEW savingaccount_csv_raw
USING csv
OPTIONS (
  path '${CSV_PATH}',
  header 'false',
  inferSchema 'false',
  mode 'FAILFAST'
);

INSERT OVERWRITE ${TABLE_NAME}
SELECT
  CAST(_c0 AS INT) AS accountID,
  CAST(_c1 AS INT) AS userID,
  CAST(_c2 AS FLOAT) AS balance,
  CAST(_c3 AS INT) AS Isblocked,
  TO_TIMESTAMP(_c4) AS ts,
  TO_TIMESTAMP(_c5) AS freshness_ts
FROM savingaccount_csv_raw;

SELECT COUNT(*) AS imported_rows FROM ${TABLE_NAME};
EOF

cat > "${INDEX_SQL_FILE}" <<EOF
SET spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS};

DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME}
USING hudi
LOCATION '${TABLE_PATH}';

ALTER TABLE ${TABLE_NAME} SET TBLPROPERTIES (
  hoodie.metadata.enable = 'true',
  hoodie.metadata.record.index.enable = 'true',
  hoodie.index.type = 'RECORD_INDEX'
);

SELECT COUNT(*) AS rows_after_index_setup FROM ${TABLE_NAME};
EOF

run_spark_sql_file() {
  local sql_file="$1"
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
    -f "${sql_file}"
}

echo "step=import csv_path=${CSV_PATH} table_path=${TABLE_PATH} table_name=${TABLE_NAME}"
run_spark_sql_file "${IMPORT_SQL_FILE}" 2>&1 | tee "${OUTPUT_LOG}"

echo "step=build_record_index table_path=${TABLE_PATH} table_name=${TABLE_NAME}"
run_spark_sql_file "${INDEX_SQL_FILE}" 2>&1 | tee -a "${OUTPUT_LOG}"

if [[ "${RUN_UPDATE_TEST}" == "true" ]]; then
  echo "step=random_update_test batch_size=${BATCH_SIZE} batch_count=${BATCH_COUNT} pk_range=${ACCOUNT_ID_MIN}-${ACCOUNT_ID_MAX}"
  TABLE_PATH="${TABLE_PATH}" \
  TABLE_NAME="${TABLE_NAME}" \
  UPDATE_ROWS="${UPDATE_ROWS}" \
  BATCH_SIZE="${BATCH_SIZE}" \
  BATCH_COUNT="${BATCH_COUNT}" \
  RANDOM_SEED="${RANDOM_SEED}" \
  ACCOUNT_ID_MIN="${ACCOUNT_ID_MIN}" \
  ACCOUNT_ID_MAX="${ACCOUNT_ID_MAX}" \
  SPARK_MASTER="${SPARK_MASTER}" \
  SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY}" \
  SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS}" \
  SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM}" \
  SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR}" \
  S3A_BUFFER_DIR="${S3A_BUFFER_DIR}" \
  S3_ENDPOINT="${S3_ENDPOINT}" \
  S3_PATH_STYLE="${S3_PATH_STYLE}" \
  S3_SSL_ENABLED="${S3_SSL_ENABLED}" \
  AWS_REGION="${AWS_REGION_VALUE}" \
  OUTPUT_LOG="${OUTPUT_LOG}" \
  "${ROOT_DIR}/scripts/test-hudi-savingaccount-random-update.sh"
else
  echo "step=random_update_test skipped reason=RUN_UPDATE_TEST=${RUN_UPDATE_TEST}"
  echo "hint run: RUN_UPDATE_TEST=true BATCH_SIZE=1000 BATCH_COUNT=100 $0"
fi

echo "done csv_path=${CSV_PATH} table_path=${TABLE_PATH} run_update_test=${RUN_UPDATE_TEST} log=${OUTPUT_LOG}"
