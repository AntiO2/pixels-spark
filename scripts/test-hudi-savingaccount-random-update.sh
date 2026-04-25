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
BATCH_SIZE="${BATCH_SIZE:-${UPDATE_ROWS}}"
BATCH_COUNT="${BATCH_COUNT:-1}"
RANDOM_SEED="${RANDOM_SEED:-20260421}"
ACCOUNT_ID_MIN="${ACCOUNT_ID_MIN:-1}"
ACCOUNT_ID_MAX="${ACCOUNT_ID_MAX:-1000000}"
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
INDEX_MODE="${INDEX_MODE:-RECORD_INDEX}"
HUDI_TABLE_TYPE="${HUDI_TABLE_TYPE:-MOR}"
UPDATE_MODE="${UPDATE_MODE:-auto}"

mkdir -p "${SPARK_LOCAL_DIR}" "${S3A_BUFFER_DIR}"

if (( BATCH_SIZE <= 0 )); then
  echo "BATCH_SIZE must be > 0, got ${BATCH_SIZE}" >&2
  exit 1
fi

if (( BATCH_COUNT <= 0 )); then
  echo "BATCH_COUNT must be > 0, got ${BATCH_COUNT}" >&2
  exit 1
fi

TOTAL_REQUESTED_ROWS=$((BATCH_SIZE * BATCH_COUNT))
SOURCE_CANDIDATE_MULTIPLIER="${SOURCE_CANDIDATE_MULTIPLIER:-3}"
if (( SOURCE_CANDIDATE_MULTIPLIER < 1 )); then
  echo "SOURCE_CANDIDATE_MULTIPLIER must be >= 1, got ${SOURCE_CANDIDATE_MULTIPLIER}" >&2
  exit 1
fi

INDEX_MODE_UPPER="$(echo "${INDEX_MODE}" | tr '[:lower:]' '[:upper:]')"
HUDI_TABLE_TYPE_UPPER="$(echo "${HUDI_TABLE_TYPE}" | tr '[:lower:]' '[:upper:]')"
if [[ "${HUDI_TABLE_TYPE_UPPER}" == "MOR" ]]; then
  HOODIE_TABLE_TYPE_VALUE="MERGE_ON_READ"
else
  HOODIE_TABLE_TYPE_VALUE="COPY_ON_WRITE"
fi

UPDATE_MODE_UPPER="$(echo "${UPDATE_MODE}" | tr '[:lower:]' '[:upper:]')"
if [[ "${UPDATE_MODE_UPPER}" == "AUTO" ]]; then
  if [[ "${HUDI_TABLE_TYPE_UPPER}" == "MOR" ]]; then
    EFFECTIVE_UPDATE_MODE="UPSERT"
  else
    EFFECTIVE_UPDATE_MODE="MERGE"
  fi
else
  EFFECTIVE_UPDATE_MODE="${UPDATE_MODE_UPPER}"
fi

if [[ "${EFFECTIVE_UPDATE_MODE}" != "MERGE" && "${EFFECTIVE_UPDATE_MODE}" != "UPSERT" ]]; then
  echo "UPDATE_MODE must be one of auto|merge|upsert, got ${UPDATE_MODE}" >&2
  exit 1
fi

if [[ "${INDEX_MODE_UPPER}" == "RECORD_INDEX" ]]; then
  HOODIE_METADATA_ENABLE_VALUE="true"
  HOODIE_METADATA_RECORD_INDEX_ENABLE_VALUE="true"
  HOODIE_INDEX_TYPE_VALUE="RECORD_INDEX"
else
  HOODIE_METADATA_ENABLE_VALUE="false"
  HOODIE_METADATA_RECORD_INDEX_ENABLE_VALUE="false"
  HOODIE_INDEX_TYPE_VALUE="${INDEX_MODE_UPPER}"
fi

SQL_FILE="$(mktemp /tmp/test-hudi-savingaccount-random-update-XXXXXX.sql)"
trap 'rm -f "${SQL_FILE}"' EXIT

cat > "${SQL_FILE}" <<EOF
SET hoodie.metadata.enable=${HOODIE_METADATA_ENABLE_VALUE};
SET hoodie.metadata.record.index.enable=${HOODIE_METADATA_RECORD_INDEX_ENABLE_VALUE};
SET hoodie.index.type=${HOODIE_INDEX_TYPE_VALUE};
SET hoodie.datasource.write.table.type=${HOODIE_TABLE_TYPE_VALUE};
SET hoodie.spark.sql.insert.into.operation=upsert;
SET spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS};

DROP TABLE IF EXISTS ${TABLE_NAME};
CREATE TABLE ${TABLE_NAME}
USING hudi
LOCATION '${TABLE_PATH}';

CACHE TABLE ${TABLE_NAME};
SELECT COUNT(*) AS table_row_count_before FROM ${TABLE_NAME};

SELECT ${BATCH_SIZE} AS batch_size, ${BATCH_COUNT} AS batch_count, ${TOTAL_REQUESTED_ROWS} AS total_requested_rows;
EOF

for ((batch_id = 1; batch_id <= BATCH_COUNT; batch_id++)); do
  seed_base=$((RANDOM_SEED + batch_id * 10))
  source_candidate_rows=$((BATCH_SIZE * SOURCE_CANDIDATE_MULTIPLIER))
  cat >> "${SQL_FILE}" <<EOF
SELECT ${batch_id} AS running_batch_id, ${BATCH_SIZE} AS running_batch_rows;

CREATE OR REPLACE TEMP VIEW source_batch_${batch_id} AS
SELECT accountID, userID, balance, Isblocked, ts, freshness_ts
FROM (
  SELECT accountID, userID, balance, Isblocked, ts, freshness_ts
  FROM (
    SELECT
      accountID,
      userID,
      balance,
      Isblocked,
      ts,
      freshness_ts,
      ROW_NUMBER() OVER (PARTITION BY accountID ORDER BY random_rank) AS rn
    FROM (
      SELECT
        CAST(${ACCOUNT_ID_MIN} + FLOOR(rand(${seed_base}) * (${ACCOUNT_ID_MAX} - ${ACCOUNT_ID_MIN} + 1)) AS INT) AS accountID,
        CAST(FLOOR(rand(${seed_base} + 1) * 1000000000) AS INT) AS userID,
        CAST((rand(${seed_base} + 2) * 1000000.0) AS FLOAT) AS balance,
        CAST(FLOOR(rand(${seed_base} + 3) * 2) AS INT) AS Isblocked,
        current_timestamp() AS ts,
        current_timestamp() AS freshness_ts,
        rand(${seed_base} + 4) AS random_rank
      FROM range(${source_candidate_rows})
    ) candidate_source
  ) deduplicated_source
  WHERE rn = 1
  LIMIT ${BATCH_SIZE}
) source_prepared_${batch_id};
EOF

  if [[ "${EFFECTIVE_UPDATE_MODE}" == "MERGE" ]]; then
    cat >> "${SQL_FILE}" <<EOF
MERGE INTO ${TABLE_NAME} AS target
USING source_batch_${batch_id} AS source
ON target.accountID = source.accountID
WHEN MATCHED THEN UPDATE SET
  target.accountID = source.accountID,
  target.userID = source.userID,
  target.balance = source.balance,
  target.Isblocked = source.Isblocked,
  target.ts = source.ts,
  target.freshness_ts = source.freshness_ts;
EOF
  else
    cat >> "${SQL_FILE}" <<EOF
INSERT INTO ${TABLE_NAME}
SELECT accountID, userID, balance, Isblocked, ts, freshness_ts
FROM source_batch_${batch_id};
EOF
  fi
done

cat >> "${SQL_FILE}" <<EOF
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
if (( spark_status == 0 && elapsed > 0 )); then
  approx_rows_per_sec=$((TOTAL_REQUESTED_ROWS / elapsed))
else
  approx_rows_per_sec=0
fi

echo "status=${spark_status} elapsed_seconds=${elapsed} update_mode=${EFFECTIVE_UPDATE_MODE} batch_size=${BATCH_SIZE} batch_count=${BATCH_COUNT} total_requested_rows=${TOTAL_REQUESTED_ROWS} approx_rows_per_sec=${approx_rows_per_sec} table_path=${TABLE_PATH} log=${OUTPUT_LOG}"

exit "${spark_status}"
