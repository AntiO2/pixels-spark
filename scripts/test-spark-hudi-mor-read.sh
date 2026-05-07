#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OPT_ROOT="${OPT_ROOT:-/home/ubuntu/disk1/opt}"
PIXELS_DELTA_ENV="${PIXELS_DELTA_ENV:-${OPT_ROOT}/conf/pixels-delta-env.sh}"

if [[ -f "${PIXELS_DELTA_ENV}" ]]; then
  # Align Hudi read tests with the repo's Spark runtime expectation.
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
elif [[ -x "/home/ubuntu/opt/spark-3.5.6-bin-hadoop3/bin/spark-sql" ]]; then
  SPARK_SQL_BIN="/home/ubuntu/opt/spark-3.5.6-bin-hadoop3/bin/spark-sql"
else
  SPARK_SQL_BIN="spark-sql"
fi

if ! command -v "${SPARK_SQL_BIN}" >/dev/null 2>&1 && [[ ! -x "${SPARK_SQL_BIN}" ]]; then
  echo "spark-sql not found. Set SPARK_HOME or SPARK_SQL_BIN." >&2
  exit 1
fi

DB_ROOT="${DB_ROOT:-s3a://home-haoyue/hudi/hudi_hybench_sf1333_2.db}"
TABLE_NAME="${TABLE_NAME:-savingaccount}"
TABLE_PATH="${TABLE_PATH:-${DB_ROOT}/${TABLE_NAME}}"
TABLE_ALIAS="${TABLE_ALIAS:-hudi_${TABLE_NAME}}"
LIMIT_ROWS="${LIMIT_ROWS:-20}"
SHOW_COMMITS="${SHOW_COMMITS:-true}"
QUERY_TYPE="${QUERY_TYPE:-snapshot}"
RUN_COUNT="${RUN_COUNT:-false}"
RUN_MAX_FRESHNESS_TS="${RUN_MAX_FRESHNESS_TS:-true}"
SPARK_MASTER="${SPARK_MASTER:-local[16]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-32g}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-16}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-16}"
SPARK_DRIVER_EXTRA_JAVA_OPTIONS="${SPARK_DRIVER_EXTRA_JAVA_OPTIONS:--Djdk.attach.allowAttachSelf=true --add-modules=jdk.attach --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED}"
SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="${SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS:--Djdk.attach.allowAttachSelf=true --add-modules=jdk.attach --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED}"

AWS_REGION="${AWS_REGION:-us-east-2}"
S3_ENDPOINT="${S3_ENDPOINT:-https://s3.us-east-2.amazonaws.com}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-${HUDI_S3_ACCESS_KEY:-}}"
S3_SECRET_KEY="${S3_SECRET_KEY:-${HUDI_S3_SECRET_KEY:-}}"
AWS_PROFILE_NAME="${AWS_PROFILE:-default}"
AWS_CREDENTIALS_FILE="${AWS_CREDENTIALS_FILE:-${HOME}/.aws/credentials}"
AWS_CONFIG_FILE="${AWS_CONFIG_FILE:-${HOME}/.aws/config}"

aws_profile_header() {
  local profile_name="$1"
  if [[ "${profile_name}" == "default" ]]; then
    printf '[default]'
  else
    printf '[profile %s]' "${profile_name}"
  fi
}

aws_get_credentials_value() {
  local profile_name="$1"
  local key_name="$2"
  local file_path="$3"

  [[ -f "${file_path}" ]] || return 0

  awk -v profile="[${profile_name}]" -v key="${key_name}" '
    BEGIN { in_profile = 0 }
    /^[[:space:]]*\[/ {
      in_profile = ($0 == profile)
      next
    }
    in_profile {
      split($0, pair, "=")
      current_key = pair[1]
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", current_key)
      if (current_key == key) {
        sub(/^[^=]*=/, "", $0)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
        print $0
        exit
      }
    }
  ' "${file_path}"
}

aws_get_config_value() {
  local profile_name="$1"
  local key_name="$2"
  local file_path="$3"
  local profile_header

  [[ -f "${file_path}" ]] || return 0
  profile_header="$(aws_profile_header "${profile_name}")"

  awk -v profile="${profile_header}" -v key="${key_name}" '
    BEGIN { in_profile = 0 }
    /^[[:space:]]*\[/ {
      in_profile = ($0 == profile)
      next
    }
    in_profile {
      split($0, pair, "=")
      current_key = pair[1]
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", current_key)
      if (current_key == key) {
        sub(/^[^=]*=/, "", $0)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
        print $0
        exit
      }
    }
  ' "${file_path}"
}

usage() {
  cat <<'EOF'
Usage:
  ./scripts/test-spark-hudi-mor-read.sh [table-name]

Examples:
  ./scripts/test-spark-hudi-mor-read.sh
  TABLE_NAME=customer ./scripts/test-spark-hudi-mor-read.sh
  TABLE_PATH=s3a://home-haoyue/hudi/hudi_hybench_sf1333_2.db/checkingaccount ./scripts/test-spark-hudi-mor-read.sh

Optional env:
  SPARK_SQL_BIN      Path to spark-sql
  DB_ROOT            Root path of the Hudi database
  TABLE_NAME         Table under DB_ROOT to test
  TABLE_PATH         Full table path; overrides DB_ROOT/TABLE_NAME
  TABLE_ALIAS        Session-local temp view name
  LIMIT_ROWS         Limit for sample query
  QUERY_TYPE         snapshot or read_optimized
  RUN_COUNT          true to run COUNT(*); default false because MOR RT count is memory-heavy
  RUN_MAX_FRESHNESS_TS true to run MAX(freshness_ts); default true
  SPARK_MASTER       Spark master, default local[16]
  SPARK_DRIVER_MEMORY Driver memory for spark-sql, default 32g
  SPARK_DRIVER_EXTRA_JAVA_OPTIONS Extra JVM options for spark driver
  SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS Extra JVM options for spark executor
  AWS_REGION         S3 region
  S3_ENDPOINT        S3 endpoint
  S3_ACCESS_KEY      S3 access key
  S3_SECRET_KEY      S3 secret key
  SHOW_COMMITS       true to print Hudi meta columns in sample query
  PIXELS_DELTA_ENV   Optional env file that defines JAVA17_HOME

Notes:
  - MOR realtime read should use QUERY_TYPE=snapshot.
  - This script reads by path and does not require Glue metastore registration.
  - By default it reads credentials from ~/.aws/credentials and region from ~/.aws/config.
  - If JAVA17_HOME is available, the script exports JAVA_HOME=JAVA17_HOME before starting Spark.
  - COUNT(*) on MOR realtime view is expensive; default is disabled even in the aggressive local profile.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ge 1 ]]; then
  TABLE_NAME="$1"
  TABLE_PATH="${DB_ROOT}/${TABLE_NAME}"
  TABLE_ALIAS="hudi_${TABLE_NAME}"
fi

if [[ -z "${S3_ACCESS_KEY}" ]]; then
  S3_ACCESS_KEY="$(aws_get_credentials_value "${AWS_PROFILE_NAME}" aws_access_key_id "${AWS_CREDENTIALS_FILE}")"
fi

if [[ -z "${S3_SECRET_KEY}" ]]; then
  S3_SECRET_KEY="$(aws_get_credentials_value "${AWS_PROFILE_NAME}" aws_secret_access_key "${AWS_CREDENTIALS_FILE}")"
fi

if [[ "${AWS_REGION}" == "us-east-2" ]]; then
  aws_config_region="$(aws_get_config_value "${AWS_PROFILE_NAME}" region "${AWS_CONFIG_FILE}")"
  if [[ -n "${aws_config_region}" ]]; then
    AWS_REGION="${aws_config_region}"
  fi
fi

if [[ -z "${S3_ACCESS_KEY}" || -z "${S3_SECRET_KEY}" ]]; then
  echo "Missing S3 credentials. Set AWS_PROFILE or S3_ACCESS_KEY/S3_SECRET_KEY." >&2
  exit 1
fi

TABLE_REF="${TABLE_ALIAS}"

if [[ "${SHOW_COMMITS}" == "true" ]]; then
  SAMPLE_SQL="SELECT _hoodie_commit_time, _hoodie_record_key, * FROM ${TABLE_REF} LIMIT ${LIMIT_ROWS};"
else
  SAMPLE_SQL="SELECT * FROM ${TABLE_REF} LIMIT ${LIMIT_ROWS};"
fi

COUNT_SQL=""
if [[ "${RUN_COUNT}" == "true" ]]; then
  COUNT_SQL="SELECT COUNT(*) AS row_count FROM ${TABLE_REF};"
fi

MAX_FRESHNESS_SQL=""
if [[ "${RUN_MAX_FRESHNESS_TS}" == "true" ]]; then
  MAX_FRESHNESS_SQL="SELECT MAX(freshness_ts) AS max_freshness_ts FROM ${TABLE_REF};"
fi

SQL_FILE="$(mktemp /tmp/hudi-mor-read-XXXXXX.sql)"
trap 'rm -f "${SQL_FILE}"' EXIT

cat > "${SQL_FILE}" <<EOF
SET hoodie.metadata.enable=true;
SET hoodie.datasource.query.type=${QUERY_TYPE};

CREATE OR REPLACE TEMPORARY VIEW ${TABLE_ALIAS}
USING hudi
OPTIONS (
  path '${TABLE_PATH}'
);

DESCRIBE TABLE ${TABLE_REF};
${COUNT_SQL}
${MAX_FRESHNESS_SQL}
EOF

# ${SAMPLE_SQL}
exec "${SPARK_SQL_BIN}" \
  --master "${SPARK_MASTER}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.sql.shuffle.partitions="${SPARK_SQL_SHUFFLE_PARTITIONS}" \
  --conf spark.default.parallelism="${SPARK_DEFAULT_PARALLELISM}" \
  --conf spark.driver.extraJavaOptions="${SPARK_DRIVER_EXTRA_JAVA_OPTIONS}" \
  --conf spark.executor.extraJavaOptions="${SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS}" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key="${S3_ACCESS_KEY}" \
  --conf spark.hadoop.fs.s3a.secret.key="${S3_SECRET_KEY}" \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT#https://}" \
  --conf spark.hadoop.fs.s3a.path.style.access=false \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true \
  --conf spark.hadoop.fs.s3a.endpoint.region="${AWS_REGION}" \
  -f "${SQL_FILE}"
