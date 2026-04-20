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

TARGET_PATH="${TARGET_PATH:-/tmp/pixels_merge_core_delta}"
TABLE_NAME="${TABLE_NAME:-loantrans}"
PRIMARY_KEY="${PRIMARY_KEY:-id}"
BUCKET_ID="${BUCKET_ID:-_pixels_bucket_id}"
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-8g}"
JAR_PATH="${JAR_PATH:-${ROOT_DIR}/target/pixels-spark-0.1.jar}"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/explain-delta-merge-core.sh

Optional env:
  TARGET_PATH         Temporary Delta target path. Default: /tmp/pixels_merge_core_delta
  TABLE_NAME          Logical example name used in comments only. Default: loantrans
  PRIMARY_KEY         Merge primary key column. Default: id
  BUCKET_ID           Bucket column name. Default: _pixels_bucket_id
  SPARK_MASTER        Spark master. Default: local[4]
  SPARK_DRIVER_MEMORY Driver memory. Default: 8g
  SPARK_SQL_BIN       Path to spark-sql
  JAR_PATH            Local shaded jar that contains Delta classes

This script prints EXPLAIN FORMATTED for a minimal Delta MERGE core:
  source view -> target Delta scan -> merge join -> update write path

It intentionally excludes CDC dedup/window logic.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "Missing jar at ${JAR_PATH}" >&2
  exit 1
fi

SQL_FILE="$(mktemp /tmp/pixels-merge-core-XXXXXX.sql)"
trap 'rm -f "${SQL_FILE}"' EXIT

cat > "${SQL_FILE}" <<EOF
DROP TABLE IF EXISTS delta.\`${TARGET_PATH}\`;

CREATE TABLE delta.\`${TARGET_PATH}\` (
  id INT,
  applicantID INT,
  appID INT,
  amount FLOAT,
  status STRING,
  ts TIMESTAMP,
  duration INT,
  contract_timestamp TIMESTAMP,
  delinquency INT,
  freshness_ts TIMESTAMP,
  ${BUCKET_ID} INT
) USING DELTA
PARTITIONED BY (${BUCKET_ID});

INSERT INTO delta.\`${TARGET_PATH}\` VALUES
  (1, 10, 100, CAST(12.3 AS FLOAT), 'old', timestamp('2026-04-16 07:00:00'), 12, timestamp('2026-04-16 07:00:00'), 0, timestamp('2026-04-16 07:00:01'), 7),
  (2, 20, 200, CAST(22.3 AS FLOAT), 'old', timestamp('2026-04-16 07:00:02'), 24, timestamp('2026-04-16 07:00:02'), 1, timestamp('2026-04-16 07:00:03'), 3);

CREATE OR REPLACE TEMP VIEW merge_source AS
SELECT * FROM VALUES
  (1, 10, 101, CAST(13.3 AS FLOAT), 'new', timestamp('2026-04-16 07:00:04'), 12, timestamp('2026-04-16 07:00:04'), 0, timestamp('2026-04-16 07:00:05'), 7),
  (2, 20, 201, CAST(23.3 AS FLOAT), 'new', timestamp('2026-04-16 07:00:06'), 24, timestamp('2026-04-16 07:00:06'), 1, timestamp('2026-04-16 07:00:07'), 3)
AS s(id, applicantID, appID, amount, status, ts, duration, contract_timestamp, delinquency, freshness_ts, ${BUCKET_ID});

EXPLAIN FORMATTED
MERGE INTO delta.\`${TARGET_PATH}\` t
USING merge_source s
ON t.${PRIMARY_KEY} <=> s.${PRIMARY_KEY}
AND t.${BUCKET_ID} <=> s.${BUCKET_ID}
WHEN MATCHED THEN UPDATE SET
  t.id = s.id,
  t.applicantID = s.applicantID,
  t.appID = s.appID,
  t.amount = s.amount,
  t.status = s.status,
  t.ts = s.ts,
  t.duration = s.duration,
  t.contract_timestamp = s.contract_timestamp,
  t.delinquency = s.delinquency,
  t.freshness_ts = s.freshness_ts,
  t.${BUCKET_ID} = s.${BUCKET_ID};
EOF

exec "${SPARK_SQL_BIN}" \
  --master "${SPARK_MASTER}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --jars "${JAR_PATH}" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -f "${SQL_FILE}"
