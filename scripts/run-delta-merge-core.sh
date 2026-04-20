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

TARGET_PATH="${TARGET_PATH:-/tmp/pixels_merge_core_delta_run}"
PRIMARY_KEY="${PRIMARY_KEY:-id}"
BUCKET_ID="${BUCKET_ID:-_pixels_bucket_id}"
SPARK_MASTER="${SPARK_MASTER:-local[4]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-8g}"
JAR_PATH="${JAR_PATH:-${ROOT_DIR}/target/pixels-spark-0.1.jar}"
SPARK_EVENTS_DIR="${SPARK_EVENTS_DIR:-/home/ubuntu/disk1/tmp/spark-events}"
SPARK_APP_NAME="${SPARK_APP_NAME:-pixels-delta-merge-core}"
SPARK_UI_PORT="${SPARK_UI_PORT:-4040}"
KEEP_DRIVER_ALIVE_SECONDS="${KEEP_DRIVER_ALIVE_SECONDS:-20}"
DISABLE_WHOLESTAGE="${DISABLE_WHOLESTAGE:-false}"
DISABLE_AQE="${DISABLE_AQE:-false}"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run-delta-merge-core.sh

This script executes a minimal Delta MERGE INTO job, writes Spark event logs,
and keeps the driver alive briefly so you can inspect the live Spark UI.

Optional env:
  TARGET_PATH               Temporary Delta target path
  PRIMARY_KEY               Merge primary key column. Default: id
  BUCKET_ID                 Bucket column name. Default: _pixels_bucket_id
  SPARK_MASTER              Spark master. Default: local[4]
  SPARK_DRIVER_MEMORY       Driver memory. Default: 8g
  SPARK_EVENTS_DIR          Event log directory. Default: /home/ubuntu/disk1/tmp/spark-events
  SPARK_APP_NAME            Spark application name
  SPARK_UI_PORT             Preferred Spark UI port. Default: 4040
  KEEP_DRIVER_ALIVE_SECONDS Seconds to sleep after MERGE. Default: 20
  DISABLE_WHOLESTAGE        true to set spark.sql.codegen.wholeStage=false
  DISABLE_AQE               true to set spark.sql.adaptive.enabled=false
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

mkdir -p "${SPARK_EVENTS_DIR}"

SQL_FILE="$(mktemp /tmp/pixels-merge-run-XXXXXX.sql)"
trap 'rm -f "${SQL_FILE}"' EXIT

WHOLESTAGE_SQL=""
if [[ "${DISABLE_WHOLESTAGE}" == "true" ]]; then
  WHOLESTAGE_SQL="SET spark.sql.codegen.wholeStage=false;"
fi

AQE_SQL=""
if [[ "${DISABLE_AQE}" == "true" ]]; then
  AQE_SQL="SET spark.sql.adaptive.enabled=false;"
fi

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

${WHOLESTAGE_SQL}
${AQE_SQL}

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

SELECT 'Spark UI should be available while this session sleeps' AS note;
EOF

echo "Spark UI: http://127.0.0.1:${SPARK_UI_PORT}"
echo "History UI: http://127.0.0.1:18080"
echo "Event log dir: ${SPARK_EVENTS_DIR}"

"${SPARK_SQL_BIN}" \
  --master "${SPARK_MASTER}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --name "${SPARK_APP_NAME}" \
  --conf spark.ui.port="${SPARK_UI_PORT}" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir="file://${SPARK_EVENTS_DIR}" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars "${JAR_PATH}" \
  -f "${SQL_FILE}" &

SPARK_PID=$!
trap 'kill "${SPARK_PID}" 2>/dev/null || true' EXIT

sleep "${KEEP_DRIVER_ALIVE_SECONDS}"
wait "${SPARK_PID}"
