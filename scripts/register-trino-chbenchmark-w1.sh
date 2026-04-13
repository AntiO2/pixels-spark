#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${ROOT_DIR}/etc/pixels-spark.properties}"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"

TRINO_CLI="${TRINO_CLI:-/home/ubuntu/disk1/opt/trino-cli/trino}"
TRINO_SERVER="${TRINO_SERVER:-http://127.0.0.1:8080}"

CATALOG="${1:-$(pixels_get_property pixels.trino.chbenchmark.w1.catalog chbench_wh1)}"
SCHEMA="${2:-$(pixels_get_property pixels.trino.chbenchmark.w1.schema chbenchmark_w1)}"
TARGET_ROOT="${3:-$(pixels_get_property pixels.trino.chbenchmark.w1.target-root s3://home-zinuo/deltalake/chBench_wh1)}"
TABLES=()
pixels_split_csv_property "$(pixels_get_property pixels.import.chbenchmark.tables warehouse,district,customer,history,neworder,order,orderline,item,stock,nation,supplier,region)" TABLES

"$TRINO_CLI" --server "$TRINO_SERVER" \
  --execute "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA}"

for table_name in "${TABLES[@]}"; do
  present="$("$TRINO_CLI" --server "$TRINO_SERVER" --output-format CSV_HEADER --execute \
    "SELECT count(*) AS present FROM ${CATALOG}.information_schema.tables WHERE table_schema = '${SCHEMA}' AND table_name = '${table_name}'" \
    | tail -n 1 | tr -d '\r' | tr -d '"')"
  if [[ "${present}" == "1" ]]; then
    echo "skip table=${table_name} reason=already-registered"
    continue
  fi

  echo "register table=${table_name} catalog=${CATALOG} schema=${SCHEMA} location=${TARGET_ROOT}/${table_name}"
  "$TRINO_CLI" --server "$TRINO_SERVER" \
    --execute "CALL ${CATALOG}.system.register_table(schema_name => '${SCHEMA}', table_name => '${table_name}', table_location => '${TARGET_ROOT}/${table_name}')"
done

"$TRINO_CLI" --server "$TRINO_SERVER" \
  --execute "SHOW TABLES FROM ${CATALOG}.${SCHEMA}"
