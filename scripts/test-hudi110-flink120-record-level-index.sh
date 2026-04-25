#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# NOTE: user asked Flink 1.2.0; Hudi 1.1.0 requires modern Flink.
# This script uses Flink 1.20.1 by default (override via FLINK_VERSION).
FLINK_VERSION="${FLINK_VERSION:-1.20.1}"
HUDI_VERSION="${HUDI_VERSION:-1.1.0}"
SCALA_BINARY_VERSION="${SCALA_BINARY_VERSION:-2.12}"
JAVA_HOME_DEFAULT="${JAVA_HOME_DEFAULT:-/home/ubuntu/disk1/opt/jdk-17.0.14+7}"
WORK_DIR="${WORK_DIR:-/tmp/hudi-flink-rli-test}"
KEEP_WORK_DIR="${KEEP_WORK_DIR:-true}"

PARTITION_COUNT="${PARTITION_COUNT:-40}"
KEYS_PER_PARTITION="${KEYS_PER_PARTITION:-2500}"
PARALLELISM="${PARALLELISM:-4}"

FLINK_ARCHIVE="flink-${FLINK_VERSION}-bin-scala_${SCALA_BINARY_VERSION}.tgz"
FLINK_URL="https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/${FLINK_ARCHIVE}"
HUDI_BUNDLE="hudi-flink1.20-bundle-${HUDI_VERSION}.jar"
HUDI_BUNDLE_URL="https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink1.20-bundle/${HUDI_VERSION}/${HUDI_BUNDLE}"
HADOOP_VERSION="${HADOOP_VERSION:-3.3.6}"
HADOOP_CLIENT_API_JAR="hadoop-client-api-${HADOOP_VERSION}.jar"
HADOOP_CLIENT_RUNTIME_JAR="hadoop-client-runtime-${HADOOP_VERSION}.jar"
HADOOP_CLIENT_API_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/${HADOOP_CLIENT_API_JAR}"
HADOOP_CLIENT_RUNTIME_URL="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/${HADOOP_CLIENT_RUNTIME_JAR}"
COMMONS_LOGGING_VERSION="${COMMONS_LOGGING_VERSION:-1.2}"
COMMONS_LOGGING_JAR="commons-logging-${COMMONS_LOGGING_VERSION}.jar"
COMMONS_LOGGING_URL="https://repo1.maven.org/maven2/commons-logging/commons-logging/${COMMONS_LOGGING_VERSION}/${COMMONS_LOGGING_JAR}"

DOWNLOAD_DIR="${WORK_DIR}/downloads"
FLINK_HOME="${WORK_DIR}/flink-${FLINK_VERSION}"
DATA_DIR="${WORK_DIR}/data"
LOG_DIR="${WORK_DIR}/logs"
TABLE_ROOT="${WORK_DIR}/tables"

mkdir -p "${DOWNLOAD_DIR}" "${DATA_DIR}" "${LOG_DIR}" "${TABLE_ROOT}"

if [[ -d "${JAVA_HOME_DEFAULT}" && -z "${JAVA_HOME:-}" ]]; then
  export JAVA_HOME="${JAVA_HOME_DEFAULT}"
fi

if [[ -z "${JAVA_HOME:-}" ]]; then
  echo "JAVA_HOME is not set and JAVA_HOME_DEFAULT not found: ${JAVA_HOME_DEFAULT}" >&2
  exit 1
fi

download_if_missing() {
  local url="$1"
  local target="$2"
  if [[ -f "${target}" ]]; then
    return 0
  fi
  echo "download file=${target} url=${url}"
  curl -fL "${url}" -o "${target}"
}

prepare_flink() {
  download_if_missing "${FLINK_URL}" "${DOWNLOAD_DIR}/${FLINK_ARCHIVE}"
  if [[ ! -d "${FLINK_HOME}" ]]; then
    tar -xzf "${DOWNLOAD_DIR}/${FLINK_ARCHIVE}" -C "${WORK_DIR}"
  fi

  download_if_missing "${HUDI_BUNDLE_URL}" "${DOWNLOAD_DIR}/${HUDI_BUNDLE}"
  cp -f "${DOWNLOAD_DIR}/${HUDI_BUNDLE}" "${FLINK_HOME}/lib/${HUDI_BUNDLE}"
  download_if_missing "${HADOOP_CLIENT_API_URL}" "${DOWNLOAD_DIR}/${HADOOP_CLIENT_API_JAR}"
  download_if_missing "${HADOOP_CLIENT_RUNTIME_URL}" "${DOWNLOAD_DIR}/${HADOOP_CLIENT_RUNTIME_JAR}"
  download_if_missing "${COMMONS_LOGGING_URL}" "${DOWNLOAD_DIR}/${COMMONS_LOGGING_JAR}"
  cp -f "${DOWNLOAD_DIR}/${HADOOP_CLIENT_API_JAR}" "${FLINK_HOME}/lib/${HADOOP_CLIENT_API_JAR}"
  cp -f "${DOWNLOAD_DIR}/${HADOOP_CLIENT_RUNTIME_JAR}" "${FLINK_HOME}/lib/${HADOOP_CLIENT_RUNTIME_JAR}"
  cp -f "${DOWNLOAD_DIR}/${COMMONS_LOGGING_JAR}" "${FLINK_HOME}/lib/${COMMONS_LOGGING_JAR}"

  # Keep output compact for repeatable parsing.
  cat > "${FLINK_HOME}/conf/flink-conf.yaml" <<EOF
jobmanager.rpc.address: localhost
parallelism.default: ${PARALLELISM}
taskmanager.numberOfTaskSlots: ${PARALLELISM}
state.backend.type: hashmap
jobmanager.memory.process.size: 1600m
taskmanager.memory.process.size: 1728m
EOF
}

generate_source_data() {
  local out_csv="$1"
  awk -v pmax="${PARTITION_COUNT}" -v kmax="${KEYS_PER_PARTITION}" 'BEGIN {
    for (p = 1; p <= pmax; p++) {
      dt = sprintf("p%03d", p);
      for (k = 1; k <= kmax; k++) {
        ts = 1700000000000 + p;
        amount = (k % 1000) + (p / 1000.0);
        printf "%d,%s,%.3f,%d\n", k, dt, amount, ts;
      }
    }
  }' > "${out_csv}"
}

write_job_sql() {
  local sql_file="$1"
  local source_csv="$2"
  local table_path="$3"
  local index_type="$4"
  local enable_partitioned_rli="$5"
  local enable_global_rli="$6"

  cat > "${sql_file}" <<EOF
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '${PARALLELISM}';
SET 'table.dml-sync' = 'true';

CREATE TEMPORARY TABLE src_input (
  id INT,
  dt STRING,
  amount DOUBLE,
  ts BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = 'file://${source_csv}',
  'format' = 'csv'
);

CREATE TABLE sink_hudi (
  id INT,
  dt STRING,
  amount DOUBLE,
  ts BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
  'connector' = 'hudi',
  'path' = 'file://${table_path}',
  'table.type' = 'MERGE_ON_READ',
  'write.operation' = 'upsert',
  'hoodie.table.name' = 'rli_test_${index_type}',
  'precombine.field' = 'ts',
  'hoodie.datasource.write.recordkey.field' = 'id',
  'hoodie.datasource.write.partitionpath.field' = 'dt',
  'hoodie.datasource.write.precombine.field' = 'ts',
  'hoodie.metadata.enable' = 'true',
  'hoodie.metadata.record.level.index.enable' = '${enable_partitioned_rli}',
  'hoodie.metadata.global.record.level.index.enable' = '${enable_global_rli}',
  'index.type' = '${index_type}',
  'compaction.async.enabled' = 'false',
  'hoodie.compact.inline' = 'false'
);

INSERT INTO sink_hudi SELECT id, dt, amount, ts FROM src_input;
EOF
}

write_count_sql() {
  local sql_file="$1"
  local table_path="$2"
  cat > "${sql_file}" <<EOF
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '${PARALLELISM}';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE TABLE sink_hudi (
  id INT,
  dt STRING,
  amount DOUBLE,
  ts BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
  'connector' = 'hudi',
  'path' = 'file://${table_path}',
  'table.type' = 'MERGE_ON_READ'
);

SELECT COUNT(*) AS cnt FROM sink_hudi;
SELECT COUNT(DISTINCT dt) AS partition_cnt FROM sink_hudi;
EOF
}

extract_first_numeric_cell() {
  local file="$1"
  local v
  v="$(grep -Eo '\+I\[[0-9]+\]' "${file}" | head -n 1 | sed -E 's/\+I\[([0-9]+)\]/\1/' || true)"
  if [[ -n "${v}" ]]; then
    echo "${v}"
    return 0
  fi
  grep -E '^\|\s*[0-9]+' "${file}" | head -n 1 | sed -E 's/^\|\s*([0-9]+).*/\1/'
}

run_sql_file() {
  local sql_file="$1"
  local log_file="$2"
  "${FLINK_HOME}/bin/sql-client.sh" embedded \
    -D execution.target=remote \
    -D rest.address=localhost \
    -D rest.port=8081 \
    -f "${sql_file}" > "${log_file}" 2>&1
}

start_cluster() {
  "${FLINK_HOME}/bin/stop-cluster.sh" >/dev/null 2>&1 || true
  "${FLINK_HOME}/bin/start-cluster.sh" >/dev/null 2>&1
  sleep 3
}

stop_cluster() {
  "${FLINK_HOME}/bin/stop-cluster.sh" >/dev/null 2>&1 || true
}

run_case() {
  local case_name="$1"
  local index_type="$2"
  local enable_partitioned_rli="$3"
  local enable_global_rli="$4"

  local source_csv="${DATA_DIR}/source.csv"
  local table_path="${TABLE_ROOT}/${case_name}"
  local job_sql="${WORK_DIR}/job-${case_name}.sql"
  local count_sql="${WORK_DIR}/count-${case_name}.sql"
  local job_log="${LOG_DIR}/job-${case_name}.log"
  local count_log="${LOG_DIR}/count-${case_name}.log"

  rm -rf "${table_path}"
  mkdir -p "${table_path}"

  write_job_sql "${job_sql}" "${source_csv}" "${table_path}" "${index_type}" "${enable_partitioned_rli}" "${enable_global_rli}"
  write_count_sql "${count_sql}" "${table_path}"

  local start_ts end_ts elapsed
  start_ts="$(date +%s)"
  run_sql_file "${job_sql}" "${job_log}"
  end_ts="$(date +%s)"
  elapsed="$((end_ts - start_ts))"

  if grep -q "\[ERROR\]" "${job_log}"; then
    local err_line
    err_line="$(awk '/\[ERROR\]/{getline; gsub(/\x1b\[[0-9;]*m/, "", $0); print; exit}' "${job_log}")"
    if [[ -z "${err_line}" ]]; then
      err_line="$(grep "\[ERROR\]" "${job_log}" | tail -n 1 | sed -E 's/.*\[ERROR\]\s*//')"
    fi
    echo "case=${case_name} index_type=${index_type} status=FAILED elapsed_seconds=${elapsed} error=\"${err_line}\" job_log=${job_log}"
    return 0
  fi

  run_sql_file "${count_sql}" "${count_log}"
  if grep -q "\[ERROR\]" "${count_log}"; then
    local count_err
    count_err="$(awk '/\[ERROR\]/{getline; gsub(/\x1b\[[0-9;]*m/, "", $0); print; exit}' "${count_log}")"
    if [[ -z "${count_err}" ]]; then
      count_err="$(grep "\[ERROR\]" "${count_log}" | tail -n 1 | sed -E 's/.*\[ERROR\]\s*//')"
    fi
    echo "case=${case_name} index_type=${index_type} status=FAILED elapsed_seconds=${elapsed} error=\"${count_err}\" job_log=${job_log} count_log=${count_log}"
    return 0
  fi

  local row_count partition_count
  row_count="$(extract_first_numeric_cell "${count_log}")"
  partition_count="$(grep -Eo '\+I\[[0-9]+\]' "${count_log}" | tail -n 1 | sed -E 's/\+I\[([0-9]+)\]/\1/' || true)"
  if [[ -z "${partition_count}" ]]; then
    partition_count="$(grep -E '^\|\s*[0-9]+' "${count_log}" | tail -n 1 | sed -E 's/^\|\s*([0-9]+).*/\1/' || true)"
  fi

  echo "case=${case_name} index_type=${index_type} status=OK elapsed_seconds=${elapsed} row_count=${row_count:-NA} partition_count=${partition_count:-NA} job_log=${job_log} count_log=${count_log}"
}

main() {
  local source_csv="${DATA_DIR}/source.csv"
  local total_rows

  prepare_flink
  start_cluster
  trap 'stop_cluster' EXIT
  generate_source_data "${source_csv}"
  total_rows="$((PARTITION_COUNT * KEYS_PER_PARTITION))"

  echo "dataset partition_count=${PARTITION_COUNT} keys_per_partition=${KEYS_PER_PARTITION} total_rows=${total_rows} source=${source_csv}"

  # Baseline: global uniqueness across partitions.
  run_case "global_rli" "GLOBAL_RECORD_LEVEL_INDEX" "false" "true"

  # Target: partitioned record level index (unique by partition_path + record_key).
  run_case "partitioned_rli" "RECORD_LEVEL_INDEX" "true" "false"

  if [[ "${KEEP_WORK_DIR}" != "true" ]]; then
    rm -rf "${WORK_DIR}"
  fi
}

main "$@"
