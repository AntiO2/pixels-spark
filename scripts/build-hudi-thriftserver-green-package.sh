#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACKAGE_VERSION="${PACKAGE_VERSION:-0.1}"
BUILD_STAMP="${BUILD_STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
PACKAGE_BASENAME="pixels-spark-hudi-thriftserver-${PACKAGE_VERSION}-${BUILD_STAMP}"
OUTPUT_ROOT="${OUTPUT_ROOT:-/home/ubuntu/disk1}"
PACKAGE_DIR="${OUTPUT_ROOT}/${PACKAGE_BASENAME}"
TARBALL_PATH="${OUTPUT_ROOT}/${PACKAGE_BASENAME}.tar.gz"

PIXELS_JAR="${PIXELS_JAR:-${ROOT_DIR}/target/pixels-spark-0.1.jar}"
SPARK_DIST="${SPARK_DIST:-/home/ubuntu/disk1/opt/spark-3.5.6-bin-hadoop3}"
HUDI_BUNDLE_JAR="${HUDI_BUNDLE_JAR:-${HOME}/.ivy2/jars/org.apache.hudi_hudi-spark3.5-bundle_2.12-0.15.0.jar}"
HIVE_STORAGE_API_JAR="${HIVE_STORAGE_API_JAR:-${HOME}/.ivy2/jars/org.apache.hive_hive-storage-api-2.8.1.jar}"
SLF4J_API_JAR="${SLF4J_API_JAR:-${HOME}/.ivy2/jars/org.slf4j_slf4j-api-1.7.36.jar}"
PIXELS_DELTA_ENV_SOURCE="${PIXELS_DELTA_ENV_SOURCE:-/home/ubuntu/disk1/opt/conf/pixels-delta-env.sh}"

require_file() {
  local path="$1"
  if [[ ! -f "${path}" ]]; then
    echo "Missing required file: ${path}" >&2
    exit 1
  fi
}

require_dir() {
  local path="$1"
  if [[ ! -d "${path}" ]]; then
    echo "Missing required directory: ${path}" >&2
    exit 1
  fi
}

require_file "${PIXELS_JAR}"
require_dir "${SPARK_DIST}"
require_file "${HUDI_BUNDLE_JAR}"
require_file "${HIVE_STORAGE_API_JAR}"
require_file "${SLF4J_API_JAR}"

rm -rf "${PACKAGE_DIR}"
mkdir -p \
  "${PACKAGE_DIR}/bin" \
  "${PACKAGE_DIR}/conf/hudi" \
  "${PACKAGE_DIR}/lib" \
  "${PACKAGE_DIR}/app" \
  "${PACKAGE_DIR}/logs" \
  "${PACKAGE_DIR}/run"

cp -a "${SPARK_DIST}" "${PACKAGE_DIR}/spark"
cp -a "${PIXELS_JAR}" "${PACKAGE_DIR}/app/"
cp -a "${HUDI_BUNDLE_JAR}" "${PACKAGE_DIR}/lib/"
cp -a "${HIVE_STORAGE_API_JAR}" "${PACKAGE_DIR}/lib/"
cp -a "${SLF4J_API_JAR}" "${PACKAGE_DIR}/lib/"

if [[ -f "${PIXELS_DELTA_ENV_SOURCE}" ]]; then
  cp -a "${PIXELS_DELTA_ENV_SOURCE}" "${PACKAGE_DIR}/conf/pixels-delta-env.sh"
fi

cat > "${PACKAGE_DIR}/conf/hudi/hudi-defaults.conf" <<'EOF'
# Optional Hudi defaults for the bundled Spark Thrift Server.
# Add shared Hudi read settings here if needed.
EOF

cat > "${PACKAGE_DIR}/conf/hadoop-metrics2.properties" <<'EOF'
# Intentionally minimal. Present to suppress noisy lookup warnings for hadoop-metrics2.
EOF

cat > "${PACKAGE_DIR}/README.md" <<'EOF'
# Pixels Spark Hudi Thrift Server Green Package

This bundle contains:

- Spark 3.5.6 runtime
- Hudi Spark 3.5 bundle
- supporting jars required by the local Hudi read path
- the current `pixels-spark` shaded jar
- wrapper scripts to start/stop a Spark Thrift Server with Hudi + S3 support

## Layout

- `spark/`: bundled Spark runtime
- `lib/`: bundled Hudi/Spark service dependencies
- `app/`: bundled `pixels-spark` application jar
- `conf/`: local config and templates
- `bin/`: service scripts
- `logs/`: runtime logs
- `run/`: pid files and temporary state

## Prerequisites

- Java 17 installed on the host
- valid AWS credentials in `~/.aws/credentials` or via env vars

## Start

```bash
./bin/start-thriftserver.sh
```

Default JDBC endpoint:

```text
jdbc:hive2://127.0.0.1:10000/default
```

Register all bundled HyBench Hudi tables after the service is up:

```bash
./bin/init-all-hudi-tables.sh
```

## Stop

```bash
./bin/stop-thriftserver.sh
```

## Notes

- The start script uses Hudi path reads and leaves the metastore untouched unless you register objects explicitly.
- Override runtime knobs with environment variables before starting the service.
EOF

cat > "${PACKAGE_DIR}/bin/start-thriftserver.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${ROOT_DIR}/conf/pixels-delta-env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/conf/pixels-delta-env.sh"
fi

if [[ -n "${JAVA17_HOME:-}" ]]; then
  export JAVA_HOME="${JAVA17_HOME}"
fi

if [[ -z "${JAVA_HOME:-}" ]]; then
  echo "JAVA_HOME is not set. Install Java 17 or source conf/pixels-delta-env.sh." >&2
  exit 1
fi

AWS_PROFILE_NAME="${AWS_PROFILE:-default}"
AWS_CREDENTIALS_FILE="${AWS_CREDENTIALS_FILE:-${HOME}/.aws/credentials}"
AWS_CONFIG_FILE="${AWS_CONFIG_FILE:-${HOME}/.aws/config}"
AWS_REGION="${AWS_REGION:-us-east-2}"
S3_ENDPOINT="${S3_ENDPOINT:-https://s3.us-east-2.amazonaws.com}"

aws_profile_header() {
  local profile_name="$1"
  if [[ "${profile_name}" == "default" ]]; then
    printf '[default]'
  else
    printf '[profile %s]' "${profile_name}"
  fi
}

aws_get_value() {
  local profile="$1"
  local key="$2"
  local file="$3"
  local section="$4"

  [[ -f "${file}" ]] || return 0

  awk -v profile_name="${section}" -v key_name="${key}" '
    BEGIN { in_profile = 0 }
    /^[[:space:]]*\[/ {
      in_profile = ($0 == profile_name)
      next
    }
    in_profile {
      split($0, pair, "=")
      current_key = pair[1]
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", current_key)
      if (current_key == key_name) {
        sub(/^[^=]*=/, "", $0)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
        print $0
        exit
      }
    }
  ' "${file}"
}

if [[ -z "${S3_ACCESS_KEY:-}" ]]; then
  S3_ACCESS_KEY="$(aws_get_value "${AWS_PROFILE_NAME}" aws_access_key_id "${AWS_CREDENTIALS_FILE}" "[${AWS_PROFILE_NAME}]")"
fi
if [[ -z "${S3_SECRET_KEY:-}" ]]; then
  S3_SECRET_KEY="$(aws_get_value "${AWS_PROFILE_NAME}" aws_secret_access_key "${AWS_CREDENTIALS_FILE}" "[${AWS_PROFILE_NAME}]")"
fi
if [[ "${AWS_REGION}" == "us-east-2" ]]; then
  config_section="$(aws_profile_header "${AWS_PROFILE_NAME}")"
  config_region="$(aws_get_value "${AWS_PROFILE_NAME}" region "${AWS_CONFIG_FILE}" "${config_section}")"
  if [[ -n "${config_region}" ]]; then
    AWS_REGION="${config_region}"
  fi
fi

if [[ -z "${S3_ACCESS_KEY:-}" || -z "${S3_SECRET_KEY:-}" ]]; then
  echo "Missing AWS credentials. Set AWS_PROFILE or S3_ACCESS_KEY/S3_SECRET_KEY." >&2
  exit 1
fi

export HUDI_CONF_DIR="${ROOT_DIR}/conf/hudi"
export SPARK_HOME="${ROOT_DIR}/spark"
export SPARK_LOG_DIR="${ROOT_DIR}/logs"
export SPARK_PID_DIR="${ROOT_DIR}/run"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${ROOT_DIR}/conf}"

THRIFT_HOST="${THRIFT_HOST:-0.0.0.0}"
THRIFT_PORT="${THRIFT_PORT:-10000}"
THRIFT_HTTP_PORT="${THRIFT_HTTP_PORT:-10001}"
THRIFT_WORK_DIR="${THRIFT_WORK_DIR:-${ROOT_DIR}/run/spark-warehouse}"
HUDI_DB_ROOT="${HUDI_DB_ROOT:-s3a://home-haoyue/hudi/hudi_hybench_sf1333_2.db}"
GLUE_ENABLED="${GLUE_ENABLED:-true}"
GLUE_REGION="${GLUE_REGION:-${AWS_REGION}}"
GLUE_CLIENT_JARS="${GLUE_CLIENT_JARS:-}"
SPARK_MASTER="${SPARK_MASTER:-local[16]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-48g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-16g}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-1}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-16}"
SPARK_DRIVER_EXTRA_JAVA_OPTIONS="${SPARK_DRIVER_EXTRA_JAVA_OPTIONS:--Djdk.attach.allowAttachSelf=true --add-modules=jdk.attach --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED}"
SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="${SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS:--Djdk.attach.allowAttachSelf=true --add-modules=jdk.attach --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED}"

mkdir -p "${SPARK_LOG_DIR}" "${SPARK_PID_DIR}" "${THRIFT_WORK_DIR}"

HUDI_BUNDLE_LOCAL_JAR="$(find "${ROOT_DIR}/lib" -maxdepth 1 -name 'org.apache.hudi_hudi-spark3.5-bundle_2.12-*.jar' | head -n 1)"
HIVE_STORAGE_LOCAL_JAR="$(find "${ROOT_DIR}/lib" -maxdepth 1 -name 'org.apache.hive_hive-storage-api-*.jar' | head -n 1)"
SLF4J_LOCAL_JAR="$(find "${ROOT_DIR}/lib" -maxdepth 1 -name 'org.slf4j_slf4j-api-*.jar' | head -n 1)"
HUDI_JARS="${HUDI_BUNDLE_LOCAL_JAR},${HIVE_STORAGE_LOCAL_JAR},${SLF4J_LOCAL_JAR}"
EXTRA_JARS=""
GLUE_CONF=()

if [[ "${GLUE_ENABLED}" == "true" ]]; then
  if [[ -z "${GLUE_CLIENT_JARS}" ]]; then
    # Optional auto-discovery for dropped-in glue jars under lib/.
    GLUE_CLIENT_JARS="$(find "${ROOT_DIR}/lib" -maxdepth 1 -name '*glue*datacatalog*.jar' -print | paste -sd',' -)"
  fi
  if [[ -z "${GLUE_CLIENT_JARS}" ]]; then
    echo "Glue mode enabled but no Glue client jars found." >&2
    echo "Set GLUE_CLIENT_JARS=/abs/path/a.jar,/abs/path/b.jar or place glue jars under ${ROOT_DIR}/lib" >&2
    exit 1
  fi
  EXTRA_JARS=",${GLUE_CLIENT_JARS}"
  GLUE_CONF=(
    --hiveconf "hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    --hiveconf "hive.metastore.glue.region=${GLUE_REGION}"
    --hiveconf "hive.metastore.glue.catalogid="
    --conf "spark.hadoop.aws.region=${GLUE_REGION}"
  )
fi

exec "${SPARK_HOME}/sbin/start-thriftserver.sh" \
  --master "${SPARK_MASTER}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY}" \
  --jars "${HUDI_JARS}${EXTRA_JARS}" \
  --hiveconf hive.server2.thrift.bind.host="${THRIFT_HOST}" \
  --hiveconf hive.server2.thrift.port="${THRIFT_PORT}" \
  --hiveconf hive.server2.transport.mode=binary \
  --conf spark.sql.warehouse.dir="${THRIFT_WORK_DIR}" \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.sql.files.maxPartitionBytes=1073741824 \
  --conf spark.sql.shuffle.partitions="${SPARK_SQL_SHUFFLE_PARTITIONS}" \
  --conf spark.default.parallelism="${SPARK_DEFAULT_PARALLELISM}" \
  --conf spark.sql.hive.thriftServer.singleSession=true \
  --conf spark.hadoop.hoodie.metadata.enable=true \
  --conf spark.hadoop.hoodie.datasource.query.type=snapshot \
  --conf spark.driver.extraJavaOptions="${SPARK_DRIVER_EXTRA_JAVA_OPTIONS}" \
  --conf spark.executor.extraJavaOptions="${SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS}" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key="${S3_ACCESS_KEY}" \
  --conf spark.hadoop.fs.s3a.secret.key="${S3_SECRET_KEY}" \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT#https://}" \
  --conf spark.hadoop.fs.s3a.endpoint.region="${AWS_REGION}" \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true \
  --conf spark.hadoop.fs.s3a.path.style.access=false \
  "${GLUE_CONF[@]}"
EOF

cat > "${PACKAGE_DIR}/bin/stop-thriftserver.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SPARK_HOME="${ROOT_DIR}/spark"
export SPARK_PID_DIR="${ROOT_DIR}/run"
exec "${SPARK_HOME}/sbin/stop-thriftserver.sh"
EOF

cat > "${PACKAGE_DIR}/bin/status-thriftserver.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$(find "${ROOT_DIR}/run" -maxdepth 1 -name '*ThriftServer*.pid' | head -n 1 || true)"

if [[ -z "${PID_FILE}" || ! -f "${PID_FILE}" ]]; then
  echo "status=stopped"
  exit 1
fi

PID="$(cat "${PID_FILE}")"
if kill -0 "${PID}" 2>/dev/null; then
  echo "status=running pid=${PID} pid_file=${PID_FILE}"
else
  echo "status=stale pid=${PID} pid_file=${PID_FILE}"
  exit 1
fi
EOF

cat > "${PACKAGE_DIR}/bin/beeline.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BEELINE_BIN="${ROOT_DIR}/spark/bin/beeline"
JDBC_URL="${JDBC_URL:-jdbc:hive2://127.0.0.1:10000/default}"

exec "${BEELINE_BIN}" -u "${JDBC_URL}" "$@"
EOF

cat > "${PACKAGE_DIR}/bin/init-all-hudi-tables.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JDBC_URL="${JDBC_URL:-jdbc:hive2://127.0.0.1:10000/default}"
SCHEMA_NAME="${SCHEMA_NAME:-hudi_hybench_sf1333_2}"
HUDI_DB_ROOT="${HUDI_DB_ROOT:-s3a://home-haoyue/hudi/hudi_hybench_sf1333_2.db}"
TABLES_RAW="${TABLES_RAW:-customer company savingaccount checkingaccount transfer checking loanapps loantrans}"
BEELINE_BIN="${ROOT_DIR}/spark/bin/beeline"
SQL_FILE="$(mktemp /tmp/pixels-hudi-init-XXXXXX.sql)"
trap 'rm -f "${SQL_FILE}"' EXIT

{
  printf 'CREATE DATABASE IF NOT EXISTS %s;\n' "${SCHEMA_NAME}"
  for table_name in ${TABLES_RAW}; do
    printf 'CREATE TABLE IF NOT EXISTS %s.%s USING hudi LOCATION '\''%s/%s'\'';\n' \
      "${SCHEMA_NAME}" "${table_name}" "${HUDI_DB_ROOT}" "${table_name}"
  done
  printf 'SHOW TABLES IN %s;\n' "${SCHEMA_NAME}"
} > "${SQL_FILE}"

exec "${BEELINE_BIN}" -u "${JDBC_URL}" -f "${SQL_FILE}"
EOF

chmod +x \
  "${PACKAGE_DIR}/bin/start-thriftserver.sh" \
  "${PACKAGE_DIR}/bin/stop-thriftserver.sh" \
  "${PACKAGE_DIR}/bin/status-thriftserver.sh" \
  "${PACKAGE_DIR}/bin/beeline.sh" \
  "${PACKAGE_DIR}/bin/init-all-hudi-tables.sh"

rm -f "${TARBALL_PATH}"
tar -C "${OUTPUT_ROOT}" -czf "${TARBALL_PATH}" "${PACKAGE_BASENAME}"

echo "package_dir=${PACKAGE_DIR}"
echo "tarball=${TARBALL_PATH}"
