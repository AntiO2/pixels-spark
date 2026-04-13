#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"
CSV_ROOT="${1:-/home/antio2/projects/pixels-benchmark/Data_1x}"
DELTA_ROOT="${2:-$(pixels_get_property pixels.import.local.delta-root /home/ubuntu/disk1/tmp/pixels-benchmark-deltalake/data_1x)}"
SPARK_MASTER="${3:-local[1]}"
BENCHMARK="${4:-$(pixels_get_property pixels.import.benchmark hybench)}"
PIXELS_SPARK_CONFIG_FILE="${PIXELS_SPARK_CONFIG:-${ROOT_DIR}/etc/pixels-spark.properties}"

cd "${ROOT_DIR}"

env JAVA_HOME="${JAVA_HOME:-/home/antio2/.sdkman/candidates/java/17.0.14-jbr}" \
  PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG_FILE}" \
  SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}" \
  SPARK_LOCAL_HOSTNAME="${SPARK_LOCAL_HOSTNAME:-localhost}" \
  MAVEN_OPTS="${MAVEN_OPTS:---add-opens=java.base/sun.nio.ch=ALL-UNNAMED}" \
  mvn -q -DskipTests \
  -Dexec.classpathScope=compile \
  -Dexec.mainClass=io.pixelsdb.spark.app.PixelsBenchmarkDeltaImportApp \
  -Dexec.args="${CSV_ROOT} ${DELTA_ROOT} ${SPARK_MASTER} ${BENCHMARK}" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
