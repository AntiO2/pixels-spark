#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSV_ROOT="${1:-/home/antio2/projects/pixels-benchmark/Data_1x}"
DELTA_ROOT="${2:-/tmp/pixels-benchmark-deltalake/data_1x}"
SPARK_MASTER="${3:-local[1]}"

cd "${ROOT_DIR}"

env JAVA_HOME="${JAVA_HOME:-/home/antio2/.sdkman/candidates/java/17.0.14-jbr}" \
  SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}" \
  SPARK_LOCAL_HOSTNAME="${SPARK_LOCAL_HOSTNAME:-localhost}" \
  MAVEN_OPTS="${MAVEN_OPTS:---add-opens=java.base/sun.nio.ch=ALL-UNNAMED}" \
  mvn -q -DskipTests \
  -Dexec.classpathScope=compile \
  -Dexec.mainClass=io.pixelsdb.spark.app.PixelsBenchmarkDeltaImportApp \
  -Dexec.args="${CSV_ROOT} ${DELTA_ROOT} ${SPARK_MASTER}" \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java
