#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

mvn -DskipTests package

echo "shaded_jar=${ROOT_DIR}/target/pixels-spark-0.1.jar"
