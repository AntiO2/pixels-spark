#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAVEN_REPO_LOCAL="${MAVEN_REPO_LOCAL:-$HOME/.m2/repository}"
cd "${ROOT_DIR}"

mkdir -p "${MAVEN_REPO_LOCAL}"
mvn -Dmaven.repo.local="${MAVEN_REPO_LOCAL}" -DskipTests package

echo "shaded_jar=${ROOT_DIR}/target/pixels-spark-0.1.jar"
