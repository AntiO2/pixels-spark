#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  cat <<'EOF' >&2
Usage:
  run-hudi-profile.sh <action> <profile> [args...]

Actions:
  import   Run full benchmark import in Hudi mode
  start    Start CDC jobs in Hudi sink mode
  stop     Stop CDC jobs for the profile
  status   Show CDC job status for the profile

Profiles:
  hybench_sf10 | sf10
  hybench_sf1000 | sf1000
  chbenchmark_w1 | w1
  chbenchmark_w10000 | w10000
EOF
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ACTION="$1"
RAW_PROFILE="$2"
shift 2

export PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${ROOT_DIR}/etc/pixels-spark.properties}"
source "${ROOT_DIR}/scripts/lib/pixels-config.sh"
source "${ROOT_DIR}/scripts/lib/hudi-profile.sh"

PROFILE="$(hudi_normalize_profile "${RAW_PROFILE}")" || {
  echo "unsupported profile: ${RAW_PROFILE}" >&2
  exit 1
}

case "${ACTION}" in
  import)
    IMPORT_SCRIPT="$(hudi_import_script_for_profile "${PROFILE}")"
    IMPORT_MODE=hudi exec "${ROOT_DIR}/scripts/${IMPORT_SCRIPT}" "$@"
    ;;
  start)
    START_SCRIPT="$(hudi_cdc_start_script_for_profile "${PROFILE}")"
    TARGET_KEY="$(hudi_cdc_target_property_key_for_profile "${PROFILE}")"
    TARGET_DEFAULT="$(hudi_cdc_target_default_for_profile "${PROFILE}")"
    TARGET_ROOT="${TARGET_ROOT:-$(pixels_get_property "${TARGET_KEY}" "${TARGET_DEFAULT}")}"
    SINK_MODE=hudi TARGET_ROOT="${TARGET_ROOT}" exec "${ROOT_DIR}/scripts/${START_SCRIPT}" "$@"
    ;;
  stop)
    STOP_SCRIPT="$(hudi_cdc_stop_script_for_profile "${PROFILE}")"
    exec "${ROOT_DIR}/scripts/${STOP_SCRIPT}" "$@"
    ;;
  status)
    STATUS_SCRIPT="$(hudi_cdc_status_script_for_profile "${PROFILE}")"
    exec "${ROOT_DIR}/scripts/${STATUS_SCRIPT}" "$@"
    ;;
  *)
    echo "unsupported action: ${ACTION}" >&2
    exit 1
    ;;
esac
