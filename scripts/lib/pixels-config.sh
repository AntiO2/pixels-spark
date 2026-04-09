#!/usr/bin/env bash

PIXELS_CONFIG_ROOT="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
PIXELS_CONFIG_PATH_DEFAULT="${PIXELS_CONFIG_ROOT}/etc/pixels-spark.properties"
PIXELS_SPARK_CONFIG="${PIXELS_SPARK_CONFIG:-${PIXELS_CONFIG_PATH_DEFAULT}}"

trim() {
  local value="${1:-}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "${value}"
}

pixels_get_property() {
  local key="$1"
  local default_value="${2:-}"

  if [[ ! -f "${PIXELS_SPARK_CONFIG}" ]]; then
    printf '%s' "${default_value}"
    return 0
  fi

  local value
  value="$(
    awk -F= -v target_key="${key}" '
      /^[[:space:]]*#/ { next }
      /^[[:space:]]*$/ { next }
      {
        current_key=$1
        sub(/^[[:space:]]+/, "", current_key)
        sub(/[[:space:]]+$/, "", current_key)
        if (current_key == target_key) {
          $1=""
          sub(/^=/, "", $0)
          sub(/^[[:space:]]+/, "", $0)
          sub(/[[:space:]]+$/, "", $0)
          print $0
        }
      }
    ' "${PIXELS_SPARK_CONFIG}" | tail -n 1
  )"

  if [[ -z "${value}" ]]; then
    printf '%s' "${default_value}"
  else
    printf '%s' "${value}"
  fi
}

pixels_split_csv_property() {
  local raw_value="$1"
  local -n out_array_ref=$2
  local item
  IFS=',' read -r -a out_array_ref <<< "${raw_value}"
  for item in "${!out_array_ref[@]}"; do
    out_array_ref["${item}"]="$(trim "${out_array_ref[${item}]}")"
  done
}

infer_local_parallelism() {
  local master="$1"
  if [[ "${master}" =~ ^local\[([0-9]+)\]$ ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return 0
  fi
  if [[ "${master}" == "local[*]" ]]; then
    nproc
    return 0
  fi
  printf '4'
}
