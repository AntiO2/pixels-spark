#!/usr/bin/env bash

hudi_normalize_profile() {
  local raw="${1:-}"
  local normalized
  normalized="$(echo "${raw}" | tr '[:upper:]' '[:lower:]')"
  case "${normalized}" in
    hybench_sf10|hybench-sf10|sf10)
      printf '%s' "hybench_sf10"
      ;;
    hybench_sf1000|hybench-sf1000|sf1000)
      printf '%s' "hybench_sf1000"
      ;;
    chbenchmark_w1|chbenchmark-w1|w1)
      printf '%s' "chbenchmark_w1"
      ;;
    chbenchmark_w10000|chbenchmark-w10000|w10000)
      printf '%s' "chbenchmark_w10000"
      ;;
    *)
      return 1
      ;;
  esac
}

hudi_import_script_for_profile() {
  local profile="$1"
  case "${profile}" in
    hybench_sf10) printf '%s' "run-import-hybench-sf10.sh" ;;
    hybench_sf1000) printf '%s' "run-import-hybench-sf1000.sh" ;;
    chbenchmark_w1) printf '%s' "run-import-chbenchmark-w1.sh" ;;
    chbenchmark_w10000) printf '%s' "run-import-chbenchmark-w10000.sh" ;;
    *) return 1 ;;
  esac
}

hudi_cdc_start_script_for_profile() {
  local profile="$1"
  case "${profile}" in
    hybench_sf10) printf '%s' "run-cdc-hybench-sf10.sh" ;;
    hybench_sf1000) printf '%s' "run-cdc-hybench-sf1000.sh" ;;
    chbenchmark_w1) printf '%s' "run-cdc-chbenchmark-w1.sh" ;;
    chbenchmark_w10000) printf '%s' "run-cdc-chbenchmark-w10000.sh" ;;
    *) return 1 ;;
  esac
}

hudi_cdc_stop_script_for_profile() {
  local profile="$1"
  case "${profile}" in
    hybench_sf10) printf '%s' "stop-cdc-hybench-sf10.sh" ;;
    hybench_sf1000) printf '%s' "stop-cdc-hybench-sf1000.sh" ;;
    chbenchmark_w1) printf '%s' "stop-cdc-chbenchmark-w1.sh" ;;
    chbenchmark_w10000) printf '%s' "stop-cdc-chbenchmark-w10000.sh" ;;
    *) return 1 ;;
  esac
}

hudi_cdc_status_script_for_profile() {
  local profile="$1"
  case "${profile}" in
    hybench_sf10) printf '%s' "status-cdc-hybench-sf10.sh" ;;
    hybench_sf1000) printf '%s' "status-cdc-hybench-sf1000.sh" ;;
    chbenchmark_w1) printf '%s' "status-cdc-chbenchmark-w1.sh" ;;
    chbenchmark_w10000) printf '%s' "status-cdc-chbenchmark-w10000.sh" ;;
    *) return 1 ;;
  esac
}

hudi_cdc_target_property_key_for_profile() {
  local profile="$1"
  case "${profile}" in
    hybench_sf10) printf '%s' "pixels.cdc.hybench.sf10.hudi.target-root" ;;
    hybench_sf1000) printf '%s' "pixels.cdc.hybench.sf1000.hudi.target-root" ;;
    chbenchmark_w1) printf '%s' "pixels.cdc.chbenchmark.w1.hudi.target-root" ;;
    chbenchmark_w10000) printf '%s' "pixels.cdc.chbenchmark.w10000.hudi.target-root" ;;
    *) return 1 ;;
  esac
}

hudi_cdc_target_default_for_profile() {
  local profile="$1"
  case "${profile}" in
    hybench_sf10) printf '%s' "s3a://home-zinuo/hudi/hybench_sf10" ;;
    hybench_sf1000) printf '%s' "s3a://home-zinuo/hudi/hybench_sf1000" ;;
    chbenchmark_w1) printf '%s' "s3a://home-zinuo/hudi/chbenchmark_w1" ;;
    chbenchmark_w10000) printf '%s' "s3a://home-zinuo/hudi/chbenchmark_w10000" ;;
    *) return 1 ;;
  esac
}
