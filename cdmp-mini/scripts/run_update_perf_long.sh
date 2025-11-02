#!/usr/bin/env bash
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_CMD=(go test ./test/iam-apiserver/user/update -run TestUpdatePerformance -count=1 -timeout 30m)
OUTPUT_JSON="$ROOT_DIR/test/iam-apiserver/user/update/output/update_perf.json"
ARCHIVE_DIR="$ROOT_DIR/test/iam-apiserver/user/update/output/history"
SUMMARY_CSV="$ARCHIVE_DIR/update_perf_summary.csv"
ROUNDS=${ROUNDS:-12}
INTERVAL_SECONDS=${INTERVAL_SECONDS:-300}
SCENARIOS=("update_parallel_put" "patch_parallel_profile")

mkdir -p "$ARCHIVE_DIR"
if [[ ! -f "$SUMMARY_CSV" ]]; then
  echo "timestamp,scenario,p95_ms,p99_ms" >"$SUMMARY_CSV"
fi

for (( round=1; round<=ROUNDS; round++ )); do
  ts="$(date +%Y%m%d-%H%M%S)"
  echo "[$ts] round ${round}/${ROUNDS} starting"
  if (cd "$ROOT_DIR" && "${TEST_CMD[@]}"); then
    if [[ -f "$OUTPUT_JSON" ]]; then
      cp "$OUTPUT_JSON" "$ARCHIVE_DIR/update_perf_${ts}.json"
      for scenario in "${SCENARIOS[@]}"; do
        if jq -e --arg s "$scenario" '.[] | select(.scenario==$s)' "$OUTPUT_JSON" >/dev/null; then
          jq -r --arg ts "$ts" --arg s "$scenario" '.[] | select(.scenario==$s) | "\($ts),\($s),\(.latency.p95_ms),\(.latency.p99_ms)"' "$OUTPUT_JSON" >>"$SUMMARY_CSV"
        else
          echo "$ts,$scenario,NA,NA" >>"$SUMMARY_CSV"
        fi
      done
    else
      echo "[$ts] missing output JSON: $OUTPUT_JSON" >&2
    fi
  else
    echo "[$ts] go test failed" >&2
  fi
  if (( round < ROUNDS )); then
    sleep "$INTERVAL_SECONDS"
  fi
done
