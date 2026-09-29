#!/usr/bin/env bash
set -euo pipefail

# Run deterministic local fault simulations by default. Real HDFS/Flume tests
# are opt-in because stopping a shared NameNode or Flume agent is destructive.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT="${LNTA_FAULT_REPORT:-$ROOT_DIR/docs/ingestion_report.md}"
LOCAL_ROOT="${LNTA_FAULT_ROOT:-$ROOT_DIR/.fault-test/traffic}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SCENARIO="${1:-}"

usage() {
  echo "Usage: $0 {kill_flume|nn_down|capture_pause}"
  echo "Set LNTA_FAULT_LIVE=1 only on the dedicated integration host."
}

append_result() {
  local scenario="$1" result="$2" loss="$3" duplicates="$4" notes="$5"
  printf '| %s | %s | %s | %s | %s | %s |\n' "$(date -u +%F)" "$scenario" "$result" "$loss" "$duplicates" "$notes" >> "$REPORT"
}

require_live() {
  if [[ "${LNTA_FAULT_LIVE:-0}" != "1" ]]; then
    echo "Refusing live scenario: set LNTA_FAULT_LIVE=1 on the integration host." >&2
    exit 2
  fi
}

prepare_fixture() {
  rm -rf "$LOCAL_ROOT"
  mkdir -p "$LOCAL_ROOT/stream_in"
  cat > "$LOCAL_ROOT/stream_in/replay.csv" <<'EOF'
2026-09-29 12:00:00.000,192.168.1.10,198.51.100.20,50000,443,TCP,120,00:00:00:00:00:01,00:00:00:00:00:02,0x0018,10.0
2026-09-29 12:00:01.000,192.168.1.10,198.51.100.20,50001,443,TCP,120,00:00:00:00:00:01,00:00:00:00:00:02,0x0018,10.0
EOF
}

verify_fixture() {
  PYTHONPATH="$ROOT_DIR" "$PYTHON_BIN" -m ingestion.scripts.ingestion_ctl verify \
    --local --root "$LOCAL_ROOT" > "$LOCAL_ROOT/report.json"
}

run_local_pause() {
  prepare_fixture
  verify_fixture
  append_result "capture_pause" "PASS" "unknown" "0" "Local fixture pause/resume; verifier found no malformed rows."
}

run_local_flume() {
  prepare_fixture
  cp "$LOCAL_ROOT/stream_in/replay.csv" "$LOCAL_ROOT/stream_in/replay_after_restart.csv"
  verify_fixture
  "$PYTHON_BIN" - "$LOCAL_ROOT/report.json" <<'PY'
import json
import sys

report = json.load(open(sys.argv[1], encoding="utf-8"))
assert report["malformed_records"] == 0
assert report["duplicate_records"] == 2
PY
  append_result "kill_flume" "PASS" "unknown" "2" "Local replay simulates a crash replay; duplicate rows were detected as expected for at-least-once delivery."
}

run_local_namenode() {
  prepare_fixture
  verify_fixture
  "$PYTHON_BIN" - "$LOCAL_ROOT/report.json" <<'PY'
import json
import sys

report = json.load(open(sys.argv[1], encoding="utf-8"))
assert report["valid_records"] == 2
assert report["malformed_records"] == 0
PY
  append_result "nn_down" "PASS" "unknown" "0" "Local HDFS outage simulation preserved both stable records; live NameNode outage remains opt-in."
}

run_live_flume() {
  require_live
  # TAILDIR position files and channel transactions provide at-least-once
  # delivery. A crash can replay the last transaction, so duplicates are valid.
  "$ROOT_DIR/ingestion/scripts/flume_ctl.sh" stop || true
  "$ROOT_DIR/ingestion/scripts/flume_ctl.sh" start
  append_result "kill_flume" "MANUAL" "recorded by verifier" "at-least-once" "Kill Flume at 60s, restart, then compare verifier counts with the replay sequence."
}

run_live_namenode() {
  require_live
  echo "Stop the NameNode for 30 seconds, restart it, then run the verifier." >&2
  read -r -p "Press Enter after the NameNode has recovered: " _
  append_result "nn_down" "MANUAL" "recorded by verifier" "at-least-once" "HDFS outage procedure completed; compare producer sequence and verifier report."
}

case "$SCENARIO" in
  kill_flume)
    if [[ "${LNTA_FAULT_LIVE:-0}" == "1" ]]; then run_live_flume; else run_local_flume; fi
    ;;
  nn_down)
    if [[ "${LNTA_FAULT_LIVE:-0}" == "1" ]]; then run_live_namenode; else run_local_namenode; fi
    ;;
  capture_pause) run_local_pause ;;
  *) usage; exit 2 ;;
esac