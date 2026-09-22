#!/usr/bin/env bash
set -euo pipefail

ROOT="${LNTA_HDFS_ROOT:-/traffic}"
hdfs dfs -mkdir -p \
  "$ROOT/raw" \
  "$ROOT/stream_in" \
  "$ROOT/processed" \
  "$ROOT/checkpoints" \
  "$ROOT/hive"
hdfs dfs -chmod -R 755 "$ROOT"
printf 'Initialized HDFS zones below %s\n' "$ROOT"