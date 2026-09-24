#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-status}"
PID_FILE="${LNTA_FLUME_PID_FILE:-logs/flume.pid}"
LOG_FILE="${LNTA_FLUME_LOG_FILE:-logs/flume.log}"
CONFIG="${LNTA_FLUME_CONFIG:-ingestion/flume/lnta-agent.conf}"
mkdir -p "$(dirname "$PID_FILE")" "$(dirname "$LOG_FILE")"

status() {
  if [[ -f "$PID_FILE" ]] && kill -0 "$(<"$PID_FILE")" 2>/dev/null; then
    printf '{"running":true,"pid":%s,"config":"%s"}\n' "$(<"$PID_FILE")" "$CONFIG"
    return 0
  fi
  rm -f "$PID_FILE"
  printf '{"running":false,"config":"%s"}\n' "$CONFIG"
  return 1
}

case "$ACTION" in
  start)
    if status >/dev/null 2>&1; then
      echo "Flume is already running"
      exit 0
    fi
    nohup flume-ng agent --conf-file "$CONFIG" --name lnta >>"$LOG_FILE" 2>&1 &
    echo $! >"$PID_FILE"
    echo "Started Flume with PID $(<"$PID_FILE")"
    ;;
  stop)
    if [[ -f "$PID_FILE" ]]; then
      kill "$(<"$PID_FILE")" 2>/dev/null || true
      rm -f "$PID_FILE"
      echo "Stopped Flume"
    else
      echo "Flume is not running"
    fi
    ;;
  status)
    status || true
    ;;
  *)
    echo "Usage: $0 {start|stop|status}" >&2
    exit 2
    ;;
esac