#!/usr/bin/env bash
# scripts/stop_all.sh — Stop all running LNTA pipeline and dashboard processes

echo "Stopping all LNTA pipeline components..."

pkill -f "capture.main" 2>/dev/null || true
pkill -f "capture.replay" 2>/dev/null || true
pkill -f "streaming.stream_app" 2>/dev/null || true
pkill -f "linkanalysis.alerts_worker" 2>/dev/null || true
pkill -f "streamlit run dashboard/app.py" 2>/dev/null || true
fuser -k 8501/tcp 2>/dev/null || true

if [ -x "./ingestion/scripts/flume_ctl.sh" ]; then
    ./ingestion/scripts/flume_ctl.sh stop 2>/dev/null || true
fi

echo "All LNTA processes stopped."
