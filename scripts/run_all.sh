#!/usr/bin/env bash
# scripts/run_all.sh — Master launcher for the complete LNTA Pipeline
# Executes: HDFS -> Flume Ingestion -> Capture -> Spark Streaming -> Alerts Engine -> Dashboard

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_DIR}"

if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

export PYTHONPATH="${REPO_DIR}:${PYTHONPATH:-}"
export LNTA_SERVING_DB="${LNTA_SERVING_DB:-serving/lnta.db}"
mkdir -p logs serving data/traffic/stream_in data/traffic/raw

INTERFACE="${1:-wlan0}"

echo "================================================================"
echo "    🚀 LNTA — Live Network Traffic Analyser Full Pipeline     "
echo "================================================================"
echo "Project Directory: ${REPO_DIR}"
echo "Serving DB:        ${LNTA_SERVING_DB}"
echo "Capture Interface: ${INTERFACE}"
echo "================================================================"

PIDS=()

cleanup() {
    echo ""
    echo "Shutting down LNTA pipeline processes..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    if [ -x "./ingestion/scripts/flume_ctl.sh" ]; then
        ./ingestion/scripts/flume_ctl.sh stop 2>/dev/null || true
    fi
    echo "All processes stopped. Exiting."
}
trap cleanup EXIT INT TERM

# Step 1: Start HDFS Services (if hadoop is installed)
if command -v start-dfs.sh >/dev/null 2>&1; then
    echo "Step 1/6: Starting HDFS Services..."
    start-dfs.sh || true
    if [ -x "./ingestion/hdfs/init_zones.sh" ]; then
        ./ingestion/hdfs/init_zones.sh || true
    fi
else
    echo "Step 1/6: HDFS command not found, using local ingestion directory (data/traffic/)..."
    python3 -m ingestion.scripts.ingestion_ctl init --local --root data/traffic 2>/dev/null || true
fi

# Step 2: Start Apache Flume Ingestion Agent
if command -v flume-ng >/dev/null 2>&1 && [ -x "./ingestion/scripts/flume_ctl.sh" ]; then
    echo "Step 2/6: Starting Apache Flume Ingestion Agent..."
    ./ingestion/scripts/flume_ctl.sh start || true
else
    echo "Step 2/6: Flume not detected in PATH, continuing with direct ingestion streaming..."
fi

# Step 3: Start Live Wi-Fi Packet Capture (or Traffic Replay fallback)
echo "Step 3/6: Starting Packet Capture / Traffic Generator..."
if [ "$(id -u)" -eq 0 ] || sudo -n true 2>/dev/null; then
    echo "  Running live capture on interface: ${INTERFACE}"
    python3 -m capture.main --interface "${INTERFACE}" > logs/capture.log 2>&1 &
    PIDS+=($!)
else
    echo "  Non-root mode: Starting realistic traffic stream replay into stream_in..."
    python3 -m capture.replay --file data/sample/normal.csv --speed 2.0 --sink file --output data/traffic/stream_in/replay.csv > logs/capture.log 2>&1 &
    PIDS+=($!)
fi

# Step 4: Start Spark Structured Streaming Engine
echo "Step 4/6: Starting Spark Structured Streaming Engine (Lane A & Lane B)..."
python3 -m streaming.stream_app > logs/streaming.log 2>&1 &
PIDS+=($!)

# Step 5: Start Background Alerts & Link Analysis Worker
echo "Step 5/6: Starting Link Analysis & Anomaly Alert Engine..."
python3 -m linkanalysis.alerts_worker --db "${LNTA_SERVING_DB}" > logs/alerts_worker.log 2>&1 &
PIDS+=($!)

# Step 6: Launch Streamlit Dashboard
echo "Step 6/6: Launching Streamlit Dashboard on http://localhost:8501..."
echo "----------------------------------------------------------------"
echo "Dashboard is ready. Press Ctrl+C to terminate all pipeline jobs."
echo "----------------------------------------------------------------"

export LNTA_MOCK="false"
python3 -m streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
