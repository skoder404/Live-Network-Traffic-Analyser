#!/usr/bin/env bash
# scripts/run_demo.sh — One-command LNTA pipeline demo launcher
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_DIR}"

RUN_DIR="${REPO_DIR}/.run"
mkdir -p "${RUN_DIR}" "logs"

MODE="replay"
SCENARIO="normal"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --scenario)
            SCENARIO="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [--mode live|replay|snapshot] [--scenario NAME]"
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

if [ -d "${REPO_DIR}/.venv" ]; then
    source "${REPO_DIR}/.venv/bin/activate"
fi
export PYTHONPATH="${REPO_DIR}:${PYTHONPATH:-}"

echo "============================================================"
echo " Starting LNTA Demo (Mode: ${MODE}, Scenario: ${SCENARIO})"
echo "============================================================"

# Snapshot mode: Dashboard only
if [[ "${MODE}" == "snapshot" ]]; then
    echo "[*] Launching Dashboard in SNAPSHOT mode..."
    export LNTA_DEMO_MODE="snapshot"
    if [ ! -f "serving/demo_snapshot.db" ] && [ -f "serving/analytics.db" ]; then
        cp "serving/analytics.db" "serving/demo_snapshot.db"
    fi
    streamlit run dashboard/app.py --server.headless=true --server.port=8501 > logs/dashboard.log 2>&1 &
    echo $! > "${RUN_DIR}/dashboard.pid"
    echo "[OK] Dashboard running at http://localhost:8501 (PID: $(<"${RUN_DIR}/dashboard.pid"))"
    exit 0
fi

# Step 1: HDFS
echo "[1/6] Initialising HDFS / storage layers..."
if [ -f "ingestion/scripts/start_hdfs.sh" ]; then
    bash ingestion/scripts/start_hdfs.sh > logs/hdfs_init.log 2>&1 || true
fi
echo "[OK] Storage layer initialized."

# Step 2: Flume
echo "[2/6] Starting Apache Flume agent..."
if [ -f "ingestion/scripts/flume_ctl.sh" ]; then
    bash ingestion/scripts/flume_ctl.sh start > logs/flume_ctl.log 2>&1 || true
fi
echo "[OK] Flume service ready."

# Step 3: Data Source (Replay or Live Capture)
echo "[3/6] Starting Traffic Ingestion (${MODE})..."
if [[ "${MODE}" == "live" ]]; then
    python3 -m capture.run_capture --auto > logs/capture.log 2>&1 &
    echo $! > "${RUN_DIR}/capture.pid"
else
    SAMPLE_FILE="data/sample/${SCENARIO}.csv"
    if [ ! -f "${SAMPLE_FILE}" ]; then
        SAMPLE_FILE="data/sample/normal.csv"
    fi
    python3 -m capture.replay --file "${SAMPLE_FILE}" --restamp --loop > logs/capture.log 2>&1 &
    echo $! > "${RUN_DIR}/capture.pid"
fi
echo "[OK] Traffic ingestion started (PID: $(<"${RUN_DIR}/capture.pid"))."

# Step 4: Spark Streaming Application
echo "[4/6] Starting Spark Structured Streaming Engine..."
bash scripts/run_stream.sh > logs/spark_stream.log 2>&1 &
echo $! > "${RUN_DIR}/spark.pid"
echo "[OK] Spark streaming started (PID: $(<"${RUN_DIR}/spark.pid"))."

# Step 5: Alerts Worker
echo "[5/6] Starting Explainable Alerts Worker..."
python3 -m linkanalysis.alerts_worker > logs/alerts_worker.log 2>&1 &
echo $! > "${RUN_DIR}/alerts.pid"
echo "[OK] Alerts worker started (PID: $(<"${RUN_DIR}/alerts.pid"))."

# Step 6: Streamlit Dashboard
echo "[6/6] Launching Streamlit Command Center..."
streamlit run dashboard/app.py --server.headless=true --server.port=8501 > logs/dashboard.log 2>&1 &
echo $! > "${RUN_DIR}/dashboard.pid"
echo "[OK] Dashboard running at http://localhost:8501 (PID: $(<"${RUN_DIR}/dashboard.pid"))."

echo "============================================================"
echo " [SUCCESS] LNTA Pipeline is fully operational!"
echo " Access UI: http://localhost:8501"
echo " Stop demo: bash scripts/stop_demo.sh"
echo "============================================================"
