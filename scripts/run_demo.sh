#!/usr/bin/env bash
# scripts/run_demo.sh — Automated Demo Launcher for LNTA Presentation
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_DIR}"
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

echo "============================================================"
echo "      📡 LNTA — Live Network Traffic Analyser Demo          "
echo "============================================================"
echo "1. Seeding demo database with realistic traffic..."
python3 scripts/seed_mock_db.py --db serving/lnta.db --reset

echo "2. Launching background Alerts & Link Analysis Engine..."
python3 -m linkanalysis.alerts_worker --db serving/lnta.db > logs/alerts_worker.log 2>&1 &
ALERTS_PID=$!

cleanup() {
    echo ""
    echo "Stopping demo background processes..."
    kill $ALERTS_PID 2>/dev/null || true
    echo "Demo ended."
}
trap cleanup EXIT

echo "3. Starting Streamlit Dashboard on http://localhost:8501..."
export PYTHONPATH="${REPO_DIR}:${PYTHONPATH:-}"
export LNTA_SERVING_DB="serving/lnta.db"
export LNTA_MOCK="true"

streamlit run dashboard/app.py --server.port 8501
