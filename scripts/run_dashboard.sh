#!/usr/bin/env bash
# scripts/run_dashboard.sh — Launch LNTA Streamlit dashboard with correct PYTHONPATH

set -euo pipefail

# Get project root (directory containing this script's parent)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default configuration
DB_PATH="${LNTA_SERVING_DB:-serving/analytics.db}"
MOCK_MODE="${LNTA_MOCK:-true}"

# Export for the Streamlit process
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"
export LNTA_SERVING_DB="${DB_PATH}"
export LNTA_MOCK="${MOCK_MODE}"

echo "Starting LNTA Dashboard..."
echo "  Project root: ${PROJECT_ROOT}"
echo "  DB path:      ${DB_PATH}"
echo "  Mock mode:    ${MOCK_MODE}"
echo "  PYTHONPATH:   ${PYTHONPATH}"

# Run Streamlit
exec streamlit run "${PROJECT_ROOT}/dashboard/app.py"
