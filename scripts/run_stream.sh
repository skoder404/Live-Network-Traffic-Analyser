#!/usr/bin/env bash
# scripts/run_stream.sh — Launcher for the LNTA Spark Streaming Application
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -d "${REPO_DIR}/.venv" ]; then
    source "${REPO_DIR}/.venv/bin/activate"
fi

export PYTHONPATH="${REPO_DIR}:${PYTHONPATH:-}"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo "Usage: bash scripts/run_stream.sh [options]"
    echo ""
    echo "Options:"
    echo "  --config PATH       Path to settings.yaml (default: config/settings.yaml)"
    echo "  --input PATH        Path to input CSV stream directory or HDFS path"
    echo "  --help, -h          Show this help message"
    exit 0
fi

echo "Starting LNTA Spark Structured Streaming Application..."
python3 -m streaming.stream_app "$@"

