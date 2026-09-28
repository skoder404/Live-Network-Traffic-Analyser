#!/usr/bin/env bash
# scripts/stop_demo.sh — Gracefully stops all LNTA pipeline demo services
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_DIR}"

RUN_DIR="${REPO_DIR}/.run"

echo "============================================================"
echo " Stopping LNTA Demo Services"
echo "============================================================"

stop_pid() {
    local name="$1"
    local pid_file="${RUN_DIR}/${name}.pid"
    if [ -f "${pid_file}" ]; then
        local pid
        pid=$(cat "${pid_file}" 2>/dev/null || true)
        if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
            echo "[*] Stopping ${name} (PID: ${pid})..."
            kill "${pid}" 2>/dev/null || true
            sleep 1
            if kill -0 "${pid}" 2>/dev/null; then
                kill -9 "${pid}" 2>/dev/null || true
            fi
            echo "[OK] Stopped ${name}."
        fi
        rm -f "${pid_file}"
    fi
}

# Stop in reverse order: dashboard -> alerts -> spark -> capture -> flume
stop_pid "dashboard"
stop_pid "alerts"
stop_pid "spark"
stop_pid "capture"

if [ -f "ingestion/scripts/flume_ctl.sh" ]; then
    bash ingestion/scripts/flume_ctl.sh stop >/dev/null 2>&1 || true
fi

echo "============================================================"
echo " [SUCCESS] All LNTA services stopped cleanly."
echo "============================================================"
