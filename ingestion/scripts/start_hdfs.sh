#!/usr/bin/env bash
set -euo pipefail

echo "[*] Checking Hadoop HDFS status..."

if command -v hdfs >/dev/null 2>&1; then
    echo "[*] HDFS command found. Checking safe mode..."
    if command -v start-dfs.sh >/dev/null 2>&1; then
        start-dfs.sh || true
    fi
    hdfs dfsadmin -safemode wait || true
    echo "[*] Ensuring HDFS stream directories exist..."
    hdfs dfs -mkdir -p /traffic/stream_in /traffic/raw || true
    echo "[OK] HDFS is ready."
elif [ -n "${HADOOP_HOME:-}" ] && [ -x "${HADOOP_HOME}/bin/hdfs" ]; then
    echo "[*] Using HADOOP_HOME: ${HADOOP_HOME}"
    "${HADOOP_HOME}/bin/hdfs" dfsadmin -safemode wait || true
    "${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p /traffic/stream_in /traffic/raw || true
    echo "[OK] HDFS is ready."
else
    echo "[INFO] HDFS not found in PATH; ensuring local fallback directories exist."
    mkdir -p data/stream_in data/raw data/checkpoint
    echo "[OK] Local fallback directory ready."
fi
