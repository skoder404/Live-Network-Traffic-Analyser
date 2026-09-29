#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${1:-$ROOT_DIR/serving/lnta.db}"
OUTPUT="${2:-$ROOT_DIR/data/sample/offline_snapshot.json}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$(dirname "$DB_PATH")" "$(dirname "$OUTPUT")"
"$PYTHON_BIN" "$ROOT_DIR/scripts/seed_mock_db.py" --db "$DB_PATH" --reset
"$PYTHON_BIN" - "$DB_PATH" "$OUTPUT" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

db_path, output_path = sys.argv[1:]
with sqlite3.connect(db_path) as connection:
    connection.row_factory = sqlite3.Row
    tables = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    snapshot = {
        row[0]: [dict(item) for item in connection.execute(f'SELECT * FROM "{row[0]}"')]
        for row in tables
    }
Path(output_path).write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(f"Snapshot saved to {output_path} ({len(snapshot)} tables)")
PY
