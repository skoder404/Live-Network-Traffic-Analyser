#!/usr/bin/env bash
# scripts/offline_snapshot.sh

set -e

DB_PATH="serving/lnta.db"

echo "1. Seeding mock DB..."
source .venv/bin/activate
python scripts/seed_mock_db.py --db "$DB_PATH" --reset

echo "2. Taking snapshot of all serving tables to a JSON file..."
python -c '
import sqlite3, json, sys
conn = sqlite3.connect(sys.argv[1])
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type=\"table\";")
tables = [row[0] for row in cur.fetchall()]
snapshot = {}
for t in tables:
    cur.execute(f"SELECT * FROM {t}")
    snapshot[t] = [dict(r) for r in cur.fetchall()]
with open("offline_snapshot.json", "w") as f:
    json.dump(snapshot, f)
print("Snapshot saved to offline_snapshot.json")
' "$DB_PATH"

echo "3. Starting dashboard in mock mode..."
export LNTA_MOCK=true
export LNTA_DB="$DB_PATH"
streamlit run dashboard/app.py
