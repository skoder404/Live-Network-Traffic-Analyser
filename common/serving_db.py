"""
common/serving_db.py — SQLite serving database client with WAL mode, partial upsert, and retention cleanup.

Reference: TECH_RULES.md §5.2
Only parameterised queries are used to prevent injection and corruption.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "contracts" / "serving_schema.sql"

TABLES_WITH_WINDOW_START = [
    "window_metrics",
    "protocol_counts",
    "port_counts",
    "filter_counts",
    "distinct_counts",
    "moments",
    "frequent_itemsets",
    "ip_edges",
    "source_stats",
]

TABLES_WITH_TS = [
    "sampling_compare",
    "counting_ones",
    "decay_traffic",
    "decay_top_keys",
    "alerts",
    "pipeline_health",
]


def current_utc_iso() -> str:
    """Returns the current UTC timestamp formatted as ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def connect(db_path: str | Path, read_only: bool = False) -> sqlite3.Connection:
    """
    Connects to the SQLite serving database.
    If read_only=True, opens in URI read-only mode (file:<abs_path>?mode=ro).
    Configures PRAGMA journal_mode=WAL, busy_timeout=5000, synchronous=NORMAL.
    """
    path_obj = Path(db_path).resolve()

    if read_only:
        if not path_obj.exists():
            raise FileNotFoundError(f"Database file not found for read-only connection: {path_obj}")
        uri = f"file:{path_obj.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    else:
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path_obj), timeout=5.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")

    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection, schema_path: str | Path | None = None) -> None:
    """
    Executes the serving schema DDL idempotently.
    Creates all 16 tables and indexes.
    """
    p = Path(schema_path) if schema_path else SCHEMA_FILE
    if not p.exists():
        raise FileNotFoundError(f"Serving schema file not found at {p}")

    ddl = p.read_text(encoding="utf-8")
    with conn:
        conn.executescript(ddl)


def upsert(
    conn: sqlite3.Connection,
    table: str,
    key_cols: list[str],
    rows: list[dict[str, Any]],
) -> int:
    """
    Performs batched partial upserts into the specified table.
    Updates only the non-key columns present in the input rows and refreshes updated_at.
    Returns the count of rows processed.
    """
    if not rows:
        return 0

    # Collect all unique columns present across the batch
    cols_set: set[str] = set()
    for row in rows:
        cols_set.update(row.keys())

    # Ensure updated_at is included in the column set
    cols_set.add("updated_at")

    # Order columns deterministically: key_cols first, then remaining
    ordered_cols: list[str] = list(key_cols) + sorted([c for c in cols_set if c not in key_cols])

    update_cols = [c for c in ordered_cols if c not in key_cols]
    if not update_cols:
        # If no non-key columns exist, only update updated_at on conflict
        update_cols = ["updated_at"]

    cols_str = ", ".join(ordered_cols)
    placeholders = ", ".join(["?"] * len(ordered_cols))
    conflict_keys = ", ".join(key_cols)
    update_clause = ", ".join([f"{col} = excluded.{col}" for col in update_cols])

    sql = (
        f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) "
        f"ON CONFLICT({conflict_keys}) DO UPDATE SET {update_clause};"
    )

    now_iso = current_utc_iso()
    batch_values: list[tuple[Any, ...]] = []
    for r in rows:
        val_row = []
        for col in ordered_cols:
            if col == "updated_at":
                val_row.append(r.get("updated_at") or now_iso)
            else:
                val_row.append(r.get(col))
        batch_values.append(tuple(val_row))

    with conn:
        conn.executemany(sql, batch_values)

    return len(rows)


def cleanup(
    conn: sqlite3.Connection,
    retention_hours: int = 24,
    tables: list[str] | None = None,
) -> dict[str, int]:
    """
    Deletes records older than retention_hours from serving tables.
    Returns a dict mapping table name to count of deleted rows.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=retention_hours)).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"

    results: dict[str, int] = {}
    target_window_tables = (
        [t for t in TABLES_WITH_WINDOW_START if t in tables]
        if tables is not None
        else TABLES_WITH_WINDOW_START
    )
    target_ts_tables = (
        [t for t in TABLES_WITH_TS if t in tables] if tables is not None else TABLES_WITH_TS
    )

    with conn:
        for t in target_window_tables:
            cur = conn.execute(
                f"DELETE FROM {t} WHERE window_start < ?;",
                (cutoff,),
            )
            results[t] = cur.rowcount

        for t in target_ts_tables:
            cur = conn.execute(
                f"DELETE FROM {t} WHERE ts < ?;",
                (cutoff,),
            )
            results[t] = cur.rowcount

    return results
