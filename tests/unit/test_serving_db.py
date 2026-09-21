"""
tests/unit/test_serving_db.py — Unit tests for SQLite serving database operations.
"""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from common.serving_db import cleanup, connect, current_utc_iso, init_schema, upsert

EXPECTED_TABLES = {
    "window_metrics",
    "protocol_counts",
    "port_counts",
    "filter_counts",
    "distinct_counts",
    "sampling_compare",
    "counting_ones",
    "moments",
    "decay_traffic",
    "decay_top_keys",
    "frequent_itemsets",
    "ip_edges",
    "source_stats",
    "alerts",
    "pipeline_health",
    "hist_results",
}


class TestServingDB(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "test_analytics.db"
        self.conn = connect(self.db_path)
        init_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.tmp_dir.cleanup()

    def test_init_schema_creates_all_16_tables_and_is_idempotent(self):
        # Call init_schema second time to verify idempotency
        init_schema(self.conn)

        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        )
        tables = {row[0] for row in cur.fetchall()}
        self.assertTrue(
            EXPECTED_TABLES.issubset(tables),
            f"Missing tables: {EXPECTED_TABLES - tables}",
        )

    def test_partial_upsert_merge(self):
        ws = "2026-09-21T12:00:00.000Z"
        wlen = 10

        # Writer A writes exact counts
        upsert(
            self.conn,
            "distinct_counts",
            key_cols=["window_start", "window_len_s"],
            rows=[
                {
                    "window_start": ws,
                    "window_len_s": wlen,
                    "src_ips_exact": 120,
                    "dst_ips_exact": 85,
                    "ports_exact": 34,
                }
            ],
        )

        # Writer B writes HLL estimates on the same key
        upsert(
            self.conn,
            "distinct_counts",
            key_cols=["window_start", "window_len_s"],
            rows=[
                {
                    "window_start": ws,
                    "window_len_s": wlen,
                    "src_ips_hll": 122,
                    "dst_ips_hll": 84,
                    "ports_hll": 35,
                }
            ],
        )

        cur = self.conn.execute(
            "SELECT * FROM distinct_counts WHERE window_start = ? AND window_len_s = ?;",
            (ws, wlen),
        )
        row = cur.fetchone()
        self.assertIsNotNone(row)
        # Both Writer A and Writer B columns must be preserved
        self.assertEqual(row["src_ips_exact"], 120)
        self.assertEqual(row["dst_ips_exact"], 85)
        self.assertEqual(row["ports_exact"], 34)
        self.assertEqual(row["src_ips_hll"], 122)
        self.assertEqual(row["dst_ips_hll"], 84)
        self.assertEqual(row["ports_hll"], 35)
        self.assertIsNotNone(row["updated_at"])

    def test_read_only_connection_rejects_writes(self):
        ro_conn = connect(self.db_path, read_only=True)
        with self.assertRaises(sqlite3.OperationalError):
            ro_conn.execute(
                "INSERT INTO alerts (alert_id, ts, type, severity, reason) "
                "VALUES ('a1', '2026-09-21T12:00:00Z', 'spike', 'WARN', 'Test');"
            )
        ro_conn.close()

    def test_cleanup_deletes_only_stale_records(self):
        old_ws = "2020-01-01T00:00:00.000Z"
        recent_ws = current_utc_iso()

        upsert(
            self.conn,
            "window_metrics",
            key_cols=["window_start", "window_len_s"],
            rows=[
                {"window_start": old_ws, "window_len_s": 10, "packets": 50},
                {"window_start": recent_ws, "window_len_s": 10, "packets": 100},
            ],
        )

        deleted = cleanup(self.conn, retention_hours=24)
        self.assertGreaterEqual(deleted.get("window_metrics", 0), 1)

        cur = self.conn.execute("SELECT window_start FROM window_metrics;")
        remaining = [r[0] for r in cur.fetchall()]
        self.assertNotIn(old_ws, remaining)
        self.assertIn(recent_ws, remaining)


if __name__ == "__main__":
    unittest.main()
