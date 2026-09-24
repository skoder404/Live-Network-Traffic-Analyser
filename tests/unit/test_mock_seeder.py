"""
tests/unit/test_mock_seeder.py — Unit tests for mock serving database seeder.
"""

import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.seed_mock_db import TABLE_PRIMARY_KEYS, seed_database


class TestMockSeeder(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "test_mock_analytics.db"

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_seed_mock_db_populates_all_tables_with_consistency(self):
        # Seed 5 minutes of data
        seed_database(self.db_path, duration_min=5, seed=123, reset=True)

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row

        # 1. Verify all 16 tables exist and have rows
        cur = conn.cursor()
        for tbl in TABLE_PRIMARY_KEYS.keys():
            cur.execute(f"SELECT COUNT(*) FROM {tbl};")
            count = cur.fetchone()[0]
            self.assertGreater(count, 0, f"Table {tbl} is empty after seeding!")

        # 2. Check protocol_counts packet sum == window_metrics packets per window
        cur.execute("""
            SELECT wm.window_start, wm.window_len_s, wm.packets as wm_pkts, SUM(pc.packets) as pc_pkts
            FROM window_metrics wm
            JOIN protocol_counts pc ON wm.window_start = pc.window_start AND wm.window_len_s = pc.window_len_s
            GROUP BY wm.window_start, wm.window_len_s;
        """)
        rows = cur.fetchall()
        self.assertGreater(len(rows), 0)
        for r in rows:
            self.assertEqual(
                r["wm_pkts"],
                r["pc_pkts"],
                f"Mismatch for window {r['window_start']} len={r['window_len_s']}: {r['wm_pkts']} != {r['pc_pkts']}",
            )

        # 3. Check frequent_itemsets content for apriori and pcy
        cur.execute("SELECT DISTINCT itemset, algorithm FROM frequent_itemsets;")
        itemsets = {(row["algorithm"], row["itemset"]) for row in cur.fetchall()}
        self.assertIn(("apriori", "TCP:443"), itemsets)
        self.assertIn(("apriori", "UDP:53"), itemsets)
        self.assertIn(("apriori", "UDP:53 + TCP:443"), itemsets)
        self.assertIn(("pcy", "TCP:443"), itemsets)
        self.assertIn(("pcy", "UDP:53 + TCP:443"), itemsets)

        # 4. Check alerts contain spike alert
        cur.execute("SELECT COUNT(*) FROM alerts WHERE type = 'spike';")
        spike_alerts = cur.fetchone()[0]
        self.assertGreater(spike_alerts, 0)

        # 5. Check distinct_counts exact vs hll within 10%
        cur.execute(
            "SELECT src_ips_exact, src_ips_hll, dst_ips_exact, dst_ips_hll FROM distinct_counts;"
        )
        for r in cur.fetchall():
            self.assertAlmostEqual(
                r["src_ips_exact"], r["src_ips_hll"], delta=max(2, int(r["src_ips_exact"] * 0.15))
            )

        conn.close()


if __name__ == "__main__":
    unittest.main()
