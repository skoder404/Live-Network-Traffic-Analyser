"""
tests/unit/test_decay.py — Unit tests for exponential decaying window algorithm and key table.
"""

import unittest

from streaming.analytics.decay import DecayingCounter, DecayingKeyTable


class TestDecay(unittest.TestCase):
    def test_half_life_exact_decay_property(self):
        """A burst of weight W added at t=0 must decay to exactly W/2 at t=half_life."""
        half_life = 10.0
        counter = DecayingCounter(half_life_s=half_life)

        counter.add(value=100.0, t=0.0)
        self.assertAlmostEqual(counter.value(t=0.0), 100.0, places=9)

        # At t = 10.0 (1 half life) -> exactly 50.0
        self.assertAlmostEqual(counter.value(t=10.0), 50.0, places=9)

        # At t = 20.0 (2 half lives) -> exactly 25.0
        self.assertAlmostEqual(counter.value(t=20.0), 25.0, places=9)

        # At t = 30.0 (3 half lives) -> exactly 12.5
        self.assertAlmostEqual(counter.value(t=30.0), 12.5, places=9)

    def test_rate_change_responsiveness_vs_fixed_window(self):
        """
        After a step-change in traffic, decaying window adapts more rapidly than
        a 60s fixed window average.
        """
        half_life = 10.0
        decay_counter = DecayingCounter(half_life_s=half_life)

        # Simulate baseline rate: 10 pkts/sec for 60s
        for t in range(60):
            decay_counter.add(value=10.0, t=float(t))

        # Baseline score at t=59
        base_decay = decay_counter.value(59.0)

        # Step increase to 100 pkts/sec starting at t=60 for 10 seconds
        for t in range(60, 70):
            decay_counter.add(value=100.0, t=float(t))

        score_at_70 = decay_counter.value(70.0)

        # Under a 60s fixed window, only 10s of high rate (10*100) + 50s of low rate (50*10) = 1500 / 60 = 25 pkts/s (2.5x increase)
        # Decaying window (half-life 10s) gives much higher weight to recent 10s, so ratio > 3.0x
        decay_ratio = score_at_70 / base_decay
        self.assertGreater(decay_ratio, 3.0)

    def test_key_table_top_k_and_pruning(self):
        """Tests heavy hitter tracking and bounded memory through epsilon pruning."""
        table = DecayingKeyTable(half_life_s=5.0, epsilon=0.1, max_keys=5)

        # Add initial keys
        table.add("8.8.8.8", 100.0, t=0.0)
        table.add("1.1.1.1", 50.0, t=0.0)
        table.add("192.168.1.1", 10.0, t=0.0)

        top2 = table.top_k(k=2, t=0.0)
        self.assertEqual(len(top2), 2)
        self.assertEqual(top2[0][0], "8.8.8.8")
        self.assertAlmostEqual(top2[0][1], 100.0, places=4)
        self.assertEqual(top2[1][0], "1.1.1.1")
        self.assertAlmostEqual(top2[1][1], 50.0, places=4)

        # Advance time by 30 seconds (6 half-lives: 10 * 2^-6 = 0.156, near epsilon)
        # At t = 40 seconds (8 half-lives: 10 * 2^-8 = 0.039 < 0.1)
        table.prune(t=40.0)
        self.assertNotIn("192.168.1.1", table.counters)

    def test_snapshot_restore_round_trip(self):
        counter = DecayingCounter(half_life_s=15.0)
        counter.add(75.5, t=10.0)
        counter.add(24.5, t=20.0)

        snap = counter.snapshot()

        counter_restored = DecayingCounter(half_life_s=15.0)
        counter_restored.restore(snap)

        self.assertEqual(counter_restored.half_life_s, 15.0)
        self.assertEqual(counter_restored.last_t, 20.0)
        self.assertAlmostEqual(counter_restored.value(t=35.0), counter.value(t=35.0), places=9)


def test_decay_analytic_process_batch(tmp_path):
    """Tests DecayAnalytic Lane B plugin micro-batch processing and SQLite table writes."""
    from common.serving_db import connect, init_schema
    from contracts.record_schema import to_spark_schema
    from streaming.analytics.base import BatchContext
    from streaming.analytics.decay import DecayAnalytic
    from streaming.common.cleaning import clean
    from streaming.common.session import get_spark

    spark = get_spark("LNTA-TestDecayAnalytic")
    db_path = tmp_path / "test_decay_analytic.db"
    conn = connect(db_path)
    init_schema(conn)

    records = [
        (
            "2026-09-24 12:00:01.000",
            "192.168.1.10",
            "8.8.8.8",
            1234,
            443,
            "TCP",
            100,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-24 12:00:02.000",
            "192.168.1.20",
            "8.8.8.8",
            1234,
            443,
            "TCP",
            200,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-24 12:00:03.000",
            "192.168.1.30",
            "1.1.1.1",
            1234,
            53,
            "UDP",
            300,
            None,
            None,
            None,
            1.0,
        ),
    ]
    raw_df = spark.createDataFrame(records, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    cfg = {
        "spark": {"trigger_s": 5, "max_rows_per_batch": 1000},
        "decay": {"half_lives_s": [10, 30, 60]},
        "serving": {"db_path": str(db_path)},
    }
    ctx = BatchContext(
        cfg=cfg,
        db_path=db_path,
        conn=conn,
        clock=100.0,
        batch_time="2026-09-24T12:00:10.000Z",
    )

    state_dir = tmp_path / "state"
    plugin = DecayAnalytic(state_dir=state_dir)
    plugin.process_batch(cleaned_df, batch_id=1, ctx=ctx)

    # Verify rows in decay_traffic
    cur = conn.cursor()
    cur.execute("SELECT ts, half_life_s, score, raw_pps FROM decay_traffic ORDER BY half_life_s;")
    traffic_rows = cur.fetchall()
    assert len(traffic_rows) == 3

    # Verify rows in decay_top_keys
    cur.execute("SELECT key_type, key, score, rank FROM decay_top_keys ORDER BY rank;")
    key_rows = cur.fetchall()
    assert len(key_rows) > 0

    conn.close()

    # Test state restore
    plugin_restored = DecayAnalytic(state_dir=state_dir)
    plugin_restored._ensure_initialized(cfg)
    assert 10 in plugin_restored.traffic_counters


if __name__ == "__main__":
    unittest.main()
