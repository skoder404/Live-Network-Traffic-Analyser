"""
tests/unit/test_dgim.py — Unit tests for DGIM sliding-window estimator and CountingOnesAnalytic.

Tests:
  1. DGIM theoretical bound (|est - exact| <= 0.5 * exact) on random bit streams.
  2. DGIM edge cases (all zeros, all ones, alternating bits, empty stream, sub-window queries).
  3. DGIM snapshot/restore round-trip.
  4. PredicateSpec parsing from config and defaults.
  5. CountingOnesAnalytic micro-batch integration with Spark and SQLite serving DB.
  6. CountingOnesAnalytic snapshot/restore persistence.
Reference: todo.md T4-009 acceptance criteria
"""

from __future__ import annotations

import random
import tempfile
from collections import deque
from pathlib import Path

import pytest

from streaming.analytics.dgim import (
    DGIM,
    CountingOnesAnalytic,
    PredicateSpec,
    parse_predicates,
)

# ─────────────────────────────────────────────────────────────────────
# Pure unit tests (no Spark)
# ─────────────────────────────────────────────────────────────────────


class TestDGIM:
    """Tests for the pure DGIM sliding-window counting algorithm."""

    def test_empty_stream(self) -> None:
        dgim = DGIM(window_n=100)
        assert dgim.query() == 0
        assert dgim.query(50) == 0

    def test_invalid_window(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            DGIM(window_n=0)
        with pytest.raises(ValueError, match="positive"):
            DGIM(window_n=-10)

    def test_all_zeros(self) -> None:
        dgim = DGIM(window_n=50)
        for _ in range(200):
            dgim.update(0)
        assert dgim.query() == 0

    def test_all_ones(self) -> None:
        window_n = 100
        dgim = DGIM(window_n=window_n)
        for _ in range(300):
            dgim.update(1)
        est = dgim.query()
        exact = window_n
        err = abs(est - exact) / exact
        assert err <= 0.5, f"All-ones error {err:.2%} exceeded 50%"

    def test_alternating_bits(self) -> None:
        window_n = 100
        dgim = DGIM(window_n=window_n)
        exact_deque: deque[int] = deque(maxlen=window_n)

        for i in range(300):
            bit = i % 2
            dgim.update(bit)
            exact_deque.append(bit)

        exact = sum(exact_deque)
        est = dgim.query()
        err = abs(est - exact) / exact
        assert err <= 0.5, f"Alternating bits error {err:.2%} exceeded 50%"

    @pytest.mark.parametrize("prob", [0.1, 0.3, 0.5, 0.7, 0.9])
    def test_random_bit_streams_bounded_error(self, prob: float) -> None:
        """Asserts that DGIM relative error is <= 50% across random bit streams."""
        rng = random.Random(42 + int(prob * 100))
        window_n = 500
        dgim = DGIM(window_n=window_n)
        exact_deque: deque[int] = deque(maxlen=window_n)
        errors: list[float] = []

        for step in range(3000):
            bit = 1 if rng.random() < prob else 0
            dgim.update(bit)
            exact_deque.append(bit)

            if step >= 200:
                exact = sum(exact_deque)
                est = dgim.query()
                if exact > 0:
                    err = abs(est - exact) / exact
                    errors.append(err)
                    assert err <= 0.501, (
                        f"DGIM relative error {err:.2%} exceeded 50% at step {step} (prob={prob})"
                    )

        mean_err = sum(errors) / len(errors) if errors else 0.0
        assert mean_err < 0.20, f"Mean error {mean_err:.2%} was higher than expected (<20%)"

    def test_sub_window_query(self) -> None:
        window_n = 200
        dgim = DGIM(window_n=window_n)
        rng = random.Random(123)

        for _ in range(400):
            dgim.update(1 if rng.random() < 0.5 else 0)

        # Query with k=50 (smaller than window_n)
        est_50 = dgim.query(k=50)
        est_full = dgim.query()
        assert est_50 <= est_full
        assert dgim.query(k=0) == 0

    def test_snapshot_restore_roundtrip(self) -> None:
        window_n = 300
        dgim1 = DGIM(window_n=window_n)
        rng = random.Random(999)

        for _ in range(500):
            dgim1.update(1 if rng.random() < 0.4 else 0)

        snap = dgim1.snapshot()
        est1 = dgim1.query()

        dgim2 = DGIM(window_n=10)
        dgim2.restore(snap)
        est2 = dgim2.query()

        assert est1 == est2
        assert dgim2.window_n == dgim1.window_n
        assert dgim2.current_time == dgim1.current_time
        assert len(dgim2.buckets) == len(dgim1.buckets)


class TestPredicateSpec:
    """Tests for predicate parsing and configuration."""

    def test_default_predicates(self) -> None:
        specs = parse_predicates()
        names = [s.name for s in specs]
        assert "is_tcp" in names
        assert "is_udp" in names
        assert "is_large_packet" in names

    def test_config_predicates(self) -> None:
        cfg = {
            "spark": {
                "predicates": [
                    {"name": "p_tcp", "protocol": "TCP"},
                    {"name": "p_big", "min_length": 1500},
                    {"name": "p_port", "dst_port": 8080},
                    {"name": "p_custom", "expr": "src_port = 53"},
                ]
            }
        }
        specs = parse_predicates(cfg)
        assert len(specs) == 4
        assert specs[0].expr == "protocol = 'TCP'"
        assert specs[1].expr == "packet_length >= 1500"
        assert specs[2].expr == "dst_port = 8080"
        assert specs[3].expr == "src_port = 53"


# ─────────────────────────────────────────────────────────────────────
# Spark integration tests
# ─────────────────────────────────────────────────────────────────────

CSV_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "sample" / "normal.csv"


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_dgim")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_counting_ones_analytic_process_batch(spark) -> None:
    """CountingOnesAnalytic should evaluate predicates and upsert to counting_ones table."""
    from common.serving_db import connect, init_schema
    from contracts.record_schema import to_spark_schema
    from streaming.analytics.base import BatchContext
    from streaming.common.cleaning import clean

    if not CSV_PATH.exists():
        pytest.skip(f"Sample data not found: {CSV_PATH}")

    raw_df = spark.read.schema(to_spark_schema(nullable_all=True)).csv(str(CSV_PATH))
    df = clean(raw_df)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_dgim.db"
        state_dir = Path(tmpdir) / "state"

        conn = connect(db_path)
        init_schema(conn)
        conn.close()

        analytic = CountingOnesAnalytic(
            window_n=500,
            predicates=[
                PredicateSpec(name="is_tcp", expr="protocol = 'TCP'"),
                PredicateSpec(name="is_large", expr="packet_length >= 500"),
            ],
            state_dir=state_dir,
        )

        ctx = BatchContext(
            cfg={"spark": {"max_rows_per_batch": 50000}},
            db_path=str(db_path),
        )

        analytic.process_batch(df, batch_id=0, ctx=ctx)

        # Verify rows were written to counting_ones
        conn = connect(db_path, read_only=True)
        cur = conn.execute(
            "SELECT ts, predicate_name, window_n, exact_ones, dgim_estimate, err_pct FROM counting_ones ORDER BY predicate_name"
        )
        rows = cur.fetchall()
        conn.close()

        assert len(rows) == 2
        for r in rows:
            assert r["window_n"] == 500
            assert r["exact_ones"] >= 0
            assert r["dgim_estimate"] >= 0
            if r["exact_ones"] > 0:
                assert r["err_pct"] <= 50.0

        # Verify state file was saved to disk
        state_file = state_dir / "dgim_state.json"
        assert state_file.exists()


def test_counting_ones_analytic_snapshot_restore(spark) -> None:
    """CountingOnesAnalytic snapshot and restore should preserve all predicate estimators."""
    from common.serving_db import connect, init_schema
    from contracts.record_schema import to_spark_schema
    from streaming.analytics.base import BatchContext
    from streaming.common.cleaning import clean

    if not CSV_PATH.exists():
        pytest.skip(f"Sample data not found: {CSV_PATH}")

    raw_df = spark.read.schema(to_spark_schema(nullable_all=True)).csv(str(CSV_PATH))
    df = clean(raw_df)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_dgim_snap.db"
        conn = connect(db_path)
        init_schema(conn)
        conn.close()

        analytic = CountingOnesAnalytic(
            window_n=200,
            predicates=[PredicateSpec(name="is_tcp", expr="protocol = 'TCP'")],
        )
        ctx = BatchContext(
            cfg={"spark": {"max_rows_per_batch": 50000}},
            db_path=str(db_path),
        )

        analytic.process_batch(df, batch_id=0, ctx=ctx)
        snap = analytic.snapshot()

        analytic2 = CountingOnesAnalytic(window_n=10)
        analytic2.restore(snap)

        assert analytic2.window_n == 200
        assert "is_tcp" in analytic2.dgim_map
        assert analytic2.dgim_map["is_tcp"].query() == analytic.dgim_map["is_tcp"].query()


def test_parse_predicates_and_edge_branches(spark, tmp_path) -> None:
    """Tests parse_predicates min_length/dst_port/fallback and CountingOnesAnalytic edge branches."""
    from common.serving_db import connect, init_schema
    from contracts.record_schema import to_spark_schema
    from streaming.analytics.base import BatchContext

    # 1. parse_predicates formats
    cfg = {
        "spark": {
            "predicates": [
                {"name": "p_min", "min_length": 1000},
                {"name": "p_port", "dst_port": 53},
                {"name": "p_other"},
            ]
        }
    }
    preds = parse_predicates(cfg)
    assert len(preds) == 3
    assert preds[0].expr == "packet_length >= 1000"
    assert preds[1].expr == "dst_port = 53"
    assert preds[2].expr == "1 = 1"

    # 2. CountingOnesAnalytic with empty batch
    db_path = str(tmp_path / "dgim_edge.db")
    conn = connect(db_path)
    init_schema(conn)

    analytic = CountingOnesAnalytic(window_n=50)
    empty_df = spark.createDataFrame([], schema=to_spark_schema())
    ctx = BatchContext(cfg={"spark": {}}, db_path=db_path, conn=conn)
    analytic.process_batch(empty_df, batch_id=0, ctx=ctx)

    # 3. Missing event_time column
    df_no_et = spark.createDataFrame([("TCP", 100)], ["protocol", "packet_length"])
    analytic.process_batch(df_no_et, batch_id=1, ctx=ctx)

    # 4. Dynamic predicate addition and row capping
    data = [
        (
            "2026-09-24 12:00:00.000",
            "192.168.1.1",
            "10.0.0.1",
            1000 + i,
            80,
            "TCP",
            100 + i * 10,
            None,
            None,
            None,
            1.0,
        )
        for i in range(10)
    ]
    raw_df = spark.createDataFrame(data, schema=to_spark_schema())
    ctx_dynamic = BatchContext(
        cfg={
            "spark": {
                "max_rows_per_batch": 5,
                "predicates": [{"name": "new_pred", "expr": "protocol = 'TCP'"}],
            }
        },
        db_path=db_path,
        conn=conn,
    )
    analytic.process_batch(raw_df, batch_id=2, ctx=ctx_dynamic)
    assert "new_pred" in analytic.dgim_map
    conn.close()
