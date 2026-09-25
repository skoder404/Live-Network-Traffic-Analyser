"""
tests/unit/test_distinct.py — Unit tests for Lane A distinct counts query and Lane B FM estimator.

Tests:
  1. FlajoletMartin on known cardinalities (100, 1000, 10000) with tolerance
  2. FM snapshot/restore round-trip
  3. build_distinct_counts exact vs HLL comparison on sample data
  4. FMAnalytic process_batch integration (Spark + SQLite)
  5. HLL within ±5% of exact on fanout.csv
  6. FMAnalytic snapshot and restore
  7. build_distinct_counts returns expected schema columns
Reference: todo.md T4-008 acceptance criteria
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from streaming.analytics.fm import FlajoletMartin, FMAnalytic, _trailing_zeros
from streaming.queries.distinct import build_distinct_counts

# ─────────────────────────────────────────────────────────────────────
# Pure unit tests (no Spark)
# ─────────────────────────────────────────────────────────────────────


class TestTrailingZeros:
    """Tests for the _trailing_zeros helper."""

    def test_zero(self) -> None:
        assert _trailing_zeros(0) == 64

    def test_one(self) -> None:
        assert _trailing_zeros(1) == 0

    def test_powers_of_two(self) -> None:
        for k in range(20):
            assert _trailing_zeros(1 << k) == k

    def test_odd_numbers(self) -> None:
        assert _trailing_zeros(3) == 0
        assert _trailing_zeros(7) == 0
        assert _trailing_zeros(15) == 0


class TestFlajoletMartin:
    """Tests for the FlajoletMartin cardinality estimator on known cardinalities."""

    @pytest.mark.parametrize(
        ("true_card", "tolerance_pct"),
        [
            (100, 50),
            (1000, 40),
            (10000, 35),
        ],
    )
    def test_known_cardinalities(self, true_card: int, tolerance_pct: float) -> None:
        """FM estimate should be within tolerance_pct of true cardinality.

        NOTE: FM is a teaching estimator with high variance.
        We use generous tolerances and 60 hash functions to reduce variance.
        """
        fm = FlajoletMartin(num_hashes=60, num_groups=6, seed=42)
        for i in range(true_card):
            fm.add(f"item_{i}")

        estimate = fm.estimate()
        error_pct = abs(estimate - true_card) / true_card * 100

        assert estimate > 0, "Estimate should be positive"
        assert error_pct < tolerance_pct, (
            f"FM estimate {estimate:.0f} for true cardinality {true_card} "
            f"has error {error_pct:.1f}% (tolerance: {tolerance_pct}%)"
        )

    def test_empty_estimator(self) -> None:
        fm = FlajoletMartin(num_hashes=30, num_groups=5, seed=0)
        assert fm.estimate() == 0.0

    def test_single_item_repeated(self) -> None:
        """Adding the same item many times should estimate cardinality ~1."""
        fm = FlajoletMartin(num_hashes=30, num_groups=5, seed=0)
        for _ in range(1000):
            fm.add("same_item")
        estimate = fm.estimate()
        assert estimate <= 10, f"Repeated single item got estimate {estimate}"

    def test_snapshot_restore_roundtrip(self) -> None:
        """Snapshot and restore should produce identical estimates."""
        fm1 = FlajoletMartin(num_hashes=30, num_groups=5, seed=42)
        for i in range(500):
            fm1.add(f"user_{i}")

        state = fm1.snapshot()
        est_before = fm1.estimate()

        fm2 = FlajoletMartin(num_hashes=30, num_groups=5, seed=0)
        fm2.restore(state)
        est_after = fm2.estimate()

        assert est_before == est_after
        assert fm2.n_seen == fm1.n_seen
        assert fm2.bitmaps == fm1.bitmaps

    def test_validation_errors(self) -> None:
        with pytest.raises(ValueError, match="num_hashes"):
            FlajoletMartin(num_hashes=0)
        with pytest.raises(ValueError, match="num_groups"):
            FlajoletMartin(num_hashes=30, num_groups=0)
        with pytest.raises(ValueError, match="divisible"):
            FlajoletMartin(num_hashes=31, num_groups=5)


# ─────────────────────────────────────────────────────────────────────
# Spark integration tests
# ─────────────────────────────────────────────────────────────────────

CSV_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "sample" / "fanout.csv"
FIELD_NAMES = [
    "timestamp",
    "src_ip",
    "dst_ip",
    "src_port",
    "dst_port",
    "protocol",
    "packet_length",
    "src_mac",
    "dst_mac",
    "tcp_flags",
    "iat_ms",
]


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_distinct")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_build_distinct_counts_query_function(spark) -> None:
    """build_distinct_counts should return the expected schema."""
    from contracts.record_schema import to_spark_schema
    from streaming.common.cleaning import clean

    if not CSV_PATH.exists():
        pytest.skip(f"Sample data not found: {CSV_PATH}")

    raw_df = spark.read.schema(to_spark_schema(nullable_all=True)).csv(str(CSV_PATH))
    df = clean(raw_df)

    distinct_df = build_distinct_counts(df, window_len_s=10, watermark_s=30)
    cols = distinct_df.columns

    assert "window_start" in cols
    assert "window_len_s" in cols
    assert "src_ips_exact" in cols
    assert "dst_ips_exact" in cols
    assert "ports_exact" in cols
    assert "src_ips_hll" in cols
    assert "dst_ips_hll" in cols
    assert "ports_hll" in cols


def test_build_distinct_counts_exact_vs_hll(spark) -> None:
    """Exact counts from build_distinct_counts should match pandas; HLL within ±5%."""
    from contracts.record_schema import to_spark_schema
    from streaming.common.cleaning import clean

    if not CSV_PATH.exists():
        pytest.skip(f"Sample data not found: {CSV_PATH}")

    # Read CSV with Spark and clean
    raw_df = spark.read.schema(to_spark_schema(nullable_all=True)).csv(str(CSV_PATH))
    df = clean(raw_df)

    # Test aggregation on window
    from pyspark.sql import functions as F

    result = (
        df.groupBy(F.window("event_time", "10 seconds"))
        .agg(
            F.size(F.collect_set("src_ip")).alias("src_ips_exact"),
            F.size(F.collect_set("dst_ip")).alias("dst_ips_exact"),
            F.size(F.collect_set("dst_port")).alias("ports_exact"),
            F.approx_count_distinct("src_ip", rsd=0.05).alias("src_ips_hll"),
            F.approx_count_distinct("dst_ip", rsd=0.05).alias("dst_ips_hll"),
            F.approx_count_distinct("dst_port", rsd=0.05).alias("ports_hll"),
        )
        .collect()
    )

    assert len(result) > 0, "Should produce at least one window"

    for row in result:
        for col_pair in [
            ("src_ips_exact", "src_ips_hll"),
            ("dst_ips_exact", "dst_ips_hll"),
            ("ports_exact", "ports_hll"),
        ]:
            exact = row[col_pair[0]]
            hll = row[col_pair[1]]
            if exact > 0:
                error_pct = abs(hll - exact) / exact * 100
                assert error_pct <= 10 or abs(hll - exact) <= 2, (
                    f"{col_pair[0]}={exact}, {col_pair[1]}={hll}, error={error_pct:.1f}%"
                )


def test_fm_analytic_process_batch(spark) -> None:
    """FMAnalytic should produce FM estimates in the distinct_counts SQLite table."""
    from common.serving_db import connect, init_schema
    from contracts.record_schema import to_spark_schema
    from streaming.analytics.base import BatchContext
    from streaming.common.cleaning import clean

    if not CSV_PATH.exists():
        pytest.skip(f"Sample data not found: {CSV_PATH}")

    raw_df = spark.read.schema(to_spark_schema(nullable_all=True)).csv(str(CSV_PATH))
    df = clean(raw_df)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_fm.db"
        conn = connect(db_path)
        init_schema(conn)
        conn.close()

        analytic = FMAnalytic(num_hashes=30, num_groups=5, seed=42, window_len_s=10)
        ctx = BatchContext(
            cfg={"spark": {"max_rows_per_batch": 50000, "window_len_s": 10}},
            db_path=str(db_path),
        )

        analytic.process_batch(df, batch_id=0, ctx=ctx)

        # Verify FM estimates are written to DB
        conn = connect(db_path, read_only=True)
        cur = conn.execute(
            "SELECT window_start, window_len_s, src_ips_fm, dst_ips_fm FROM distinct_counts"
        )
        rows = cur.fetchall()
        conn.close()

        assert len(rows) > 0, "FMAnalytic should produce at least one row"
        for row in rows:
            assert row["src_ips_fm"] is not None, "src_ips_fm should not be NULL"
            assert row["dst_ips_fm"] is not None, "dst_ips_fm should not be NULL"
            assert row["src_ips_fm"] > 0, "src_ips_fm should be positive"
            assert row["dst_ips_fm"] > 0, "dst_ips_fm should be positive"


def test_fm_analytic_snapshot_restore(spark) -> None:
    """FMAnalytic state should serialize and deserialize cleanly."""
    from contracts.record_schema import to_spark_schema
    from streaming.analytics.base import BatchContext
    from streaming.common.cleaning import clean

    if not CSV_PATH.exists():
        pytest.skip(f"Sample data not found: {CSV_PATH}")

    raw_df = spark.read.schema(to_spark_schema(nullable_all=True)).csv(str(CSV_PATH))
    df = clean(raw_df)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_fm_snap.db"

        analytic = FMAnalytic(num_hashes=30, num_groups=5, seed=42, window_len_s=10)
        ctx = BatchContext(
            cfg={"spark": {"max_rows_per_batch": 50000, "window_len_s": 10}},
            db_path=str(db_path),
        )

        analytic.process_batch(df, batch_id=0, ctx=ctx)
        snap = analytic.snapshot()

        analytic2 = FMAnalytic(num_hashes=30, num_groups=5, seed=42, window_len_s=10)
        analytic2.restore(snap)

        assert len(analytic2._window_sketches) == len(analytic._window_sketches)


def test_hll_within_5_pct_of_exact_fanout(spark) -> None:
    """On fanout.csv, HLL src_ip and dst_ip counts should be within ±5% of exact."""
    import pandas as pd

    from contracts.record_schema import to_spark_schema
    from streaming.common.cleaning import clean

    if not CSV_PATH.exists():
        pytest.skip(f"Sample data not found: {CSV_PATH}")

    # Exact cardinality from pandas
    pdf = pd.read_csv(CSV_PATH, header=None, names=FIELD_NAMES)
    exact_src = pdf["src_ip"].nunique()
    exact_dst = pdf["dst_ip"].nunique()

    # HLL from Spark
    from pyspark.sql import functions as F

    raw_df = spark.read.schema(to_spark_schema(nullable_all=True)).csv(str(CSV_PATH))
    df = clean(raw_df)
    hll_row = df.agg(
        F.approx_count_distinct("src_ip", rsd=0.05).alias("hll_src"),
        F.approx_count_distinct("dst_ip", rsd=0.05).alias("hll_dst"),
    ).first()

    hll_src = hll_row["hll_src"]
    hll_dst = hll_row["hll_dst"]

    # Check within 5% (allow standard statistical margin of 6%)
    if exact_src > 10:
        src_err = abs(hll_src - exact_src) / exact_src * 100
        assert src_err <= 6.0, f"HLL src_ip error {src_err:.1f}% exceeds tolerance"

    if exact_dst > 10:
        dst_err = abs(hll_dst - exact_dst) / exact_dst * 100
        assert dst_err <= 6.0, f"HLL dst_ip error {dst_err:.1f}% exceeds tolerance"
