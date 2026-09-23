"""
tests/unit/test_moments_window.py — Unit tests for Lane A windowed moments calculation.
"""

import pandas as pd
import pytest

try:
    from contracts.record_schema import to_spark_schema
    from streaming.common.cleaning import clean
    from streaming.common.session import get_spark
    from streaming.queries.moments_window import build_moments_window

    HAS_SPARK = True
except (ImportError, ModuleNotFoundError):
    HAS_SPARK = False


@pytest.fixture(scope="module")
def spark():
    if not HAS_SPARK:
        pytest.skip("PySpark not installed")
    s = get_spark("LNTA-TestMomentsWindow")
    yield s


@pytest.mark.spark
def test_build_moments_window_multi_packets(spark):
    if not HAS_SPARK:
        pytest.skip("PySpark not installed")

    # Sample data with 3 packets in window 1, 1 packet in window 2
    data = [
        # Window 1: 12:00:00 - 12:00:10
        (
            "2026-09-23 12:00:01.000",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            443,
            "TCP",
            100,
            None,
            None,
            None,
            1.5,
        ),
        (
            "2026-09-23 12:00:03.000",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            443,
            "TCP",
            200,
            None,
            None,
            None,
            2.5,
        ),
        (
            "2026-09-23 12:00:05.000",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            443,
            "TCP",
            300,
            None,
            None,
            None,
            3.5,
        ),
        # Window 2: 12:00:10 - 12:00:20 (single packet -> variance should be None)
        (
            "2026-09-23 12:00:12.000",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            443,
            "TCP",
            500,
            None,
            None,
            None,
            1.0,
        ),
    ]

    raw_df = spark.createDataFrame(data, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    moments_df = build_moments_window(cleaned_df, window_len_s=10, watermark_s=30)
    rows = moments_df.orderBy("window_start").collect()

    assert len(rows) == 2

    # Window 1 check: n=3, packet lengths: 100, 200, 300
    # mean = 200.0, sample variance = ((100-200)^2 + (200-200)^2 + (300-200)^2)/(3-1) = 20000/2 = 10000.0
    # std = 100.0
    # iat: 1.5, 2.5, 3.5 -> mean = 2.5, sample var = 1.0, std = 1.0
    w1 = rows[0]
    assert w1["window_start"] == "2026-09-23T12:00:00.000Z"
    assert w1["n"] == 3
    assert abs(w1["mean_len"] - 200.0) < 1e-4
    assert abs(w1["var_len"] - 10000.0) < 1e-4
    assert abs(w1["std_len"] - 100.0) < 1e-4
    assert abs(w1["iat_mean_ms"] - 2.5) < 1e-4
    assert abs(w1["var_len"] - 10000.0) < 1e-4

    # Window 2 check: single packet -> var and std must be None / Null
    w2 = rows[1]
    assert w2["window_start"] == "2026-09-23T12:00:10.000Z"
    assert w2["n"] == 1
    assert abs(w2["mean_len"] - 500.0) < 1e-4
    assert w2["var_len"] is None
    assert w2["std_len"] is None


@pytest.mark.spark
def test_moments_vs_pandas_comparison(spark):
    if not HAS_SPARK:
        pytest.skip("PySpark not installed")

    records = [
        (
            f"2026-09-23 12:00:{sec:02d}.000",
            "192.168.1.10",
            "8.8.8.8",
            50000,
            443,
            "TCP",
            100 + (sec * 25),
            None,
            None,
            None,
            float(sec * 0.5),
        )
        for sec in range(10)
    ]

    raw_df = spark.createDataFrame(records, schema=to_spark_schema())
    cleaned_df = clean(raw_df)
    spark_rows = (
        build_moments_window(cleaned_df, window_len_s=10, watermark_s=30)
        .collect()
    )

    assert len(spark_rows) == 1
    s_row = spark_rows[0]

    # Pandas ground truth
    lengths = [r[6] for r in records]
    iats = [r[10] for r in records]

    s_series = pd.Series(lengths)
    iat_series = pd.Series(iats)

    assert abs(s_row["mean_len"] - s_series.mean()) < 1e-4
    assert abs(s_row["var_len"] - s_series.var(ddof=1)) < 1e-4
    assert abs(s_row["std_len"] - s_series.std(ddof=1)) < 1e-4
    assert abs(s_row["iat_mean_ms"] - iat_series.mean()) < 1e-4
    assert abs(s_row["iat_var_ms"] - iat_series.var(ddof=1)) < 1e-4
