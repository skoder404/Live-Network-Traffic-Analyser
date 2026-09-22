"""
tests/unit/test_window_metrics.py — Unit tests for Lane A window metrics computation.
"""

import pandas as pd
import pytest

from contracts.record_schema import to_spark_schema
from streaming.common.cleaning import clean
from streaming.common.session import get_spark
from streaming.queries.window_metrics import build_window_metrics


@pytest.fixture(scope="module")
def spark():
    s = get_spark("LNTA-TestWindowMetrics")
    yield s


def test_build_window_metrics_calculation(spark):
    data = [
        ("2026-09-22 12:00:01.000", "192.168.1.1", "8.8.8.8", 1234, 443, "TCP", 100, None, None, None, 1.0),
        ("2026-09-22 12:00:03.000", "192.168.1.1", "8.8.8.8", 1234, 443, "TCP", 200, None, None, None, 1.0),
        ("2026-09-22 12:00:05.000", "192.168.1.1", "8.8.8.8", 1234, 443, "TCP", 300, None, None, None, 1.0),
        # Window 2: 12:00:10 to 12:00:20
        ("2026-09-22 12:00:12.000", "192.168.1.1", "8.8.8.8", 1234, 443, "TCP", 400, None, None, None, 1.0),
    ]
    raw_df = spark.createDataFrame(data, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    metrics_df = build_window_metrics(cleaned_df, window_len_s=10, watermark_s=30)
    rows = metrics_df.orderBy("window_start").collect()

    assert len(rows) == 2

    # Window 1: 3 packets, 600 bytes
    w1 = rows[0]
    assert w1["window_start"] == "2026-09-22T12:00:00.000Z"
    assert w1["window_len_s"] == 10
    assert w1["packets"] == 3
    assert w1["bytes"] == 600
    assert w1["pps"] == 0.3
    assert w1["bps"] == 60.0

    # Window 2: 1 packet, 400 bytes
    w2 = rows[1]
    assert w2["window_start"] == "2026-09-22T12:00:10.000Z"
    assert w2["window_len_s"] == 10
    assert w2["packets"] == 1
    assert w2["bytes"] == 400
    assert w2["pps"] == 0.1
    assert w2["bps"] == 40.0


def test_window_metrics_vs_pandas_ground_truth(spark):
    records = []
    # Generate 50 packets spanning two 10s windows
    for sec in range(20):
        records.append((
            f"2026-09-22 12:00:{sec:02d}.000",
            "192.168.1.5",
            "8.8.8.8",
            50000,
            443,
            "TCP",
            100 + sec * 10,
            None,
            None,
            None,
            1.0,
        ))

    raw_df = spark.createDataFrame(records, schema=to_spark_schema())
    cleaned_df = clean(raw_df)
    spark_res = build_window_metrics(cleaned_df, window_len_s=10, watermark_s=30).orderBy("window_start").collect()

    # Pandas ground truth
    pdf = pd.DataFrame(records, columns=[
        "timestamp", "src_ip", "dst_ip", "src_port", "dst_port",
        "protocol", "packet_length", "src_mac", "dst_mac", "tcp_flags", "iat_ms"
    ])
    pdf["event_time"] = pd.to_datetime(pdf["timestamp"], utc=True)
    pdf["window_bucket"] = pdf["event_time"].dt.floor("10s")
    grouped = pdf.groupby("window_bucket").agg(
        packets=("packet_length", "count"),
        total_bytes=("packet_length", "sum")
    ).reset_index()

    assert len(spark_res) == len(grouped)
    for i in range(len(spark_res)):
        assert spark_res[i]["packets"] == int(grouped.iloc[i]["packets"])
        assert spark_res[i]["bytes"] == int(grouped.iloc[i]["total_bytes"])
