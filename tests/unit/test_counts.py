"""
tests/unit/test_counts.py — Unit tests for Lane A protocol and port counts queries.

Verifies:
  1. Protocol counts calculation and consistency with window_metrics total packet count.
  2. Port counts aggregation, NULL dst_port exclusion, and Top-N ranking.
  3. Ground-truth validation against pandas for data/sample/dns_heavy.csv.
Reference: TECH_RULES §3.5, todo.md T4-005
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from contracts.record_schema import to_spark_schema
from streaming.common.cleaning import clean
from streaming.common.session import get_spark
from streaming.queries.counts import build_port_counts, build_protocol_counts
from streaming.queries.window_metrics import build_window_metrics


@pytest.fixture(scope="module")
def spark():
    s = get_spark("LNTA-TestCounts")
    yield s


def test_protocol_counts_calculation_and_consistency(spark):
    """Verifies protocol aggregation and consistency with window_metrics."""
    data = [
        # Window 1: 12:00:00 - 12:00:10
        (
            "2026-09-23 12:00:01.000",
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
            "2026-09-23 12:00:02.000",
            "192.168.1.10",
            "8.8.8.8",
            1235,
            443,
            "TCP",
            200,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-23 12:00:03.000",
            "192.168.1.11",
            "1.1.1.1",
            5000,
            53,
            "UDP",
            50,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-23 12:00:04.000",
            "192.168.1.12",
            "8.8.4.4",
            None,
            None,
            "ICMP",
            64,
            None,
            None,
            None,
            1.0,
        ),
        # Window 2: 12:00:10 - 12:00:20
        (
            "2026-09-23 12:00:12.000",
            "192.168.1.10",
            "8.8.8.8",
            1236,
            80,
            "TCP",
            500,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-23 12:00:15.000",
            "192.168.1.11",
            "8.8.8.8",
            5001,
            53,
            "UDP",
            80,
            None,
            None,
            None,
            1.0,
        ),
    ]

    raw_df = spark.createDataFrame(data, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    proto_df = build_protocol_counts(cleaned_df, window_len_s=10, watermark_s=30)
    metrics_df = build_window_metrics(cleaned_df, window_len_s=10, watermark_s=30)

    proto_rows = proto_df.orderBy("window_start", "protocol").collect()
    metrics_rows = metrics_df.orderBy("window_start").collect()

    # Total 2 windows in metrics
    assert len(metrics_rows) == 2
    w1_metrics = metrics_rows[0]
    w2_metrics = metrics_rows[1]

    # Window 1: TCP (2 pkts, 300 B), UDP (1 pkt, 50 B), ICMP (1 pkt, 64 B) -> total 4 pkts, 414 B
    w1_proto = [r for r in proto_rows if r["window_start"] == "2026-09-23T12:00:00.000Z"]
    assert len(w1_proto) == 3
    assert sum(r["packets"] for r in w1_proto) == w1_metrics["packets"] == 4
    assert sum(r["bytes"] for r in w1_proto) == w1_metrics["bytes"] == 414

    # Window 2: TCP (1 pkt, 500 B), UDP (1 pkt, 80 B) -> total 2 pkts, 580 B
    w2_proto = [r for r in proto_rows if r["window_start"] == "2026-09-23T12:00:10.000Z"]
    assert len(w2_proto) == 2
    assert sum(r["packets"] for r in w2_proto) == w2_metrics["packets"] == 2
    assert sum(r["bytes"] for r in w2_proto) == w2_metrics["bytes"] == 580


def test_port_counts_top_n_and_null_exclusion(spark):
    """Verifies that NULL destination ports are excluded and top-N ranking functions."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    data = [
        # Window 1
        (
            "2026-09-23 12:00:01.000",
            "192.168.1.1",
            "8.8.8.8",
            1000,
            53,
            "UDP",
            60,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-23 12:00:02.000",
            "192.168.1.2",
            "8.8.8.8",
            1001,
            53,
            "UDP",
            70,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-23 12:00:03.000",
            "192.168.1.3",
            "8.8.8.8",
            1002,
            53,
            "UDP",
            80,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-23 12:00:04.000",
            "192.168.1.1",
            "1.1.1.1",
            1003,
            443,
            "TCP",
            1000,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-23 12:00:05.000",
            "192.168.1.2",
            "1.1.1.1",
            1004,
            443,
            "TCP",
            500,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-23 12:00:06.000",
            "192.168.1.1",
            "1.1.1.1",
            1005,
            80,
            "TCP",
            300,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-23 12:00:07.000",
            "192.168.1.1",
            "1.1.1.1",
            1006,
            22,
            "TCP",
            100,
            None,
            None,
            None,
            1.0,
        ),
        # NULL port (ICMP) -> must be excluded
        (
            "2026-09-23 12:00:08.000",
            "192.168.1.1",
            "1.1.1.1",
            None,
            None,
            "ICMP",
            64,
            None,
            None,
            None,
            1.0,
        ),
    ]

    raw_df = spark.createDataFrame(data, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    port_df = build_port_counts(cleaned_df, window_len_s=10, watermark_s=30)
    all_port_rows = port_df.collect()

    # Ports present: 53 (3 pkts, 210B), 443 (2 pkts, 1500B), 80 (1 pkt, 300B), 22 (1 pkt, 100B)
    assert len(all_port_rows) == 4
    ports_found = {r["port"] for r in all_port_rows}
    assert ports_found == {53, 443, 80, 22}
    assert None not in ports_found

    # Test top-2 ranking
    top_n = 2
    window_spec = Window.partitionBy("window_start").orderBy(
        F.col("packets").desc(),
        F.col("bytes").desc(),
        F.col("port").asc(),
    )
    ranked_df = port_df.withColumn("_rank", F.row_number().over(window_spec))
    top2_rows = ranked_df.filter(F.col("_rank") <= top_n).orderBy("_rank").collect()

    assert len(top2_rows) == 2
    # Rank 1: port 53 (3 packets)
    assert top2_rows[0]["port"] == 53
    assert top2_rows[0]["packets"] == 3
    assert top2_rows[0]["bytes"] == 210
    # Rank 2: port 443 (2 packets)
    assert top2_rows[1]["port"] == 443
    assert top2_rows[1]["packets"] == 2
    assert top2_rows[1]["bytes"] == 1500


def test_counts_vs_pandas_dns_heavy(spark):
    """Cross-validates protocol and top-N port counts against pandas on dns_heavy.csv."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    csv_path = Path("data/sample/dns_heavy.csv")
    assert csv_path.exists(), "data/sample/dns_heavy.csv must exist"

    raw_df = spark.read.option("header", "true").schema(to_spark_schema()).csv(str(csv_path))
    cleaned_df = clean(raw_df)

    # 1. Protocol counts validation
    proto_df = build_protocol_counts(cleaned_df, window_len_s=10, watermark_s=30)
    spark_proto_rows = proto_df.orderBy("window_start", "protocol").collect()

    pdf = pd.read_csv(csv_path)
    pdf["event_time"] = pd.to_datetime(pdf["timestamp"], utc=True)
    pdf["window_bucket"] = pdf["event_time"].dt.floor("10s")
    pdf["window_start"] = pdf["window_bucket"].dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    grouped_proto = (
        pdf.groupby(["window_start", "protocol"])
        .agg(packets=("packet_length", "count"), bytes=("packet_length", "sum"))
        .reset_index()
        .sort_values(["window_start", "protocol"])
    )

    assert len(spark_proto_rows) == len(grouped_proto)
    for i, s_row in enumerate(spark_proto_rows):
        p_row = grouped_proto.iloc[i]
        assert s_row["window_start"] == p_row["window_start"]
        assert s_row["protocol"] == p_row["protocol"]
        assert s_row["packets"] == int(p_row["packets"])
        assert s_row["bytes"] == int(p_row["bytes"])

    # 2. Port counts validation (top 3)
    top_n = 3
    port_df = build_port_counts(cleaned_df, window_len_s=10, watermark_s=30)
    window_spec = Window.partitionBy("window_start").orderBy(
        F.col("packets").desc(),
        F.col("bytes").desc(),
        F.col("port").asc(),
    )
    ranked_df = port_df.withColumn("_rank", F.row_number().over(window_spec))
    spark_port_rows = (
        ranked_df.filter(F.col("_rank") <= top_n).orderBy("window_start", "_rank").collect()
    )

    pdf_ports = pdf[pdf["dst_port"].notna()].copy()
    pdf_ports["port"] = pdf_ports["dst_port"].astype(int)

    grouped_ports = (
        pdf_ports.groupby(["window_start", "port"])
        .agg(packets=("packet_length", "count"), bytes=("packet_length", "sum"))
        .reset_index()
        .sort_values(
            ["window_start", "packets", "bytes", "port"], ascending=[True, False, False, True]
        )
    )
    top_pandas = grouped_ports.groupby("window_start").head(top_n).reset_index(drop=True)

    assert len(spark_port_rows) == len(top_pandas)
    for i, s_row in enumerate(spark_port_rows):
        p_row = top_pandas.iloc[i]
        assert s_row["window_start"] == p_row["window_start"]
        assert s_row["port"] == int(p_row["port"])
        assert s_row["packets"] == int(p_row["packets"])
        assert s_row["bytes"] == int(p_row["bytes"])
