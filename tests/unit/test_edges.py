"""
tests/unit/test_edges.py — Unit tests for Lane A IP edges and source stats queries.
"""

import pytest

try:
    from contracts.record_schema import to_spark_schema
    from streaming.common.cleaning import clean
    from streaming.common.session import get_spark
    from streaming.queries.edges import build_ip_edges, build_source_stats

    HAS_SPARK = True
except (ImportError, ModuleNotFoundError):
    HAS_SPARK = False


@pytest.fixture(scope="module")
def spark():
    if not HAS_SPARK:
        pytest.skip("PySpark not installed")
    s = get_spark("LNTA-TestEdges")
    yield s


@pytest.mark.spark
def test_build_ip_edges(spark):
    if not HAS_SPARK:
        pytest.skip("PySpark not installed")

    data = [
        ("2026-09-23 12:00:01.000", "192.168.1.10", "8.8.8.8", 1234, 443, "TCP", 100, None, None, None, 1.0),
        ("2026-09-23 12:00:02.000", "192.168.1.10", "8.8.8.8", 1234, 443, "TCP", 200, None, None, None, 1.0),
        ("2026-09-23 12:00:03.000", "192.168.1.10", "1.1.1.1", 1234, 53, "UDP", 50, None, None, None, 1.0),
        ("2026-09-23 12:00:04.000", "192.168.1.20", "8.8.8.8", 5678, 443, "TCP", 300, None, None, None, 1.0),
    ]

    raw_df = spark.createDataFrame(data, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    edges_df = build_ip_edges(cleaned_df, window_len_s=10, watermark_s=30)
    rows = edges_df.orderBy("src_ip", "dst_ip").collect()

    assert len(rows) == 3

    # Edge 1: 192.168.1.10 -> 1.1.1.1 (1 pkt, 50 bytes)
    e1 = rows[0]
    assert e1["src_ip"] == "192.168.1.10"
    assert e1["dst_ip"] == "1.1.1.1"
    assert e1["packets"] == 1
    assert e1["bytes"] == 50

    # Edge 2: 192.168.1.10 -> 8.8.8.8 (2 pkts, 300 bytes)
    e2 = rows[1]
    assert e2["src_ip"] == "192.168.1.10"
    assert e2["dst_ip"] == "8.8.8.8"
    assert e2["packets"] == 2
    assert e2["bytes"] == 300

    # Edge 3: 192.168.1.20 -> 8.8.8.8 (1 pkt, 300 bytes)
    e3 = rows[2]
    assert e3["src_ip"] == "192.168.1.20"
    assert e3["dst_ip"] == "8.8.8.8"
    assert e3["packets"] == 1
    assert e3["bytes"] == 300


@pytest.mark.spark
def test_build_source_stats(spark):
    if not HAS_SPARK:
        pytest.skip("PySpark not installed")

    # Client scanning 3 distinct ports and 2 distinct destinations
    data = [
        ("2026-09-23 12:00:01.000", "192.168.1.50", "8.8.8.8", 1234, 443, "TCP", 100, None, None, None, 1.0),
        ("2026-09-23 12:00:02.000", "192.168.1.50", "8.8.8.8", 1234, 80, "TCP", 100, None, None, None, 1.0),
        ("2026-09-23 12:00:03.000", "192.168.1.50", "1.1.1.1", 1234, 53, "UDP", 100, None, None, None, 1.0),
        ("2026-09-23 12:00:04.000", "192.168.1.50", "1.1.1.1", 1234, None, "ICMP", 64, None, None, None, 1.0),
    ]

    raw_df = spark.createDataFrame(data, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    stats_df = build_source_stats(cleaned_df, window_len_s=10, watermark_s=30)
    rows = stats_df.collect()

    assert len(rows) == 1
    s = rows[0]
    assert s["src_ip"] == "192.168.1.50"
    assert s["packets"] == 4
    assert s["bytes"] == 364
    assert s["unique_dst_ips"] == 2  # 8.8.8.8 and 1.1.1.1
    assert s["unique_dst_ports"] == 3  # 443, 80, 53 (None ignored)
