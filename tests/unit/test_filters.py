"""
tests/unit/test_filters.py — Unit tests for stream filtering and windowed filter counts.

Verifies:
  1. Filter specification parsing from expressions, ports, protocol, length, and IP.
  2. Fail-fast validation of invalid SQL filter expressions against the contract schema.
  3. Filter application on streaming / static DataFrames.
  4. Windowed filter counts aggregation consistency with protocol counts.
Reference: TECH_RULES §3.5, todo.md T4-006
"""

from __future__ import annotations

import pytest

from contracts.record_schema import to_spark_schema
from streaming.analytics.filters import (
    FilterSpec,
    apply_filter,
    parse_filters,
    validate_filters,
)
from streaming.common.cleaning import clean
from streaming.common.session import get_spark
from streaming.queries.counts import build_protocol_counts
from streaming.queries.filter_counts import build_filter_counts


@pytest.fixture(scope="module")
def spark():
    s = get_spark("LNTA-TestFilters")
    yield s


def test_parse_filters_formats():
    """Tests parsing diverse filter configuration formats and defaults."""
    # 1. Raw expressions
    cfg1 = {"filters": [{"name": "f1", "expr": "protocol = 'TCP'"}]}
    specs1 = parse_filters(cfg1)
    assert len(specs1) == 1
    assert specs1[0].name == "f1"
    assert specs1[0].expr == "protocol = 'TCP'"

    # 2. Port list and single port
    cfg2 = {
        "spark": {
            "filters": [
                {"name": "web", "dst_ports": [80, 443]},
                {"name": "dns", "dst_ports": 53},
            ]
        }
    }
    specs2 = parse_filters(cfg2)
    assert len(specs2) == 2
    assert specs2[0].expr == "dst_port IN (80, 443)"
    assert specs2[1].expr == "dst_port = 53"

    # 3. Protocol, min_length, src_ip
    cfg3 = [
        {"name": "is_udp", "protocol": "UDP"},
        {"name": "large", "min_length": 1500},
        {"name": "from_gateway", "src_ip": "192.168.1.1"},
    ]
    specs3 = parse_filters(cfg3)
    assert specs3[0].expr == "protocol = 'UDP'"
    assert specs3[1].expr == "packet_length >= 1500"
    assert specs3[2].expr == "src_ip = '192.168.1.1'"

    # 4. Defaults on empty
    specs_def = parse_filters(None)
    assert len(specs_def) >= 4
    names = {s.name for s in specs_def}
    assert "tcp_only" in names
    assert "web_traffic" in names

    # 5. Invalid spec errors
    with pytest.raises(ValueError):
        parse_filters([{"expr": "protocol = 'TCP'"}])  # missing name
    with pytest.raises(ValueError):
        parse_filters([{"name": "bad"}])  # missing criteria


def test_validate_filters_syntax_and_schema(spark):
    """Tests that invalid SQL syntax or non-existent columns fail validation."""
    valid_specs = [
        FilterSpec(name="tcp", expr="protocol = 'TCP'"),
        FilterSpec(name="web", expr="dst_port IN (80, 443) AND packet_length > 100"),
    ]
    validate_filters(valid_specs, spark)

    invalid_syntax = [FilterSpec(name="bad_syntax", expr="protocol = = 'TCP'")]
    with pytest.raises(ValueError, match="Invalid filter expression"):
        validate_filters(invalid_syntax, spark)

    invalid_column = [FilterSpec(name="unknown_col", expr="invalid_column_name > 10")]
    with pytest.raises(ValueError, match="Invalid filter expression"):
        validate_filters(invalid_column, spark)


def test_apply_filter(spark):
    """Tests filtering a DataFrame using apply_filter."""
    data = [
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
            "192.168.1.10",
            "8.8.8.8",
            1234,
            53,
            "UDP",
            200,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-24 12:00:03.000",
            "192.168.1.10",
            "8.8.8.8",
            1234,
            80,
            "TCP",
            300,
            None,
            None,
            None,
            1.0,
        ),
    ]
    raw_df = spark.createDataFrame(data, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    tcp_spec = FilterSpec(name="tcp_only", expr="protocol = 'TCP'")
    filtered_df = apply_filter(cleaned_df, tcp_spec)
    assert filtered_df.count() == 2

    # Filter by name with specs
    udp_df = apply_filter(cleaned_df, "udp_only")
    assert udp_df.count() == 1


def test_build_filter_counts_window_aggregation_and_consistency(spark):
    """Tests windowed filter counts aggregation and checks consistency against protocol counts."""
    data = [
        # Window 1: 12:00:00 - 12:00:10
        (
            "2026-09-24 12:00:01.000",
            "192.168.1.10",
            "8.8.8.8",
            1001,
            443,
            "TCP",
            1200,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-24 12:00:02.000",
            "192.168.1.10",
            "8.8.8.8",
            1002,
            80,
            "TCP",
            300,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-24 12:00:03.000",
            "192.168.1.11",
            "1.1.1.1",
            1003,
            53,
            "UDP",
            80,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-24 12:00:04.000",
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
            "2026-09-24 12:00:12.000",
            "192.168.1.10",
            "8.8.8.8",
            1004,
            443,
            "TCP",
            1500,
            None,
            None,
            None,
            1.0,
        ),
        (
            "2026-09-24 12:00:14.000",
            "192.168.1.11",
            "8.8.8.8",
            1005,
            53,
            "UDP",
            90,
            None,
            None,
            None,
            1.0,
        ),
    ]
    raw_df = spark.createDataFrame(data, schema=to_spark_schema())
    cleaned_df = clean(raw_df)

    filter_specs = [
        FilterSpec(name="tcp_only", expr="protocol = 'TCP'"),
        FilterSpec(name="dport_443", expr="dst_port = 443"),
        FilterSpec(name="udp_only", expr="protocol = 'UDP'"),
        FilterSpec(name="size_gt_1000", expr="packet_length > 1000"),
    ]

    fcounts_df = build_filter_counts(
        cleaned_df, filter_specs=filter_specs, window_len_s=10, watermark_s=30
    )
    proto_df = build_protocol_counts(cleaned_df, window_len_s=10, watermark_s=30)

    f_rows = fcounts_df.orderBy("window_start", "filter_name").collect()
    p_rows = proto_df.orderBy("window_start", "protocol").collect()

    # Window 1 checks
    w1_f = {r["filter_name"]: r for r in f_rows if r["window_start"] == "2026-09-24T12:00:00.000Z"}
    w1_p = {r["protocol"]: r for r in p_rows if r["window_start"] == "2026-09-24T12:00:00.000Z"}

    # tcp_only count equals protocol_counts TCP packets in the same window
    assert w1_f["tcp_only"]["packets"] == w1_p["TCP"]["packets"] == 2
    assert w1_f["tcp_only"]["bytes"] == w1_p["TCP"]["bytes"] == 1500

    # udp_only count equals protocol_counts UDP packets in the same window
    assert w1_f["udp_only"]["packets"] == w1_p["UDP"]["packets"] == 1
    assert w1_f["udp_only"]["bytes"] == w1_p["UDP"]["bytes"] == 80

    # dport_443 has 1 packet (1200 bytes)
    assert w1_f["dport_443"]["packets"] == 1
    assert w1_f["dport_443"]["bytes"] == 1200

    # size_gt_1000 has 1 packet (1200 bytes)
    assert w1_f["size_gt_1000"]["packets"] == 1
    assert w1_f["size_gt_1000"]["bytes"] == 1200

    # Window 2 checks
    w2_f = {r["filter_name"]: r for r in f_rows if r["window_start"] == "2026-09-24T12:00:10.000Z"}
    assert w2_f["tcp_only"]["packets"] == 1
    assert w2_f["dport_443"]["packets"] == 1
    assert w2_f["size_gt_1000"]["packets"] == 1
    assert w2_f["udp_only"]["packets"] == 1
