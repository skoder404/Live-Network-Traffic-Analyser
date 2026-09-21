"""
tests/unit/test_cleaning.py — Unit tests for data cleaning and schema validation.
"""

import pytest

from contracts.record_schema import to_spark_schema
from streaming.common.cleaning import clean, split_valid
from streaming.common.session import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("LNTA-TestCleaning")
    yield s


def test_clean_valid_records(spark):
    data = [
        (
            "2026-09-21 12:00:00.123",
            "192.168.1.50",
            "8.8.8.8",
            54321,
            443,
            "TCP",
            1420,
            "aa:bb:cc:dd:ee:ff",
            "11:22:33:44:55:66",
            "0x0018",
            1.5,
        ),
        (
            "2026-09-21 12:00:00.200",
            "10.0.0.1",
            "172.16.0.2",
            53,
            53,
            "UDP",
            120,
            None,
            None,
            None,
            0.2,
        ),
        (
            "2026-09-21 12:00:00.300",
            "192.168.1.1",
            "1.1.1.1",
            None,
            None,
            "ICMP",
            64,
            None,
            None,
            None,
            5.0,
        ),
        (
            "2026-09-21 12:00:00.400",
            "127.0.0.1",
            "127.0.0.1",
            8000,
            8000,
            "OTHER",
            100,
            None,
            None,
            None,
            0.1,
        ),
    ]
    df = spark.createDataFrame(data, schema=to_spark_schema())
    cleaned = clean(df)
    rows = cleaned.collect()

    assert len(rows) == 4
    # Row 1: TCP to 443
    assert rows[0]["is_valid"] is True
    assert rows[0]["is_private_src"] is True
    assert rows[0]["is_private_dst"] is False
    assert rows[0]["port_class"] == "well-known"

    # Row 2: UDP 10.x to 172.16.x
    assert rows[1]["is_valid"] is True
    assert rows[1]["is_private_src"] is True
    assert rows[1]["is_private_dst"] is True
    assert rows[1]["port_class"] == "well-known"

    # Row 3: ICMP with null ports
    assert rows[2]["is_valid"] is True
    assert rows[2]["is_private_src"] is True
    assert rows[2]["is_private_dst"] is False
    assert rows[2]["port_class"] == "none"

    # Row 4: OTHER loopback
    assert rows[3]["is_valid"] is True
    assert rows[3]["is_private_src"] is True
    assert rows[3]["is_private_dst"] is True
    assert rows[3]["port_class"] == "registered"


def test_clean_ipv6_records(spark):
    ipv6_data = [
        # Private / link-local IPv6
        (
            "2026-09-21 12:00:00.000",
            "fe80::1",
            "fc00::2",
            50000,
            443,
            "TCP",
            1200,
            None,
            None,
            None,
            1.0,
        ),
        # Global unicast public IPv6
        (
            "2026-09-21 12:00:00.000",
            "2001:4860:4860::8888",
            "2404:6800:4007:80c::200e",
            51234,
            443,
            "TCP",
            1400,
            None,
            None,
            None,
            1.0,
        ),
    ]
    df = spark.createDataFrame(ipv6_data, schema=to_spark_schema())
    cleaned = clean(df)
    rows = cleaned.collect()

    assert rows[0]["is_valid"] is True
    assert rows[0]["is_private_src"] is True
    assert rows[0]["is_private_dst"] is True

    assert rows[1]["is_valid"] is True
    assert rows[1]["is_private_src"] is False
    assert rows[1]["is_private_dst"] is False


def test_clean_invalid_edge_cases(spark):
    invalid_data = [
        # 1. Bad timestamp string
        (
            "INVALID_TIME",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            80,
            "TCP",
            500,
            None,
            None,
            None,
            1.0,
        ),
        # 2. Empty timestamp
        (None, "192.168.1.1", "8.8.8.8", 1234, 80, "TCP", 500, None, None, None, 1.0),
        # 3. Packet length 0
        (
            "2026-09-21 12:00:01.000",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            80,
            "TCP",
            0,
            None,
            None,
            None,
            1.0,
        ),
        # 4. Packet length > 65535
        (
            "2026-09-21 12:00:01.000",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            80,
            "TCP",
            70000,
            None,
            None,
            None,
            1.0,
        ),
        # 5. Negative packet length
        (
            "2026-09-21 12:00:01.000",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            80,
            "TCP",
            -10,
            None,
            None,
            None,
            1.0,
        ),
        # 6. Invalid Protocol "HTTP"
        (
            "2026-09-21 12:00:01.000",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            80,
            "HTTP",
            100,
            None,
            None,
            None,
            1.0,
        ),
        # 7. Invalid Protocol "DNS"
        (
            "2026-09-21 12:00:01.000",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            80,
            "DNS",
            100,
            None,
            None,
            None,
            1.0,
        ),
        # 8. Dest Port > 65535
        (
            "2026-09-21 12:00:01.000",
            "192.168.1.1",
            "8.8.8.8",
            1234,
            70000,
            "TCP",
            100,
            None,
            None,
            None,
            1.0,
        ),
        # 9. Source Port < 0
        (
            "2026-09-21 12:00:01.000",
            "192.168.1.1",
            "8.8.8.8",
            -5,
            80,
            "TCP",
            100,
            None,
            None,
            None,
            1.0,
        ),
        # 10. Null src_ip
        (
            "2026-09-21 12:00:01.000",
            None,
            "8.8.8.8",
            1234,
            80,
            "TCP",
            100,
            None,
            None,
            None,
            1.0,
        ),
        # 11. Empty src_ip
        (
            "2026-09-21 12:00:01.000",
            "",
            "8.8.8.8",
            1234,
            80,
            "TCP",
            100,
            None,
            None,
            None,
            1.0,
        ),
        # 12. Null dst_ip
        (
            "2026-09-21 12:00:01.000",
            "192.168.1.1",
            None,
            1234,
            80,
            "TCP",
            100,
            None,
            None,
            None,
            1.0,
        ),
    ]
    df = spark.createDataFrame(invalid_data, schema=to_spark_schema())
    cleaned = clean(df)
    valid_df, invalid_df = split_valid(cleaned)

    assert valid_df.count() == 0
    assert invalid_df.count() == len(invalid_data)


def test_port_classification(spark):
    data = [
        (
            "2026-09-21 12:00:00.000",
            "1.1.1.1",
            "2.2.2.2",
            50000,
            80,
            "TCP",
            100,
            None,
            None,
            None,
            1.0,
        ),  # well-known
        (
            "2026-09-21 12:00:00.000",
            "1.1.1.1",
            "2.2.2.2",
            50000,
            8080,
            "TCP",
            100,
            None,
            None,
            None,
            1.0,
        ),  # registered
        (
            "2026-09-21 12:00:00.000",
            "1.1.1.1",
            "2.2.2.2",
            50000,
            55000,
            "TCP",
            100,
            None,
            None,
            None,
            1.0,
        ),  # dynamic
        (
            "2026-09-21 12:00:00.000",
            "1.1.1.1",
            "2.2.2.2",
            None,
            None,
            "ICMP",
            64,
            None,
            None,
            None,
            1.0,
        ),  # none
    ]
    df = spark.createDataFrame(data, schema=to_spark_schema())
    cleaned = clean(df)
    classes = [r["port_class"] for r in cleaned.collect()]
    assert classes == ["well-known", "registered", "dynamic", "none"]
