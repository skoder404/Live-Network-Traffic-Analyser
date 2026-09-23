"""
tests/unit/test_record_schema.py — Unit tests for contracts/record_schema.py.
"""

from contracts.record_schema import (
    FIELD_NAMES,
    RECORD_SCHEMA_VERSION,
    format_row,
    parse_line,
    to_spark_schema,
    validate_row,
)


def test_schema_metadata():
    assert RECORD_SCHEMA_VERSION == "1"
    assert len(FIELD_NAMES) == 11


def test_valid_example_record():
    valid_line = (
        "2026-09-19 18:20:01.482,192.168.1.10,8.8.8.8,52341,443,TCP,1420,"
        "aa:bb:cc:dd:ee:ff,11:22:33:44:55:66,0x0018,0.412"
    )
    parsed = parse_line(valid_line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert ok, f"Expected valid, got reason: {reason}"
    assert reason == "OK"


def test_valid_nullable_fields():
    # ICMP packet with nullable ports, macs, tcp_flags, iat_ms
    # 1:ts, 2:src_ip, 3:dst_ip, 4:src_port, 5:dst_port, 6:proto, 7:len, 8:smac, 9:dmac, 10:flags, 11:iat
    line = "2026-09-19 18:20:01.000,10.0.0.1,10.0.0.2,,,ICMP,64,,,,0.0"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert ok, reason


def test_round_trip():
    valid_line = (
        "2026-09-19 18:20:01.482,192.168.1.10,8.8.8.8,52341,443,TCP,1420,"
        "aa:bb:cc:dd:ee:ff,11:22:33:44:55:66,0x0018,0.412"
    )
    parsed = parse_line(valid_line)
    formatted = format_row(parsed)
    assert formatted == valid_line

    # Also test round-trip from dictionary
    rec_dict = dict(zip(FIELD_NAMES, parsed, strict=True))
    assert format_row(rec_dict) == valid_line


# 12+ Invalid cases asserting specific reason strings:

def test_invalid_column_count_too_few():
    fields = ["2026-09-19 18:20:01.482", "192.168.1.10", "8.8.8.8"]
    ok, reason = validate_row(fields)
    assert not ok
    assert "Expected 11 columns, got 3" in reason


def test_invalid_column_count_too_many():
    fields = ["val"] * 12
    ok, reason = validate_row(fields)
    assert not ok
    assert "Expected 11 columns, got 12" in reason


def test_invalid_timestamp_missing_millis():
    line = "2026-09-19 18:20:01,192.168.1.10,8.8.8.8,52341,443,TCP,1420,,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Invalid timestamp format" in reason


def test_invalid_timestamp_empty():
    line = ",192.168.1.10,8.8.8.8,52341,443,TCP,1420,,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Timestamp cannot be null or empty" in reason


def test_invalid_src_ip():
    line = "2026-09-19 18:20:01.123,999.999.999.999,8.8.8.8,52341,443,TCP,1420,,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Invalid source IP address" in reason


def test_invalid_dst_ip():
    line = "2026-09-19 18:20:01.123,192.168.1.10,not_an_ip,52341,443,TCP,1420,,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Invalid destination IP address" in reason


def test_invalid_src_port_range():
    line = "2026-09-19 18:20:01.123,192.168.1.10,8.8.8.8,70000,443,TCP,1420,,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Source port out of range (0-65535)" in reason


def test_invalid_src_port_not_int():
    line = "2026-09-19 18:20:01.123,192.168.1.10,8.8.8.8,abc,443,TCP,1420,,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Source port must be an integer" in reason


def test_invalid_dst_port_negative():
    line = "2026-09-19 18:20:01.123,192.168.1.10,8.8.8.8,52341,-5,TCP,1420,,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Destination port out of range (0-65535)" in reason


def test_invalid_protocol_http():
    line = "2026-09-19 18:20:01.123,192.168.1.10,8.8.8.8,52341,443,HTTP,1420,,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Invalid protocol (must be TCP, UDP, ICMP, or OTHER)" in reason


def test_invalid_packet_length_zero():
    line = "2026-09-19 18:20:01.123,192.168.1.10,8.8.8.8,52341,443,TCP,0,,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Packet length out of range (1-65535)" in reason


def test_invalid_packet_length_empty():
    line = "2026-09-19 18:20:01.123,192.168.1.10,8.8.8.8,52341,443,TCP,,,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Packet length cannot be null or empty" in reason


def test_invalid_src_mac():
    line = "2026-09-19 18:20:01.123,192.168.1.10,8.8.8.8,52341,443,TCP,1420,NOT_A_MAC,,,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Invalid source MAC format" in reason


def test_invalid_tcp_flags():
    line = "2026-09-19 18:20:01.123,192.168.1.10,8.8.8.8,52341,443,TCP,1420,,,SYN_ACK,0.1"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Invalid TCP flags format" in reason


def test_invalid_iat_negative():
    line = "2026-09-19 18:20:01.123,192.168.1.10,8.8.8.8,52341,443,TCP,1420,,,,-1.5"
    parsed = parse_line(line)
    assert len(parsed) == 11
    ok, reason = validate_row(parsed)
    assert not ok
    assert "Inter-arrival time must be non-negative" in reason


def test_spark_schema_fields():
    try:
        schema = to_spark_schema()
        assert len(schema.fields) == 11
        assert [f.name for f in schema.fields] == FIELD_NAMES
    except ImportError:
        pass
