"""
tests/unit/test_parser.py — Unit tests for capture/parser.py.
"""

from capture.parser import (
    Record,
    Reject,
    RejectReason,
    parse_line,
    parse_tshark_line,
)
from contracts.record_schema import validate_row


def make_raw_line(**overrides) -> str:
    """Helper to assemble a valid 16-field raw TShark line with overrides."""
    fields = [
        "1758638400.123456",  # 0: frame.time_epoch
        "192.168.1.10",  # 1: ip.src
        "8.8.8.8",  # 2: ip.dst
        "",  # 3: ipv6.src
        "",  # 4: ipv6.dst
        "54321",  # 5: tcp.srcport
        "443",  # 6: tcp.dstport
        "",  # 7: udp.srcport
        "",  # 8: udp.dstport
        "6",  # 9: ip.proto (TCP)
        "",  # 10: ipv6.nxt
        "1500",  # 11: frame.len
        "aa:bb:cc:dd:ee:ff",  # 12: eth.src
        "11:22:33:44:55:66",  # 13: eth.dst
        "0x0018",  # 14: tcp.flags
        "0.025",  # 15: frame.time_delta
    ]
    names = [
        "epoch",
        "ipv4_src",
        "ipv4_dst",
        "ipv6_src",
        "ipv6_dst",
        "tcp_src",
        "tcp_dst",
        "udp_src",
        "udp_dst",
        "ip_proto",
        "ipv6_nxt",
        "len",
        "eth_src",
        "eth_dst",
        "tcp_flags",
        "delta",
    ]
    for k, v in overrides.items():
        fields[names.index(k)] = v
    return ",".join(fields)


def test_parse_valid_tcp_ipv4():
    raw = make_raw_line()
    res = parse_line(raw)
    assert isinstance(res, Record)
    assert res.protocol == "TCP"
    assert res.src_ip == "192.168.1.10"
    assert res.dst_ip == "8.8.8.8"
    assert res.src_port == 54321
    assert res.dst_port == 443
    assert res.packet_length == 1500
    assert res.tcp_flags == "0x0018"
    assert res.iat_ms == 25.0
    assert res.timestamp.endswith(".123")  # Truncated, not rounded
    ok, reason = validate_row(res.to_dict())
    assert ok, reason


def test_parse_valid_udp_ipv4():
    raw = make_raw_line(
        tcp_src="",
        tcp_dst="",
        udp_src="53200",
        udp_dst="53",
        ip_proto="17",
        tcp_flags="",
        len="78",
    )
    res = parse_line(raw)
    assert isinstance(res, Record)
    assert res.protocol == "UDP"
    assert res.src_port == 53200
    assert res.dst_port == 53
    assert res.tcp_flags is None


def test_parse_valid_udp_ipv6_dns():
    raw = make_raw_line(
        ipv4_src="",
        ipv4_dst="",
        ipv6_src="2001:db8::1",
        ipv6_dst="2001:db8::2",
        tcp_src="",
        tcp_dst="",
        udp_src="61234",
        udp_dst="53",
        ip_proto="",
        ipv6_nxt="17",
        len="96",
        tcp_flags="",
    )
    res = parse_line(raw)
    assert isinstance(res, Record)
    assert res.protocol == "UDP"
    assert res.src_ip == "2001:db8::1"
    assert res.dst_ip == "2001:db8::2"
    assert res.src_port == 61234
    assert res.dst_port == 53


def test_parse_valid_icmp():
    raw = make_raw_line(
        tcp_src="",
        tcp_dst="",
        udp_src="",
        udp_dst="",
        ip_proto="1",
        tcp_flags="",
        len="64",
    )
    res = parse_line(raw)
    assert isinstance(res, Record)
    assert res.protocol == "ICMP"
    assert res.src_port is None
    assert res.dst_port is None
    assert res.packet_length == 64


def test_parse_valid_icmpv6():
    raw = make_raw_line(
        ipv4_src="",
        ipv4_dst="",
        ipv6_src="fe80::1",
        ipv6_dst="fe80::2",
        tcp_src="",
        tcp_dst="",
        udp_src="",
        udp_dst="",
        ip_proto="",
        ipv6_nxt="58",
        tcp_flags="",
        len="80",
    )
    res = parse_line(raw)
    assert isinstance(res, Record)
    assert res.protocol == "ICMP"
    assert res.src_port is None


def test_parse_other_protocol():
    raw = make_raw_line(
        tcp_src="",
        tcp_dst="",
        udp_src="",
        udp_dst="",
        ip_proto="132",  # SCTP -> OTHER
        tcp_flags="",
        len="200",
    )
    res = parse_line(raw)
    assert isinstance(res, Record)
    assert res.protocol == "OTHER"
    assert res.src_port is None
    assert res.dst_port is None


def test_reject_arp_non_ip():
    raw = make_raw_line(ip_proto="", ipv6_nxt="")
    res = parse_line(raw)
    assert isinstance(res, Reject)
    assert res.reason == RejectReason.NON_IP


def test_reject_wrong_column_count_few():
    res = parse_line("1758638400.123,192.168.1.1,8.8.8.8")
    assert isinstance(res, Reject)
    assert res.reason == RejectReason.WRONG_COLUMNS


def test_reject_wrong_column_count_many():
    res = parse_line(make_raw_line() + ",extra_col")
    assert isinstance(res, Reject)
    assert res.reason == RejectReason.WRONG_COLUMNS


def test_reject_bad_timestamp():
    raw = make_raw_line(epoch="not_a_number")
    res = parse_line(raw)
    assert isinstance(res, Reject)
    assert res.reason == RejectReason.BAD_TIMESTAMP


def test_reject_bad_ip_source():
    raw = make_raw_line(ipv4_src="999.999.999.999")
    res = parse_line(raw)
    assert isinstance(res, Reject)
    assert res.reason == RejectReason.BAD_IP


def test_reject_bad_length_non_numeric():
    raw = make_raw_line(len="not_a_len")
    res = parse_line(raw)
    assert isinstance(res, Reject)
    assert res.reason == RejectReason.BAD_LENGTH


def test_reject_bad_length_zero():
    raw = make_raw_line(len="0")
    res = parse_line(raw)
    assert isinstance(res, Reject)
    assert res.reason == RejectReason.BAD_LENGTH


def test_reject_bad_port_out_of_range():
    raw = make_raw_line(tcp_src="70000")
    res = parse_line(raw)
    assert isinstance(res, Reject)
    assert res.reason == RejectReason.BAD_PORT


def test_empty_mac_and_delta():
    raw = make_raw_line(eth_src="", eth_dst="", delta="")
    res = parse_line(raw)
    assert isinstance(res, Record)
    assert res.src_mac is None
    assert res.dst_mac is None
    assert res.iat_ms is None


def test_mac_case_normalization():
    raw = make_raw_line(eth_src="AA:BB:CC:DD:EE:FF", eth_dst="11:22:33:44:55:66")
    res = parse_line(raw)
    assert isinstance(res, Record)
    assert res.src_mac == "aa:bb:cc:dd:ee:ff"
    assert res.dst_mac == "11:22:33:44:55:66"


def test_parse_tshark_line_backward_compatibility():
    raw = make_raw_line()
    d = parse_tshark_line(raw)
    assert isinstance(d, dict)
    assert d["src_ip"] == "192.168.1.10"

    # Reject causes ValueError with reason string
    try:
        parse_tshark_line(make_raw_line(ip_proto="", ipv6_nxt=""))
    except ValueError as exc:
        assert str(exc) == "NON_IP"
    else:
        raise AssertionError("Expected ValueError on NON_IP")
