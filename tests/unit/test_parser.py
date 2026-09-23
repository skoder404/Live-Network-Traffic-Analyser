from capture.parser import (
    REJECT_BAD_IP,
    REJECT_BAD_LENGTH,
    REJECT_BAD_PORT,
    REJECT_BAD_TIMESTAMP,
    REJECT_NON_IP,
    REJECT_VALIDATION,
    REJECT_WRONG_COLUMNS,
    parse_tshark_line,
)


def make_line(**overrides):
    values = [
        "1758638400.123456",
        "192.168.1.10",
        "8.8.8.8",
        "",
        "",
        "54321",
        "443",
        "",
        "",
        "6",
        "",
        "1500",
        "AA:BB:CC:DD:EE:FF",
        "11:22:33:44:55:66",
        "0x0018",
        "0.025",
    ]

    names = [
        "timestamp",
        "ip_src",
        "ip_dst",
        "ipv6_src",
        "ipv6_dst",
        "tcp_src",
        "tcp_dst",
        "udp_src",
        "udp_dst",
        "ip_proto",
        "ipv6_nxt",
        "length",
        "src_mac",
        "dst_mac",
        "tcp_flags",
        "iat",
    ]

    for name, value in overrides.items():
        values[names.index(name)] = value

    return ",".join(values)


def test_parse_tcp_record():
    record = parse_tshark_line(make_line())

    assert record["protocol"] == "TCP"
    assert record["src_ip"] == "192.168.1.10"
    assert record["dst_ip"] == "8.8.8.8"
    assert record["src_port"] == 54321
    assert record["dst_port"] == 443


def test_timestamp_is_utc_milliseconds():
    record = parse_tshark_line(make_line(timestamp="1758638400.123456"))

    assert record["timestamp"].endswith(".123")


def test_packet_length_is_integer():
    record = parse_tshark_line(make_line(length="512"))

    assert record["packet_length"] == 512


def test_iat_is_converted_to_milliseconds():
    record = parse_tshark_line(make_line(iat="0.025"))

    assert record["iat_ms"] == 25.0


def test_mac_addresses_are_lowercase():
    record = parse_tshark_line(make_line())

    assert record["src_mac"] == "aa:bb:cc:dd:ee:ff"
    assert record["dst_mac"] == "11:22:33:44:55:66"


def test_udp_ports_are_used_for_udp():
    record = parse_tshark_line(
        make_line(
            tcp_src="",
            tcp_dst="",
            udp_src="12345",
            udp_dst="53",
            ip_proto="17",
        )
    )

    assert record["protocol"] == "UDP"
    assert record["src_port"] == 12345
    assert record["dst_port"] == 53


def test_icmp_has_no_ports():
    record = parse_tshark_line(
        make_line(
            tcp_src="",
            tcp_dst="",
            udp_src="",
            udp_dst="",
            ip_proto="1",
        )
    )

    assert record["protocol"] == "ICMP"
    assert record["src_port"] is None
    assert record["dst_port"] is None


def test_icmpv6_is_icmp():
    record = parse_tshark_line(
        make_line(
            ip_src="",
            ip_dst="",
            ipv6_src="2001:db8::1",
            ipv6_dst="2001:db8::2",
            tcp_src="",
            tcp_dst="",
            ip_proto="",
            ipv6_nxt="58",
        )
    )

    assert record["protocol"] == "ICMP"


def test_other_protocol():
    record = parse_tshark_line(
        make_line(
            tcp_src="",
            tcp_dst="",
            udp_src="",
            udp_dst="",
            ip_proto="132",
        )
    )

    assert record["protocol"] == "OTHER"


def test_ipv6_addresses_are_used():
    record = parse_tshark_line(
        make_line(
            ip_src="",
            ip_dst="",
            ipv6_src="2001:db8::1",
            ipv6_dst="2001:db8::2",
        )
    )

    assert record["src_ip"] == "2001:db8::1"
    assert record["dst_ip"] == "2001:db8::2"


def test_empty_iat_is_none():
    record = parse_tshark_line(make_line(iat=""))

    assert record["iat_ms"] is None


def test_empty_mac_is_none():
    record = parse_tshark_line(
        make_line(src_mac="", dst_mac="")
    )

    assert record["src_mac"] is None
    assert record["dst_mac"] is None


def test_wrong_column_count():
    line = make_line() + ",extra"

    try:
        parse_tshark_line(line)
    except ValueError as exc:
        assert str(exc) == REJECT_WRONG_COLUMNS
    else:
        raise AssertionError("Expected WRONG_COLUMNS")


def test_non_ip_packet():
    line = make_line(ip_proto="", ipv6_nxt="")

    try:
        parse_tshark_line(line)
    except ValueError as exc:
        assert str(exc) == REJECT_NON_IP
    else:
        raise AssertionError("Expected NON_IP")


def test_bad_timestamp():
    try:
        parse_tshark_line(make_line(timestamp="not-a-time"))
    except ValueError as exc:
        assert str(exc) == REJECT_BAD_TIMESTAMP
    else:
        raise AssertionError("Expected BAD_TIMESTAMP")


def test_bad_length():
    try:
        parse_tshark_line(make_line(length="abc"))
    except ValueError as exc:
        assert str(exc) == REJECT_BAD_LENGTH
    else:
        raise AssertionError("Expected BAD_LENGTH")


def test_bad_port():
    try:
        parse_tshark_line(make_line(tcp_src="99999"))
    except ValueError as exc:
        assert str(exc) == REJECT_BAD_PORT
    else:
        raise AssertionError("Expected BAD_PORT")


def test_bad_ip():
    try:
        parse_tshark_line(make_line(ip_src="999.999.999.999"))
    except ValueError as exc:
        assert str(exc) == REJECT_BAD_IP
    else:
        raise AssertionError("Expected BAD_IP")


def test_invalid_protocol_number():
    try:
        parse_tshark_line(make_line(ip_proto="abc"))
    except ValueError as exc:
        assert str(exc) == REJECT_VALIDATION
    else:
        raise AssertionError("Expected VALIDATION")

