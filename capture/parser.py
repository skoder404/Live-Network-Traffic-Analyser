"""Parse and normalise raw TShark field output into traffic records."""

from __future__ import annotations

import ipaddress
from datetime import datetime, timezone


TsharkRecord = dict[str, object]

EXPECTED_COLUMNS = 16

REJECT_WRONG_COLUMNS = "WRONG_COLUMNS"
REJECT_NON_IP = "NON_IP"
REJECT_BAD_TIMESTAMP = "BAD_TIMESTAMP"
REJECT_BAD_LENGTH = "BAD_LENGTH"
REJECT_BAD_PORT = "BAD_PORT"
REJECT_BAD_IP = "BAD_IP"
REJECT_VALIDATION = "VALIDATION"


def _parse_timestamp(value: str) -> str:
    """Convert TShark epoch timestamp to UTC contract format."""
    try:
        timestamp = float(value)
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise ValueError(REJECT_BAD_TIMESTAMP) from exc

    milliseconds = dt.microsecond // 1000
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{milliseconds:03d}"


def _parse_ip(value: str) -> str:
    """Validate and return an IP address."""
    try:
        return str(ipaddress.ip_address(value))
    except ValueError as exc:
        raise ValueError(REJECT_BAD_IP) from exc


def _parse_port(value: str) -> int | None:
    """Validate and return a TCP/UDP port."""
    if value == "":
        return None

    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(REJECT_BAD_PORT) from exc

    if not 0 <= port <= 65535:
        raise ValueError(REJECT_BAD_PORT)

    return port


def _parse_length(value: str) -> int:
    """Validate and return packet length."""
    try:
        length = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(REJECT_BAD_LENGTH) from exc

    if length < 0:
        raise ValueError(REJECT_BAD_LENGTH)

    return length


def _parse_iat(value: str) -> float | None:
    """Convert TShark frame delta from seconds to milliseconds."""
    if value == "":
        return None

    try:
        return float(value) * 1000.0
    except (TypeError, ValueError) as exc:
        raise ValueError(REJECT_VALIDATION) from exc


def parse_tshark_line(line: str) -> TsharkRecord:
    """Parse one raw TShark CSV line into a normalised traffic record.

    Expected TShark field order:
    frame.time_epoch, ip.src, ip.dst, ipv6.src, ipv6.dst,
    tcp.srcport, tcp.dstport, udp.srcport, udp.dstport,
    ip.proto, ipv6.nxt, frame.len, eth.src, eth.dst,
    tcp.flags, frame.time_delta
    """
    columns = line.rstrip("\r\n").split(",")

    if len(columns) != EXPECTED_COLUMNS:
        raise ValueError(REJECT_WRONG_COLUMNS)

    (
        timestamp_raw,
        ipv4_src,
        ipv4_dst,
        ipv6_src,
        ipv6_dst,
        tcp_src,
        tcp_dst,
        udp_src,
        udp_dst,
        ip_proto,
        ipv6_next_header,
        packet_length_raw,
        src_mac,
        dst_mac,
        tcp_flags,
        iat_raw,
    ) = columns

    if not ip_proto and not ipv6_next_header:
        raise ValueError(REJECT_NON_IP)

    timestamp = _parse_timestamp(timestamp_raw)

    src_raw = ipv4_src or ipv6_src
    dst_raw = ipv4_dst or ipv6_dst

    if not src_raw or not dst_raw:
        raise ValueError(REJECT_BAD_IP)

    src_ip = _parse_ip(src_raw)
    dst_ip = _parse_ip(dst_raw)

    packet_length = _parse_length(packet_length_raw)

    protocol_number = ip_proto or ipv6_next_header

    try:
        protocol_value = int(protocol_number)
    except (TypeError, ValueError) as exc:
        raise ValueError(REJECT_VALIDATION) from exc

    if protocol_value == 6:
        protocol = "TCP"
    elif protocol_value == 17:
        protocol = "UDP"
    elif protocol_value in (1, 58):
        protocol = "ICMP"
    else:
        protocol = "OTHER"

    if protocol == "TCP":
        src_port_raw = tcp_src
        dst_port_raw = tcp_dst
    elif protocol == "UDP":
        src_port_raw = udp_src
        dst_port_raw = udp_dst
    else:
        src_port_raw = ""
        dst_port_raw = ""

    src_port = _parse_port(src_port_raw)
    dst_port = _parse_port(dst_port_raw)

    iat_ms = _parse_iat(iat_raw)

    return {
        "timestamp": timestamp,
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "src_port": src_port,
        "dst_port": dst_port,
        "protocol": protocol,
        "packet_length": packet_length,
        "src_mac": src_mac.lower() if src_mac else None,
        "dst_mac": dst_mac.lower() if dst_mac else None,
        "tcp_flags": tcp_flags or None,
        "iat_ms": iat_ms,
    }
