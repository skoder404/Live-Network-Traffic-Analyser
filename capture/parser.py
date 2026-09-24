"""
capture/parser.py — Line parser and normaliser for raw TShark output.

Converts raw TShark comma-separated lines into contract-compliant Record instances
or structured Reject instances with specific RejectReason enums.
"""

from __future__ import annotations

import ipaddress
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from capture.tshark_cmd import RAW_FIELDS
from contracts.record_schema import format_row, validate_row

EXPECTED_RAW_COLUMNS = len(RAW_FIELDS)  # 16


class RejectReason(str, Enum):
    WRONG_COLUMNS = "WRONG_COLUMNS"
    NON_IP = "NON_IP"
    BAD_TIMESTAMP = "BAD_TIMESTAMP"
    BAD_LENGTH = "BAD_LENGTH"
    BAD_PORT = "BAD_PORT"
    BAD_IP = "BAD_IP"
    VALIDATION = "VALIDATION"


# Backward compatibility constants
REJECT_WRONG_COLUMNS = RejectReason.WRONG_COLUMNS.value
REJECT_NON_IP = RejectReason.NON_IP.value
REJECT_BAD_TIMESTAMP = RejectReason.BAD_TIMESTAMP.value
REJECT_BAD_LENGTH = RejectReason.BAD_LENGTH.value
REJECT_BAD_PORT = RejectReason.BAD_PORT.value
REJECT_BAD_IP = RejectReason.BAD_IP.value
REJECT_VALIDATION = RejectReason.VALIDATION.value


@dataclass(frozen=True)
class Record:
    timestamp: str
    src_ip: str
    dst_ip: str
    src_port: int | None
    dst_port: int | None
    protocol: str
    packet_length: int
    src_mac: str | None
    dst_mac: str | None
    tcp_flags: str | None
    iat_ms: float | None

    def to_dict(self) -> dict[str, Any]:
        """Convert record to a dictionary with exact contract field names."""
        return asdict(self)

    def to_csv_row(self) -> str:
        """Convert record to contract CSV row (no header)."""
        return format_row(self.to_dict())


@dataclass(frozen=True)
class Reject:
    reason: RejectReason
    raw_line: str
    detail: str = ""


def _parse_timestamp(value: str) -> str:
    """Convert TShark epoch timestamp to UTC contract format (truncating milliseconds)."""
    ts = float(value)
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    millis = dt.microsecond // 1000
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{millis:03d}"


def parse_line(raw: str) -> Record | Reject:
    """
    Parse one raw TShark CSV line into a contract Record or Reject.

    Expected 16 raw fields:
    0: frame.time_epoch, 1: ip.src, 2: ip.dst, 3: ipv6.src, 4: ipv6.dst,
    5: tcp.srcport, 6: tcp.dstport, 7: udp.srcport, 8: udp.dstport,
    9: ip.proto, 10: ipv6.nxt, 11: frame.len, 12: eth.src, 13: eth.dst,
    14: tcp.flags, 15: frame.time_delta
    """
    clean_line = raw.rstrip("\r\n")
    cols = clean_line.split(",")

    if len(cols) != EXPECTED_RAW_COLUMNS:
        return Reject(
            reason=RejectReason.WRONG_COLUMNS,
            raw_line=raw,
            detail=f"Expected {EXPECTED_RAW_COLUMNS} columns, got {len(cols)}",
        )

    (
        epoch_str,
        ipv4_src,
        ipv4_dst,
        ipv6_src,
        ipv6_dst,
        tcp_src,
        tcp_dst,
        udp_src,
        udp_dst,
        ip_proto,
        ipv6_nxt,
        len_str,
        eth_src,
        eth_dst,
        tcp_flags,
        delta_str,
    ) = cols

    # Reject non-IP packets (ARP, etc.) where neither IPv4 proto nor IPv6 next-header is present
    if not ip_proto and not ipv6_nxt:
        return Reject(
            reason=RejectReason.NON_IP,
            raw_line=raw,
            detail="Neither ip.proto nor ipv6.nxt present",
        )

    # Coalesce source and destination IP addresses
    src_raw = ipv4_src or ipv6_src
    dst_raw = ipv4_dst or ipv6_dst

    if not src_raw or not dst_raw:
        return Reject(
            reason=RejectReason.BAD_IP,
            raw_line=raw,
            detail="Missing source or destination IP",
        )

    try:
        src_ip = str(ipaddress.ip_address(src_raw.strip()))
        dst_ip = str(ipaddress.ip_address(dst_raw.strip()))
    except ValueError as exc:
        return Reject(
            reason=RejectReason.BAD_IP,
            raw_line=raw,
            detail=str(exc),
        )

    # Timestamp conversion
    if not epoch_str:
        return Reject(
            reason=RejectReason.BAD_TIMESTAMP,
            raw_line=raw,
            detail="Empty timestamp",
        )
    try:
        timestamp = _parse_timestamp(epoch_str)
    except (ValueError, OverflowError, OSError) as exc:
        return Reject(
            reason=RejectReason.BAD_TIMESTAMP,
            raw_line=raw,
            detail=str(exc),
        )

    # Frame length conversion
    if not len_str:
        return Reject(
            reason=RejectReason.BAD_LENGTH,
            raw_line=raw,
            detail="Empty packet length",
        )
    try:
        packet_len = int(len_str)
        if not 1 <= packet_len <= 65535:
            return Reject(
                reason=RejectReason.BAD_LENGTH,
                raw_line=raw,
                detail=f"Packet length {packet_len} out of bounds (1-65535)",
            )
    except ValueError as exc:
        return Reject(
            reason=RejectReason.BAD_LENGTH,
            raw_line=raw,
            detail=str(exc),
        )

    # Protocol mapping: 6 -> TCP, 17 -> UDP, 1 or 58 -> ICMP, else OTHER
    proto_num_str = ip_proto or ipv6_nxt
    try:
        proto_num = int(proto_num_str)
    except ValueError:
        return Reject(
            reason=RejectReason.VALIDATION,
            raw_line=raw,
            detail=f"Invalid protocol number: {proto_num_str}",
        )

    if proto_num == 6:
        protocol = "TCP"
    elif proto_num == 17:
        protocol = "UDP"
    elif proto_num in (1, 58):
        protocol = "ICMP"
    else:
        protocol = "OTHER"

    # Ports extraction
    src_port: int | None = None
    dst_port: int | None = None

    if protocol == "TCP":
        src_p_raw, dst_p_raw = tcp_src, tcp_dst
    elif protocol == "UDP":
        src_p_raw, dst_p_raw = udp_src, udp_dst
    else:
        src_p_raw, dst_p_raw = "", ""

    if src_p_raw:
        try:
            sp = int(src_p_raw)
            if not 0 <= sp <= 65535:
                return Reject(
                    reason=RejectReason.BAD_PORT,
                    raw_line=raw,
                    detail=f"Source port {sp} out of range",
                )
            src_port = sp
        except ValueError:
            return Reject(
                reason=RejectReason.BAD_PORT,
                raw_line=raw,
                detail=f"Invalid source port: {src_p_raw}",
            )

    if dst_p_raw:
        try:
            dp = int(dst_p_raw)
            if not 0 <= dp <= 65535:
                return Reject(
                    reason=RejectReason.BAD_PORT,
                    raw_line=raw,
                    detail=f"Destination port {dp} out of range",
                )
            dst_port = dp
        except ValueError:
            return Reject(
                reason=RejectReason.BAD_PORT,
                raw_line=raw,
                detail=f"Invalid destination port: {dst_p_raw}",
            )

    # IAT ms calculation (frame.time_delta * 1000)
    iat_ms: float | None = None
    if delta_str:
        try:
            val = float(delta_str) * 1000.0
            if val < 0.0:
                return Reject(
                    reason=RejectReason.VALIDATION,
                    raw_line=raw,
                    detail="Negative frame time delta",
                )
            iat_ms = val
        except ValueError:
            return Reject(
                reason=RejectReason.VALIDATION,
                raw_line=raw,
                detail=f"Invalid frame time delta: {delta_str}",
            )

    # MACs and TCP flags normalisation
    src_mac = eth_src.strip().lower() if eth_src.strip() else None
    dst_mac = eth_dst.strip().lower() if eth_dst.strip() else None
    flags = tcp_flags.strip() if tcp_flags.strip() else None

    record = Record(
        timestamp=timestamp,
        src_ip=src_ip,
        dst_ip=dst_ip,
        src_port=src_port,
        dst_port=dst_port,
        protocol=protocol,
        packet_length=packet_len,
        src_mac=src_mac,
        dst_mac=dst_mac,
        tcp_flags=flags,
        iat_ms=iat_ms,
    )

    # Validate against data contract
    ok, reason_str = validate_row(record.to_dict())
    if not ok:
        return Reject(
            reason=RejectReason.VALIDATION,
            raw_line=raw,
            detail=reason_str,
        )

    return record


def parse_tshark_line(line: str) -> dict[str, Any]:
    """
    Backward-compatibility helper that parses a raw TShark line into a dict,
    raising ValueError on rejection matching previous behavior.
    """
    res = parse_line(line)
    if isinstance(res, Reject):
        raise ValueError(res.reason.value)
    return res.to_dict()
