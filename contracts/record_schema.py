"""
contracts/record_schema.py — Data Contract for Traffic Records (v1).

Single source of truth for the 11-field CSV format produced by the capture layer
and ingested by Flume, HDFS, and Spark Structured Streaming.
"""

from __future__ import annotations

import ipaddress
import re
from datetime import datetime
from typing import Any

RECORD_SCHEMA_VERSION = "1"

# Exact 11-field contract definition
FIELDS: list[dict[str, Any]] = [
    {
        "name": "timestamp",
        "type": "string",
        "nullable": False,
        "description": "UTC YYYY-MM-DD HH:MM:SS.mmm",
    },
    {
        "name": "src_ip",
        "type": "string",
        "nullable": False,
        "description": "Source IPv4/IPv6",
    },
    {
        "name": "dst_ip",
        "type": "string",
        "nullable": False,
        "description": "Destination IPv4/IPv6",
    },
    {
        "name": "src_port",
        "type": "int",
        "nullable": True,
        "description": "Source Port 0-65535",
    },
    {
        "name": "dst_port",
        "type": "int",
        "nullable": True,
        "description": "Destination Port 0-65535",
    },
    {
        "name": "protocol",
        "type": "string",
        "nullable": False,
        "description": "TCP, UDP, ICMP, or OTHER",
    },
    {
        "name": "packet_length",
        "type": "int",
        "nullable": False,
        "description": "Frame length in bytes (1-65535)",
    },
    {
        "name": "src_mac",
        "type": "string",
        "nullable": True,
        "description": "Source MAC address",
    },
    {
        "name": "dst_mac",
        "type": "string",
        "nullable": True,
        "description": "Destination MAC address",
    },
    {
        "name": "tcp_flags",
        "type": "string",
        "nullable": True,
        "description": "TCP flags hex string e.g. 0x0018",
    },
    {
        "name": "iat_ms",
        "type": "double",
        "nullable": True,
        "description": "Inter-arrival time since last packet in ms",
    },
]

FIELD_NAMES: list[str] = [f["name"] for f in FIELDS]
NUM_FIELDS: int = len(FIELDS)

ALLOWED_PROTOCOLS: set[str] = {"TCP", "UDP", "ICMP", "OTHER"}
_MAC_REGEX = re.compile(r"^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")
_TCP_FLAGS_REGEX = re.compile(r"^0x[0-9a-fA-F]+$")
_TIMESTAMP_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$")


def parse_line(line: str) -> list[str | None]:
    """
    Parse a comma-separated line into a list of 11 field values.
    Empty strings are converted to None.
    Trailing newlines are stripped.
    """
    clean_line = line.rstrip("\r\n")
    raw_fields = clean_line.split(",")
    return [val if val != "" else None for val in raw_fields]


def format_row(record: dict[str, Any] | list[Any]) -> str:
    """
    Format a record dictionary or list into an 11-field CSV row string.
    None values become empty strings. No header is added.
    """
    if isinstance(record, dict):
        values = [record.get(name) for name in FIELD_NAMES]
    else:
        values = list(record)

    str_parts: list[str] = []
    for val in values:
        if val is None:
            str_parts.append("")
        else:
            str_parts.append(str(val))

    return ",".join(str_parts)


def validate_row(fields: list[Any] | dict[str, Any]) -> tuple[bool, str]:
    """
    Validates that a row of fields satisfies the contract rules.
    Accepts either an 11-element sequence or a dict keyed by FIELD_NAMES.
    Returns (True, "OK") or (False, "<specific failure reason>").
    """
    if isinstance(fields, dict):
        if len(fields) != NUM_FIELDS or not all(k in fields for k in FIELD_NAMES):
            # Check if all required keys exist
            missing = [k for k in FIELD_NAMES if k not in fields]
            if missing:
                return False, f"Missing columns in record dict: {missing}"
        row = [fields.get(name) for name in FIELD_NAMES]
    else:
        row = list(fields)

    if len(row) != NUM_FIELDS:
        return False, f"Expected {NUM_FIELDS} columns, got {len(row)}"

    (
        timestamp,
        src_ip,
        dst_ip,
        src_port,
        dst_port,
        protocol,
        packet_length,
        src_mac,
        dst_mac,
        tcp_flags,
        iat_ms,
    ) = row

    # 1. Timestamp validation
    if not timestamp:
        return False, "Timestamp cannot be null or empty"
    ts_str = str(timestamp).strip()
    if not _TIMESTAMP_REGEX.match(ts_str):
        return False, f"Invalid timestamp format (expected YYYY-MM-DD HH:MM:SS.mmm): {timestamp}"
    try:
        # Validate date/time components
        base_time, _ = ts_str.split(".")
        datetime.strptime(base_time, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return False, f"Invalid timestamp calendar values: {timestamp}"

    # 2. Source IP validation
    if not src_ip:
        return False, "Source IP cannot be null or empty"
    try:
        ipaddress.ip_address(str(src_ip).strip())
    except ValueError:
        return False, f"Invalid source IP address: {src_ip}"

    # 3. Destination IP validation
    if not dst_ip:
        return False, "Destination IP cannot be null or empty"
    try:
        ipaddress.ip_address(str(dst_ip).strip())
    except ValueError:
        return False, f"Invalid destination IP address: {dst_ip}"

    # 4. Source Port validation (nullable)
    if src_port is not None and str(src_port).strip() != "":
        try:
            sp = int(src_port)
            if not 0 <= sp <= 65535:
                return False, f"Source port out of range (0-65535): {src_port}"
        except (ValueError, TypeError):
            return False, f"Source port must be an integer: {src_port}"

    # 5. Destination Port validation (nullable)
    if dst_port is not None and str(dst_port).strip() != "":
        try:
            dp = int(dst_port)
            if not 0 <= dp <= 65535:
                return False, f"Destination port out of range (0-65535): {dst_port}"
        except (ValueError, TypeError):
            return False, f"Destination port must be an integer: {dst_port}"

    # 6. Protocol validation
    if not protocol:
        return False, "Protocol cannot be null or empty"
    proto_str = str(protocol).strip().upper()
    if proto_str not in ALLOWED_PROTOCOLS:
        return False, f"Invalid protocol (must be TCP, UDP, ICMP, or OTHER): {protocol}"

    # 7. Packet Length validation
    if packet_length is None or str(packet_length).strip() == "":
        return False, "Packet length cannot be null or empty"
    try:
        plen = int(packet_length)
        if not 1 <= plen <= 65535:
            return False, f"Packet length out of range (1-65535): {packet_length}"
    except (ValueError, TypeError):
        return False, f"Packet length must be an integer: {packet_length}"

    # 8. Source MAC validation (nullable)
    if src_mac is not None and str(src_mac).strip() != "":
        smac = str(src_mac).strip().lower()
        if not _MAC_REGEX.match(smac):
            return False, f"Invalid source MAC format (expected aa:bb:cc:dd:ee:ff): {src_mac}"

    # 9. Destination MAC validation (nullable)
    if dst_mac is not None and str(dst_mac).strip() != "":
        dmac = str(dst_mac).strip().lower()
        if not _MAC_REGEX.match(dmac):
            return False, f"Invalid destination MAC format (expected aa:bb:cc:dd:ee:ff): {dst_mac}"

    # 10. TCP Flags validation (nullable)
    if tcp_flags is not None and str(tcp_flags).strip() != "":
        tflags = str(tcp_flags).strip()
        if not _TCP_FLAGS_REGEX.match(tflags):
            return False, f"Invalid TCP flags format (expected hex e.g. 0x0018): {tcp_flags}"

    # 11. Inter-arrival time (iat_ms) validation (nullable)
    if iat_ms is not None and str(iat_ms).strip() != "":
        try:
            iat = float(iat_ms)
            if iat < 0.0:
                return False, f"Inter-arrival time must be non-negative: {iat_ms}"
        except (ValueError, TypeError):
            return False, f"Inter-arrival time must be a float: {iat_ms}"

    return True, "OK"


def to_spark_schema(nullable_all: bool = True) -> Any:
    """
    Returns the PySpark StructType schema corresponding to the contract.
    By default nullable_all is True for permissive CSV streaming ingestion.
    Imports PySpark lazily so the module can be loaded in non-Spark environments.
    """
    try:
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )
    except ImportError as e:
        raise ImportError(
            "pyspark is required to build the Spark schema. Install it via 'pip install pyspark'."
        ) from e

    return StructType(
        [
            StructField("timestamp", StringType(), nullable_all or False),
            StructField("src_ip", StringType(), nullable_all or False),
            StructField("dst_ip", StringType(), nullable_all or False),
            StructField("src_port", IntegerType(), True),
            StructField("dst_port", IntegerType(), True),
            StructField("protocol", StringType(), nullable_all or False),
            StructField("packet_length", IntegerType(), nullable_all or False),
            StructField("src_mac", StringType(), True),
            StructField("dst_mac", StringType(), True),
            StructField("tcp_flags", StringType(), True),
            StructField("iat_ms", DoubleType(), True),
        ]
    )
