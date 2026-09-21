"""
contracts/record_schema.py — Data Contract for Traffic Records (v1).

Single source of truth for the 11-field CSV format produced by the capture layer
and ingested by Flume, HDFS, and Spark Structured Streaming.
"""

from typing import Any

RECORD_SCHEMA_VERSION = "1.0.0"

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
            "pyspark is required to build the Spark schema. "
            "Install it via 'pip install pyspark'."
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
