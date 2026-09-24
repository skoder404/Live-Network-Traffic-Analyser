"""
streaming/common/cleaning.py — Data sanitization and transformation for network traffic records.

Validates contract constraints, casts fields, adds derived attributes (RFC1918 private IP tags,
port classifications), and separates valid from invalid rows without using Python UDFs.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
)

# RFC 1918, link-local, and unique local IPv6 regex patterns
PRIVATE_IP_REGEX = (
    r"^(10\.\d{1,3}\.\d{1,3}\.\d{1,3}|"
    r"172\.(1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|"
    r"192\.168\.\d{1,3}\.\d{1,3}|"
    r"169\.254\.\d{1,3}\.\d{1,3}|"
    r"127\.\d{1,3}\.\d{1,3}\.\d{1,3}|"
    r"::1|"
    r"[fF][cCdD][0-9a-fA-F]{2}:.*|"
    r"[fF][eE]80:.*)$"
)


def clean(df: DataFrame) -> DataFrame:
    """
    Cleans raw contract DataFrame: parses event timestamp, sanitizes fields,
    evaluates record validity, and enriches with network metadata.

    Args:
        df: Raw DataFrame matching contracts.record_schema.

    Returns:
        Enriched DataFrame with event_time, is_valid, is_private_src, is_private_dst, and port_class.
    """
    # 1. Parse timestamp string to TimestampType (UTC)
    # Support 'yyyy-MM-dd HH:mm:ss.SSS' and 'yyyy-MM-dd HH:mm:ss'
    parsed_time = F.coalesce(
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col("timestamp")),
    )

    # 2. Cast numeric columns safely
    casted_df = (
        df.withColumn("event_time", parsed_time)
        .withColumn("src_port", F.col("src_port").cast(IntegerType()))
        .withColumn("dst_port", F.col("dst_port").cast(IntegerType()))
        .withColumn("packet_length", F.col("packet_length").cast(IntegerType()))
        .withColumn("iat_ms", F.col("iat_ms").cast(DoubleType()))
        .withColumn("protocol", F.upper(F.trim(F.col("protocol"))))
    )

    # 3. Validity condition according to TECH_RULES §5.1 & PRD §FR-CAP
    is_valid_cond = (
        F.col("event_time").isNotNull()
        & F.col("src_ip").isNotNull()
        & (F.trim(F.col("src_ip")) != "")
        & F.col("dst_ip").isNotNull()
        & (F.trim(F.col("dst_ip")) != "")
        & F.col("packet_length").isNotNull()
        & F.col("packet_length").between(1, 65535)
        & F.col("protocol").isin("TCP", "UDP", "ICMP", "OTHER")
        & (F.col("src_port").isNull() | F.col("src_port").between(0, 65535))
        & (F.col("dst_port").isNull() | F.col("dst_port").between(0, 65535))
    )

    # 4. RFC1918 / Private IP detection
    is_private_src = F.col("src_ip").rlike(PRIVATE_IP_REGEX)
    is_private_dst = F.col("dst_ip").rlike(PRIVATE_IP_REGEX)

    # 5. Port classification (well-known: 0-1023, registered: 1024-49151, dynamic: 49152-65535)
    port_class = (
        F.when(F.col("dst_port").between(0, 1023), "well-known")
        .when(F.col("dst_port").between(1024, 49151), "registered")
        .when(F.col("dst_port").between(49152, 65535), "dynamic")
        .otherwise("none")
    )

    return (
        casted_df.withColumn("is_valid", is_valid_cond)
        .withColumn(
            "is_private_src",
            F.when(F.col("src_ip").isNotNull(), is_private_src).otherwise(False),
        )
        .withColumn(
            "is_private_dst",
            F.when(F.col("dst_ip").isNotNull(), is_private_dst).otherwise(False),
        )
        .withColumn("port_class", port_class)
    )


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Splits a DataFrame into (valid_records, invalid_records).

    Args:
        df: DataFrame processed with clean().

    Returns:
        Tuple of (valid_df, invalid_df).
    """
    cleaned = clean(df) if "is_valid" not in df.columns else df
    valid_df = cleaned.filter(F.col("is_valid"))
    invalid_df = cleaned.filter(~F.col("is_valid"))
    return valid_df, invalid_df
