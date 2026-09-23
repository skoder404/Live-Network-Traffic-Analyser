"""
streaming/queries/counts.py — Lane A windowed protocol and top-N port counts streaming queries.

Computes tumbling window aggregations with event-time watermarking:
  1. protocol_counts: (window_start, window_len_s, protocol, packets, bytes)
  2. port_counts: (window_start, window_len_s, port, packets, bytes) with top-N ranking
Reference: TECH_RULES §3.5, §5.2, todo.md T4-005
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

from common.serving_db import connect, upsert

logger = logging.getLogger("CountsQuery")


def build_protocol_counts(
    df: DataFrame, window_len_s: int = 10, watermark_s: int = 30
) -> DataFrame:
    """
    Constructs windowed protocol distribution aggregation from cleaned packet stream.

    Args:
        df: Cleaned packet DataFrame with event_time, protocol, and packet_length.
        window_len_s: Window length in seconds (default 10).
        watermark_s: Watermark delay threshold in seconds (default 30).

    Returns:
        Aggregated DataFrame with window_start, window_len_s, protocol, packets, and bytes.
    """
    from pyspark.sql import functions as F

    return (
        df.withWatermark("event_time", f"{watermark_s} seconds")
        .groupBy(F.window("event_time", f"{window_len_s} seconds"), "protocol")
        .agg(
            F.count("*").alias("packets"),
            F.coalesce(F.sum("packet_length"), F.lit(0)).alias("bytes"),
        )
        .select(
            F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.000'Z'").alias(
                "window_start"
            ),
            F.lit(window_len_s).alias("window_len_s"),
            F.col("protocol"),
            F.col("packets").cast("int").alias("packets"),
            F.col("bytes").cast("int").alias("bytes"),
        )
    )


def start_protocol_counts(
    spark: SparkSession,
    stream_df: DataFrame,
    cfg: dict[str, Any],
    window_len_s: int = 10,
) -> StreamingQuery:
    """
    Starts the streaming protocol counts query.

    Args:
        spark: Active SparkSession.
        stream_df: Valid cleaned streaming DataFrame.
        cfg: System configuration dictionary.
        window_len_s: Window length in seconds (default 10).

    Returns:
        Active StreamingQuery instance.
    """
    spark_cfg = cfg.get("spark", {})
    watermark_s = spark_cfg.get("watermark_s", 30)
    trigger_s = spark_cfg.get("trigger_s", 5)
    db_path = cfg.get("serving", {}).get("db_path", "serving/analytics.db")
    checkpoint_root = spark_cfg.get("checkpoint_root", "data/checkpoints")
    checkpoint_dir = f"{checkpoint_root}/q_protocol_counts_{window_len_s}"

    protocol_df = build_protocol_counts(
        stream_df, window_len_s=window_len_s, watermark_s=watermark_s
    )

    def write_to_sqlite(batch_df: DataFrame, batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.collect()]
        if not rows:
            return
        conn = connect(db_path)
        try:
            upsert(
                conn,
                "protocol_counts",
                ["window_start", "window_len_s", "protocol"],
                rows,
            )
            logger.debug(
                "Upserted %d protocol_counts rows for window_len %ds (batch %d)",
                len(rows),
                window_len_s,
                batch_id,
            )
        finally:
            conn.close()

    query = (
        protocol_df.writeStream.outputMode("update")
        .foreachBatch(write_to_sqlite)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    logger.info(
        "Started protocol_counts streaming query (window=%ds, query_id=%s)",
        window_len_s,
        query.id,
    )
    return query


def build_port_counts(df: DataFrame, window_len_s: int = 10, watermark_s: int = 30) -> DataFrame:
    """
    Constructs windowed destination port traffic aggregation from cleaned packet stream.
    Excludes null/empty ports (e.g. ICMP or non-port protocols).

    Args:
        df: Cleaned packet DataFrame with event_time, dst_port, and packet_length.
        window_len_s: Window length in seconds (default 10).
        watermark_s: Watermark delay threshold in seconds (default 30).

    Returns:
        Aggregated DataFrame with window_start, window_len_s, port, packets, and bytes.
    """
    from pyspark.sql import functions as F

    return (
        df.filter(F.col("dst_port").isNotNull())
        .withWatermark("event_time", f"{watermark_s} seconds")
        .groupBy(F.window("event_time", f"{window_len_s} seconds"), "dst_port")
        .agg(
            F.count("*").alias("packets"),
            F.coalesce(F.sum("packet_length"), F.lit(0)).alias("bytes"),
        )
        .select(
            F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.000'Z'").alias(
                "window_start"
            ),
            F.lit(window_len_s).alias("window_len_s"),
            F.col("dst_port").cast("int").alias("port"),
            F.col("packets").cast("int").alias("packets"),
            F.col("bytes").cast("int").alias("bytes"),
        )
    )


def start_port_counts(
    spark: SparkSession,
    stream_df: DataFrame,
    cfg: dict[str, Any],
    window_len_s: int = 10,
) -> StreamingQuery:
    """
    Starts the streaming top-N destination port counts query.

    Args:
        spark: Active SparkSession.
        stream_df: Valid cleaned streaming DataFrame.
        cfg: System configuration dictionary.
        window_len_s: Window length in seconds (default 10).

    Returns:
        Active StreamingQuery instance.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    spark_cfg = cfg.get("spark", {})
    watermark_s = spark_cfg.get("watermark_s", 30)
    trigger_s = spark_cfg.get("trigger_s", 5)
    top_n = spark_cfg.get("top_n_ports", 10)
    db_path = cfg.get("serving", {}).get("db_path", "serving/analytics.db")
    checkpoint_root = spark_cfg.get("checkpoint_root", "data/checkpoints")
    checkpoint_dir = f"{checkpoint_root}/q_port_counts_{window_len_s}"

    port_df = build_port_counts(stream_df, window_len_s=window_len_s, watermark_s=watermark_s)

    def write_to_sqlite(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        window_spec = Window.partitionBy("window_start").orderBy(
            F.col("packets").desc(),
            F.col("bytes").desc(),
            F.col("port").asc(),
        )
        ranked_df = batch_df.withColumn("_rank", F.row_number().over(window_spec))
        top_df = ranked_df.filter(F.col("_rank") <= top_n).drop("_rank")

        rows = [row.asDict() for row in top_df.collect()]
        if not rows:
            return

        conn = connect(db_path)
        try:
            upsert(
                conn,
                "port_counts",
                ["window_start", "window_len_s", "port"],
                rows,
            )
            logger.debug(
                "Upserted %d top-%d port_counts rows for window_len %ds (batch %d)",
                len(rows),
                top_n,
                window_len_s,
                batch_id,
            )
        finally:
            conn.close()

    query = (
        port_df.writeStream.outputMode("update")
        .foreachBatch(write_to_sqlite)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    logger.info(
        "Started port_counts streaming query (window=%ds, top_n=%d, query_id=%s)",
        window_len_s,
        top_n,
        query.id,
    )
    return query
