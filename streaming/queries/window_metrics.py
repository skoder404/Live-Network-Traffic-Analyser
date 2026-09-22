"""
streaming/queries/window_metrics.py — Lane A windowed packet and byte metrics query.

Computes tumbling window aggregations (packets/sec, bytes/sec) with event-time watermarking
and upserts rows into SQLite serving store.
Reference: TECH_RULES §3.5, PRD §FR-STR
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common.serving_db import connect, upsert

logger = logging.getLogger("WindowMetricsQuery")


def build_window_metrics(df: DataFrame, window_len_s: int = 10, watermark_s: int = 30) -> DataFrame:
    """
    Constructs the windowed metrics aggregation DataFrame from cleaned packet stream.

    Args:
        df: Cleaned packet DataFrame with event_time and packet_length.
        window_len_s: Window length in seconds (default 10).
        watermark_s: Watermark delay threshold in seconds (default 30).

    Returns:
        Aggregated DataFrame with window_start, window_len_s, packets, bytes, pps, and bps.
    """
    return (
        df.withWatermark("event_time", f"{watermark_s} seconds")
        .groupBy(F.window("event_time", f"{window_len_s} seconds"))
        .agg(
            F.count("*").alias("packets"),
            F.coalesce(F.sum("packet_length"), F.lit(0)).alias("bytes"),
        )
        .select(
            F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.000'Z'").alias("window_start"),
            F.lit(window_len_s).alias("window_len_s"),
            F.col("packets").cast("int").alias("packets"),
            F.col("bytes").cast("int").alias("bytes"),
            (F.col("packets") / F.lit(float(window_len_s))).alias("pps"),
            (F.col("bytes") / F.lit(float(window_len_s))).alias("bps"),
        )
    )


def start(
    spark: SparkSession,
    stream_df: DataFrame,
    cfg: dict[str, Any],
    window_len_s: int = 10,
) -> StreamingQuery:
    """
    Starts the streaming window metrics query.

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
    checkpoint_dir = f"{checkpoint_root}/q_window_metrics_{window_len_s}"

    metrics_df = build_window_metrics(stream_df, window_len_s=window_len_s, watermark_s=watermark_s)

    def write_to_sqlite(batch_df: DataFrame, batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.collect()]
        if not rows:
            return
        conn = connect(db_path)
        try:
            upsert(conn, "window_metrics", ["window_start", "window_len_s"], rows)
            logger.debug(
                "Upserted %d window_metrics rows for window_len %ds (batch %d)",
                len(rows),
                window_len_s,
                batch_id,
            )
        finally:
            conn.close()

    query = (
        metrics_df.writeStream.outputMode("update")
        .foreachBatch(write_to_sqlite)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    logger.info("Started window_metrics streaming query (window=%ds, query_id=%s)", window_len_s, query.id)
    return query
