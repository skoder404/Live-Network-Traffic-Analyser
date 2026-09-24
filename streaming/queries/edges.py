"""
streaming/queries/edges.py — Lane A IP edge and per-source aggregation streaming queries.

Feeds the Link Analysis layer and Alert Engine with:
  1. ip_edges: (window_start, window_len_s, src_ip, dst_ip, packets, bytes)
  2. source_stats: (window_start, window_len_s, src_ip, packets, bytes, unique_dst_ips, unique_dst_ports)
Reference: TECH_RULES §3.5, §5.2, todo.md T5-005
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

from common.serving_db import connect, upsert

logger = logging.getLogger("EdgesQuery")


def build_ip_edges(df: DataFrame, window_len_s: int = 10, watermark_s: int = 30) -> DataFrame:
    """
    Aggregates directed traffic between (src_ip, dst_ip) pairs per event-time window.
    """
    from pyspark.sql import functions as F

    return (
        df.withWatermark("event_time", f"{watermark_s} seconds")
        .groupBy(F.window("event_time", f"{window_len_s} seconds"), "src_ip", "dst_ip")
        .agg(
            F.count("*").alias("packets"),
            F.coalesce(F.sum("packet_length"), F.lit(0)).alias("bytes"),
        )
        .select(
            F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.000'Z'").alias(
                "window_start"
            ),
            F.lit(window_len_s).alias("window_len_s"),
            F.col("src_ip"),
            F.col("dst_ip"),
            F.col("packets").cast("int").alias("packets"),
            F.col("bytes").cast("int").alias("bytes"),
        )
    )


def build_source_stats(df: DataFrame, window_len_s: int = 10, watermark_s: int = 30) -> DataFrame:
    """
    Aggregates per-source activity: total packets, bytes, and unique destinations/ports.
    """
    from pyspark.sql import functions as F

    return (
        df.withWatermark("event_time", f"{watermark_s} seconds")
        .groupBy(F.window("event_time", f"{window_len_s} seconds"), "src_ip")
        .agg(
            F.count("*").alias("packets"),
            F.coalesce(F.sum("packet_length"), F.lit(0)).alias("bytes"),
            F.size(F.collect_set("dst_ip")).alias("unique_dst_ips"),
            F.size(F.collect_set(F.when(F.col("dst_port").isNotNull(), F.col("dst_port")))).alias(
                "unique_dst_ports"
            ),
        )
        .select(
            F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.000'Z'").alias(
                "window_start"
            ),
            F.lit(window_len_s).alias("window_len_s"),
            F.col("src_ip"),
            F.col("packets").cast("int").alias("packets"),
            F.col("bytes").cast("int").alias("bytes"),
            F.col("unique_dst_ips").cast("int").alias("unique_dst_ips"),
            F.col("unique_dst_ports").cast("int").alias("unique_dst_ports"),
        )
    )


def start_edges(
    spark: SparkSession,
    stream_df: DataFrame,
    cfg: dict[str, Any],
    window_len_s: int = 10,
) -> StreamingQuery:
    """
    Starts the streaming IP edges aggregation query.
    """
    spark_cfg = cfg.get("spark", {})
    watermark_s = spark_cfg.get("watermark_s", 30)
    trigger_s = spark_cfg.get("trigger_s", 5)
    max_edges = int(spark_cfg.get("max_edges_per_window", 500))
    db_path = cfg.get("serving", {}).get("db_path", "serving/analytics.db")
    checkpoint_root = spark_cfg.get("checkpoint_root", "data/checkpoints")
    checkpoint_dir = f"{checkpoint_root}/q_edges_{window_len_s}"

    edges_df = build_ip_edges(stream_df, window_len_s=window_len_s, watermark_s=watermark_s)

    def write_edges(batch_df: DataFrame, batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.collect()]
        if not rows:
            return

        # Prune / sort top max_edges by bytes per window_start
        by_window: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            ws = r["window_start"]
            by_window.setdefault(ws, []).append(r)

        capped_rows: list[dict[str, Any]] = []
        for _ws, w_rows in by_window.items():
            w_rows.sort(key=lambda x: x.get("bytes", 0), reverse=True)
            capped_rows.extend(w_rows[:max_edges])

        conn = connect(db_path)
        try:
            upsert(
                conn,
                "ip_edges",
                ["window_start", "window_len_s", "src_ip", "dst_ip"],
                capped_rows,
            )
            logger.debug(
                "Upserted %d ip_edges rows for window_len %ds (batch %d)",
                len(capped_rows),
                window_len_s,
                batch_id,
            )
        finally:
            conn.close()

    query = (
        edges_df.writeStream.outputMode("update")
        .foreachBatch(write_edges)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    logger.info("Started ip_edges query (window=%ds, id=%s)", window_len_s, query.id)
    return query


def start_source_stats(
    spark: SparkSession,
    stream_df: DataFrame,
    cfg: dict[str, Any],
    window_len_s: int = 10,
) -> StreamingQuery:
    """
    Starts the streaming per-source activity metrics query.
    """
    spark_cfg = cfg.get("spark", {})
    watermark_s = spark_cfg.get("watermark_s", 30)
    trigger_s = spark_cfg.get("trigger_s", 5)
    db_path = cfg.get("serving", {}).get("db_path", "serving/analytics.db")
    checkpoint_root = spark_cfg.get("checkpoint_root", "data/checkpoints")
    checkpoint_dir = f"{checkpoint_root}/q_source_stats_{window_len_s}"

    stats_df = build_source_stats(stream_df, window_len_s=window_len_s, watermark_s=watermark_s)

    def write_stats(batch_df: DataFrame, batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.collect()]
        if not rows:
            return

        conn = connect(db_path)
        try:
            upsert(
                conn,
                "source_stats",
                ["window_start", "window_len_s", "src_ip"],
                rows,
            )
            logger.debug(
                "Upserted %d source_stats rows for window_len %ds (batch %d)",
                len(rows),
                window_len_s,
                batch_id,
            )
        finally:
            conn.close()

    query = (
        stats_df.writeStream.outputMode("update")
        .foreachBatch(write_stats)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    logger.info("Started source_stats query (window=%ds, id=%s)", window_len_s, query.id)
    return query

