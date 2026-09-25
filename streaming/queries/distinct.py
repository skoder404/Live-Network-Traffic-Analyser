"""
streaming/queries/distinct.py — Lane A windowed distinct-count streaming query.

Computes tumbling window aggregations producing:
  - src_ips_exact, dst_ips_exact, ports_exact: exact cardinality via size(collect_set(...))
  - src_ips_hll, dst_ips_hll, ports_hll: approximate cardinality via approx_count_distinct(..., 0.05)
Upserts into SQLite ``distinct_counts`` table.
Reference: TECH_RULES §3.5, §5.2, todo.md T4-008
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

from common.serving_db import connect, upsert

logger = logging.getLogger("DistinctQuery")


def build_distinct_counts(
    df: DataFrame, window_len_s: int = 10, watermark_s: int = 30
) -> DataFrame:
    """
    Constructs windowed exact and HLL distinct-count aggregation from cleaned packet stream.

    Args:
        df: Cleaned packet DataFrame with event_time, src_ip, dst_ip, dst_port.
        window_len_s: Window length in seconds (default 10).
        watermark_s: Watermark delay threshold in seconds (default 30).

    Returns:
        Aggregated DataFrame with columns:
        (window_start, window_len_s, src_ips_exact, dst_ips_exact, ports_exact,
         src_ips_hll, dst_ips_hll, ports_hll).
    """
    from pyspark.sql import functions as F

    return (
        df.withWatermark("event_time", f"{watermark_s} seconds")
        .groupBy(F.window("event_time", f"{window_len_s} seconds"))
        .agg(
            # Exact counts via collect_set -> size
            F.size(F.collect_set("src_ip")).alias("src_ips_exact"),
            F.size(F.collect_set("dst_ip")).alias("dst_ips_exact"),
            F.size(F.collect_set("dst_port")).alias("ports_exact"),
            # HLL approximate counts with 5% relative standard deviation
            F.approx_count_distinct("src_ip", rsd=0.05).alias("src_ips_hll"),
            F.approx_count_distinct("dst_ip", rsd=0.05).alias("dst_ips_hll"),
            F.approx_count_distinct("dst_port", rsd=0.05).alias("ports_hll"),
        )
        .select(
            F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.000'Z'").alias(
                "window_start"
            ),
            F.lit(window_len_s).alias("window_len_s"),
            F.col("src_ips_exact").cast("int"),
            F.col("dst_ips_exact").cast("int"),
            F.col("ports_exact").cast("int"),
            F.col("src_ips_hll").cast("int"),
            F.col("dst_ips_hll").cast("int"),
            F.col("ports_hll").cast("int"),
        )
    )


def start_distinct_counts(
    spark: SparkSession,
    stream_df: DataFrame,
    cfg: dict[str, Any],
    window_len_s: int = 10,
) -> StreamingQuery:
    """
    Starts the streaming windowed distinct counts query (exact + HLL).

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
    checkpoint_dir = f"{checkpoint_root}/q_distinct_counts_{window_len_s}"

    distinct_df = build_distinct_counts(
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
                "distinct_counts",
                ["window_start", "window_len_s"],
                rows,
            )
            logger.debug(
                "Upserted %d distinct_counts rows for window_len %ds (batch %d)",
                len(rows),
                window_len_s,
                batch_id,
            )
        finally:
            conn.close()

    query = (
        distinct_df.writeStream.outputMode("update")
        .foreachBatch(write_to_sqlite)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    logger.info(
        "Started distinct_counts streaming query (window=%ds, query_id=%s)",
        window_len_s,
        query.id,
    )
    return query
