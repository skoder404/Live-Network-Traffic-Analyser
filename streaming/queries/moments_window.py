"""
streaming/queries/moments_window.py — Lane A windowed packet length and inter-arrival moments query.

Computes windowed moments (n, mean_len, var_len, std_len, iat_mean_ms, iat_var_ms, iat_std_ms)
with event-time watermarking and upserts rows into SQLite serving store table `moments`.
Reference: TECH_RULES §3.5, §5.2, todo.md T5-001
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

from common.serving_db import connect, upsert

logger = logging.getLogger("MomentsWindowQuery")


def build_moments_window(
    df: DataFrame, window_len_s: int = 10, watermark_s: int = 30
) -> DataFrame:
    """
    Constructs the windowed moments aggregation DataFrame from cleaned packet stream.

    Args:
        df: Cleaned packet DataFrame with event_time, packet_length, and iat_ms.
        window_len_s: Window length in seconds (default 10).
        watermark_s: Watermark delay threshold in seconds (default 30).

    Returns:
        Aggregated DataFrame with window_start, window_len_s, n, mean_len, var_len,
        std_len, iat_mean_ms, iat_var_ms, iat_std_ms.
    """
    from pyspark.sql import functions as F

    return (
        df.withWatermark("event_time", f"{watermark_s} seconds")
        .groupBy(F.window("event_time", f"{window_len_s} seconds"))
        .agg(
            F.count("*").alias("n"),
            F.avg("packet_length").alias("mean_len"),
            F.var_samp("packet_length").alias("var_len"),
            F.stddev_samp("packet_length").alias("std_len"),
            F.avg("iat_ms").alias("iat_mean_ms"),
            F.var_samp("iat_ms").alias("iat_var_ms"),
            F.stddev_samp("iat_ms").alias("iat_std_ms"),
        )
        .select(
            F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.000'Z'").alias(
                "window_start"
            ),
            F.lit(window_len_s).alias("window_len_s"),
            F.col("n").cast("int").alias("n"),
            F.col("mean_len").cast("double").alias("mean_len"),
            F.col("var_len").cast("double").alias("var_len"),
            F.col("std_len").cast("double").alias("std_len"),
            F.col("iat_mean_ms").cast("double").alias("iat_mean_ms"),
            F.col("iat_var_ms").cast("double").alias("iat_var_ms"),
            F.col("iat_std_ms").cast("double").alias("iat_std_ms"),
        )
    )


def start(
    spark: SparkSession,
    stream_df: DataFrame,
    cfg: dict[str, Any],
    window_len_s: int = 10,
) -> StreamingQuery:
    """
    Starts the streaming windowed moments query.

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
    checkpoint_dir = f"{checkpoint_root}/q_moments_{window_len_s}"

    moments_df = build_moments_window(
        stream_df, window_len_s=window_len_s, watermark_s=watermark_s
    )

    def write_to_sqlite(batch_df: DataFrame, batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.collect()]
        if not rows:
            return

        # Sanitize any NaN values to None for SQL compliance
        clean_rows: list[dict[str, Any]] = []
        for r in rows:
            clean_row = {}
            for k, v in r.items():
                if isinstance(v, float) and (v != v):  # math.isnan check
                    clean_row[k] = None
                else:
                    clean_row[k] = v
            clean_rows.append(clean_row)

        conn = connect(db_path)
        try:
            upsert(conn, "moments", ["window_start", "window_len_s"], clean_rows)
            logger.debug(
                "Upserted %d moments rows for window_len %ds (batch %d)",
                len(clean_rows),
                window_len_s,
                batch_id,
            )
        finally:
            conn.close()

    query = (
        moments_df.writeStream.outputMode("update")
        .foreachBatch(write_to_sqlite)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    logger.info(
        "Started moments streaming query (window=%ds, query_id=%s)",
        window_len_s,
        query.id,
    )
    return query
