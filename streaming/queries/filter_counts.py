"""
streaming/queries/filter_counts.py — Lane A windowed filter counts query.

Computes tumbling window aggregations for named filter streams (packets, bytes)
using conditional aggregation, exploding results into (window_start, window_len_s, filter_name, packets, bytes),
and upserting rows into SQLite serving store table `filter_counts`.
Reference: TECH_RULES §3.5, §5.2, todo.md T4-006
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

from common.serving_db import connect, upsert
from streaming.analytics.filters import FilterSpec, parse_filters

logger = logging.getLogger("FilterCountsQuery")


def build_filter_counts(
    df: DataFrame,
    filter_specs: list[FilterSpec] | None = None,
    window_len_s: int = 10,
    watermark_s: int = 30,
) -> DataFrame:
    """
    Constructs the windowed filter counts aggregation DataFrame from cleaned packet stream.

    Args:
        df: Cleaned packet DataFrame with event_time and packet columns.
        filter_specs: List of FilterSpec instances. If None, parses defaults.
        window_len_s: Window length in seconds (default 10).
        watermark_s: Watermark delay threshold in seconds (default 30).

    Returns:
        Aggregated DataFrame with columns:
        (window_start, window_len_s, filter_name, packets, bytes).
    """
    from pyspark.sql import functions as F

    specs = filter_specs or parse_filters()
    if not specs:
        raise ValueError("At least one FilterSpec is required to build filter counts")

    # Build conditional aggregation expressions for all filter specs
    agg_exprs = []
    for f in specs:
        cond = F.expr(f.expr)
        agg_exprs.append(F.sum(F.when(cond, 1).otherwise(0)).cast("int").alias(f"_pkts__{f.name}"))
        agg_exprs.append(
            F.coalesce(F.sum(F.when(cond, F.col("packet_length"))), F.lit(0))
            .cast("int")
            .alias(f"_bytes__{f.name}")
        )

    grouped = (
        df.withWatermark("event_time", f"{watermark_s} seconds")
        .groupBy(F.window("event_time", f"{window_len_s} seconds"))
        .agg(*agg_exprs)
    )

    # Pack into array of structs to unpivot / explode into normalized rows
    struct_array = F.array(
        [
            F.struct(
                F.lit(f.name).alias("filter_name"),
                F.col(f"_pkts__{f.name}").alias("packets"),
                F.col(f"_bytes__{f.name}").alias("bytes"),
            )
            for f in specs
        ]
    )

    return grouped.withColumn("_f_struct", F.explode(struct_array)).select(
        F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.000'Z'").alias("window_start"),
        F.lit(window_len_s).alias("window_len_s"),
        F.col("_f_struct.filter_name").alias("filter_name"),
        F.col("_f_struct.packets").cast("int").alias("packets"),
        F.col("_f_struct.bytes").cast("int").alias("bytes"),
    )


def start_filter_counts(
    spark: SparkSession,
    stream_df: DataFrame,
    cfg: dict[str, Any],
    window_len_s: int = 10,
    filter_specs: list[FilterSpec] | None = None,
) -> StreamingQuery:
    """
    Starts the streaming windowed filter counts query.

    Args:
        spark: Active SparkSession.
        stream_df: Valid cleaned streaming DataFrame.
        cfg: System configuration dictionary.
        window_len_s: Window length in seconds (default 10).
        filter_specs: Optional pre-parsed FilterSpec list.

    Returns:
        Active StreamingQuery instance.
    """
    spark_cfg = cfg.get("spark", {})
    watermark_s = spark_cfg.get("watermark_s", 30)
    trigger_s = spark_cfg.get("trigger_s", 5)
    db_path = cfg.get("serving", {}).get("db_path", "serving/analytics.db")
    checkpoint_root = spark_cfg.get("checkpoint_root", "data/checkpoints")
    checkpoint_dir = f"{checkpoint_root}/q_filter_counts_{window_len_s}"

    specs = filter_specs or parse_filters(cfg)

    filter_counts_df = build_filter_counts(
        stream_df,
        filter_specs=specs,
        window_len_s=window_len_s,
        watermark_s=watermark_s,
    )

    def write_to_sqlite(batch_df: DataFrame, batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.collect()]
        if not rows:
            return
        conn = connect(db_path)
        try:
            upsert(
                conn,
                "filter_counts",
                ["window_start", "window_len_s", "filter_name"],
                rows,
            )
            logger.debug(
                "Upserted %d filter_counts rows for window_len %ds (batch %d)",
                len(rows),
                window_len_s,
                batch_id,
            )
        finally:
            conn.close()

    query = (
        filter_counts_df.writeStream.outputMode("update")
        .foreachBatch(write_to_sqlite)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_s} seconds")
        .start()
    )

    logger.info(
        "Started filter_counts streaming query (window=%ds, filters=%d, query_id=%s)",
        window_len_s,
        len(specs),
        query.id,
    )
    return query
