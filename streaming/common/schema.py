"""
streaming/common/schema.py — Stream schema and ingestion loader for PySpark.

Reads CSV streams from HDFS or local filesystem adhering to the Data Contract.
"""

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from contracts.record_schema import to_spark_schema


def read_stream(
    spark: SparkSession,
    input_path: str | None = None,
    config: dict[str, Any] | None = None,
) -> DataFrame:
    """
    Constructs a streaming DataFrame from the input CSV stream path.

    Args:
        spark: Active SparkSession.
        input_path: Path to the stream folder (e.g. hdfs:///traffic/stream_in or local dir).
        config: Optional configuration dictionary.

    Returns:
        Streaming DataFrame with contract schema applied.
    """
    cfg = config or {}
    spark_cfg = cfg.get("spark", {})

    path = input_path or spark_cfg.get("stream_in_path", "data/stream_in")
    max_files = spark_cfg.get("max_files_per_trigger", 100)

    schema = to_spark_schema()

    return (
        spark.readStream.format("csv")
        .schema(schema)
        .option("header", "false")
        .option("mode", "PERMISSIVE")
        .option("maxFilesPerTrigger", max_files)
        .load(path)
    )
