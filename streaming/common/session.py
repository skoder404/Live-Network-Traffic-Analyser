"""
streaming/common/session.py — SparkSession factory for LNTA.

Provides a unified SparkSession builder enforcing UTC timezone, minimal shuffle partitions,
and streaming options per TECH_RULES §3.5.
"""

from typing import Any

from pyspark.sql import SparkSession


def get_spark(
    app_name: str = "LNTA-Streaming",
    hive: bool = False,
    config: dict[str, Any] | None = None,
) -> SparkSession:
    """
    Creates or retrieves a configured SparkSession.

    Args:
        app_name: Name of the Spark application.
        hive: If True, enables Hive metastore support.
        config: Optional configuration dictionary.

    Returns:
        Configured SparkSession instance.
    """
    cfg = config or {}
    spark_cfg = cfg.get("spark", {})

    master = spark_cfg.get("master", "local[2]")
    shuffle_partitions = str(spark_cfg.get("shuffle_partitions", 4))
    ui_port = str(spark_cfg.get("ui_port", 4040))

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.ui.port", ui_port)
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if hive:
        builder = builder.enableHiveSupport()

    return builder.getOrCreate()
