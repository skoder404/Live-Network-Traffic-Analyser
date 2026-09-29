#!/usr/bin/env python3
"""Compact one closed raw-traffic hour into an idempotent Parquet partition."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.record_schema import to_spark_schema  # noqa: E402


def select_hour(
    date_value: str | None = None,
    hour_value: str | None = None,
    now: datetime | None = None,
) -> tuple[str, str]:
    """Return a target UTC date/hour from explicit values or the last closed hour."""
    if (date_value is None) != (hour_value is None):
        raise ValueError("--dt and --hr must be provided together")
    if date_value is not None and hour_value is not None:
        datetime.strptime(f"{date_value} {hour_value}", "%Y-%m-%d %H")
        return date_value, f"{int(hour_value):02d}"
    reference = now or datetime.now(timezone.utc)
    target = reference.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    return target.strftime("%Y-%m-%d"), target.strftime("%H")


def compact_hour(
    raw_root: str,
    historical_root: str,
    date_value: str,
    hour_value: str,
    master: str = "local[2]",
    partitions: int = 1,
) -> tuple[int, int]:
    """Read a closed CSV hour and overwrite only its matching Parquet partition."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    input_path = f"{raw_root.rstrip('/')}/dt={date_value}/hr={hour_value}"
    output_path = f"{historical_root.rstrip('/')}/dt={date_value}/hr={hour_value}"
    spark = (
        SparkSession.builder.appName("LNTA-Raw-Compaction")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    try:
        rows = (
            spark.read.schema(to_spark_schema())
            .option("header", "false")
            .csv(input_path)
            .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss.SSS"))
            .withColumn("dt", F.lit(date_value))
            .withColumn("hr", F.lit(hour_value))
        )
        input_count = rows.count()
        if input_count == 0:
            raise ValueError(f"No raw rows found under {input_path}")
        (
            rows.coalesce(max(1, partitions))
            .write.mode("overwrite")
            .partitionBy("dt", "hr")
            .parquet(historical_root)
        )
        output_count = spark.read.parquet(output_path).count()
        if output_count != input_count:
            raise RuntimeError(
                f"Compaction count mismatch for {date_value}/{hour_value}: "
                f"input={input_count}, output={output_count}"
            )
        return input_count, output_count
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", help="UTC date in YYYY-MM-DD format")
    parser.add_argument("--hr", help="UTC hour in 00-23 format")
    parser.add_argument("--raw-root", default="/traffic/raw")
    parser.add_argument("--historical-root", default="/traffic/historical")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--partitions", type=int, default=1)
    args = parser.parse_args()
    date_value, hour_value = select_hour(args.dt, args.hr)
    before, after = compact_hour(
        args.raw_root,
        args.historical_root,
        date_value,
        hour_value,
        args.master,
        args.partitions,
    )
    print(f"PASS: compacted {date_value}/{hour_value}; rows before={before}, after={after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
