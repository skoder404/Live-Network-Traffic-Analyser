"""
streaming/stream_app.py — Main Spark Structured Streaming Application Runner.

Orchestrates Lane A native streaming queries and Lane B micro-batch algorithm dispatcher,
monitors pipeline health, and handles graceful shutdowns.
Reference: TECH_RULES §1.1, §3.5, PRD §FR-STR
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common.config import load_config
from common.logging_setup import setup_logging
from common.serving_db import connect, current_utc_iso, init_schema, upsert
from streaming.analytics.base import BatchContext
from streaming.analytics.decay import DecayAnalytic
from streaming.analytics.dgim import CountingOnesAnalytic
from streaming.analytics.fm import FMAnalytic
from streaming.analytics.moments import MomentsAnalytic
from streaming.analytics.sampling import SamplingAnalytic
from streaming.common.cleaning import clean, split_valid
from streaming.common.schema import read_stream
from streaming.common.session import get_spark
from streaming.queries import counts, distinct, filter_counts, window_metrics
from streaming.registry import get_enabled, get_registered, register

logger = logging.getLogger("StreamApp")


class StreamingApplication:
    """Manages lifecycle of all streaming queries, plugin dispatching, and health telemetry."""

    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.spark_cfg = cfg.get("spark", {})
        self.serving_cfg = cfg.get("serving", {})
        self.db_path = self.serving_cfg.get("db_path", "serving/analytics.db")
        self.queries: list[StreamingQuery] = []
        self._running = True
        self._consecutive_slow_batches = 0

    def init_storage(self) -> None:
        """Ensures the SQLite serving store schema is initialized and default plugins registered."""
        conn = connect(self.db_path)
        try:
            init_schema(conn)
            logger.info("Serving schema initialized at %s", self.db_path)
        finally:
            conn.close()

        # Register default Lane B plugins if not already registered
        if get_registered("decay") is None:
            register(DecayAnalytic())
        if get_registered("sampling") is None:
            register(SamplingAnalytic())
        if get_registered("moments") is None:
            register(MomentsAnalytic())
        if get_registered("fm") is None:
            register(FMAnalytic())
        if get_registered("counting_ones") is None:
            register(CountingOnesAnalytic())

    def handle_shutdown(self, signum: int, _frame: Any) -> None:
        """Gracefully stops all running streaming queries on signal."""
        sig_name = signal.Signals(signum).name
        logger.info("Received %s — initiating graceful streaming query shutdown...", sig_name)
        self._running = False
        for q in self.queries:
            if q.isActive:
                logger.info("Stopping streaming query '%s' (%s)...", q.name or "unnamed", q.id)
                q.stop()

    def create_dispatcher(self) -> Any:
        """Builds the foreachBatch dispatcher for Lane B algorithm plugins and health metrics."""
        cfg = self.cfg
        db_path = self.db_path
        trigger_s = float(self.spark_cfg.get("trigger_s", 5))

        def dispatch_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
            batch_start_wall = time.time()
            ts = current_utc_iso()

            batch_df.persist()
            input_rows = batch_df.count()

            # Estimate lag (now - max(event_time))
            lag_s = 0.0
            if input_rows > 0 and "event_time" in batch_df.columns:
                max_row = batch_df.select(F.max("event_time").alias("max_et")).first()
                if max_row and max_row["max_et"]:
                    max_dt: datetime = max_row["max_et"]
                    if max_dt.tzinfo is None:
                        max_dt = max_dt.replace(tzinfo=timezone.utc)
                    lag_s = max(0.0, (datetime.now(timezone.utc) - max_dt).total_seconds())

            # Dispatch to Lane B plugins
            active_plugins = get_enabled(cfg)
            plugin_errors = 0
            ctx = BatchContext(
                cfg=cfg,
                db_path=db_path,
                clock=batch_start_wall,
                logger=logger,
                batch_time=ts,
            )

            for plugin in active_plugins:
                try:
                    plugin.process_batch(batch_df, batch_id, ctx)
                except Exception:
                    plugin_errors += 1
                    logger.exception(
                        "Error executing Lane B plugin '%s' in batch %d",
                        plugin.name,
                        batch_id,
                    )

            batch_df.unpersist()

            duration_s = time.time() - batch_start_wall

            # Warn on slow micro-batches (processing duration exceeding trigger)
            if duration_s > trigger_s:
                self._consecutive_slow_batches += 1
                if self._consecutive_slow_batches >= 3:
                    logger.warning(
                        "Batch %d duration (%.2fs) exceeded trigger (%0.2fs) for %d consecutive batches!",
                        batch_id,
                        duration_s,
                        trigger_s,
                        self._consecutive_slow_batches,
                    )
            else:
                self._consecutive_slow_batches = 0

            # Write pipeline health metrics
            health_rows = [
                {
                    "ts": ts,
                    "component": "spark_runner",
                    "metric": "batch_duration_s",
                    "value": float(duration_s),
                },
                {
                    "ts": ts,
                    "component": "spark_runner",
                    "metric": "input_rows",
                    "value": float(input_rows),
                },
                {
                    "ts": ts,
                    "component": "spark_runner",
                    "metric": "lag_s",
                    "value": float(lag_s),
                },
                {
                    "ts": ts,
                    "component": "spark_runner",
                    "metric": "plugin_error",
                    "value": float(plugin_errors),
                },
            ]

            conn = connect(db_path)
            try:
                upsert(conn, "pipeline_health", ["ts", "component", "metric"], health_rows)
            finally:
                conn.close()

            logger.info(
                "Batch %d processed: %d rows, duration=%.2fs, lag=%.2fs, errors=%d",
                batch_id,
                input_rows,
                duration_s,
                lag_s,
                plugin_errors,
            )

        return dispatch_micro_batch

    def run(self, input_path: str | None = None) -> None:
        """Starts all streaming queries and blocks until termination."""
        self.init_storage()

        # Signal hooks
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        spark: SparkSession = get_spark(app_name="LNTA-StreamingEngine", config=self.cfg)
        logger.info(
            "Spark session initialized: version=%s, tz=%s",
            spark.version,
            spark.conf.get("spark.sql.session.timeZone"),
        )

        # Build raw and cleaned stream
        raw_stream = read_stream(spark, input_path=input_path, config=self.cfg)
        cleaned_stream = clean(raw_stream)
        valid_stream, _ = split_valid(cleaned_stream)

        # 1. Start Lane A queries
        # Tumbling window metrics (10s window)
        q_window = window_metrics.start(spark, valid_stream, self.cfg, window_len_s=10)
        self.queries.append(q_window)

        # Protocol counts (10s window)
        q_proto = counts.start_protocol_counts(spark, valid_stream, self.cfg, window_len_s=10)
        self.queries.append(q_proto)

        # Port counts (10s window)
        q_ports = counts.start_port_counts(spark, valid_stream, self.cfg, window_len_s=10)
        self.queries.append(q_ports)

        # Filter counts (10s window)
        q_filters = filter_counts.start_filter_counts(
            spark, valid_stream, self.cfg, window_len_s=10
        )
        self.queries.append(q_filters)

        # Distinct counts (10s window)
        q_distinct = distinct.start_distinct_counts(spark, valid_stream, self.cfg, window_len_s=10)
        self.queries.append(q_distinct)

        # 2. Start Lane B Dispatcher query
        checkpoint_root = self.spark_cfg.get("checkpoint_root", "data/checkpoints")
        trigger_s = self.spark_cfg.get("trigger_s", 5)

        dispatcher = self.create_dispatcher()
        q_lane_b = (
            valid_stream.writeStream.foreachBatch(dispatcher)
            .option("checkpointLocation", f"{checkpoint_root}/q_batch_plugins")
            .trigger(processingTime=f"{trigger_s} seconds")
            .start()
        )
        self.queries.append(q_lane_b)

        logger.info("All streaming queries started successfully. Awaiting termination...")

        for q in self.queries:
            q.awaitTermination()


def main() -> None:
    """CLI entry point for stream_app."""
    parser = argparse.ArgumentParser(description="LNTA Spark Structured Streaming Application")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to settings.yaml")
    parser.add_argument("--input", default=None, help="Input stream directory or HDFS URI")
    args = parser.parse_args()

    cfg_path = Path(args.config)
    if not cfg_path.exists():
        cfg_path = Path("config/settings.example.yaml")

    cfg = load_config(cfg_path)
    setup_logging(cfg)

    app = StreamingApplication(cfg)
    try:
        app.run(input_path=args.input)
    except Exception as exc:
        logger.critical("Fatal error in streaming application: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
