"""
streaming/analytics/dgim.py — Datar-Gionis-Indyk-Motwani (DGIM) sliding-window estimator and CountingOnesAnalytic.

Implements:
  1. DGIM: Pure-Python sliding-window estimator for counting 1-bits over the last N bits
     using power-of-two buckets with at most 2 buckets per size and theoretical error <= 50%.
  2. PredicateSpec & parse_predicates: Configurable network packet predicates.
  3. CountingOnesAnalytic (Lane B plugin): Evaluates bit streams for configured predicates over
     micro-batches, updates DGIM and exact counters, and upserts comparisons to table `counting_ones`.
Reference: TECH_RULES §3.5, §5.2, todo.md T4-009
"""

from __future__ import annotations

import json
import logging
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from common.serving_db import connect, current_utc_iso, upsert
from streaming.analytics.base import Analytic, BatchContext

logger = logging.getLogger("DGIMAnalytic")


@dataclass
class DGIMBucket:
    """Represents a single bucket in the DGIM algorithm."""

    timestamp: int  # Stream position of the most recent 1-bit in this bucket
    size: int  # Power-of-two size (1, 2, 4, 8, ...)


class DGIM:
    """
    Datar-Gionis-Indyk-Motwani (DGIM) algorithm for estimating the number of 1-bits
    in a sliding window of the last ``window_n`` stream elements.

    Guarantees:
      - Relative error <= 50% for any bit stream.
      - O(log^2(N)) space complexity.

    Args:
        window_n: The size of the sliding window (number of stream bits).
    """

    def __init__(self, window_n: int = 1000) -> None:
        if window_n <= 0:
            raise ValueError(f"window_n must be positive, got {window_n}")
        self.window_n = int(window_n)
        self.current_time: int = 0
        self.buckets: list[DGIMBucket] = []

    def update(self, bit: int) -> None:
        """
        Observes the next bit (0 or 1) from the stream and updates DGIM buckets.

        Args:
            bit: Integer 0 or 1 (non-zero treated as 1).
        """
        self.current_time += 1
        b_val = 1 if bit else 0

        # Expire buckets older than the sliding window
        cutoff = self.current_time - self.window_n
        while self.buckets and self.buckets[-1].timestamp <= cutoff:
            self.buckets.pop()

        if b_val == 1:
            # Insert new bucket of size 1 at the head (newest)
            self.buckets.insert(0, DGIMBucket(timestamp=self.current_time, size=1))
            self._merge_buckets()

    def _merge_buckets(self) -> None:
        """
        Cascades merges so that at most 2 buckets of any given power-of-two size exist.
        When 3 buckets of the same size are detected, the two oldest are merged into
        one bucket of double size with the timestamp of the newer of the two.
        """
        i = 0
        while i < len(self.buckets) - 2:
            b1 = self.buckets[i]
            b2 = self.buckets[i + 1]
            b3 = self.buckets[i + 2]
            if b1.size == b2.size == b3.size:
                # Merge the two oldest of this size (b2 and b3)
                merged = DGIMBucket(timestamp=b2.timestamp, size=b2.size * 2)
                self.buckets.pop(i + 2)
                self.buckets[i + 1] = merged
                # Step back to check if this merge created 3 of the new size
                i = max(0, i - 1)
            else:
                i += 1

    def query(self, k: int | None = None) -> int:
        """
        Estimates the count of 1-bits in the last ``k`` bits (default ``window_n``).

        The estimate is the sum of sizes of all buckets strictly within the query window,
        plus half the size of the oldest overlapping bucket.

        Args:
            k: Query window size <= window_n. Defaults to window_n.

        Returns:
            Estimated number of 1-bits.
        """
        if not self.buckets:
            return 0

        query_len = self.window_n if k is None else min(k, self.window_n)
        if query_len <= 0:
            return 0

        cutoff = self.current_time - query_len
        total = 0

        for i, b in enumerate(self.buckets):
            if b.timestamp <= cutoff:
                break
            # If this is the last bucket or the next bucket is outside the window,
            # this bucket is the oldest one overlapping the window boundary
            if i == len(self.buckets) - 1 or self.buckets[i + 1].timestamp <= cutoff:
                total += b.size // 2
            else:
                total += b.size

        return total

    def snapshot(self) -> dict[str, Any]:
        """Returns JSON-serializable snapshot of the DGIM state."""
        return {
            "window_n": self.window_n,
            "current_time": self.current_time,
            "buckets": [{"timestamp": b.timestamp, "size": b.size} for b in self.buckets],
        }

    def restore(self, state: dict[str, Any]) -> None:
        """Restores state from a snapshot dictionary."""
        self.window_n = int(state["window_n"])
        self.current_time = int(state["current_time"])
        self.buckets = [
            DGIMBucket(timestamp=int(b["timestamp"]), size=int(b["size"]))
            for b in state.get("buckets", [])
        ]


@dataclass
class PredicateSpec:
    """Specification of a boolean predicate evaluated on network traffic records."""

    name: str
    expr: str


def parse_predicates(cfg: dict[str, Any] | None = None) -> list[PredicateSpec]:
    """
    Parses predicate specifications from system configuration or returns defaults.

    Args:
        cfg: Configuration dictionary with optional ``spark.predicates`` list.

    Returns:
        List of PredicateSpec instances.
    """
    if cfg:
        spark_cfg = cfg.get("spark", {})
        raw_list = spark_cfg.get("predicates", [])
        if raw_list:
            specs: list[PredicateSpec] = []
            for item in raw_list:
                name = item.get("name", "unnamed_predicate")
                if "expr" in item:
                    expr = item["expr"]
                elif "protocol" in item:
                    expr = f"protocol = '{item['protocol']}'"
                elif "min_length" in item:
                    expr = f"packet_length >= {item['min_length']}"
                elif "dst_port" in item:
                    expr = f"dst_port = {item['dst_port']}"
                else:
                    expr = "1 = 1"
                specs.append(PredicateSpec(name=name, expr=expr))
            return specs

    # Default predicates
    return [
        PredicateSpec(name="is_tcp", expr="protocol = 'TCP'"),
        PredicateSpec(name="is_udp", expr="protocol = 'UDP'"),
        PredicateSpec(name="is_large_packet", expr="packet_length >= 1000"),
        PredicateSpec(name="is_https", expr="dst_port = 443"),
    ]


class CountingOnesAnalytic(Analytic):
    """
    Lane B micro-batch analytic plugin for DGIM sliding-window counting of 1-bits.

    Evaluates configured boolean predicates over incoming packets (ordered by event_time),
    feeds bit streams to both DGIM and exact sliding-window deques, and records
    accuracy comparisons into SQLite serving table ``counting_ones``.
    """

    name = "counting_ones"

    def __init__(
        self,
        window_n: int = 1000,
        predicates: list[PredicateSpec] | None = None,
        state_dir: Path | str | None = None,
    ) -> None:
        self.window_n = window_n
        self.predicates = predicates or parse_predicates()
        self.state_dir = Path(state_dir) if state_dir else None

        # Per-predicate DGIM estimators and exact deques
        self.dgim_map: dict[str, DGIM] = {
            p.name: DGIM(window_n=self.window_n) for p in self.predicates
        }
        self.exact_map: dict[str, deque[int]] = {
            p.name: deque(maxlen=self.window_n) for p in self.predicates
        }

    def process_batch(self, batch_df: DataFrame, batch_id: int, ctx: BatchContext) -> None:
        """Processes micro-batch: evaluates predicates, feeds bits, upserts to counting_ones."""
        from pyspark.sql import functions as F

        from streaming.common.cleaning import clean

        spark_cfg = ctx.cfg.get("spark", {})
        max_rows = spark_cfg.get("max_rows_per_batch", 50000)
        configured_predicates = (
            parse_predicates(ctx.cfg) if "predicates" in spark_cfg else self.predicates
        )

        # Ensure all configured predicates have active DGIM estimators
        for p in configured_predicates:
            if p.name not in self.dgim_map:
                self.dgim_map[p.name] = DGIM(window_n=self.window_n)
                self.exact_map[p.name] = deque(maxlen=self.window_n)
        self.predicates = configured_predicates

        # Ensure event_time exists (clean raw DataFrame if needed)
        df = batch_df
        if "event_time" not in df.columns and "timestamp" in df.columns:
            df = clean(df)

        if "event_time" not in df.columns:
            logger.warning("Batch %d missing event_time column. Skipping DGIM.", batch_id)
            return

        # Build projection expressions for each predicate
        select_exprs = [F.col("event_time")]
        for p in self.predicates:
            select_exprs.append(F.expr(p.expr).cast("int").alias(f"_pred__{p.name}"))

        projected_df = df.select(*select_exprs).orderBy(F.col("event_time").asc())
        rows = projected_df.collect()
        if not rows:
            return

        if len(rows) > max_rows:
            logger.warning(
                "Batch %d rows (%d) exceeded max_rows_per_batch (%d). Capping.",
                batch_id,
                len(rows),
                max_rows,
            )
            rows = rows[:max_rows]

        # Feed bits into DGIM estimators and exact deques
        for row in rows:
            for p in self.predicates:
                col_name = f"_pred__{p.name}"
                bit_val = 1 if row[col_name] else 0
                self.dgim_map[p.name].update(bit_val)
                self.exact_map[p.name].append(bit_val)

        # Compute DGIM estimates vs exact counts
        ts_iso = ctx.batch_time or current_utc_iso()
        upsert_rows: list[dict[str, Any]] = []

        for p in self.predicates:
            exact_ones = sum(self.exact_map[p.name])
            dgim_est = self.dgim_map[p.name].query()
            err_pct = abs(dgim_est - exact_ones) / exact_ones * 100.0 if exact_ones > 0 else 0.0

            upsert_rows.append(
                {
                    "ts": ts_iso,
                    "predicate_name": p.name,
                    "window_n": self.window_n,
                    "exact_ones": int(exact_ones),
                    "dgim_estimate": int(dgim_est),
                    "err_pct": round(float(err_pct), 2),
                }
            )

        # Upsert rows into SQLite serving store table counting_ones
        conn = ctx.conn
        if conn is not None:
            upsert(conn, "counting_ones", ["ts", "predicate_name"], upsert_rows)
        else:
            conn = connect(ctx.db_path)
            try:
                upsert(conn, "counting_ones", ["ts", "predicate_name"], upsert_rows)
            finally:
                conn.close()

        # Save snapshot if state_dir is configured
        if self.state_dir:
            self._save_state_to_disk()

        logger.debug(
            "Batch %d DGIM counting_ones updated %d predicates (ts=%s)",
            batch_id,
            len(upsert_rows),
            ts_iso,
        )

    def _save_state_to_disk(self) -> None:
        """Persists JSON snapshot to state_dir."""
        if not self.state_dir:
            return
        try:
            self.state_dir.mkdir(parents=True, exist_ok=True)
            state_file = self.state_dir / "dgim_state.json"
            state_file.write_text(json.dumps(self.snapshot()), encoding="utf-8")
        except Exception:
            logger.exception("Failed to write DGIM state to disk")

    def snapshot(self) -> dict[str, Any]:
        """Returns JSON-serializable snapshot of all predicate estimators."""
        return {
            "window_n": self.window_n,
            "predicates": [
                {
                    "name": p.name,
                    "expr": p.expr,
                    "dgim": self.dgim_map[p.name].snapshot(),
                    "exact": list(self.exact_map[p.name]),
                }
                for p in self.predicates
                if p.name in self.dgim_map
            ],
        }

    def restore(self, state: dict[str, Any]) -> None:
        """Restores all predicate estimators from a snapshot dictionary."""
        self.window_n = int(state["window_n"])
        self.predicates.clear()
        self.dgim_map.clear()
        self.exact_map.clear()

        for p_state in state.get("predicates", []):
            name = p_state["name"]
            expr = p_state["expr"]
            p_spec = PredicateSpec(name=name, expr=expr)
            self.predicates.append(p_spec)

            dgim = DGIM(window_n=self.window_n)
            dgim.restore(p_state["dgim"])
            self.dgim_map[name] = dgim

            self.exact_map[name] = deque(p_state.get("exact", []), maxlen=self.window_n)
