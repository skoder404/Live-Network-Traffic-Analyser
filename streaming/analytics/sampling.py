"""
streaming/analytics/sampling.py — Stream sampling algorithms and comparison analytic.

Implements:
  1. ReservoirSampler (Algorithm R) with uniform guarantees and state snapshot/restore.
  2. bernoulli_sample for independent coin-flip sampling.
  3. SamplingAnalytic (Lane B plugin) that computes sample mean packet length vs full-batch mean,
     error percentages, and upserts comparisons to table `sampling_compare`.
Reference: TECH_RULES §3.5, §5.2, todo.md T4-007
"""

from __future__ import annotations

import logging
import random
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from common.serving_db import connect, current_utc_iso, upsert
from streaming.analytics.base import Analytic, BatchContext

logger = logging.getLogger("SamplingAnalytic")


class ReservoirSampler:
    """
    Reservoir sampling implementation using Vitter's Algorithm R.
    Maintains a uniform random sample of size at most k over an unbounded stream.
    """

    def __init__(self, k: int = 1000, seed: int | None = None) -> None:
        if k <= 0:
            raise ValueError(f"Reservoir size k must be positive, got {k}")
        self.k: int = int(k)
        self.seed = seed
        self._rng = random.Random(seed)
        self.sample_list: list[Any] = []
        self.n_seen: int = 0

    def add(self, item: Any) -> bool:
        """
        Observes an item from the stream and conditionally includes it in the reservoir.

        Returns:
            True if the item was placed into the reservoir sample, False otherwise.
        """
        if self.n_seen < self.k:
            self.sample_list.append(item)
            self.n_seen += 1
            return True

        j = self._rng.randint(0, self.n_seen)
        self.n_seen += 1
        if j < self.k:
            self.sample_list[j] = item
            return True
        return False

    def sample(self) -> list[Any]:
        """Returns a shallow copy of current sample items in the reservoir."""
        return list(self.sample_list)

    def reset(self) -> None:
        """Resets the sampler state and random generator."""
        self.sample_list.clear()
        self.n_seen = 0
        self._rng = random.Random(self.seed)

    def snapshot(self) -> dict[str, Any]:
        """Returns serializable snapshot of the current reservoir state."""
        return {
            "k": self.k,
            "n_seen": self.n_seen,
            "sample": list(self.sample_list),
        }

    def restore(self, state: dict[str, Any]) -> None:
        """Restores state from a snapshot dictionary."""
        self.k = int(state["k"])
        self.n_seen = int(state["n_seen"])
        self.sample_list = list(state["sample"])


def bernoulli_sample(iterable: Iterable[Any], p: float, seed: int | None = None) -> list[Any]:
    """
    Performs Bernoulli sampling on an iterable with inclusion probability p in [0.0, 1.0].
    """
    if not (0.0 <= p <= 1.0):
        raise ValueError(f"Bernoulli probability p must be in [0.0, 1.0], got {p}")

    rng = random.Random(seed)
    return [item for item in iterable if rng.random() < p]


class SamplingAnalytic(Analytic):
    """
    Lane B micro-batch analytic plugin evaluating stream sampling fidelity.
    Compares reservoir sample mean packet length and Bernoulli sample mean
    against the full micro-batch ground truth and records error metrics into `sampling_compare`.
    """

    name = "sampling"

    def __init__(
        self,
        k: int = 1000,
        p: float = 0.1,
        seed: int = 42,
    ) -> None:
        self.k = k
        self.p = p
        self.seed = seed
        self.reservoir = ReservoirSampler(k=k, seed=seed)

    def process_batch(self, batch_df: DataFrame, batch_id: int, ctx: BatchContext) -> None:
        """Processes micro-batch, collects packet lengths, and updates sampling statistics."""
        spark_cfg = ctx.cfg.get("spark", {})
        max_rows = spark_cfg.get("max_rows_per_batch", 50000)
        target_k = spark_cfg.get("sampling", {}).get("k", self.k)

        if target_k != self.reservoir.k:
            self.reservoir = ReservoirSampler(k=target_k, seed=self.seed)
            self.k = target_k

        # Project only packet_length to protect driver memory
        lengths_df = batch_df.select("packet_length").filter("packet_length IS NOT NULL")
        rows = lengths_df.collect()
        row_count = len(rows)

        if row_count == 0:
            return

        if row_count > max_rows:
            logger.warning(
                "Batch %d packet length rows (%d) exceeded max_rows_per_batch (%d). Capping.",
                batch_id,
                row_count,
                max_rows,
            )
            rows = rows[:max_rows]

        packet_lengths = [int(r["packet_length"]) for r in rows]
        full_mean_len = sum(packet_lengths) / len(packet_lengths)

        # 1. Update Reservoir Sampler
        for length in packet_lengths:
            self.reservoir.add(length)

        res_sample = self.reservoir.sample()
        res_sample_n = len(res_sample)
        res_mean_len = (sum(res_sample) / res_sample_n) if res_sample_n > 0 else 0.0
        res_err_pct = (
            abs(res_mean_len - full_mean_len) / full_mean_len * 100.0 if full_mean_len > 0 else 0.0
        )

        # 2. Compute Bernoulli Sample on current batch
        bern_sample = bernoulli_sample(packet_lengths, p=self.p, seed=self.seed + batch_id)
        bern_sample_n = len(bern_sample)
        bern_mean_len = (sum(bern_sample) / bern_sample_n) if bern_sample_n > 0 else 0.0
        bern_err_pct = (
            abs(bern_mean_len - full_mean_len) / full_mean_len * 100.0 if full_mean_len > 0 else 0.0
        )

        ts_iso = ctx.batch_time or current_utc_iso()

        compare_rows = [
            {
                "ts": ts_iso,
                "method": "reservoir",
                "k": int(self.reservoir.k),
                "sample_n": int(res_sample_n),
                "sample_mean_len": round(float(res_mean_len), 2),
                "full_mean_len": round(float(full_mean_len), 2),
                "err_pct": round(float(res_err_pct), 2),
            },
            {
                "ts": ts_iso,
                "method": "bernoulli",
                "k": int(len(packet_lengths) * self.p),
                "sample_n": int(bern_sample_n),
                "sample_mean_len": round(float(bern_mean_len), 2),
                "full_mean_len": round(float(full_mean_len), 2),
                "err_pct": round(float(bern_err_pct), 2),
            },
        ]

        # Upsert into serving store table sampling_compare
        conn = ctx.conn
        if conn is not None:
            upsert(conn, "sampling_compare", ["ts", "method"], compare_rows)
        else:
            conn = connect(ctx.db_path)
            try:
                upsert(conn, "sampling_compare", ["ts", "method"], compare_rows)
            finally:
                conn.close()

        logger.debug(
            "Batch %d sampling comparison: full_mean=%.1f, res_mean=%.1f (err=%.1f%%), bern_mean=%.1f (err=%.1f%%)",
            batch_id,
            full_mean_len,
            res_mean_len,
            res_err_pct,
            bern_mean_len,
            bern_err_pct,
        )
