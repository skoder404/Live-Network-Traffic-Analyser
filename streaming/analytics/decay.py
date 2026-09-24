"""
streaming/analytics/decay.py — Exponentially decaying window analytics.

Maintains running recent-traffic scores and top-k heavy hitters with exponential
time decay, bounded memory footprint, and restart persistence.
References: TECH_RULES §5.2, todo.md T5-003
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from common.serving_db import current_utc_iso, upsert
from streaming.analytics.base import Analytic, BatchContext

logger = logging.getLogger("DecayAnalytic")


class DecayingCounter:
    """
    Maintains a continuous exponential time-decayed score.
    Decay factor: score(t) = score(t0) * 2 ** (-(t - t0) / half_life_s)
    """

    def __init__(self, half_life_s: float) -> None:
        if half_life_s <= 0:
            raise ValueError("half_life_s must be positive")
        self.half_life_s = float(half_life_s)
        self.score: float = 0.0
        self.last_t: float | None = None

    def add(self, value: float, t: float) -> None:
        """Applies lazy decay up to timestamp t, then adds value."""
        if self.last_t is None:
            self.score = float(value)
            self.last_t = float(t)
            return

        dt = float(t) - self.last_t
        if dt > 0:
            decay_factor = 2.0 ** (-dt / self.half_life_s)
            self.score = (self.score * decay_factor) + float(value)
            self.last_t = float(t)
        else:
            # Events in same or earlier batch time
            self.score += float(value)

    def value(self, t: float) -> float:
        """Returns the decayed score at timestamp t without updating internal state."""
        if self.last_t is None:
            return 0.0
        dt = float(t) - self.last_t
        if dt <= 0:
            return self.score
        decay_factor = 2.0 ** (-dt / self.half_life_s)
        return self.score * decay_factor

    def snapshot(self) -> dict[str, Any]:
        return {
            "half_life_s": self.half_life_s,
            "score": self.score,
            "last_t": self.last_t,
        }

    def restore(self, data: dict[str, Any]) -> None:
        self.half_life_s = float(data["half_life_s"])
        self.score = float(data["score"])
        self.last_t = float(data["last_t"]) if data.get("last_t") is not None else None


class DecayingKeyTable:
    """
    Maintains decaying counters for a set of categorical keys (e.g. destination IPs/ports).
    Prunes keys below epsilon to guarantee bounded memory.
    """

    def __init__(
        self,
        half_life_s: float,
        epsilon: float = 1e-4,
        max_keys: int = 1000,
    ) -> None:
        self.half_life_s = float(half_life_s)
        self.epsilon = float(epsilon)
        self.max_keys = int(max_keys)
        self.counters: dict[str, DecayingCounter] = {}

    def add(self, key: str, value: float, t: float) -> None:
        if key not in self.counters:
            self.counters[key] = DecayingCounter(self.half_life_s)
        self.counters[key].add(value, t)

        if len(self.counters) > self.max_keys:
            self.prune(t)

    def top_k(self, k: int, t: float) -> list[tuple[str, float]]:
        """Returns the top-k highest scoring keys evaluated at timestamp t."""
        scored = [(key, counter.value(t)) for key, counter in self.counters.items()]
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:k]

    def prune(self, t: float) -> None:
        """Evicts keys whose decayed score falls below epsilon."""
        keys_to_remove = [k for k, c in self.counters.items() if c.value(t) < self.epsilon]
        for k in keys_to_remove:
            del self.counters[k]

    def snapshot(self) -> dict[str, Any]:
        return {
            "half_life_s": self.half_life_s,
            "epsilon": self.epsilon,
            "max_keys": self.max_keys,
            "counters": {k: c.snapshot() for k, c in self.counters.items()},
        }

    def restore(self, data: dict[str, Any]) -> None:
        self.half_life_s = float(data["half_life_s"])
        self.epsilon = float(data.get("epsilon", 1e-4))
        self.max_keys = int(data.get("max_keys", 1000))
        self.counters = {}
        for k, c_data in data.get("counters", {}).items():
            counter = DecayingCounter(self.half_life_s)
            counter.restore(c_data)
            self.counters[k] = counter


class DecayAnalytic(Analytic):
    """
    Lane B Analytic Plugin: Exponentially Decaying Window Analysis.
    Writes to serving tables:
      - decay_traffic: (ts, half_life_s, score, raw_pps)
      - decay_top_keys: (ts, key_type, key, score, rank)
    """

    name = "decay"

    def __init__(self, state_dir: str | Path = "serving/.state/decay") -> None:
        self.state_dir = Path(state_dir)
        self.half_lives: list[int] = [10, 30, 60]
        self.traffic_counters: dict[int, DecayingCounter] = {}
        self.dst_ip_tables: dict[int, DecayingKeyTable] = {}
        self.dst_port_tables: dict[int, DecayingKeyTable] = {}
        self._initialized = False

    def _ensure_initialized(self, cfg: dict[str, Any]) -> None:
        if self._initialized:
            return
        decay_cfg = cfg.get("decay", {})
        self.half_lives = list(decay_cfg.get("half_lives_s", [10, 30, 60]))

        for hl in self.half_lives:
            self.traffic_counters[hl] = DecayingCounter(hl)
            self.dst_ip_tables[hl] = DecayingKeyTable(hl)
            self.dst_port_tables[hl] = DecayingKeyTable(hl)

        self._restore_state()
        self._initialized = True

    def _get_state_file(self) -> Path:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        return self.state_dir / "decay_state.json"

    def _persist_state(self) -> None:
        try:
            state = {
                "traffic_counters": {hl: c.snapshot() for hl, c in self.traffic_counters.items()},
                "dst_ip_tables": {hl: t.snapshot() for hl, t in self.dst_ip_tables.items()},
                "dst_port_tables": {hl: t.snapshot() for hl, t in self.dst_port_tables.items()},
            }
            state_file = self._get_state_file()
            tmp_file = state_file.with_suffix(".tmp")
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(state, f)
            tmp_file.replace(state_file)
        except Exception as e:
            logger.warning("Failed to persist decay state: %s", e)

    def _restore_state(self) -> None:
        state_file = self._get_state_file()
        if not state_file.exists():
            return
        try:
            with open(state_file, encoding="utf-8") as f:
                state = json.load(f)

            for hl, c_data in state.get("traffic_counters", {}).items():
                hl_int = int(hl)
                if hl_int in self.traffic_counters:
                    self.traffic_counters[hl_int].restore(c_data)

            for hl, t_data in state.get("dst_ip_tables", {}).items():
                hl_int = int(hl)
                if hl_int in self.dst_ip_tables:
                    self.dst_ip_tables[hl_int].restore(t_data)

            for hl, t_data in state.get("dst_port_tables", {}).items():
                hl_int = int(hl)
                if hl_int in self.dst_port_tables:
                    self.dst_port_tables[hl_int].restore(t_data)

            logger.info("Restored decay state from %s", state_file)
        except Exception as e:
            logger.warning("Failed to restore decay state from %s: %s", state_file, e)

    def process_batch(self, batch_df: DataFrame, batch_id: int, ctx: BatchContext) -> None:
        from pyspark.sql import functions as F

        self._ensure_initialized(ctx.cfg)

        row_count = batch_df.count()
        if row_count == 0:
            return

        # Estimate batch timestamp
        current_t = float(ctx.clock if isinstance(ctx.clock, (int, float)) else time.time())
        ts_iso = ctx.batch_time or current_utc_iso()

        trigger_s = float(ctx.cfg.get("spark", {}).get("trigger_s", 5))
        raw_pps = round(float(row_count) / max(trigger_s, 1.0), 2)

        # Collect small aggregated heavy hitters from batch DataFrame
        max_rows = int(ctx.cfg.get("spark", {}).get("max_rows_per_batch", 50000))
        capped_df = batch_df.limit(max_rows)

        ip_counts = (
            capped_df.groupBy("dst_ip")
            .agg(F.count("*").alias("count"))
            .orderBy(F.desc("count"))
            .limit(20)
            .collect()
        )

        port_counts = (
            capped_df.filter(F.col("dst_port").isNotNull())
            .groupBy("dst_port")
            .agg(F.count("*").alias("count"))
            .orderBy(F.desc("count"))
            .limit(20)
            .collect()
        )

        # Update decay models
        traffic_rows: list[dict[str, Any]] = []
        top_key_rows: list[dict[str, Any]] = []

        for hl in self.half_lives:
            # 1. Total traffic score
            counter = self.traffic_counters[hl]
            counter.add(row_count, current_t)
            score_val = round(counter.value(current_t), 2)

            traffic_rows.append(
                {
                    "ts": ts_iso,
                    "half_life_s": hl,
                    "score": score_val,
                    "raw_pps": raw_pps,
                }
            )

            # 2. Key tables
            ip_tbl = self.dst_ip_tables[hl]
            for row in ip_counts:
                if row["dst_ip"]:
                    ip_tbl.add(str(row["dst_ip"]), float(row["count"]), current_t)

            port_tbl = self.dst_port_tables[hl]
            for row in port_counts:
                if row["dst_port"] is not None:
                    port_tbl.add(str(row["dst_port"]), float(row["count"]), current_t)

            # Extract top-10 for primary half-life
            for rank, (ip, score) in enumerate(ip_tbl.top_k(10, current_t), start=1):
                top_key_rows.append(
                    {
                        "ts": ts_iso,
                        "key_type": "dst_ip",
                        "key": ip,
                        "score": round(score, 2),
                        "rank": rank,
                    }
                )

            for rank, (port, score) in enumerate(port_tbl.top_k(10, current_t), start=1):
                top_key_rows.append(
                    {
                        "ts": ts_iso,
                        "key_type": "dst_port",
                        "key": port,
                        "score": round(score, 2),
                        "rank": rank,
                    }
                )

        # Upsert into serving store
        conn = ctx.conn
        if conn is not None:
            upsert(conn, "decay_traffic", ["ts", "half_life_s"], traffic_rows)
            upsert(conn, "decay_top_keys", ["ts", "key_type", "key"], top_key_rows)

        self._persist_state()
