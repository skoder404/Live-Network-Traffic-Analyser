"""
streaming/analytics/moments.py — Alon-Matias-Szegedy (AMS) F2 second-moment estimator and Moments Analytic.

Provides:
  1. AMSF2: Pure streaming estimator for second frequency moment F2 = sum(m_i^2)
     using reservoir-sampled positions and median-of-means aggregation.
  2. exact_f2: Exact second moment calculation over frequency counters.
  3. compute_iat_from_timestamps: Inter-arrival gap calculator for missing iat_ms fields.
  4. MomentsAnalytic: Lane B Analytic Plugin computing f2_exact and f2_ams per window
     and performing partial upsert into SQLite serving table `moments`.
References: TECH_RULES §3.5, §5.2, todo.md T5-002
"""

from __future__ import annotations

import json
import logging
import random
from collections import Counter
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from common.serving_db import upsert
from streaming.analytics.base import Analytic, BatchContext

logger = logging.getLogger("MomentsAnalytic")


def exact_f2(counter: Counter | dict[str, int]) -> float:
    """
    Computes the exact second frequency moment F2 = sum(m_i^2).
    """
    return float(sum(count**2 for count in counter.values()))


def compute_iat_from_timestamps(
    timestamps: Sequence[datetime | float | int | str],
) -> list[float]:
    """
    Computes inter-arrival times (in milliseconds) from a sequence of timestamps.
    Missing/None values are skipped; output list has length max(0, len(valid_ts) - 1).
    """
    epoch_ms_list: list[float] = []

    for ts in timestamps:
        if ts is None:
            continue
        if isinstance(ts, (int, float)):
            # Assume seconds if small (< 1e11), otherwise milliseconds
            ms = float(ts * 1000.0) if ts < 1e11 else float(ts)
            epoch_ms_list.append(ms)
        elif isinstance(ts, datetime):
            epoch_ms_list.append(ts.timestamp() * 1000.0)
        elif isinstance(ts, str):
            clean_ts = ts.strip()
            if not clean_ts:
                continue
            # Try parsing ISO or standard format
            try:
                # Replace trailing Z if present for fromisoformat
                dt = datetime.fromisoformat(clean_ts.replace("Z", "+00:00"))
                epoch_ms_list.append(dt.timestamp() * 1000.0)
            except ValueError:
                try:
                    dt = datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S.%f")
                    epoch_ms_list.append(dt.timestamp() * 1000.0)
                except ValueError:
                    try:
                        dt = datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S")
                        epoch_ms_list.append(dt.timestamp() * 1000.0)
                    except ValueError:
                        continue

    if len(epoch_ms_list) < 2:
        return []

    epoch_ms_list.sort()
    return [round(epoch_ms_list[i] - epoch_ms_list[i - 1], 3) for i in range(1, len(epoch_ms_list))]


class AMSF2:
    """
    Alon–Matias–Szegedy (AMS) F2 second-moment estimator.

    Maintains `num_estimators` random variables:
    Each estimator picks a random stream position uniformly via reservoir sampling,
    tracks count `r` of subsequent occurrences of the selected item, and computes
    X = n * (2r - 1).
    Estimators are partitioned into `num_groups`; estimates within each group are averaged,
    and the median of group averages is taken to guarantee (epsilon, delta) bounds.
    """

    def __init__(
        self,
        num_estimators: int = 60,
        num_groups: int = 6,
        seed: int = 42,
    ) -> None:
        if num_estimators <= 0:
            raise ValueError("num_estimators must be positive")
        if num_groups <= 0 or num_groups > num_estimators:
            num_groups = max(1, min(6, num_estimators))

        self.num_estimators = int(num_estimators)
        self.num_groups = int(num_groups)
        self.seed = int(seed)
        self._rng = random.Random(self.seed)

        self.n: int = 0
        # Per-estimator state: (tracked_item, r, chosen_position)
        self._tracked: list[str | None] = [None] * self.num_estimators
        self._counts: list[int] = [0] * self.num_estimators
        self._positions: list[int] = [0] * self.num_estimators

    def add(self, item: str) -> None:
        """Processes a single stream item."""
        self.n += 1
        str_item = str(item)

        for i in range(self.num_estimators):
            # Reservoir sampling choice for position k in [1, n]
            # Probability 1/n to select current item
            if self._rng.randint(1, self.n) == 1:
                self._tracked[i] = str_item
                self._counts[i] = 1
                self._positions[i] = self.n
            elif self._tracked[i] == str_item:
                self._counts[i] += 1

    def add_many(self, items: Sequence[str]) -> None:
        """Processes multiple items in sequence."""
        for item in items:
            self.add(item)

    def estimate(self) -> float:
        """
        Calculates the AMS median-of-means F2 estimate.
        """
        if self.n == 0:
            return 0.0

        # Compute individual estimator values: X_i = n * (2 * r_i - 1)
        values: list[float] = []
        for i in range(self.num_estimators):
            r = self._counts[i]
            x_i = float(self.n * (2 * r - 1))
            values.append(max(0.0, x_i))

        # Partition into groups and compute mean per group
        group_size = max(1, self.num_estimators // self.num_groups)
        group_means: list[float] = []

        for g in range(self.num_groups):
            start_idx = g * group_size
            end_idx = (g + 1) * group_size if g < self.num_groups - 1 else self.num_estimators
            slice_vals = values[start_idx:end_idx]
            if slice_vals:
                group_means.append(sum(slice_vals) / len(slice_vals))

        if not group_means:
            return 0.0

        group_means.sort()
        mid = len(group_means) // 2
        if len(group_means) % 2 == 1:
            return round(group_means[mid], 2)
        return round((group_means[mid - 1] + group_means[mid]) / 2.0, 2)

    def snapshot(self) -> dict[str, Any]:
        rng_state = self._rng.getstate()
        # Convert state tuple (int, tuple of ints, float) to JSON-serializable structure
        serializable_rng = [rng_state[0], list(rng_state[1]), rng_state[2]]
        return {
            "num_estimators": self.num_estimators,
            "num_groups": self.num_groups,
            "seed": self.seed,
            "n": self.n,
            "tracked": self._tracked,
            "counts": self._counts,
            "positions": self._positions,
            "rng_state": serializable_rng,
        }

    def restore(self, data: dict[str, Any]) -> None:
        self.num_estimators = int(data["num_estimators"])
        self.num_groups = int(data["num_groups"])
        self.seed = int(data["seed"])
        self.n = int(data["n"])
        self._tracked = list(data["tracked"])
        self._counts = list(data["counts"])
        self._positions = list(data["positions"])

        if "rng_state" in data:
            s = data["rng_state"]
            restored_state = (s[0], tuple(s[1]), s[2])
            self._rng = random.Random()
            self._rng.setstate(restored_state)
        else:
            self._rng = random.Random(self.seed + self.n)


class MomentsAnalytic(Analytic):
    """
    Lane B Analytic Plugin: AMS F2 Second Moment Estimator.
    Maintains per-window frequency counters and AMS estimators over destination IPs,
    writing `f2_exact` and `f2_ams` to SQLite `moments` table via partial upsert.
    """

    name = "moments"

    def __init__(self, state_dir: str | Path = "serving/.state/moments") -> None:
        self.state_dir = Path(state_dir)
        self.window_len_s: int = 10
        self.window_exact: dict[str, Counter[str]] = {}
        self.window_ams: dict[str, AMSF2] = {}
        self._initialized = False

    def _ensure_initialized(self, cfg: dict[str, Any]) -> None:
        if self._initialized:
            return
        spark_cfg = cfg.get("spark", {})
        windows = spark_cfg.get("windows_s", [10])
        self.window_len_s = int(windows[0]) if windows else 10
        self._restore_state()
        self._initialized = True

    def _get_state_file(self) -> Path:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        return self.state_dir / "moments_state.json"

    def _persist_state(self) -> None:
        try:
            state = {
                "window_len_s": self.window_len_s,
                "window_exact": {w: dict(c) for w, c in self.window_exact.items()},
                "window_ams": {w: ams.snapshot() for w, ams in self.window_ams.items()},
            }
            state_file = self._get_state_file()
            tmp_file = state_file.with_suffix(".tmp")
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(state, f)
            tmp_file.replace(state_file)
        except Exception as e:
            logger.warning("Failed to persist moments state: %s", e)

    def _restore_state(self) -> None:
        state_file = self._get_state_file()
        if not state_file.exists():
            return
        try:
            with open(state_file, encoding="utf-8") as f:
                state = json.load(f)

            self.window_len_s = int(state.get("window_len_s", 10))
            self.window_exact = {w: Counter(c) for w, c in state.get("window_exact", {}).items()}
            self.window_ams = {}
            for w, ams_data in state.get("window_ams", {}).items():
                ams = AMSF2()
                ams.restore(ams_data)
                self.window_ams[w] = ams
            logger.info("Restored moments analytic state from %s", state_file)
        except Exception as e:
            logger.warning("Failed to restore moments state from %s: %s", state_file, e)

    def process_batch(self, batch_df: DataFrame, batch_id: int, ctx: BatchContext) -> None:
        self._ensure_initialized(ctx.cfg)

        if batch_df.count() == 0:
            return

        # Collect event_time and dst_ip from batch DataFrame
        max_rows = int(ctx.cfg.get("spark", {}).get("max_rows_per_batch", 50000))
        collected_rows = (
            batch_df.select("event_time", "dst_ip")
            .filter("dst_ip IS NOT NULL")
            .limit(max_rows)
            .collect()
        )

        if not collected_rows:
            return

        # Group by 10s window bucket
        updated_windows: set[str] = set()

        for row in collected_rows:
            dt = row["event_time"]
            dst_ip = str(row["dst_ip"])
            if dt is None:
                continue

            # Floor to window_len_s
            epoch_s = int(dt.timestamp())
            floor_epoch_s = epoch_s - (epoch_s % self.window_len_s)
            w_start_dt = datetime.fromtimestamp(floor_epoch_s, timezone.utc)
            ws_iso = w_start_dt.strftime("%Y-%m-%dT%H:%M:%S.000") + "Z"

            if ws_iso not in self.window_exact:
                self.window_exact[ws_iso] = Counter()
                self.window_ams[ws_iso] = AMSF2(num_estimators=64, num_groups=8)

            self.window_exact[ws_iso][dst_ip] += 1
            self.window_ams[ws_iso].add(dst_ip)
            updated_windows.add(ws_iso)

        # Evict stale windows older than 10 minutes to bound memory
        if len(self.window_exact) > 60:
            sorted_windows = sorted(self.window_exact.keys())
            stale_windows = sorted_windows[:-60]
            for sw in stale_windows:
                del self.window_exact[sw]
                if sw in self.window_ams:
                    del self.window_ams[sw]

        # Prepare partial upsert rows for moments table
        moments_rows: list[dict[str, Any]] = []
        for ws in updated_windows:
            f2_ex = exact_f2(self.window_exact[ws])
            f2_est = self.window_ams[ws].estimate()

            moments_rows.append(
                {
                    "window_start": ws,
                    "window_len_s": self.window_len_s,
                    "f2_exact": f2_ex,
                    "f2_ams": f2_est,
                }
            )

        # Partial upsert: write ONLY f2_exact and f2_ams to moments table
        conn = ctx.conn
        if conn is not None and moments_rows:
            upsert(conn, "moments", ["window_start", "window_len_s"], moments_rows)
            logger.debug("Upserted %d moments F2 rows (batch %d)", len(moments_rows), batch_id)

        self._persist_state()
