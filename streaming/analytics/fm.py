"""
streaming/analytics/fm.py — Flajolet–Martin cardinality estimator and FMAnalytic Lane B plugin.

Implements:
  1. FlajoletMartin: Teaching estimator using multiple independent hash functions
     with bitmap lowest-unset-bit, median-of-means aggregation, and state snapshot/restore.
  2. FMAnalytic (Lane B plugin): Per micro-batch, groups rows by window_start, maintains
     per-window FM sketches for src_ip and dst_ip, and partially upserts src_ips_fm/dst_ips_fm
     into distinct_counts.

NOTE: Flajolet–Martin is a *teaching* estimator with higher variance than HyperLogLog.
The FM estimates are provided for educational comparison against exact and HLL values.
Reference: TECH_RULES §3.5, §5.2, todo.md T4-008
"""

from __future__ import annotations

import hashlib
import logging
import struct
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from common.serving_db import connect, upsert
from streaming.analytics.base import Analytic, BatchContext

logger = logging.getLogger("FMAnalytic")

# Flajolet–Martin correction constant (phi ~ 0.77351)
FM_PHI: float = 0.77351


def _trailing_zeros(value: int) -> int:
    """Returns number of trailing zero bits in a 64-bit integer. Returns 64 for value=0."""
    if value == 0:
        return 64
    count = 0
    while (value & 1) == 0:
        count += 1
        value >>= 1
    return count


def _salted_hash_64(item: str, salt: int) -> int:
    """
    Produces a deterministic 64-bit hash of (item, salt) via SHA-256 truncation.
    Using cryptographic hashing with salts provides near-independent hash functions.
    """
    data = f"{salt}:{item}".encode()
    digest = hashlib.sha256(data).digest()
    # Unpack first 8 bytes as unsigned 64-bit little-endian integer
    return struct.unpack("<Q", digest[:8])[0]


class FlajoletMartin:
    """
    Flajolet–Martin cardinality estimator (teaching implementation).

    Uses ``num_hashes`` independent hash functions partitioned into ``num_groups`` groups.
    Each hash function maintains a 64-bit bitmap where bit position r (trailing zeros) is set.
    Per hash function, R is the position of the first 0-bit in the bitmap.
    Within each group, the mean of R is computed: E_group = 2^(mean(R)) / phi (phi = 0.77351).
    The final cardinality estimate is the median of group estimates.

    Args:
        num_hashes: Total number of independent hash functions (default 30).
        num_groups: Number of groups for median-of-means (default 5; must divide num_hashes).
        seed: Base seed for hash salt generation (default 0).
    """

    def __init__(
        self,
        num_hashes: int = 30,
        num_groups: int = 5,
        seed: int = 0,
    ) -> None:
        if num_hashes <= 0:
            raise ValueError(f"num_hashes must be positive, got {num_hashes}")
        if num_groups <= 0:
            raise ValueError(f"num_groups must be positive, got {num_groups}")
        if num_hashes % num_groups != 0:
            raise ValueError(
                f"num_hashes ({num_hashes}) must be divisible by num_groups ({num_groups})"
            )

        self.num_hashes = num_hashes
        self.num_groups = num_groups
        self.seed = seed
        self.group_size = num_hashes // num_groups

        # bitmaps[i] stores the 64-bit bitmap for hash function i
        self.bitmaps: list[int] = [0] * num_hashes
        self.n_seen: int = 0

    def add(self, item: str) -> None:
        """Observes a single item string and updates all hash function bitmaps."""
        for i in range(self.num_hashes):
            salt = self.seed * 10000 + i
            h = _salted_hash_64(item, salt)
            tz = _trailing_zeros(h)
            if tz < 64:
                self.bitmaps[i] |= 1 << tz
        self.n_seen += 1

    def _first_zero_bit(self, bitmap: int) -> int:
        """Returns the index of the first unset (0) bit in the bitmap."""
        r = 0
        while (bitmap & (1 << r)) != 0:
            r += 1
        return r

    def estimate(self) -> float:
        """
        Returns the cardinality estimate using median-of-means aggregation.
        For each group of hash functions, computes mean(R) where R is the first zero bit,
        and group estimate = 2^(mean(R)) / 0.77351.
        Returns the median of all group estimates (0.0 if no items seen).
        """
        if self.n_seen == 0:
            return 0.0

        r_values = [self._first_zero_bit(bm) for bm in self.bitmaps]

        group_estimates: list[float] = []
        for g in range(self.num_groups):
            start_idx = g * self.group_size
            end_idx = start_idx + self.group_size
            r_avg = sum(r_values[start_idx:end_idx]) / self.group_size
            group_est = (2.0**r_avg) / FM_PHI
            group_estimates.append(group_est)

        group_estimates.sort()
        mid = len(group_estimates) // 2
        if len(group_estimates) % 2 == 1:
            return group_estimates[mid]
        return (group_estimates[mid - 1] + group_estimates[mid]) / 2.0

    def snapshot(self) -> dict[str, Any]:
        """Returns a JSON-serializable snapshot of the estimator state."""
        return {
            "num_hashes": self.num_hashes,
            "num_groups": self.num_groups,
            "seed": self.seed,
            "bitmaps": list(self.bitmaps),
            "n_seen": self.n_seen,
        }

    def restore(self, state: dict[str, Any]) -> None:
        """Restores estimator state from a snapshot dictionary."""
        self.num_hashes = int(state["num_hashes"])
        self.num_groups = int(state["num_groups"])
        self.seed = int(state["seed"])
        self.group_size = self.num_hashes // self.num_groups
        self.bitmaps = list(state["bitmaps"])
        self.n_seen = int(state["n_seen"])


class FMAnalytic(Analytic):
    """
    Lane B micro-batch analytic plugin for Flajolet–Martin distinct-count estimation.

    Per batch, groups rows by window_start (floor of event_time to config window length),
    maintains per-window FM sketches for src_ip and dst_ip, and partially upserts
    src_ips_fm and dst_ips_fm into the ``distinct_counts`` table.

    NOTE: FM is a teaching estimator with high variance compared to HLL.
    """

    name = "fm"

    def __init__(
        self,
        num_hashes: int = 30,
        num_groups: int = 5,
        seed: int = 42,
        window_len_s: int = 10,
    ) -> None:
        self.num_hashes = num_hashes
        self.num_groups = num_groups
        self.seed = seed
        self.window_len_s = window_len_s

        # Per-window FM sketches: {window_start_str: {"src": FlajoletMartin, "dst": FlajoletMartin}}
        self._window_sketches: dict[str, dict[str, FlajoletMartin]] = {}

    def _get_or_create_sketches(self, window_key: str) -> dict[str, FlajoletMartin]:
        """Returns existing or creates new FM sketches for a window."""
        if window_key not in self._window_sketches:
            self._window_sketches[window_key] = {
                "src": FlajoletMartin(self.num_hashes, self.num_groups, self.seed),
                "dst": FlajoletMartin(self.num_hashes, self.num_groups, self.seed + 1),
            }
        return self._window_sketches[window_key]

    def process_batch(self, batch_df: DataFrame, batch_id: int, ctx: BatchContext) -> None:
        """Processes micro-batch: groups by window_start, updates FM sketches, upserts to DB."""
        from pyspark.sql import functions as F

        from streaming.common.cleaning import clean

        spark_cfg = ctx.cfg.get("spark", {})
        max_rows = spark_cfg.get("max_rows_per_batch", 50000)
        window_len_s = spark_cfg.get("window_len_s", self.window_len_s)

        # Ensure event_time is available (clean if raw DataFrame passed)
        df = batch_df
        if "event_time" not in df.columns and "timestamp" in df.columns:
            df = clean(df)

        if "event_time" not in df.columns:
            logger.warning(
                "Batch %d missing event_time and timestamp columns. Skipping FM.", batch_id
            )
            return

        # Select needed columns and compute window_start from event_time
        windowed_df = df.select(
            F.date_format(
                F.window("event_time", f"{window_len_s} seconds").getField("start"),
                "yyyy-MM-dd'T'HH:mm:ss.000'Z'",
            ).alias("window_start"),
            F.coalesce(F.col("src_ip"), F.lit("")).alias("src_ip"),
            F.coalesce(F.col("dst_ip"), F.lit("")).alias("dst_ip"),
        )

        rows = windowed_df.collect()
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

        # Group collected rows by window_start and feed to FM sketches
        for row in rows:
            ws = row["window_start"]
            sketches = self._get_or_create_sketches(ws)
            src_ip = row["src_ip"]
            dst_ip = row["dst_ip"]
            if src_ip:
                sketches["src"].add(src_ip)
            if dst_ip:
                sketches["dst"].add(dst_ip)

        # Upsert FM estimates for all active windows
        upsert_rows: list[dict[str, Any]] = []
        for ws, sketches in self._window_sketches.items():
            upsert_rows.append(
                {
                    "window_start": ws,
                    "window_len_s": window_len_s,
                    "src_ips_fm": int(round(sketches["src"].estimate())),
                    "dst_ips_fm": int(round(sketches["dst"].estimate())),
                }
            )

        if upsert_rows:
            conn = ctx.conn
            if conn is not None:
                upsert(conn, "distinct_counts", ["window_start", "window_len_s"], upsert_rows)
            else:
                conn = connect(ctx.db_path)
                try:
                    upsert(conn, "distinct_counts", ["window_start", "window_len_s"], upsert_rows)
                finally:
                    conn.close()

        # Expire old window sketches (keep only last 10 windows to bound memory)
        max_windows = 10
        if len(self._window_sketches) > max_windows:
            sorted_keys = sorted(self._window_sketches.keys())
            for old_key in sorted_keys[: len(sorted_keys) - max_windows]:
                del self._window_sketches[old_key]

        logger.debug(
            "Batch %d: updated FM sketches for %d windows",
            batch_id,
            len(upsert_rows),
        )

    def snapshot(self) -> dict[str, Any]:
        """Returns JSON-serializable snapshot of all window sketches."""
        return {
            "windows": {
                ws: {"src": s["src"].snapshot(), "dst": s["dst"].snapshot()}
                for ws, s in self._window_sketches.items()
            }
        }

    def restore(self, state: dict[str, Any]) -> None:
        """Restores window sketches from a snapshot dictionary."""
        self._window_sketches.clear()
        for ws, sketch_state in state.get("windows", {}).items():
            src_fm = FlajoletMartin(self.num_hashes, self.num_groups, self.seed)
            dst_fm = FlajoletMartin(self.num_hashes, self.num_groups, self.seed + 1)
            src_fm.restore(sketch_state["src"])
            dst_fm.restore(sketch_state["dst"])
            self._window_sketches[ws] = {"src": src_fm, "dst": dst_fm}
