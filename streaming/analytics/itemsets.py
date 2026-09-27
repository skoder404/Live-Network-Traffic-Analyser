"""
streaming/analytics/itemsets.py — Market-basket model and limited-pass frequent itemsets.

Implements A-Priori, PCY (Park-Chen-Yu), and brute-force baseline algorithms
over a rolling 1-second-slice basket buffer for detecting frequent protocol and port service patterns.
References: TECH_RULES §5.2, PRD §FR-STR, todo.md T5-004
"""

from __future__ import annotations

import itertools
import json
import logging
import math
import time
import zlib
from collections import Counter
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from common.serving_db import connect, upsert
from streaming.analytics.base import Analytic, BatchContext

logger = logging.getLogger("ItemsetsAnalytic")


def make_baskets(rows: list[dict[str, Any]]) -> dict[tuple[str, int], frozenset[str]]:
    """
    Groups traffic rows into baskets keyed by (src_ip, slice_start_s).
    Items are formatted as 'PROTO:port' (e.g. 'TCP:443') or 'PROTO' if port is null.
    """
    baskets_map: dict[tuple[str, int], set[str]] = {}

    for r in rows:
        src_ip = r.get("src_ip")
        if not src_ip:
            continue

        raw_time = r.get("event_time") or r.get("timestamp")
        if isinstance(raw_time, datetime):
            slice_s = int(raw_time.timestamp())
        elif isinstance(raw_time, (int, float)):
            slice_s = int(raw_time)
        elif isinstance(raw_time, str):
            try:
                # Handle ISO timestamps like 2026-09-21T12:00:00.000Z
                dt = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                slice_s = int(dt.timestamp())
            except Exception:
                slice_s = int(time.time())
        else:
            slice_s = int(time.time())

        protocol = str(r.get("protocol") or "UNKNOWN").upper().strip()
        dst_port = r.get("dst_port")

        if dst_port is not None and str(dst_port).strip() not in ("", "None", "nan", "NULL"):
            try:
                port_int = int(float(dst_port))
                item_str = f"{protocol}:{port_int}"
            except (ValueError, TypeError):
                item_str = protocol
        else:
            item_str = protocol

        key = (str(src_ip), slice_s)
        if key not in baskets_map:
            baskets_map[key] = set()
        baskets_map[key].add(item_str)

    return {k: frozenset(v) for k, v in baskets_map.items()}


class RollingBasketBuffer:
    """Maintains a sliding-window collection of baskets with time eviction and capacity cap."""

    def __init__(self, window_s: int = 60, max_baskets: int = 20000) -> None:
        self.window_s = int(window_s)
        self.max_baskets = int(max_baskets)
        self.baskets: dict[tuple[str, int], frozenset[str]] = {}

    def add(self, key: tuple[str, int], items: frozenset[str]) -> None:
        """Adds or unions items for the (src_ip, slice_start_s) key."""
        if key in self.baskets:
            self.baskets[key] = self.baskets[key] | items
        else:
            self.baskets[key] = items

    def add_baskets(self, baskets_dict: dict[tuple[str, int], frozenset[str]]) -> None:
        """Adds a batch of baskets."""
        for key, items in baskets_dict.items():
            self.add(key, items)

    def evict(self, current_time_s: int) -> int:
        """Removes baskets older than current_time_s - window_s, and caps at max_baskets."""
        cutoff = int(current_time_s) - self.window_s
        keys_to_remove = [k for k in self.baskets if k[1] <= cutoff]
        for k in keys_to_remove:
            del self.baskets[k]

        evicted_count = len(keys_to_remove)
        if len(self.baskets) > self.max_baskets:
            excess = len(self.baskets) - self.max_baskets
            sorted_keys = sorted(self.baskets.keys(), key=lambda k: k[1])
            for k in sorted_keys[:excess]:
                del self.baskets[k]
            evicted_count += excess

        return evicted_count

    def get_baskets(self) -> list[frozenset[str]]:
        """Returns the list of active baskets."""
        return list(self.baskets.values())

    def snapshot(self) -> dict[str, Any]:
        """Serializes buffer state for checkpointing."""
        return {
            "window_s": self.window_s,
            "max_baskets": self.max_baskets,
            "baskets": [
                {"src_ip": k[0], "slice_start_s": k[1], "items": sorted(v)}
                for k, v in self.baskets.items()
            ],
        }

    def restore(self, data: dict[str, Any]) -> None:
        """Restores buffer state from snapshot."""
        self.window_s = int(data.get("window_s", self.window_s))
        self.max_baskets = int(data.get("max_baskets", self.max_baskets))
        self.baskets = {}
        for item in data.get("baskets", []):
            key = (str(item["src_ip"]), int(item["slice_start_s"]))
            self.baskets[key] = frozenset(item["items"])


def format_itemset(itemset: frozenset[str] | set[str] | Sequence[str]) -> str:
    """Formats an itemset into sorted, human-readable representation: e.g. 'TCP:443 + UDP:53'."""
    return " + ".join(sorted(itemset))


def hash_pair(i1: str, i2: str, num_buckets: int) -> int:
    """Deterministic pair hash for PCY bucket counting."""
    u, v = (i1, i2) if i1 <= i2 else (i2, i1)
    payload = f"{u}|{v}".encode()
    return zlib.crc32(payload) % num_buckets


def apriori(
    baskets: list[frozenset[str]],
    min_support_ratio: float,
    max_size: int = 2,
) -> tuple[dict[frozenset[str], int], dict[str, int]]:
    """
    Standard 2-pass A-Priori algorithm for frequent itemsets.
    Pass 1: Count single item frequencies.
    Pass 2: Form candidate pairs from frequent singletons, count pairs.
    """
    if not baskets:
        return {}, {"passes": 0, "candidate_pairs": 0}

    n = len(baskets)
    min_support_count = max(1, math.ceil(min_support_ratio * n))

    # Pass 1: item counts
    item_counts: Counter[str] = Counter()
    for b in baskets:
        for item in b:
            item_counts[item] += 1

    frequent_1: dict[str, int] = {
        item: cnt for item, cnt in item_counts.items() if cnt >= min_support_count
    }

    results: dict[frozenset[str], int] = {
        frozenset([item]): cnt for item, cnt in frequent_1.items()
    }

    if max_size < 2 or len(frequent_1) < 2:
        return results, {"passes": 1, "candidate_pairs": 0}

    # Pass 2: candidate pairs
    f1_list = sorted(frequent_1.keys())
    candidate_pairs = list(itertools.combinations(f1_list, 2))
    num_candidates = len(candidate_pairs)
    candidate_set = {frozenset(p) for p in candidate_pairs}

    pair_counts: Counter[frozenset[str]] = Counter()
    f1_set = set(f1_list)

    for b in baskets:
        basket_f1 = [item for item in b if item in f1_set]
        if len(basket_f1) >= 2:
            for u, v in itertools.combinations(basket_f1, 2):
                p_fs = frozenset([u, v])
                if p_fs in candidate_set:
                    pair_counts[p_fs] += 1

    for p_fs, cnt in pair_counts.items():
        if cnt >= min_support_count:
            results[p_fs] = cnt

    return results, {"passes": 2, "candidate_pairs": num_candidates}


def pcy(
    baskets: list[frozenset[str]],
    min_support_ratio: float,
    num_buckets: int = 10007,
    max_size: int = 2,
) -> tuple[dict[frozenset[str], int], dict[str, int]]:
    """
    Park-Chen-Yu (PCY) frequent itemset algorithm using hash buckets.
    Pass 1: Count singletons and hash all basket pairs to bucket counters.
    Pass 2: Count only candidate pairs that are frequent singletons AND hash to frequent buckets.
    """
    if not baskets:
        return {}, {"passes": 0, "candidate_pairs": 0}

    n = len(baskets)
    min_support_count = max(1, math.ceil(min_support_ratio * n))

    # Pass 1: item counts and pair hashing
    item_counts: Counter[str] = Counter()
    bucket_counts = [0] * num_buckets

    for b in baskets:
        items = sorted(b)
        for item in items:
            item_counts[item] += 1
        for u, v in itertools.combinations(items, 2):
            b_idx = hash_pair(u, v, num_buckets)
            bucket_counts[b_idx] += 1

    frequent_1: dict[str, int] = {
        item: cnt for item, cnt in item_counts.items() if cnt >= min_support_count
    }

    results: dict[frozenset[str], int] = {
        frozenset([item]): cnt for item, cnt in frequent_1.items()
    }

    if max_size < 2 or len(frequent_1) < 2:
        return results, {"passes": 1, "candidate_pairs": 0}

    # Bitmap of frequent buckets
    bitmap = [c >= min_support_count for c in bucket_counts]

    # Candidate pairs: frequent items whose hash bucket is frequent
    f1_list = sorted(frequent_1.keys())
    candidate_pairs = [
        (u, v)
        for u, v in itertools.combinations(f1_list, 2)
        if bitmap[hash_pair(u, v, num_buckets)]
    ]
    num_candidates = len(candidate_pairs)
    candidate_set = {frozenset(p) for p in candidate_pairs}

    pair_counts: Counter[frozenset[str]] = Counter()
    f1_set = set(f1_list)

    # Pass 2: count filtered candidates
    for b in baskets:
        basket_f1 = [item for item in b if item in f1_set]
        if len(basket_f1) >= 2:
            for u, v in itertools.combinations(basket_f1, 2):
                p_fs = frozenset([u, v])
                if p_fs in candidate_set:
                    pair_counts[p_fs] += 1

    for p_fs, cnt in pair_counts.items():
        if cnt >= min_support_count:
            results[p_fs] = cnt

    return results, {"passes": 2, "candidate_pairs": num_candidates}


def brute_force(
    baskets: list[frozenset[str]],
    min_support_ratio: float,
    max_size: int = 2,
) -> tuple[dict[frozenset[str], int], dict[str, int]]:
    """Brute-force baseline counting all item subsets up to max_size for validation."""
    if not baskets:
        return {}, {"passes": 1, "candidate_pairs": 0}

    n = len(baskets)
    min_support_count = max(1, math.ceil(min_support_ratio * n))
    counts: Counter[frozenset[str]] = Counter()

    for b in baskets:
        # Size 1
        for item in b:
            counts[frozenset([item])] += 1
        # Size 2
        if max_size >= 2 and len(b) >= 2:
            for u, v in itertools.combinations(sorted(b), 2):
                counts[frozenset([u, v])] += 1

    results = {itemset: cnt for itemset, cnt in counts.items() if cnt >= min_support_count}
    return results, {"passes": 1, "candidate_pairs": 0}


class ItemsetsAnalytic(Analytic):
    """
    Lane B Analytic plugin for frequent itemset mining on rolling 1-second service baskets.
    Executes A-Priori and PCY periodically and persists results to the serving store.
    """

    def __init__(self) -> None:
        self.buffer: RollingBasketBuffer | None = None
        self.window_s: int = 60
        self.every_s: int = 10
        self.min_support: float = 0.05
        self.num_buckets: int = 10007
        self.last_run_time: float | None = None
        self.state_dir = Path("serving/.state/itemsets")
        self._initialized = False

    @property
    def name(self) -> str:
        return "itemsets"

    def _ensure_initialized(self, cfg: dict[str, Any]) -> None:
        if self._initialized and self.buffer is not None:
            return

        itemsets_cfg = cfg.get("itemsets", {})
        self.window_s = int(itemsets_cfg.get("window_s", 60))
        self.every_s = int(itemsets_cfg.get("every_s", 10))
        self.min_support = float(itemsets_cfg.get("min_support", 0.05))
        self.num_buckets = int(itemsets_cfg.get("num_buckets", 10007))

        self.buffer = RollingBasketBuffer(window_s=self.window_s, max_baskets=20000)
        self._restore_state()
        self._initialized = True

    def _save_state(self) -> None:
        if self.buffer is None:
            return
        try:
            self.state_dir.mkdir(parents=True, exist_ok=True)
            state_file = self.state_dir / "itemsets_state.json"
            tmp_file = self.state_dir / "itemsets_state.json.tmp"
            payload = {
                "last_run_time": self.last_run_time,
                "buffer": self.buffer.snapshot(),
            }
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            tmp_file.replace(state_file)
        except Exception as e:
            logger.warning("Failed to save itemsets state: %s", e)

    def _restore_state(self) -> None:
        state_file = self.state_dir / "itemsets_state.json"
        if not state_file.exists():
            return
        try:
            with open(state_file, encoding="utf-8") as f:
                payload = json.load(f)
            self.last_run_time = payload.get("last_run_time")
            if "buffer" in payload and self.buffer is not None:
                self.buffer.restore(payload["buffer"])
            logger.info("Restored itemsets state from %s", state_file)
        except Exception as e:
            logger.warning("Failed to restore itemsets state from %s: %s", state_file, e)

    def process_batch(self, batch_df: DataFrame, batch_id: int, ctx: BatchContext) -> None:
        self._ensure_initialized(ctx.cfg)
        if self.buffer is None:
            return

        row_count = batch_df.count()
        if row_count == 0:
            return

        current_t = float(ctx.clock if isinstance(ctx.clock, (int, float)) else time.time())

        # Collect only required columns with batch row cap
        max_rows = int(ctx.cfg.get("spark", {}).get("max_rows_per_batch", 50000))
        needed_cols = [
            c for c in ["event_time", "src_ip", "protocol", "dst_port"] if c in batch_df.columns
        ]

        if not needed_cols or "src_ip" not in needed_cols:
            return

        collected_rows = batch_df.select(*needed_cols).limit(max_rows).collect()
        rows = [r.asDict() for r in collected_rows]

        # Convert to baskets and update buffer
        batch_baskets = make_baskets(rows)
        self.buffer.add_baskets(batch_baskets)
        self.buffer.evict(int(current_t))

        # Check periodic execution condition
        if self.last_run_time is None or (current_t - self.last_run_time >= self.every_s):
            baskets_list = self.buffer.get_baskets()
            if baskets_list:
                total_baskets = len(baskets_list)
                window_start_dt = datetime.fromtimestamp(current_t - self.window_s, tz=timezone.utc)
                window_start_iso = window_start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

                ap_itemsets, ap_stats = apriori(baskets_list, self.min_support, max_size=2)
                pcy_itemsets, pcy_stats = pcy(
                    baskets_list,
                    self.min_support,
                    num_buckets=self.num_buckets,
                    max_size=2,
                )

                itemset_rows: list[dict[str, Any]] = []

                for itemset, cnt in ap_itemsets.items():
                    itemset_rows.append(
                        {
                            "window_start": window_start_iso,
                            "window_len_s": self.window_s,
                            "algorithm": "apriori",
                            "itemset": format_itemset(itemset),
                            "size": len(itemset),
                            "support_count": cnt,
                            "support_ratio": round(float(cnt) / total_baskets, 4),
                            "passes": ap_stats["passes"],
                        }
                    )

                for itemset, cnt in pcy_itemsets.items():
                    itemset_rows.append(
                        {
                            "window_start": window_start_iso,
                            "window_len_s": self.window_s,
                            "algorithm": "pcy",
                            "itemset": format_itemset(itemset),
                            "size": len(itemset),
                            "support_count": cnt,
                            "support_ratio": round(float(cnt) / total_baskets, 4),
                            "passes": pcy_stats["passes"],
                        }
                    )

                if itemset_rows:
                    conn = ctx.conn if ctx.conn else connect(ctx.db_path)
                    try:
                        upsert(
                            conn,
                            "frequent_itemsets",
                            ["window_start", "window_len_s", "algorithm", "itemset"],
                            itemset_rows,
                        )
                    finally:
                        if not ctx.conn:
                            conn.close()

            self.last_run_time = current_t
            self._save_state()
