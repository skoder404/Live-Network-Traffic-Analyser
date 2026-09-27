"""
tests/unit/test_itemsets.py — Unit tests for market-basket frequent itemsets mining.

Tests:
  - make_baskets row parsing and item formatting
  - RollingBasketBuffer time-based eviction and max-capacity constraints
  - A-Priori == PCY == brute_force correctness on fixed and random baskets
  - PCY candidate pairs <= A-Priori candidate pairs
  - Frequent itemsets on normal.csv ground truth
  - ItemsetsAnalytic micro-batch execution and SQLite serving store upsert
  - State snapshot and restore round-trip

Reference: todo.md T5-004, TECH_RULES §5.2
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from common.serving_db import connect, init_schema
from contracts.record_schema import FIELD_NAMES
from streaming.analytics.base import BatchContext
from streaming.analytics.itemsets import (
    ItemsetsAnalytic,
    RollingBasketBuffer,
    apriori,
    brute_force,
    format_itemset,
    make_baskets,
    pcy,
)

SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sample"


def test_make_baskets_and_format() -> None:
    dt1 = datetime(2026, 9, 26, 10, 0, 0, tzinfo=timezone.utc)
    t1 = int(dt1.timestamp())

    rows: list[dict[str, Any]] = [
        {"event_time": dt1, "src_ip": "192.168.1.10", "protocol": "TCP", "dst_port": 443},
        {"event_time": dt1, "src_ip": "192.168.1.10", "protocol": "UDP", "dst_port": 53},
        {"event_time": dt1, "src_ip": "192.168.1.20", "protocol": "ICMP", "dst_port": None},
        {
            "event_time": "2026-09-26T10:00:00.000Z",
            "src_ip": "192.168.1.10",
            "protocol": "TCP",
            "dst_port": "80",
        },
    ]

    baskets = make_baskets(rows)
    assert len(baskets) == 2
    assert baskets[("192.168.1.10", t1)] == frozenset(["TCP:443", "UDP:53", "TCP:80"])
    assert baskets[("192.168.1.20", t1)] == frozenset(["ICMP"])

    formatted = format_itemset(baskets[("192.168.1.10", t1)])
    assert formatted == "TCP:443 + TCP:80 + UDP:53"


def test_rolling_basket_buffer_eviction_and_snapshot() -> None:
    buf = RollingBasketBuffer(window_s=10, max_baskets=3)

    buf.add(("10.0.0.1", 100), frozenset(["TCP:80"]))
    buf.add(("10.0.0.1", 100), frozenset(["TCP:443"]))  # Union items
    buf.add(("10.0.0.2", 105), frozenset(["UDP:53"]))
    buf.add(("10.0.0.3", 108), frozenset(["TCP:22"]))

    assert len(buf.baskets) == 3
    assert buf.baskets[("10.0.0.1", 100)] == frozenset(["TCP:80", "TCP:443"])

    # Evict at t=112 (cutoff = 102 -> t=100 gets evicted)
    evicted = buf.evict(112)
    assert evicted == 1
    assert ("10.0.0.1", 100) not in buf.baskets
    assert len(buf.baskets) == 2

    # Test max_baskets cap eviction
    buf.add(("10.0.0.4", 110), frozenset(["TCP:80"]))
    buf.add(("10.0.0.5", 111), frozenset(["UDP:53"]))
    # Current len is 4 > max_baskets=3
    evicted_cap = buf.evict(112)
    assert evicted_cap >= 1
    assert len(buf.baskets) <= 3

    # Snapshot and restore
    snap = buf.snapshot()
    new_buf = RollingBasketBuffer(window_s=10, max_baskets=3)
    new_buf.restore(snap)
    assert len(new_buf.baskets) == len(buf.baskets)
    assert set(new_buf.get_baskets()) == set(buf.get_baskets())


def test_apriori_vs_pcy_vs_brute_force_fixed() -> None:
    baskets = [
        frozenset(["TCP:443", "UDP:53"]),
        frozenset(["TCP:443", "UDP:53", "TCP:80"]),
        frozenset(["TCP:443", "TCP:22"]),
        frozenset(["UDP:53", "TCP:80"]),
        frozenset(["TCP:443", "UDP:53"]),
        frozenset(["UDP:53", "TCP:22"]),
    ]
    min_sup = 0.3  # 30% of 6 is ceil(1.8) = 2

    ap_res, ap_stats = apriori(baskets, min_support_ratio=min_sup, max_size=2)
    pcy_res, pcy_stats = pcy(baskets, min_support_ratio=min_sup, num_buckets=20, max_size=2)
    bf_res, _ = brute_force(baskets, min_support_ratio=min_sup, max_size=2)

    # 1. Exact agreement between all three algorithms
    assert ap_res == bf_res
    assert pcy_res == bf_res
    assert ap_res == pcy_res

    # 2. PCY candidates <= A-Priori candidates
    assert pcy_stats["candidate_pairs"] <= ap_stats["candidate_pairs"]
    print(
        f"Fixed test candidates — Apriori: {ap_stats['candidate_pairs']}, "
        f"PCY: {pcy_stats['candidate_pairs']}"
    )


def test_apriori_vs_pcy_vs_brute_force_random() -> None:
    rng = random.Random(42)
    items_universe = [f"PROTO_{i}:{p}" for i in range(1, 6) for p in [80, 443, 53, 22, 8080]]

    baskets: list[frozenset[str]] = []
    for _ in range(200):
        k = rng.randint(1, 5)
        baskets.append(frozenset(rng.sample(items_universe, k)))

    min_sup = 0.05
    ap_res, ap_stats = apriori(baskets, min_support_ratio=min_sup, max_size=2)
    pcy_res, pcy_stats = pcy(baskets, min_support_ratio=min_sup, num_buckets=10007, max_size=2)
    bf_res, _ = brute_force(baskets, min_support_ratio=min_sup, max_size=2)

    assert ap_res == bf_res
    assert pcy_res == bf_res
    assert pcy_stats["candidate_pairs"] <= ap_stats["candidate_pairs"]


def test_itemsets_normal_csv_ground_truth() -> None:
    csv_file = SAMPLE_DIR / "normal.csv"
    if not csv_file.exists():
        pytest.skip("normal.csv not found")

    df = pd.read_csv(csv_file, header=None, names=FIELD_NAMES)
    rows = df[["timestamp", "src_ip", "protocol", "dst_port"]].to_dict(orient="records")

    baskets_dict = make_baskets(rows)
    baskets = list(baskets_dict.values())
    assert len(baskets) > 0

    ap_res, ap_stats = apriori(baskets, min_support_ratio=0.05, max_size=2)
    pcy_res, pcy_stats = pcy(baskets, min_support_ratio=0.05, num_buckets=10007, max_size=2)

    assert ap_res == pcy_res
    assert pcy_stats["candidate_pairs"] <= ap_stats["candidate_pairs"]

    # Verify TCP:443 and UDP:53 singletons exist
    assert frozenset(["TCP:443"]) in ap_res
    assert frozenset(["UDP:53"]) in ap_res

    # Check if {UDP:53, TCP:443} appears as a frequent pair
    pair = frozenset(["TCP:443", "UDP:53"])
    if pair in ap_res:
        assert ap_res[pair] >= 1


def test_empty_and_edge_baskets() -> None:
    empty_baskets: list[frozenset[str]] = []
    ap_res, ap_stats = apriori(empty_baskets, min_support_ratio=0.1)
    pcy_res, pcy_stats = pcy(empty_baskets, min_support_ratio=0.1)
    bf_res, _ = brute_force(empty_baskets, min_support_ratio=0.1)

    assert ap_res == {}
    assert pcy_res == {}
    assert bf_res == {}
    assert ap_stats["passes"] == 0
    assert pcy_stats["passes"] == 0

    single_basket = [frozenset(["TCP:80"])]
    ap_res2, _ = apriori(single_basket, min_support_ratio=0.5, max_size=1)
    assert frozenset(["TCP:80"]) in ap_res2


@pytest.mark.spark
def test_itemsets_analytic_process_batch(tmp_path: Path) -> None:
    try:
        from pyspark.sql import SparkSession

        from streaming.common.cleaning import clean
        from streaming.common.schema import to_spark_schema
    except ImportError:
        pytest.skip("PySpark not available")

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-itemsets-analytic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    db_path = tmp_path / "itemsets_test.db"
    conn = connect(db_path)
    init_schema(conn)

    csv_file = SAMPLE_DIR / "normal.csv"
    if not csv_file.exists():
        conn.close()
        pytest.skip("normal.csv not found")

    raw_spark_df = spark.read.schema(to_spark_schema()).csv(str(csv_file)).limit(500)
    cleaned_df = clean(raw_spark_df)

    cfg = {
        "spark": {"max_rows_per_batch": 50000},
        "itemsets": {"window_s": 60, "every_s": 0, "min_support": 0.01, "num_buckets": 10007},
        "serving": {"db_path": str(db_path)},
    }

    ctx = BatchContext(
        cfg=cfg,
        db_path=str(db_path),
        conn=conn,
        clock=1700000060.0,
        logger=None,
        batch_time="2026-09-26T12:00:00.000Z",
    )

    analytic = ItemsetsAnalytic()
    analytic.process_batch(cleaned_df, batch_id=0, ctx=ctx)

    # Verify rows in frequent_itemsets table
    cur = conn.execute(
        "SELECT algorithm, count(*) as cnt FROM frequent_itemsets GROUP BY algorithm"
    )
    rows = cur.fetchall()
    counts = {r["algorithm"]: r["cnt"] for r in rows}

    assert "apriori" in counts
    assert "pcy" in counts
    assert counts["apriori"] > 0
    assert counts["apriori"] == counts["pcy"]

    conn.close()
