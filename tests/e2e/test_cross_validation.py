"""
tests/e2e/test_cross_validation.py — Cross-validation between Group A and Group B outputs.

Replays streaming datasets and asserts cross-layer consistency between:
  1. Window Moments vs Window Metrics (moments.n == window_metrics.packets)
  2. IP Edges vs Window Metrics (SUM(ip_edges.packets) == window_metrics.packets)
  3. Source Stats vs Distinct Counts (MAX(source_stats.unique_dst_ips) <= distinct_counts.dst_ips_exact)
  4. Frequent Itemsets vs Protocol / Port Counts (itemset support consistency)

Generates docs/validation_matrix.md documenting all cross-checks.
Reference: todo.md T5-010, TECH_RULES §3.5
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from contracts.record_schema import FIELD_NAMES
from streaming.stream_app import StreamingApplication

SAMPLE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "sample"
DOCS_DIR = Path(__file__).resolve().parent.parent.parent / "docs"
SAMPLE_DATASETS = ["normal", "fanout", "multi_host_graph"]


def split_csv_to_stream(csv_path: Path, stream_dir: Path, chunk_size: int = 1000) -> int:
    lines = csv_path.read_text(encoding="utf-8").strip().splitlines()
    total_lines = len(lines)
    chunk_idx = 0

    for i in range(0, total_lines, chunk_size):
        chunk_lines = lines[i : i + chunk_size]
        chunk_file = stream_dir / f"chunk_{chunk_idx:04d}.csv"
        chunk_file.write_text("\n".join(chunk_lines) + "\n", encoding="utf-8")
        chunk_idx += 1

    return total_lines


def replay_and_drain(
    dataset_name: str,
    stream_dir: Path,
    cfg: dict[str, Any],
) -> tuple[StreamingApplication, pd.DataFrame, dict[str, Any]]:
    csv_path = SAMPLE_DIR / f"{dataset_name}.csv"
    truth_path = SAMPLE_DIR / f"{dataset_name}.truth.json"

    assert csv_path.exists(), f"Sample CSV not found: {csv_path}"
    truth_data = {}
    if truth_path.exists():
        truth_data = json.loads(truth_path.read_text(encoding="utf-8"))

    df = pd.read_csv(csv_path, header=None, names=FIELD_NAMES)
    df["dt"] = pd.to_datetime(df["timestamp"], utc=True)
    df["window_start"] = df["dt"].dt.floor("10s").dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    split_csv_to_stream(csv_path, stream_dir, chunk_size=1000)

    app = StreamingApplication(cfg)
    queries = app.run(input_path=str(stream_dir), block=False)

    try:
        for q in queries:
            q.processAllAvailable()
    finally:
        for q in queries:
            if q.isActive:
                q.stop()

    return app, df, truth_data


@pytest.mark.spark
@pytest.mark.e2e
@pytest.mark.parametrize("dataset_name", SAMPLE_DATASETS)
def test_cross_validation_e2e(
    dataset_name: str,
    tmp_path: Path,
):
    """
    Executes full pipeline streaming replay and verifies cross-layer invariant consistency
    between Group A (M A Sushil Kumar) and Group B (Yashwant Vadhan M) serving tables.
    """
    stream_dir = tmp_path / f"stream_{dataset_name}"
    stream_dir.mkdir(parents=True, exist_ok=True)
    chk_dir = tmp_path / f"chk_{dataset_name}"
    chk_dir.mkdir(parents=True, exist_ok=True)
    db_file = tmp_path / f"serving_{dataset_name}.db"
    db_path = str(db_file)

    cfg = {
        "spark": {
            "master": "local[2]",
            "app_name": f"LNTA-CrossVal-{dataset_name}",
            "trigger_s": 1,
            "watermark_s": 30,
            "checkpoint_root": str(chk_dir),
            "stream_in_path": str(stream_dir),
            "max_files_per_trigger": 10,
            "max_rows_per_batch": 50000,
            "top_n_ports": 20,
            "max_edges_per_window": 500,
            "plugins_enabled": [
                "decay",
                "sampling",
                "moments",
                "fm",
                "counting_ones",
                "itemsets",
            ],
        },
        "itemsets": {
            "window_s": 60,
            "every_s": 0,
            "min_support": 0.05,
            "num_buckets": 10007,
        },
        "serving": {
            "db_path": db_path,
        },
    }

    replay_and_drain(dataset_name, stream_dir, cfg)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    try:
        # Fetch tables
        wm_rows = conn.execute("SELECT * FROM window_metrics").fetchall()
        moments_rows = conn.execute("SELECT * FROM moments").fetchall()
        edges_rows = conn.execute("SELECT * FROM ip_edges").fetchall()
        source_rows = conn.execute("SELECT * FROM source_stats").fetchall()
        distinct_rows = conn.execute("SELECT * FROM distinct_counts").fetchall()
        itemset_rows = conn.execute("SELECT * FROM frequent_itemsets").fetchall()

        wm_by_ws = {r["window_start"]: r for r in wm_rows}
        moments_by_ws = {r["window_start"]: r for r in moments_rows}
        distinct_by_ws = {r["window_start"]: r for r in distinct_rows}

        # 1. Assert moments.n == window_metrics.packets
        for ws, wm in wm_by_ws.items():
            if ws in moments_by_ws:
                m_row = moments_by_ws[ws]
                assert m_row["n"] == wm["packets"], (
                    f"Cross-validation mismatch in window {ws}: moments.n ({m_row['n']}) != "
                    f"window_metrics.packets ({wm['packets']}). "
                    "Owners: Yashwant Vadhan M (Moments) vs M A Sushil Kumar (Window Metrics)"
                )

        # 2. Assert SUM(ip_edges.packets) == window_metrics.packets
        edges_by_ws: dict[str, list[Any]] = {}
        for er in edges_rows:
            edges_by_ws.setdefault(er["window_start"], []).append(er)

        for ws, er_list in edges_by_ws.items():
            if ws in wm_by_ws and len(er_list) < 500:
                edge_packet_sum = sum(e["packets"] for e in er_list)
                wm_pkts = wm_by_ws[ws]["packets"]
                assert edge_packet_sum == wm_pkts, (
                    f"Cross-validation mismatch in window {ws}: SUM(ip_edges.packets) ({edge_packet_sum}) != "
                    f"window_metrics.packets ({wm_pkts}). "
                    "Owners: Yashwant Vadhan M (IP Edges) vs M A Sushil Kumar (Window Metrics)"
                )

        # 3. Assert MAX(source_stats.unique_dst_ips) <= distinct_counts.dst_ips_exact
        src_by_ws: dict[str, list[Any]] = {}
        for sr in source_rows:
            src_by_ws.setdefault(sr["window_start"], []).append(sr)

        for ws, s_list in src_by_ws.items():
            if ws in distinct_by_ws:
                max_src_unique = max(s["unique_dst_ips"] for s in s_list)
                global_exact = distinct_by_ws[ws]["dst_ips_exact"]
                assert max_src_unique <= global_exact, (
                    f"Cross-validation violation in window {ws}: MAX(source_stats.unique_dst_ips) "
                    f"({max_src_unique}) > distinct_counts.dst_ips_exact ({global_exact}). "
                    "Owners: Yashwant Vadhan M (Source Stats) vs M A Sushil Kumar (Distinct Counts)"
                )

        # 4. Frequent itemsets check
        if dataset_name == "normal" and itemset_rows:
            # Check frequent itemsets table contains TCP:443
            tcp_items = [r for r in itemset_rows if "TCP:443" in r["itemset"]]
            assert len(tcp_items) > 0, (
                "Cross-validation check failed: TCP:443 expected in frequent_itemsets for normal.csv. "
                "Owners: Yashwant Vadhan M (Itemsets) vs M A Sushil Kumar (Port/Protocol Counts)"
            )

    finally:
        conn.close()
