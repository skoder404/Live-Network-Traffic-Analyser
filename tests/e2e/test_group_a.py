"""
tests/e2e/test_group_a.py — End-to-end streaming validation tests for Group A algorithms.

Validates the full streaming pipeline replay against:
  1. data/sample/normal.csv
  2. data/sample/dns_heavy.csv
  3. data/sample/fanout.csv

Asserts SQLite serving tables:
  - window_metrics: packets and bytes match pandas ground truth per 10s window.
  - protocol_counts: protocol sums match dataset ground truth.
  - filter_counts: filter counts (e.g. tcp_only) match exact filtered counts.
  - distinct_counts: exact counts match pandas nunique, and HLL is within 5%.
  - counting_ones: DGIM sliding-window estimates satisfy theoretical <= 50% relative error.
  - sampling_compare: Reservoir sampling (k=1000) mean error is < 10%.

Reference: TECH_RULES §3.5, PRD §FR-STR, todo.md T4-010
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
SAMPLE_DATASETS = ["normal", "dns_heavy", "fanout"]


def split_csv_to_stream(csv_path: Path, stream_dir: Path, chunk_size: int = 1000) -> int:
    """
    Splits a CSV file into chunk files of ~chunk_size rows and writes them to stream_dir.
    Returns the total number of lines written.
    """
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
    """
    Populates stream_dir with chunk files from data/sample/{dataset_name}.csv,
    runs StreamingApplication queries, drains all available micro-batches,
    stops queries, and returns (app, pandas_df, truth_dict).
    """
    csv_path = SAMPLE_DIR / f"{dataset_name}.csv"
    truth_path = SAMPLE_DIR / f"{dataset_name}.truth.json"

    assert csv_path.exists(), f"Sample CSV not found: {csv_path}"
    truth_data = {}
    if truth_path.exists():
        truth_data = json.loads(truth_path.read_text(encoding="utf-8"))

    # Load into pandas for ground-truth comparison
    df = pd.read_csv(csv_path, header=None, names=FIELD_NAMES)
    df["dt"] = pd.to_datetime(df["timestamp"], utc=True)
    df["window_start"] = df["dt"].dt.floor("10s").dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Split into stream_dir
    split_csv_to_stream(csv_path, stream_dir, chunk_size=1000)

    # Start StreamingApplication non-blocking
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
def test_group_a_window_metrics_e2e(
    dataset_name: str,
    tmp_path: Path,
    spark,
):
    """Validates window_metrics packets and bytes against pandas groupby on 10s tumbling windows."""
    stream_dir = tmp_path / f"stream_{dataset_name}"
    stream_dir.mkdir(parents=True, exist_ok=True)
    chk_dir = tmp_path / f"chk_{dataset_name}"
    chk_dir.mkdir(parents=True, exist_ok=True)
    db_file = tmp_path / f"serving_{dataset_name}.db"
    db_path = str(db_file)

    cfg = {
        "spark": {
            "master": "local[2]",
            "app_name": f"LNTA-TestGroupA-{dataset_name}",
            "trigger_s": 1,
            "watermark_s": 30,
            "checkpoint_root": str(chk_dir),
            "stream_in_path": str(stream_dir),
            "max_files_per_trigger": 10,
            "max_rows_per_batch": 50000,
            "top_n_ports": 10,
            "enabled_analytics": ["decay", "sampling", "moments", "fm", "counting_ones"],
        },
        "serving": {
            "db_path": db_path,
        },
    }

    _, df, truth_data = replay_and_drain(dataset_name, stream_dir, cfg)

    # Open SQLite read-only
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        wm_rows = conn.execute("SELECT * FROM window_metrics ORDER BY window_start ASC").fetchall()
        assert len(wm_rows) > 0, "window_metrics table should not be empty"

        db_total_packets = sum(r["packets"] for r in wm_rows)
        db_total_bytes = sum(r["bytes"] for r in wm_rows)

        assert db_total_packets == len(df), f"Expected {len(df)} packets, got {db_total_packets}"
        assert db_total_bytes == int(df["packet_length"].sum()), (
            f"Expected {df['packet_length'].sum()} bytes, got {db_total_bytes}"
        )

        # Per-window packets check
        expected_per_window = (
            df.groupby("window_start")
            .agg(packets=("packet_length", "count"), bytes=("packet_length", "sum"))
            .to_dict("index")
        )

        for r in wm_rows:
            ws = r["window_start"]
            assert ws in expected_per_window, f"Unexpected window_start {ws} in DB"
            assert r["packets"] == expected_per_window[ws]["packets"]
            assert r["bytes"] == expected_per_window[ws]["bytes"]
            # Validate pps and bps
            assert abs(r["pps"] - (r["packets"] / 10.0)) < 1e-4
            assert abs(r["bps"] - (r["bytes"] / 10.0)) < 1e-4

        # Cross-check with truth.json if per_10s_window_packets is present
        if "per_10s_window_packets" in truth_data:
            for ws, exp_pkts in truth_data["per_10s_window_packets"].items():
                db_matches = [r for r in wm_rows if r["window_start"] == ws]
                assert len(db_matches) == 1, f"Window {ws} not found in DB window_metrics"
                assert db_matches[0]["packets"] == exp_pkts
    finally:
        conn.close()


@pytest.mark.spark
@pytest.mark.e2e
@pytest.mark.parametrize("dataset_name", SAMPLE_DATASETS)
def test_group_a_protocol_and_filter_counts_e2e(
    dataset_name: str,
    tmp_path: Path,
    spark,
):
    """Validates protocol_counts sums and filter_counts (tcp_only, udp_only) against dataset truth."""
    stream_dir = tmp_path / f"stream_{dataset_name}_pf"
    stream_dir.mkdir(parents=True, exist_ok=True)
    chk_dir = tmp_path / f"chk_{dataset_name}_pf"
    chk_dir.mkdir(parents=True, exist_ok=True)
    db_file = tmp_path / f"serving_{dataset_name}_pf.db"
    db_path = str(db_file)

    cfg = {
        "spark": {
            "master": "local[2]",
            "app_name": f"LNTA-TestPF-{dataset_name}",
            "trigger_s": 1,
            "watermark_s": 30,
            "checkpoint_root": str(chk_dir),
            "stream_in_path": str(stream_dir),
            "max_files_per_trigger": 10,
            "max_rows_per_batch": 50000,
            "top_n_ports": 10,
            "enabled_analytics": ["decay", "sampling", "moments", "fm", "counting_ones"],
            "filters": [
                {"name": "tcp_only", "protocol": "TCP"},
                {"name": "udp_only", "protocol": "UDP"},
            ],
        },
        "serving": {
            "db_path": db_path,
        },
    }

    _, df, truth_data = replay_and_drain(dataset_name, stream_dir, cfg)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        # 1. Validate protocol_counts
        proto_rows = conn.execute("SELECT * FROM protocol_counts").fetchall()
        assert len(proto_rows) > 0, "protocol_counts should not be empty"

        proto_sums: dict[str, int] = {}
        for r in proto_rows:
            proto = r["protocol"]
            proto_sums[proto] = proto_sums.get(proto, 0) + r["packets"]

        for proto, count in df["protocol"].value_counts().items():
            assert proto_sums.get(proto, 0) == count, f"Mismatch for protocol {proto}"

        if "protocol_counts" in truth_data:
            for proto, exp_count in truth_data["protocol_counts"].items():
                assert proto_sums.get(proto, 0) == exp_count

        # 2. Validate filter_counts
        fc_rows = conn.execute("SELECT * FROM filter_counts").fetchall()
        assert len(fc_rows) > 0, "filter_counts should not be empty"

        tcp_filter_pkts = sum(r["packets"] for r in fc_rows if r["filter_name"] == "tcp_only")
        udp_filter_pkts = sum(r["packets"] for r in fc_rows if r["filter_name"] == "udp_only")

        expected_tcp = int((df["protocol"] == "TCP").sum())
        expected_udp = int((df["protocol"] == "UDP").sum())

        assert tcp_filter_pkts == expected_tcp, (
            f"tcp_only filter expected {expected_tcp}, got {tcp_filter_pkts}"
        )
        assert udp_filter_pkts == expected_udp, (
            f"udp_only filter expected {expected_udp}, got {udp_filter_pkts}"
        )
    finally:
        conn.close()


@pytest.mark.spark
@pytest.mark.e2e
@pytest.mark.parametrize("dataset_name", ["normal", "dns_heavy"])
def test_group_a_distinct_counts_e2e(
    dataset_name: str,
    tmp_path: Path,
    spark,
):
    """Validates distinct counts: exact equals pandas nunique, and HLL is within 5% error."""
    stream_dir = tmp_path / f"stream_{dataset_name}_dist"
    stream_dir.mkdir(parents=True, exist_ok=True)
    chk_dir = tmp_path / f"chk_{dataset_name}_dist"
    chk_dir.mkdir(parents=True, exist_ok=True)
    db_file = tmp_path / f"serving_{dataset_name}_dist.db"
    db_path = str(db_file)

    cfg = {
        "spark": {
            "master": "local[2]",
            "app_name": f"LNTA-TestDist-{dataset_name}",
            "trigger_s": 1,
            "watermark_s": 30,
            "checkpoint_root": str(chk_dir),
            "stream_in_path": str(stream_dir),
            "max_files_per_trigger": 10,
            "max_rows_per_batch": 50000,
            "top_n_ports": 10,
            "enabled_analytics": ["fm"],
        },
        "serving": {
            "db_path": db_path,
        },
    }

    _, df, _ = replay_and_drain(dataset_name, stream_dir, cfg)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        dist_rows = conn.execute(
            "SELECT * FROM distinct_counts ORDER BY window_start ASC"
        ).fetchall()
        assert len(dist_rows) > 0, "distinct_counts should not be empty"

        # Per-window pandas nunique
        expected_src_distinct = df.groupby("window_start")["src_ip"].nunique().to_dict()
        expected_dst_distinct = df.groupby("window_start")["dst_ip"].nunique().to_dict()

        for r in dist_rows:
            ws = r["window_start"]
            if ws in expected_src_distinct:
                exp_src = expected_src_distinct[ws]
                exp_dst = expected_dst_distinct[ws]

                # Exact count assertion
                assert r["src_ips_exact"] == exp_src, (
                    f"Exact src_ips mismatch at {ws}: expected {exp_src}, got {r['src_ips_exact']}"
                )
                assert r["dst_ips_exact"] == exp_dst, (
                    f"Exact dst_ips mismatch at {ws}: expected {exp_dst}, got {r['dst_ips_exact']}"
                )

                # HLL count assertion: within 5% (or absolute diff <= 1 for small cardinalities)
                hll_src = r["src_ips_hll"]
                hll_dst = r["dst_ips_hll"]
                assert hll_src is not None
                assert hll_dst is not None

                if exp_src > 0:
                    src_err = abs(hll_src - exp_src) / exp_src
                    # For small distinct counts (e.g. <= 20), allow +/- 2 absolute tolerance
                    assert src_err <= 0.05 or abs(hll_src - exp_src) <= 2
                if exp_dst > 0:
                    dst_err = abs(hll_dst - exp_dst) / exp_dst
                    assert dst_err <= 0.05 or abs(hll_dst - exp_dst) <= 2
    finally:
        conn.close()


@pytest.mark.spark
@pytest.mark.e2e
def test_group_a_dgim_and_sampling_e2e(tmp_path: Path, spark):
    """Validates Lane B algorithms: DGIM theoretical error bound and Reservoir sampling fidelity."""
    stream_dir = tmp_path / "stream_dgim_samp"
    stream_dir.mkdir(parents=True, exist_ok=True)
    chk_dir = tmp_path / "chk_dgim_samp"
    chk_dir.mkdir(parents=True, exist_ok=True)
    db_file = tmp_path / "serving_dgim_samp.db"
    db_path = str(db_file)

    cfg = {
        "spark": {
            "master": "local[2]",
            "app_name": "LNTA-TestDGIMSamp",
            "trigger_s": 1,
            "watermark_s": 30,
            "checkpoint_root": str(chk_dir),
            "stream_in_path": str(stream_dir),
            "max_files_per_trigger": 10,
            "max_rows_per_batch": 50000,
            "top_n_ports": 10,
            "enabled_analytics": ["sampling", "counting_ones"],
            "sampling": {
                "k": 1000,
                "p": 0.1,
                "seed": 42,
            },
            "predicates": [
                {"name": "tcp_traffic", "expr": "protocol = 'TCP'"},
                {"name": "large_packets", "expr": "packet_length > 1000"},
            ],
        },
        "serving": {
            "db_path": db_path,
        },
    }

    _, df, _ = replay_and_drain("normal", stream_dir, cfg)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        # 1. DGIM validation on counting_ones
        dgim_rows = conn.execute("SELECT * FROM counting_ones").fetchall()
        assert len(dgim_rows) > 0, "counting_ones should have recorded rows"

        for r in dgim_rows:
            exact = r["exact_ones"]
            estimate = r["dgim_estimate"]
            err_pct = r["err_pct"]

            if exact > 0:
                # DGIM theoretical bound is <= 50% relative error
                assert err_pct <= 50.0, (
                    f"DGIM error {err_pct}% exceeded theoretical 50% bound for {r['predicate_name']}"
                )
                assert abs(estimate - exact) <= int(0.5 * exact) + 1

        # 2. Reservoir sampling validation on sampling_compare
        samp_rows = conn.execute(
            "SELECT * FROM sampling_compare WHERE method = 'reservoir'"
        ).fetchall()
        assert len(samp_rows) > 0, "sampling_compare should have recorded reservoir rows"

        reservoir_errs = [r["err_pct"] for r in samp_rows]
        mean_reservoir_err = sum(reservoir_errs) / len(reservoir_errs)
        # Mean error across batches for k=1000 should be < 10%
        assert mean_reservoir_err < 10.0, (
            f"Mean reservoir sampling error {mean_reservoir_err:.2f}% exceeded 10%"
        )
    finally:
        conn.close()
