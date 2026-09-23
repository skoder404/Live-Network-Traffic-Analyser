"""
tests/unit/test_generate.py — Unit tests for capture/generate.py.
"""

import json
from pathlib import Path

import pandas as pd

from capture.generate import (
    SCENARIOS,
    compute_ground_truth,
    generate_records,
    write_dataset,
)
from contracts.record_schema import FIELD_NAMES, validate_row


def test_determinism_by_seed():
    records1 = generate_records(scenario="normal", duration_s=10.0, rate_pps=20.0, seed=123)
    records2 = generate_records(scenario="normal", duration_s=10.0, rate_pps=20.0, seed=123)

    assert len(records1) == len(records2)
    for r1, r2 in zip(records1, records2, strict=True):
        assert r1 == r2


def test_different_seed_different_records():
    records1 = generate_records(scenario="normal", duration_s=10.0, rate_pps=20.0, seed=123)
    records2 = generate_records(scenario="normal", duration_s=10.0, rate_pps=20.0, seed=999)

    assert [r.packet_length for r in records1] != [r.packet_length for r in records2]


def test_all_scenarios_contract_validity():
    for scenario in SCENARIOS:
        records = generate_records(scenario=scenario, duration_s=15.0, rate_pps=25.0, seed=42)
        assert len(records) > 0
        for r in records:
            ok, reason = validate_row(r.to_dict())
            assert ok, f"Scenario '{scenario}' produced invalid row: {reason}"


def test_spike_scenario_rate_increase():
    # 60s test: last 20s should have significantly higher packet count than first 20s
    records = generate_records(scenario="spike", duration_s=60.0, rate_pps=20.0, seed=42)
    truth = compute_ground_truth(records)
    window_counts = list(truth["per_10s_window_packets"].values())

    first_half_avg = sum(window_counts[:3]) / 3.0
    last_two_avg = sum(window_counts[-2:]) / 2.0
    assert last_two_avg > 2.5 * first_half_avg


def test_portscan_scenario_target():
    records = generate_records(scenario="portscan_like", duration_s=20.0, rate_pps=20.0, seed=42)
    # Check that scanner probes many ports
    scanner_records = [r for r in records if r.src_ip == "192.168.1.99"]
    dst_ports = {r.dst_port for r in scanner_records if r.dst_port is not None}
    assert len(dst_ports) >= 25


def test_fanout_scenario_target():
    records = generate_records(scenario="fanout", duration_s=20.0, rate_pps=20.0, seed=42)
    scanner_records = [r for r in records if r.src_ip == "192.168.1.10"]
    dst_ips = {r.dst_ip for r in scanner_records}
    assert len(dst_ips) >= 40


def test_dns_heavy_scenario():
    records = generate_records(scenario="dns_heavy", duration_s=20.0, rate_pps=20.0, seed=42)
    truth = compute_ground_truth(records)
    assert truth["protocol_counts"].get("UDP", 0) > truth["protocol_counts"].get("TCP", 0) * 2


def test_multi_host_graph_scenario():
    records = generate_records(scenario="multi_host_graph", duration_s=20.0, rate_pps=20.0, seed=42)
    ips = {r.src_ip for r in records} | {r.dst_ip for r in records}
    assert "10.0.0.1" in ips
    assert "10.0.0.2" in ips
    assert "10.0.0.3" in ips
    assert "10.0.0.4" in ips


def test_ground_truth_matches_pandas(tmp_path: Path):
    csv_file = tmp_path / "test_normal.csv"
    records = generate_records(scenario="normal", duration_s=20.0, rate_pps=25.0, seed=42)
    write_dataset(records, csv_file)

    truth_file = tmp_path / "test_normal.truth.json"
    with open(truth_file, encoding="utf-8") as f:
        truth = json.load(f)

    df = pd.read_csv(csv_file, names=FIELD_NAMES, header=None)

    assert truth["packets"] == len(df)
    assert truth["bytes"] == int(df["packet_length"].sum())
    assert truth["unique_src_ips"] == int(df["src_ip"].nunique())
    assert truth["unique_dst_ips"] == int(df["dst_ip"].nunique())

    # Check mean and sample variance
    assert abs(truth["packet_length_mean"] - float(df["packet_length"].mean())) < 1e-3
    assert abs(truth["packet_length_variance_sample"] - float(df["packet_length"].var(ddof=1))) < 1e-3
