"""
tests/unit/test_load_test.py — Unit tests for load testing script and report generator (T5-007).
"""

from scripts.load_test import (
    generate_markdown_report,
    get_cpu_times,
    get_ram_usage_mb,
    percentile,
    simulate_rate_measurements,
)


def test_percentile_calculation():
    data = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert percentile(data, 50) == 3.0
    assert percentile(data, 0) == 1.0
    assert percentile(data, 100) == 5.0
    assert percentile([], 50) == 0.0


def test_simulate_rate_measurements():
    res_1k = simulate_rate_measurements(1000, 60.0)
    assert res_1k.rate_pps == 1000
    assert res_1k.pass_fail == "PASS"
    assert res_1k.median_batch_duration_s < 5.0

    res_2k = simulate_rate_measurements(2000, 60.0)
    assert res_2k.rate_pps == 2000
    assert res_2k.pass_fail == "PASS"

    res_5k = simulate_rate_measurements(5000, 60.0)
    assert res_5k.rate_pps == 5000
    assert res_5k.pass_fail == "PASS"


def test_generate_markdown_report():
    r1 = simulate_rate_measurements(1000, 180.0)
    r2 = simulate_rate_measurements(2000, 180.0)
    r3 = simulate_rate_measurements(5000, 180.0)

    report = generate_markdown_report([r1, r2, r3])
    assert "LNTA Pipeline Load & High-Volume Test Report" in report
    assert "1,000 pkts/s" in report
    assert "2,000 pkts/s" in report
    assert "5,000 pkts/s" in report
    assert "Bottleneck Analysis" in report
    assert "PASS" in report


def test_proc_metrics():
    # Verify non-crashing execution on Linux /proc
    tot, idle = get_cpu_times()
    assert tot >= 0.0
    assert idle >= 0.0

    used_mb, pct = get_ram_usage_mb()
    assert used_mb >= 0.0
    assert pct >= 0.0
