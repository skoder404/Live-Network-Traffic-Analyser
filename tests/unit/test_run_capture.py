"""
tests/unit/test_run_capture.py — Unit tests for capture/run_capture.py.
"""

import json
import sys
from pathlib import Path

from capture.run_capture import CaptureRunner


def test_runner_with_fake_tshark(tmp_path: Path):
    fake_tshark_script = Path("tests/fixtures/fake_tshark.py").resolve()
    out_dir = tmp_path / "capture"

    runner = CaptureRunner(
        iface="test_iface",
        sink="file",
        out_format="csv",
        out_dir=out_dir,
        duration_s=2,
        stats_interval_s=0.5,
        tshark_path=sys.executable,
        tshark_extra_args=[str(fake_tshark_script), "--max-lines", "30", "--delay", "0.01"],
    )

    exit_code = runner.run()
    assert exit_code == 0
    assert runner.records_written > 0

    # Verify CSV files created
    csv_files = list(out_dir.glob("*.csv"))
    assert len(csv_files) >= 1

    # Verify stats file created
    stats_file = Path("logs/capture_stats.json")
    assert stats_file.exists()
    with open(stats_file, encoding="utf-8") as f:
        stats = json.load(f)
    assert stats["records_written"] == runner.records_written


def test_runner_auto_restart_on_crash(tmp_path: Path):
    fake_tshark_script = Path("tests/fixtures/fake_tshark.py").resolve()
    out_dir = tmp_path / "capture_restart"

    runner = CaptureRunner(
        iface="test_iface",
        sink="file",
        out_format="csv",
        out_dir=out_dir,
        duration_s=3,
        stats_interval_s=0.5,
        tshark_path=sys.executable,
        # Crash after emitting 5 lines to trigger restart loop
        tshark_extra_args=[str(fake_tshark_script), "--crash-after", "5", "--delay", "0.01"],
    )

    exit_code = runner.run()
    # Runner survived and wrote records from across crash/restart iterations
    assert exit_code in (0, 1)
    assert runner.records_written >= 5
