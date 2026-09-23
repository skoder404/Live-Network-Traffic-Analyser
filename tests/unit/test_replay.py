"""
tests/unit/test_replay.py — Unit tests for capture/replay.py.
"""

from pathlib import Path

from capture.replay import ReplayEngine, _parse_ts_epoch
from capture.writers import Writer
from contracts.record_schema import validate_row


class MemoryWriter(Writer):
    def __init__(self):
        self.records = []

    def write(self, record):
        self.records.append(record)

    def flush(self):
        pass

    def close(self):
        pass


def create_test_csv(path: Path, num_rows: int = 5) -> None:
    lines = []
    for i in range(num_rows):
        ts = f"2026-09-22 10:00:{i:02d}.000"
        lines.append(
            f"{ts},192.168.1.10,8.8.8.8,50000,443,TCP,100,aa:bb:cc:dd:ee:ff,11:22:33:44:55:66,0x0018,1000.0"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_replay_speed_scaling(tmp_path: Path):
    csv_file = tmp_path / "sample.csv"
    create_test_csv(csv_file, num_rows=5)  # 4 intervals of 1.0s = 4.0s total

    slept_intervals = []

    def fake_sleeper(gap: float):
        slept_intervals.append(gap)

    writer = MemoryWriter()
    engine = ReplayEngine(
        file_path=csv_file,
        writer=writer,
        speed=2.0,  # 2x speed
        restamp=False,
        sleeper=fake_sleeper,
    )
    count = engine.replay()
    assert count == 5
    # Total slept time at 2x should be 2.0s
    assert abs(sum(slept_intervals) - 2.0) < 1e-4


def test_replay_restamp_monotonicity(tmp_path: Path):
    csv_file = tmp_path / "sample.csv"
    create_test_csv(csv_file, num_rows=4)

    wall_start = 1758500000.0

    def fake_clock():
        return wall_start

    writer = MemoryWriter()
    engine = ReplayEngine(
        file_path=csv_file,
        writer=writer,
        speed=1.0,
        restamp=True,
        clock=fake_clock,
        sleeper=lambda s: None,
    )
    count = engine.replay()
    assert count == 4

    timestamps = [r["timestamp"] for r in writer.records]
    assert len(timestamps) == 4

    # Assert monotonic increase
    epochs = [_parse_ts_epoch(ts) for ts in timestamps]
    for i in range(len(epochs) - 1):
        assert epochs[i + 1] > epochs[i]

    # Validate output contract
    for r in writer.records:
        ok, reason = validate_row(r)
        assert ok, reason


def test_replay_loop_mode(tmp_path: Path):
    csv_file = tmp_path / "sample.csv"
    create_test_csv(csv_file, num_rows=3)

    writer = MemoryWriter()
    engine = ReplayEngine(
        file_path=csv_file,
        writer=writer,
        speed=10.0,
        loop=True,
        sleeper=lambda s: None,
    )

    # Let writer stop after 6 writes (2 loops)
    original_write = writer.write

    def write_and_stop(rec):
        original_write(rec)
        if len(writer.records) >= 6:
            engine.stop()

    writer.write = write_and_stop

    count = engine.replay()
    assert count >= 6
    assert len(writer.records) >= 6
