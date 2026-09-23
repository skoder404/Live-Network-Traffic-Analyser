"""
capture/replay.py — Paced and restamped traffic replay tool.

Replays contract-compliant CSV captures through capture writers (rotating CSV, TCP,
or stdout) with:
- Configurable speed scaling (--speed X) or fixed rate (--pps N)
- Dynamic timestamp restamping to wall clock (--restamp) with monotonic iat_ms
- Infinite looping (--loop) for continuous development and soak testing
"""

from __future__ import annotations

import argparse
import signal
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from capture.writers import (
    CsvRotatingWriter,
    StdoutWriter,
    TcpLineWriter,
    Writer,
)
from common.config import get_config
from contracts.record_schema import FIELD_NAMES, parse_line


def _parse_ts_epoch(ts_str: str) -> float:
    """Parse contract timestamp string 'YYYY-MM-DD HH:MM:SS.mmm' into float epoch."""
    parts = ts_str.strip().split(".")
    dt = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    millis = int(parts[1]) if len(parts) > 1 else 0
    return dt.timestamp() + (millis / 1000.0)


def _format_ts_epoch(epoch: float) -> str:
    """Format float epoch into contract timestamp 'YYYY-MM-DD HH:MM:SS.mmm'."""
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    millis = int((epoch - int(epoch)) * 1000)
    if millis < 0:
        millis = 0
    elif millis > 999:
        millis = 999
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{millis:03d}"


class ReplayEngine:
    def __init__(
        self,
        file_path: str | Path,
        writer: Writer,
        speed: float = 1.0,
        pps: float | None = None,
        restamp: bool = False,
        loop: bool = False,
        clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self.file_path = Path(file_path)
        self.writer = writer
        self.speed = speed if speed > 0 else 1.0
        self.pps = pps if pps is not None and pps > 0 else None
        self.restamp = restamp
        self.loop = loop
        self.clock = clock or time.time
        self.sleeper = sleeper or time.sleep

        self._stop = False
        self.records_replayed = 0

    def stop(self) -> None:
        self._stop = True

    def replay(self) -> int:
        if not self.file_path.exists():
            raise FileNotFoundError(f"Input file not found: {self.file_path}")

        wall_start = self.clock()
        cum_simulated = 0.0

        while not self._stop:
            prev_orig_epoch: float | None = None

            with open(self.file_path, encoding="utf-8") as f:
                for line in f:
                    if self._stop:
                        break

                    clean = line.strip()
                    if not clean:
                        continue

                    fields = parse_line(clean)
                    if len(fields) != len(FIELD_NAMES):
                        continue

                    rec_dict = dict(zip(FIELD_NAMES, fields, strict=True))
                    orig_ts = rec_dict.get("timestamp")
                    if not orig_ts:
                        continue

                    try:
                        orig_epoch = _parse_ts_epoch(str(orig_ts))
                    except Exception:
                        continue

                    # Pacing calculation
                    if self.pps is not None:
                        gap = 1.0 / self.pps
                    elif prev_orig_epoch is not None:
                        gap = max(0.0, (orig_epoch - prev_orig_epoch) / self.speed)
                    else:
                        gap = 0.0

                    prev_orig_epoch = orig_epoch
                    cum_simulated += gap

                    if gap > 0:
                        self.sleeper(gap)

                    # Restamping
                    if self.restamp:
                        new_epoch = wall_start + cum_simulated
                        rec_dict["timestamp"] = _format_ts_epoch(new_epoch)
                        rec_dict["iat_ms"] = round(gap * 1000.0, 3) if self.records_replayed > 0 else 0.0

                    try:
                        self.writer.write(rec_dict)
                    except (BrokenPipeError, OSError):
                        self._stop = True
                        break
                    self.records_replayed += 1

            if not self.loop:
                break

        self.writer.flush()
        return self.records_replayed


def main() -> None:
    cfg = get_config()

    parser = argparse.ArgumentParser(description="Paced & restamped traffic replay tool.")
    parser.add_argument("--file", required=True, help="Input sample CSV file.")
    parser.add_argument("--speed", type=float, default=1.0, help="Speed scaling factor (e.g. 2.0 = 2x faster).")
    parser.add_argument("--pps", type=float, help="Fixed replay rate in packets/sec.")
    parser.add_argument("--restamp", action="store_true", help="Rewrite timestamps to current wall clock.")
    parser.add_argument("--loop", action="store_true", help="Repeat file continuously until stopped.")
    parser.add_argument("--sink", choices=["file", "tcp", "stdout"], default="file")
    parser.add_argument("--out-dir", default=cfg.capture.out_dir)
    args = parser.parse_args()

    if args.sink == "file":
        writer: Writer = CsvRotatingWriter(
            out_dir=args.out_dir,
            rotate_seconds=cfg.capture.rotate_seconds,
            rotate_mb=cfg.capture.rotate_mb,
        )
    elif args.sink == "tcp":
        writer = TcpLineWriter(host=cfg.flume.tcp_host, port=cfg.flume.tcp_port)
    elif args.sink == "stdout":
        writer = StdoutWriter()
    else:
        raise ValueError(f"Unknown sink: {args.sink}")

    engine = ReplayEngine(
        file_path=args.file,
        writer=writer,
        speed=args.speed,
        pps=args.pps,
        restamp=args.restamp,
        loop=args.loop,
    )

    def sig_handler(sig, frame):
        engine.stop()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    try:
        count = engine.replay()
        print(f"Replay finished. Replayed {count} records.")
    finally:
        writer.close()


if __name__ == "__main__":
    main()
