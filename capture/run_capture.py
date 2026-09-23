"""
capture/run_capture.py — Production capture runner for Live Network Traffic Analyser.

Spawns TShark as a subprocess, ingests output asynchronously via a bounded queue,
normalises raw lines into contract records, manages rejects, auto-restarts on crash,
and periodically writes atomic health statistics to logs/capture_stats.json.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from capture.list_interfaces import auto_select_wifi, list_interfaces
from capture.parser import Record, Reject, parse_line
from capture.tshark_cmd import build_command
from capture.writers import (
    CsvRotatingWriter,
    JsonLinesWriter,
    StdoutWriter,
    TcpLineWriter,
    Writer,
)
from common.config import get_config

logger = logging.getLogger("capture.runner")


class CaptureRunner:
    def __init__(
        self,
        iface: str,
        sink: str = "file",
        out_format: str = "csv",
        capture_filter: str | None = None,
        duration_s: int | None = None,
        out_dir: str | Path = "data/capture",
        tcp_host: str = "127.0.0.1",
        tcp_port: int = 44444,
        rotate_seconds: int = 60,
        rotate_mb: int = 10,
        queue_size: int = 10000,
        stats_interval_s: float = 10.0,
        tshark_path: str = "tshark",
        tshark_extra_args: list[str] | None = None,
    ) -> None:
        self.iface = iface
        self.sink = sink
        self.out_format = out_format
        self.capture_filter = capture_filter
        self.duration_s = duration_s
        self.out_dir = Path(out_dir)
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.rotate_seconds = rotate_seconds
        self.rotate_mb = rotate_mb
        self.queue_size = queue_size
        self.stats_interval_s = stats_interval_s
        self.tshark_path = tshark_path
        self.tshark_extra_args = tshark_extra_args or []

        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = Path("logs")
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        self._queue: queue.Queue[str] = queue.Queue(maxsize=queue_size)
        self._stop_event = threading.Event()
        self._proc: subprocess.Popen[str] | None = None
        self._reader_thread: threading.Thread | None = None

        # Metrics
        self.frames_seen = 0
        self.records_written = 0
        self.rejects_by_reason: dict[str, int] = {}
        self.dropped_queue_full = 0
        self.start_time = 0.0

        # Writer
        self.writer: Writer = self._init_writer()

    def _init_writer(self) -> Writer:
        if self.sink == "file":
            if self.out_format == "json":
                return JsonLinesWriter(self.out_dir / "traffic.json")
            return CsvRotatingWriter(
                out_dir=self.out_dir,
                rotate_seconds=self.rotate_seconds,
                rotate_mb=self.rotate_mb,
            )
        elif self.sink == "tcp":
            return TcpLineWriter(host=self.tcp_host, port=self.tcp_port)
        elif self.sink == "stdout":
            return StdoutWriter()
        raise ValueError(f"Unknown sink type: {self.sink}")

    def _get_rejects_file(self) -> Path:
        dt_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        return self.out_dir / f"rejects_{dt_str}.log"

    def _log_reject(self, reject: Reject) -> None:
        self.rejects_by_reason[reject.reason.value] = (
            self.rejects_by_reason.get(reject.reason.value, 0) + 1
        )
        try:
            with open(self._get_rejects_file(), "a", encoding="utf-8") as f:
                f.write(f"{reject.reason.value} | {reject.detail} | {reject.raw_line.strip()}\n")
        except Exception as e:
            logger.warning("Failed to write to rejects log: %s", e)

    def write_stats(self) -> None:
        """Atomically dump capture statistics to logs/capture_stats.json."""
        uptime = round(time.time() - self.start_time, 2) if self.start_time else 0.0
        rps = round(self.records_written / max(1.0, uptime), 2)
        stats = {
            "frames_seen": self.frames_seen,
            "records_written": self.records_written,
            "rejects_by_reason": self.rejects_by_reason,
            "dropped_queue_full": self.dropped_queue_full,
            "uptime_s": uptime,
            "records_per_sec": rps,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        stats_path = self.logs_dir / "capture_stats.json"
        tmp_path = self.logs_dir / "capture_stats.json.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2)
            if os.name == "nt" and stats_path.exists():
                stats_path.unlink()
            tmp_path.rename(stats_path)
        except Exception as e:
            logger.warning("Failed to write capture stats: %s", e)

    def _start_subprocess(self) -> subprocess.Popen[str]:
        cmd = build_command(
            iface=self.iface,
            capture_filter=self.capture_filter,
            duration_s=self.duration_s,
            tshark_path=self.tshark_path,
        )
        if self.tshark_extra_args:
            if "python" in Path(self.tshark_path).name.lower():
                cmd = [self.tshark_path] + self.tshark_extra_args + cmd[1:]
            else:
                cmd.extend(self.tshark_extra_args)

        logger.info("Starting TShark command: %s", " ".join(cmd))
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

    def _stdout_reader(self, proc: subprocess.Popen[str]) -> None:
        """Thread worker to continuously read TShark stdout into the bounded queue."""
        assert proc.stdout is not None
        for line in proc.stdout:
            if self._stop_event.is_set():
                break
            try:
                self._queue.put(line, block=False)
            except queue.Full:
                self.dropped_queue_full += 1

    def run(self) -> int:
        """Main runner loop with process supervisor, restart backoff and stats."""
        self.start_time = time.time()
        last_stats_time = self.start_time
        restarts = 0
        backoffs = [1.0, 2.0, 4.0, 8.0, 16.0]
        max_restarts = len(backoffs)

        def sig_handler(signum, frame):
            logger.info("Shutdown signal received (%s), terminating...", signum)
            self._stop_event.set()

        signal.signal(signal.SIGINT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)

        while not self._stop_event.is_set() and restarts <= max_restarts:
            try:
                self._proc = self._start_subprocess()
            except Exception as e:
                logger.error("Failed to start TShark subprocess: %s", e)
                return 1

            self._reader_thread = threading.Thread(
                target=self._stdout_reader,
                args=(self._proc,),
                daemon=True,
            )
            self._reader_thread.start()

            # Consumer loop while child process is alive
            while not self._stop_event.is_set():
                # Process queued items
                try:
                    line = self._queue.get(timeout=0.1)
                    self.frames_seen += 1
                    result = parse_line(line)
                    if isinstance(result, Record):
                        self.writer.write(result)
                        self.records_written += 1
                    else:
                        self._log_reject(result)
                except queue.Empty:
                    pass

                # Periodic stats
                now = time.time()
                if (now - last_stats_time) >= self.stats_interval_s:
                    self.writer.flush()
                    self.write_stats()
                    last_stats_time = now

                # Check duration limit if configured
                if self.duration_s is not None and (now - self.start_time) >= self.duration_s:
                    self._stop_event.set()
                    break

                # Check if TShark exited
                if self._proc.poll() is not None:
                    if self._reader_thread:
                        self._reader_thread.join(timeout=1.0)
                    while not self._queue.empty():
                        try:
                            line = self._queue.get_nowait()
                            self.frames_seen += 1
                            result = parse_line(line)
                            if isinstance(result, Record):
                                self.writer.write(result)
                                self.records_written += 1
                            else:
                                self._log_reject(result)
                        except queue.Empty:
                            break
                    break

            # Handle unexpected termination
            if not self._stop_event.is_set():
                exit_code = self._proc.poll()
                logger.warning("TShark subprocess exited with code %s", exit_code)
                if exit_code == 0:
                    # Clean completion (e.g. duration reached)
                    break
                else:
                    restarts += 1
                    if restarts > max_restarts:
                        logger.error("Max restarts (%s) reached. Exiting.", max_restarts)
                        self._stop_event.set()
                        return 1
                    delay = backoffs[restarts - 1]
                    logger.info(
                        "Auto-restarting TShark in %s seconds (attempt %s/%s)...",
                        delay,
                        restarts,
                        max_restarts,
                    )
                    time.sleep(delay)

        # Clean shutdown: terminate child, drain queue, flush writer
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self._proc.kill()

        # Drain queue
        while not self._queue.empty():
            try:
                line = self._queue.get_nowait()
                self.frames_seen += 1
                result = parse_line(line)
                if isinstance(result, Record):
                    self.writer.write(result)
                    self.records_written += 1
                else:
                    self._log_reject(result)
            except queue.Empty:
                break

        self.writer.flush()
        self.writer.close()
        self.write_stats()
        logger.info("Capture cleanly shutdown. Written %s records.", self.records_written)
        return 0


def main() -> None:
    cfg = get_config()

    parser = argparse.ArgumentParser(description="Live Network Traffic Analyser capture runner.")
    parser.add_argument("--iface", help="Capture network interface name or index.")
    parser.add_argument(
        "--auto", action="store_true", help="Automatically detect active Wi-Fi interface."
    )
    parser.add_argument("--sink", choices=["file", "tcp", "stdout"], default=cfg.capture.sink)
    parser.add_argument("--format", choices=["csv", "json"], default="csv")
    parser.add_argument("--filter", help="BPF capture filter expression.")
    parser.add_argument("--duration", type=int, help="Capture duration limit in seconds.")
    parser.add_argument("--out-dir", default=cfg.capture.out_dir)
    parser.add_argument("--tshark-path", default="tshark")
    args = parser.parse_args()

    chosen_iface = args.iface
    if args.auto:
        ifaces = list_interfaces(tshark_path=args.tshark_path)
        selected = auto_select_wifi(ifaces)
        if selected is None:
            sys.stderr.write("Error: Could not detect an active Wi-Fi interface.\n")
            sys.exit(2)
        chosen_iface = selected.name
    elif not chosen_iface:
        chosen_iface = cfg.capture.iface

    runner = CaptureRunner(
        iface=chosen_iface,
        sink=args.sink,
        out_format=args.format,
        capture_filter=args.filter,
        duration_s=args.duration,
        out_dir=args.out_dir,
        tcp_host=cfg.flume.tcp_host,
        tcp_port=cfg.flume.tcp_port,
        rotate_seconds=cfg.capture.rotate_seconds,
        rotate_mb=cfg.capture.rotate_mb,
        tshark_path=args.tshark_path,
    )

    sys.exit(runner.run())


if __name__ == "__main__":
    main()
