"""
capture/writers.py — Pluggable output writers for captured traffic records.

Provides an abstract Writer interface and four implementations:
1. CsvRotatingWriter: Rotates by time (rotate_seconds) or size (rotate_mb), headerless,
   lazy file creation on first packet, flushes <= 1s.
2. JsonLinesWriter: Writes JSON objects per line with contract field names.
3. TcpLineWriter: Emits lines to a TCP socket (e.g. Flume netcat source) with reconnection
   and bounded buffering / drop counting.
4. StdoutWriter: Emits CSV rows to standard output.
"""

from __future__ import annotations

import json
import socket
import sys
import time
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from contracts.record_schema import FIELD_NAMES, format_row


class Writer(ABC):
    """Abstract base class for traffic record writers."""

    @abstractmethod
    def write(self, record: Any) -> None:
        """Write a single record (Record instance or dict)."""
        pass

    @abstractmethod
    def flush(self) -> None:
        """Flush any pending buffered records."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close writer and release associated resources."""
        pass


def record_to_dict(record: Any) -> dict[str, Any]:
    """Helper to normalise Record instances, dicts, or lists to contract dicts."""
    if hasattr(record, "to_dict"):
        return record.to_dict()
    if isinstance(record, dict):
        return record
    if isinstance(record, (list, tuple)):
        return dict(zip(FIELD_NAMES, record, strict=True))
    raise TypeError(f"Unsupported record type: {type(record)}")


class CsvRotatingWriter(Writer):
    """
    Rotating CSV writer.
    - Creates files matching `traffic_YYYYMMDD_HHMMSS.csv` in `out_dir`.
    - No header line.
    - Creates the first file only when the first record arrives.
    - Rotates when elapsed time >= rotate_seconds OR file size >= rotate_mb.
    - After rotation, previous files are never modified again.
    - Flushes buffer at least every 1 second.
    """

    def __init__(
        self,
        out_dir: str | Path,
        rotate_seconds: int = 60,
        rotate_mb: int = 10,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.out_dir = Path(out_dir)
        self.rotate_seconds = rotate_seconds
        self.rotate_bytes = rotate_mb * 1024 * 1024
        self.clock = clock or time.time

        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._current_file = None
        self._current_path: Path | None = None
        self._file_start_time: float | None = None
        self._bytes_written: int = 0
        self._last_flush_time: float = 0.0

    def _should_rotate(self, now: float) -> bool:
        if self._current_file is None:
            return True
        if self.rotate_seconds > 0 and self._file_start_time is not None:
            if (now - self._file_start_time) >= self.rotate_seconds:
                return True
        if self.rotate_bytes > 0:
            if self._bytes_written >= self.rotate_bytes:
                return True
        return False

    def _rotate_if_needed(self, now: float) -> None:
        if self._should_rotate(now):
            self._close_current()
            self._open_new_file(now)

    def _open_new_file(self, now: float) -> None:
        dt = datetime.fromtimestamp(now, tz=timezone.utc)
        base_name = f"traffic_{dt.strftime('%Y%m%d_%H%M%S')}"
        file_path = self.out_dir / f"{base_name}.csv"

        counter = 1
        while file_path.exists():
            file_path = self.out_dir / f"{base_name}_{counter}.csv"
            counter += 1

        self._current_path = file_path
        self._current_file = open(file_path, "a", encoding="utf-8", buffering=1)
        self._file_start_time = now
        self._bytes_written = 0
        self._last_flush_time = now

    def _close_current(self) -> None:
        if self._current_file is not None:
            try:
                self._current_file.flush()
                self._current_file.close()
            except Exception:
                pass
            self._current_file = None
            self._current_path = None
            self._file_start_time = None
            self._bytes_written = 0

    def write(self, record: Any) -> None:
        now = self.clock()
        self._rotate_if_needed(now)

        rec_dict = record_to_dict(record)
        line = format_row(rec_dict) + "\n"
        encoded = line.encode("utf-8")

        assert self._current_file is not None
        self._current_file.write(line)
        self._bytes_written += len(encoded)

        if (now - self._last_flush_time) >= 1.0:
            self._current_file.flush()
            self._last_flush_time = now

    def flush(self) -> None:
        if self._current_file is not None:
            self._current_file.flush()
            self._last_flush_time = self.clock()

    def close(self) -> None:
        self._close_current()


class JsonLinesWriter(Writer):
    """Writes records as JSON-lines, one JSON object per row, nulls preserved."""

    def __init__(self, file_path: str | Path) -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.file_path, "a", encoding="utf-8", buffering=1)

    def write(self, record: Any) -> None:
        rec_dict = record_to_dict(record)
        # Ensure exact contract field keys are represented
        ordered_dict = {k: rec_dict.get(k) for k in FIELD_NAMES}
        line = json.dumps(ordered_dict) + "\n"
        self._file.write(line)

    def flush(self) -> None:
        if self._file is not None and not self._file.closed:
            self._file.flush()

    def close(self) -> None:
        if self._file is not None and not self._file.closed:
            self._file.flush()
            self._file.close()


class TcpLineWriter(Writer):
    """
    Sends contract CSV lines over TCP (e.g. to Apache Flume Netcat source).
    Buffers records when disconnected up to buffer_size, then drops with counter.
    Automatically reconnects with exponential back-off.
    """

    def __init__(
        self,
        host: str,
        port: int,
        buffer_size: int = 1000,
        connect_timeout: float = 2.0,
    ) -> None:
        self.host = host
        self.port = port
        self.buffer_size = buffer_size
        self.connect_timeout = connect_timeout

        self._buffer: deque[str] = deque(maxlen=buffer_size)
        self._socket: socket.socket | None = None
        self.dropped_records: int = 0
        self.records_sent: int = 0

        self._last_connect_attempt: float = 0.0
        self._backoff: float = 0.5
        self._max_backoff: float = 8.0

    def _ensure_connected(self) -> bool:
        if self._socket is not None:
            return True

        now = time.time()
        if (now - self._last_connect_attempt) < self._backoff:
            return False

        self._last_connect_attempt = now
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.connect_timeout)
            s.connect((self.host, self.port))
            s.settimeout(None)
            self._socket = s
            self._backoff = 0.5  # reset backoff on success

            # Flush buffered lines upon connection
            while self._buffer:
                line = self._buffer.popleft()
                self._socket.sendall(line.encode("utf-8"))
                self.records_sent += 1
            return True
        except Exception:
            self._socket = None
            self._backoff = min(self._backoff * 2.0, self._max_backoff)
            return False

    def write(self, record: Any) -> None:
        rec_dict = record_to_dict(record)
        line = format_row(rec_dict) + "\n"

        connected = self._ensure_connected()
        if connected and self._socket is not None:
            try:
                self._socket.sendall(line.encode("utf-8"))
                self.records_sent += 1
                return
            except Exception:
                self._socket = None

        # Disconnected: queue into buffer
        if len(self._buffer) >= self.buffer_size:
            self.dropped_records += 1
        self._buffer.append(line)

    def flush(self) -> None:
        self._ensure_connected()
        if self._socket is not None and self._buffer:
            try:
                while self._buffer:
                    line = self._buffer.popleft()
                    self._socket.sendall(line.encode("utf-8"))
                    self.records_sent += 1
            except Exception:
                self._socket = None

    def close(self) -> None:
        self.flush()
        if self._socket is not None:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None


class StdoutWriter(Writer):
    """Outputs CSV records directly to stdout."""

    def write(self, record: Any) -> None:
        rec_dict = record_to_dict(record)
        line = format_row(rec_dict)
        try:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
        except (BrokenPipeError, OSError):
            pass

    def flush(self) -> None:
        try:
            sys.stdout.flush()
        except (BrokenPipeError, OSError):
            pass

    def close(self) -> None:
        self.flush()
