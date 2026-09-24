"""Offline ingestion verification for CSV files produced by the capture layer."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from contracts.record_schema import FIELD_NAMES


def _parse_timestamp(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _row_hash(row: list[str]) -> str:
    return hashlib.sha256(",".join(row).encode("utf-8")).hexdigest()


def verify_files(paths: Iterable[str | Path], now: datetime | None = None) -> dict[str, Any]:
    """Measure malformed rows, duplicates, and event-to-observation latency.

    A duplicate is an identical complete CSV row. Packet loss cannot be inferred
    without a producer sequence number, so the report exposes that limitation and
    uses the observed duplicate/malformed counts as the actionable loss signal.
    """
    rows = 0
    malformed = 0
    hashes: Counter[str] = Counter()
    latencies_ms: list[float] = []
    event_times: list[datetime] = []
    file_count = 0
    reference = now or datetime.now(timezone.utc)

    for raw_path in paths:
        path = Path(raw_path)
        if not path.is_file() or path.name.startswith(".") or path.name.endswith(".tmp"):
            continue
        file_count += 1
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.reader(handle):
                if not row or not any(row):
                    continue
                rows += 1
                if len(row) != len(FIELD_NAMES) or not row[0]:
                    malformed += 1
                    continue
                hashes[_row_hash(row)] += 1
                event_time = _parse_timestamp(row[0])
                if event_time is not None:
                    event_times.append(event_time)
                    latencies_ms.append(max(0.0, (reference - event_time).total_seconds() * 1000))

    duplicate_rows = sum(count - 1 for count in hashes.values() if count > 1)
    return {
        "files": file_count,
        "records": rows,
        "valid_records": rows - malformed,
        "malformed_records": malformed,
        "duplicate_records": duplicate_rows,
        "loss_check": "not_available_without_sequence_id",
        "latency_ms": {
            "min": round(min(latencies_ms), 2) if latencies_ms else None,
            "max": round(max(latencies_ms), 2) if latencies_ms else None,
            "avg": round(sum(latencies_ms) / len(latencies_ms), 2) if latencies_ms else None,
        },
        "first_event": min(event_times).isoformat() if event_times else None,
        "last_event": max(event_times).isoformat() if event_times else None,
        "verified_at": reference.isoformat(),
    }


def verify_directory(directory: str | Path) -> dict[str, Any]:
    """Verify all stable CSV files below a directory."""
    root = Path(directory)
    return verify_files(sorted(root.rglob("*.csv")) if root.exists() else [])


def write_report(report: dict[str, Any], destination: str | Path) -> None:
    """Write a status report atomically so readers never see partial JSON."""
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    temporary.replace(target)


def freshness_seconds(report: dict[str, Any]) -> float | None:
    """Return age of the latest observed event relative to report generation."""
    last_event = report.get("last_event")
    verified_at = report.get("verified_at")
    if not last_event or not verified_at:
        return None
    return max(
        0.0,
        (datetime.fromisoformat(verified_at) - datetime.fromisoformat(last_event)).total_seconds(),
    )
