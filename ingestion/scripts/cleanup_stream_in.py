#!/usr/bin/env python3
"""Remove stable stream_in files older than a retention cutoff."""

from __future__ import annotations

import argparse
import shlex
import subprocess
from datetime import datetime, timedelta, timezone


def parse_listing_line(line: str) -> tuple[datetime, str] | None:
    """Parse a standard ``hdfs dfs -ls`` file line into mtime and path."""
    fields = line.split()
    if len(fields) < 8 or not fields[0].startswith("-"):
        return None
    try:
        modified = datetime.strptime(f"{fields[5]} {fields[6]}", "%Y-%m-%d %H:%M").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    return modified, fields[-1]


def eligible_files(listing: str, cutoff: datetime, root: str = "/traffic/stream_in") -> list[str]:
    """Return only stable files below root older than cutoff."""
    root_prefix = root.rstrip("/") + "/"
    paths: list[str] = []
    for line in listing.splitlines():
        parsed = parse_listing_line(line)
        if parsed is None:
            continue
        modified, path = parsed
        name = path.rsplit("/", 1)[-1]
        if (
            path.startswith(root_prefix)
            and modified < cutoff
            and not name.startswith(".")
            and not name.endswith(".tmp")
        ):
            paths.append(path)
    return paths


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="/traffic/stream_in")
    parser.add_argument("--retention-hours", type=float, default=24)
    parser.add_argument("--hdfs-bin", default="hdfs")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    listing = subprocess.run(
        [args.hdfs_bin, "dfs", "-ls", "-R", args.root],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.retention_hours)
    paths = eligible_files(listing, cutoff, args.root)
    for path in paths:
        command = [args.hdfs_bin, "dfs", "-rm", "-skipTrash", path]
        print("DRY-RUN" if args.dry_run else "REMOVE", shlex.join(command))
        if not args.dry_run:
            subprocess.run(command, check=True)
    print(f"PASS: {len(paths)} eligible stream_in file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
