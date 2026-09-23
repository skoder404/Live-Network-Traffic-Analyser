#!/usr/bin/env python3
"""Control and verify the ingestion pipeline without requiring a live capture."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ingestion.layout import (
    StorageLayout,
    create_hdfs_zones,
    create_local_zones,
    hdfs_available,
)
from ingestion.verification import verify_directory, write_report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "status", "verify"))
    parser.add_argument("--root", default="/traffic", help="HDFS root or local demo root")
    parser.add_argument("--local", action="store_true", help="Use local folders instead of HDFS")
    parser.add_argument("--report", default="logs/ingestion_status.json")
    parser.add_argument("--hdfs-bin", default="hdfs")
    return parser


def main() -> int:
    args = _parser().parse_args()
    layout = StorageLayout(args.root)
    use_hdfs = not args.local and hdfs_available(args.hdfs_bin)
    if args.command == "init":
        if use_hdfs:
            create_hdfs_zones(layout, args.hdfs_bin)
            print(json.dumps({"backend": "hdfs", "zones": list(layout.all_zones())}, indent=2))
        else:
            create_local_zones(layout)
            print(json.dumps({"backend": "local", "zones": list(layout.all_zones())}, indent=2))
        return 0

    report = verify_directory(Path(layout.stream_in))
    write_report(report, args.report)
    print(json.dumps(report, indent=2))
    if args.command == "status":
        return 0
    return 0 if report["malformed_records"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
