"""
scripts/compare_capture.py — Validation script comparing pcap captures with pipeline CSVs.

Counts IP/IPv6 frames in a raw .pcap capture via tshark and compares against
the number of parsed records in the output CSV files, verifying capture completeness
and asserting loss <= 1%.
"""

from __future__ import annotations

import argparse
import glob
import subprocess
import sys
from pathlib import Path


def count_pcap_ip_frames(pcap_path: str | Path, tshark_path: str = "tshark") -> int:
    """Execute tshark on pcap to count frames matching 'ip || ipv6'."""
    cmd = [
        tshark_path,
        "-r",
        str(pcap_path),
        "-Y",
        "ip || ipv6",
        "-T",
        "fields",
        "-e",
        "frame.number",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        lines = [line for line in proc.stdout.strip().splitlines() if line.strip()]
        return len(lines)
    except FileNotFoundError:
        sys.stderr.write(f"Error: {tshark_path} not found.\n")
        raise
    except subprocess.CalledProcessError as e:
        sys.stderr.write(f"Error executing tshark: {e.stderr}\n")
        raise


def count_csv_records(csv_glob_pattern: str) -> int:
    """Count total data lines across matching CSV files."""
    matched_files = glob.glob(csv_glob_pattern)
    total_records = 0
    for file_path in matched_files:
        with open(file_path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    total_records += 1
    return total_records


def compare_counts(
    pcap_count: int,
    csv_count: int,
    threshold_pct: float = 1.0,
) -> tuple[bool, float]:
    """Compare counts and return (pass/fail, percentage difference)."""
    if pcap_count == 0 and csv_count == 0:
        return True, 0.0
    if pcap_count == 0:
        return False, 100.0

    diff = abs(csv_count - pcap_count)
    diff_pct = (diff / float(pcap_count)) * 100.0
    passed = diff_pct <= threshold_pct
    return passed, diff_pct


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare pcap frame count with CSV records.")
    parser.add_argument("--pcap", required=True, help="Path to reference .pcap file.")
    parser.add_argument(
        "--csv-glob",
        default="data/capture/*.csv",
        help="Glob pattern matching captured CSV files.",
    )
    parser.add_argument(
        "--threshold-pct",
        type=float,
        default=1.0,
        help="Allowed percentage difference (default: 1.0%%).",
    )
    parser.add_argument("--tshark-path", default="tshark", help="Path to tshark executable.")
    args = parser.parse_args()

    try:
        pcap_count = count_pcap_ip_frames(args.pcap, tshark_path=args.tshark_path)
    except Exception as e:
        sys.stderr.write(f"Failed to inspect PCAP: {e}\n")
        sys.exit(2)

    csv_count = count_csv_records(args.csv_glob)

    passed, diff_pct = compare_counts(pcap_count, csv_count, threshold_pct=args.threshold_pct)

    status = "PASS" if passed else "FAIL"
    print(f"\nCapture Completeness Validation: [{status}]")
    print(f"  Reference PCAP IP Frames: {pcap_count}")
    print(f"  Captured CSV Records:     {csv_count}")
    print(f"  Difference:               {abs(csv_count - pcap_count)} ({diff_pct:.2f}%)")
    print(f"  Allowed Threshold:        {args.threshold_pct:.2f}%\n")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
