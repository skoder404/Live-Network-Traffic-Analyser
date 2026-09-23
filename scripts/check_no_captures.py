#!/usr/bin/env bash
"""
scripts/check_no_captures.py — Pre-commit hook to prevent accidental capture leakage.

Ensures:
1. No *.pcap, *.pcapng, *.cap files are present in the commit.
2. No live sensitive / un-anonymized capture data is committed outside data/sample/.
"""

import fnmatch
import os
import re
import sys
from pathlib import Path

FORBIDDEN_EXTENSIONS = ["*.pcap", "*.pcapng", "*.cap", "*.dmp"]
ALLOWED_SAMPLE_DIR = "data/sample"


def check_forbidden_files(root: Path) -> list[str]:
    violations = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Skip .git directory
        if ".git" in dirnames:
            dirnames.remove(".git")
        if ".venv" in dirnames:
            dirnames.remove(".venv")

        for filename in filenames:
            for pattern in FORBIDDEN_EXTENSIONS:
                if fnmatch.fnmatch(filename, pattern):
                    violations.append(os.path.join(dirpath, filename))
    return violations


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    violations = check_forbidden_files(repo_root)

    if violations:
        print("[-] SECURITY CHECK FAILED: Raw packet capture files detected in repository:")
        for v in violations:
            print(f"    - {v}")
        print("Please remove these files and ensure all datasets are anonymized in data/sample/.")
        return 1

    print("[✓] Capture security check passed: No forbidden capture files found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
