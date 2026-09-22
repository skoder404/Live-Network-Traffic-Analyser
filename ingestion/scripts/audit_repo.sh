#!/usr/bin/env bash
set -euo pipefail

echo "Checking for tracked raw captures and common secret files..."
if git ls-files | grep -E '(^|/)(capture|pcap|raw|traffic).*(\.pcap|\.pcapng|\.csv)$' >/dev/null; then
  echo "ERROR: tracked capture-like files found" >&2
  exit 1
fi
if git ls-files | grep -E '(^|/)(\.env|settings\.yaml|.*\.pem|.*\.key)$' >/dev/null; then
  echo "ERROR: tracked secret/config files found" >&2
  exit 1
fi
echo "Repository sanitization checks passed"