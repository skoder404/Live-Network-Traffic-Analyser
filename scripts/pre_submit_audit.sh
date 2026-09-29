#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RUFF_BIN="${RUFF_BIN:-$ROOT_DIR/.venv/bin/ruff}"
PYTEST_BIN="${PYTEST_BIN:-$ROOT_DIR/.venv/bin/pytest}"
fail=0

check() {
  local name="$1"; shift
  if "$@"; then echo "PASS: $name"; else echo "FAIL: $name"; fail=1; fi
}

check captures "$PYTHON_BIN" scripts/check_no_captures.py
check wording bash -c '! git grep -niE "unit[ -]?(iv|4)" -- . ":!scripts/pre_submit_audit.sh"'
check tracked-data bash -c '! git log --all --name-only --format= -- data | grep -Ev "^data/sample(/|$)"'
check history-files bash -c '! git log --all --name-only --format= | grep -Ev "(^|/)logs/\.gitkeep$" | grep -E "(\.pcap(ng)?$|(^|/)\.env($|\.)|(^|/)config/settings\.yaml$|(^|/)serving/.*\.db$|(^|/)logs/)"'
check ruff "$RUFF_BIN" check .
check tests "$PYTEST_BIN" -m 'not spark and not e2e'

if git show-ref --tags --verify --quiet refs/tags/demo-v1; then
  echo "PASS: release-tag"
else
  echo "FAIL: release-tag (create locally with: git tag demo-v1)"
  fail=1
fi

if [[ "$fail" -eq 0 ]]; then
  echo "Audit clean"
else
  echo "Audit failed"
  exit 1
fi
