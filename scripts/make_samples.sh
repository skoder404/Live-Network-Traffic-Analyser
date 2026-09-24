#!/usr/bin/env bash
# scripts/make_samples.sh — Shell runner for sample dataset generation
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

python3 "$REPO_ROOT/scripts/make_samples.py"
