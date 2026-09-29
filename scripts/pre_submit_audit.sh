#!/usr/bin/env bash
# Sanitization audit: no secrets, real captures or leftover markers in Priyan's files
fail=0
for p in "password" "secret" "api_key" "/mnt/Project" "TODO" "FIXME" "\.pcap"; do
  grep -rniIE --exclude-dir=.git --exclude-dir=__pycache__ --exclude=pre_submit_audit.sh "$p" dashboard config docs tests && { echo "FOUND: $p"; fail=1; }
done
ruff check dashboard/ linkanalysis/ tests/ 2>/dev/null
[ $fail -eq 0 ] && echo "Audit clean" || { echo "Audit failed"; exit 1; }
