#!/usr/bin/env bash
# Verify every binary fails fast when its config directory is missing.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

services=(
  zim-gate zim-node-msg zim-node-conv zim-mesh-auth zim-mesh-user
  zim-mesh-relation zim-mesh-group zim-transfer zim-push zim-api zim-crontask
)

fail=0
for svc in "${services[@]}"; do
  out="$(timeout 5s "build/bin/$svc" -etc /nonexistent 2>&1 || true)"
  if [[ -z "$out" ]]; then
    echo "FAIL $svc: no error output"
    fail=1
    continue
  fi
  if [[ "$out" == *"panic"* && "$out" != *"config"* && "$out" != *"etc"* ]]; then
    echo "FAIL $svc: panic without config error"
    fail=1
    continue
  fi
  echo "PASS $svc: $(echo "$out" | tail -n 1)"
done

exit "$fail"
