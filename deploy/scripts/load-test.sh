#!/usr/bin/env bash
# One-command local load coordinator: connection burst + send latency.
# Capacity numbers are local evidence only; Linux multi-source is required
# before quoting 100k.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONNECTIONS="${CONNECTIONS:-1000}"
MESSAGES="${MESSAGES:-1000}"
GATE="${GATE:-ws://127.0.0.1:3553/ws}"

cd "$ROOT"
echo "== connection burst: $CONNECTIONS =="
go run ./tools/load -connections "$CONNECTIONS" -hold 10s -gate "$GATE"

echo "== send latency: $MESSAGES =="
go run ./tools/msgload -messages "$MESSAGES" -gate "$GATE"

echo "== local coordinator done =="
echo "记录：连接数、成功率、P50/P95/P99；未在 Linux 多源验证前不得作为容量承诺。"
