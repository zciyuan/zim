#!/bin/bash
# ZIM docker-compose redeploy entry point (works in any checkout layout:
# deploy/scripts/redeploy.sh -> deploy/compose).
#
#   redeploy.sh            # restart with existing image (no build, no disk growth)
#   redeploy.sh --build    # rebuild image, start, then purge build cache
#
# Disk discipline (see ZIM-03 §2.14): every `docker compose build` appends
# ~4GB of BuildKit layers that nothing reaps automatically. Build only when
# the code changed, and always let this script purge right after.
set -eu
cd "$(dirname "$0")/../compose"

if [ "${1:-}" = "--build" ]; then
  echo "=== rebuild zim-server:dev ==="
  docker compose -f docker-compose.dev.yml build
fi

echo "=== start/update stack ==="
docker compose -f docker-compose.dev.yml up -d

if [ "${1:-}" = "--build" ]; then
  echo "=== purge build cache and dangling images ==="
  docker builder prune -af
  docker image prune -f
fi

echo "=== status ==="
docker compose -f docker-compose.dev.yml ps --format '{{.Name}}\t{{.Status}}'
echo "=== disk ==="
df -h / | tail -1
