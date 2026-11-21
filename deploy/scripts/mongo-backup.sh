#!/usr/bin/env bash
# Backup/restore the ZIM MongoDB database from the dev compose stack.
# Usage:
#   ./mongo-backup.sh backup  [out-dir]
#   ./mongo-backup.sh restore <archive.gz>
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE="$ROOT/deploy/compose/docker-compose.dev.yml"
CONTAINER="zim-dev-mongo-1"
DB="zim"

case "${1:-}" in
  backup)
    OUT="${2:-$ROOT/run/backup}"
    mkdir -p "$OUT"
    NAME="zim-$(date +%Y%m%d-%H%M%S).archive.gz"
    docker exec "$CONTAINER" mongodump --db "$DB" --archive="/tmp/$NAME" --gzip
    docker cp "$CONTAINER:/tmp/$NAME" "$OUT/$NAME"
    docker exec "$CONTAINER" rm -f "/tmp/$NAME"
    echo "backup: $OUT/$NAME"
    ;;
  restore)
    ARCHIVE="${2:?usage: $0 restore <archive.gz>}"
    TARGET="/tmp/zim-restore-$$.archive.gz"
    docker cp "$ARCHIVE" "$CONTAINER:$TARGET"
    docker exec "$CONTAINER" mongorestore --drop --archive="$TARGET" --gzip
    docker exec "$CONTAINER" rm -f "$TARGET"
    echo "restored: $ARCHIVE"
    ;;
  *)
    echo "usage: $0 {backup [out-dir]|restore <archive.gz>}" >&2
    exit 2
    ;;
esac
