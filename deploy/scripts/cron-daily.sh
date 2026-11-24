#!/bin/bash
# Daily docker-compose disk maintenance (install via /etc/cron.d/zim-maintenance,
# see ZIM-03 §2.14). Only removes safe-to-drop data:
#   - nacos logs older than 3 days (the nacos-log volume grows ~450MB/day)
#   - dangling docker images (referenced by no container)
#   - transport tarballs in $HOME older than 7 days
# Never touches running containers, named data volumes, or other projects.
set -u

log() { echo "[$(date '+%F %T')] $*"; }

log "before: $(df -h / | tail -1 | awk '{print $4}') free"

# 1. Nacos log rotation. The volume name follows the compose project name;
#    the container path is /home/nacos/logs inside the nacos service.
NACOS_LOGS=/var/lib/docker/volumes/zim-dev_nacos-logs/_data
if [ -d "$NACOS_LOGS" ]; then
  before=$(du -sm "$NACOS_LOGS" 2>/dev/null | cut -f1)
  find "$NACOS_LOGS" -type f -mtime +3 -delete 2>/dev/null
  after=$(du -sm "$NACOS_LOGS" 2>/dev/null | cut -f1)
  log "nacos logs: ${before}MB -> ${after}MB"
else
  log "nacos logs dir missing, skip"
fi

# 2. Dangling images only (-f, not -a): referenced images stay.
docker image prune -f | tail -1

# 3. Old transport tarballs.
find /root -maxdepth 1 -name '*.tar*' -mtime +7 -delete 2>/dev/null

log "after: $(df -h / | tail -1 | awk '{print $4}') free"
