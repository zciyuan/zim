#!/usr/bin/env bash
# Rejects public IPv4 literals in tracked deployment/config/doc files.
#
# Rationale: this repository is published, so real host addresses must live in
# server-side .env files or command-line arguments, never in committed files.
# Allowed: RFC1918 private, loopback, link-local, and RFC5737 documentation ranges.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PATHS=(deploy configs docs README.md AGENTS.md)

allowed() {
  local ip="$1" o1 o2 o3
  IFS=. read -r o1 o2 o3 _ <<<"$ip"
  [[ "$o1" == 10 || "$o1" == 127 || "$o1" == 0 ]] && return 0
  [[ "$o1" == 169 && "$o2" == 254 ]] && return 0
  [[ "$o1" == 192 && "$o2" == 168 ]] && return 0
  [[ "$o1" == 172 && "$o2" -ge 16 && "$o2" -le 31 ]] && return 0
  # RFC 5737 documentation ranges (explicit placeholders).
  [[ "$o1" == 192 && "$o2" == 0 && "$o3" == 2 ]] && return 0
  [[ "$o1" == 198 && "$o2" == 51 && "$o3" == 100 ]] && return 0
  [[ "$o1" == 203 && "$o2" == 0 && "$o3" == 113 ]] && return 0
  return 1
}

bad=0
while IFS=: read -r file line ip; do
  [[ -z "${ip:-}" ]] && continue
  if ! allowed "$ip"; then
    printf '%s:%s: public address %s (use an intranet placeholder or an env var)\n' \
      "$file" "$line" "$ip" >&2
    bad=$((bad + 1))
  fi
# --untracked so a new file cannot bypass the check before it is staged.
done < <(git grep -nEo --untracked '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b' -- "${PATHS[@]}" 2>/dev/null || true)

if ((bad > 0)); then
  echo "found $bad public address(es); keep them in server-side .env/args only" >&2
  exit 1
fi

echo "ok: no public IPv4 literals in ${PATHS[*]}"
