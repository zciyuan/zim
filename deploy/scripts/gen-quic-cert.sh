#!/usr/bin/env bash
# Generates a self-signed TLS certificate for the ZIM QUIC gate (LAN/staging).
# Production replaces these files with a real CA certificate; the server paths
# stay the same (/etc/zim/quic/tls.crt|key) so no config change is needed.
set -euo pipefail

OUT_DIR="${1:-deploy/quic}"
HOST="${QUIC_HOST:-192.168.6.88}"
PORT="${QUIC_PORT:-12000}"
DAYS="${QUIC_DAYS:-3650}"

mkdir -p "$OUT_DIR"
openssl req -x509 -newkey rsa:2048 -sha256 -days "$DAYS" -nodes \
  -keyout "$OUT_DIR/tls.key" -out "$OUT_DIR/tls.crt" \
  -subj "/CN=$HOST" \
  -addext "subjectAltName=IP:$HOST,DNS:localhost,DNS:$HOST" \
  -addext "keyUsage=digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=serverAuth"

chmod 600 "$OUT_DIR/tls.key"
echo "generated $OUT_DIR/tls.crt and $OUT_DIR/tls.key for $HOST:$PORT (ALPN zim/1)"
