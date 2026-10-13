#!/usr/bin/env bash
set -euo pipefail

if ss -ltn | grep -q "127.0.0.1:18091"; then
  echo "BGE-M3 隧道已监听 127.0.0.1:18091"
  exit 0
fi

ssh -fN \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -L 127.0.0.1:18091:127.0.0.1:8091 capg2
curl -sS --max-time 10 http://127.0.0.1:18091/health
