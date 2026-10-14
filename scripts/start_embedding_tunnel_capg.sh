#!/usr/bin/env bash
set -euo pipefail

embedding_host="${PAPER_EMBEDDING_SSH_HOST:?set PAPER_EMBEDDING_SSH_HOST to the embedding worker SSH target}"
local_port="${PAPER_EMBEDDING_LOCAL_PORT:-18091}"
remote_port="${PAPER_EMBEDDING_REMOTE_PORT:-8091}"

if ss -ltn | grep -q "127.0.0.1:${local_port}"; then
  echo "BGE-M3 隧道已监听 127.0.0.1:${local_port}"
  exit 0
fi

ssh -fN \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -L "127.0.0.1:${local_port}:127.0.0.1:${remote_port}" "$embedding_host"
curl -sS --max-time 10 "http://127.0.0.1:${local_port}/health"
