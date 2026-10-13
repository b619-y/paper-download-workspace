#!/usr/bin/env bash
set -euo pipefail

root="/data1/sg/paper-workspace"
asreview="/data1/sg/tools/asreview/.venv/bin/asreview"
mkdir -p "$root/runtime/asreview" "$root/runtime/logs"

if ss -ltn | grep -q "127.0.0.1:5001"; then
  echo "ASReview 已监听 127.0.0.1:5001"
  exit 0
fi

nohup env ASREVIEW_PATH="$root/runtime/asreview" \
  "$asreview" lab --host 127.0.0.1 --port 5001 --port-retries 0 \
  --no-browser --skip-update-check \
  > "$root/runtime/logs/asreview.log" 2>&1 < /dev/null &
echo $! > "$root/runtime/asreview.pid"
echo "ASReview 启动中；日志：$root/runtime/logs/asreview.log"
