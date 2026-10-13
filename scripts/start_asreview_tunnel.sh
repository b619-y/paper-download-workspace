#!/usr/bin/env bash
set -euo pipefail
echo "浏览器打开 http://127.0.0.1:5001；按 Ctrl-C 关闭隧道。"
ssh -N -L 5001:127.0.0.1:5001 capg
