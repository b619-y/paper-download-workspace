#!/usr/bin/env bash
set -euo pipefail
asreview_host="${PAPER_ASREVIEW_SSH_HOST:?set PAPER_ASREVIEW_SSH_HOST to the ASReview worker SSH target}"
echo "浏览器打开 http://127.0.0.1:5001；按 Ctrl-C 关闭隧道。"
ssh -N -L 5001:127.0.0.1:5001 "$asreview_host"
