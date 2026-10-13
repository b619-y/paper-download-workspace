#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
apply="${2:-}"
mac_root="/Users/b/Desktop/智能体/论文下载工作区/runtime"
remote_root="/data1/sg/paper-workspace/runtime"
opts=(-av --ignore-existing --itemize-changes)

if [[ "$apply" != "--apply" ]]; then
  opts+=(--dry-run)
  echo "预览模式；确认无误后追加 --apply"
fi

case "$mode" in
  push-inputs)
    rsync "${opts[@]}" "$mac_root/inputs/" "capg:$remote_root/inputs/"
    ;;
  pull-results)
    rsync "${opts[@]}" "capg:$remote_root/results/" "$mac_root/results/"
    ;;
  *)
    echo "用法: $0 {push-inputs|pull-results} [--apply]" >&2
    exit 2
    ;;
esac
