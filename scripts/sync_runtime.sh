#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
apply="${2:-}"
mac_root="/Users/b/Desktop/智能体/论文下载工作区/runtime"
remote_root="/data1/sg/paper-workspace/runtime"
opts=(-av --ignore-existing --itemize-changes)
state_opts=(-av --itemize-changes --backup --backup-dir="$remote_root/conflict_backups/workflow-state")

if [[ "$apply" != "--apply" ]]; then
  opts+=(--dry-run)
  state_opts+=(--dry-run)
  echo "预览模式；确认无误后追加 --apply"
fi

case "$mode" in
  push-inputs)
    rsync "${opts[@]}" "$mac_root/inputs/" "capg:$remote_root/inputs/"
    ;;
  push-workflow)
    if [[ ! -f "$mac_root/ACTIVE_TASK.json" ]]; then
      echo "缺少 $mac_root/ACTIVE_TASK.json；先在 Mac 创建当前任务指针" >&2
      exit 2
    fi
    if [[ ! -d "$mac_root/tasks" ]]; then
      echo "缺少 $mac_root/tasks；先在 Mac 初始化任务" >&2
      exit 2
    fi
    rsync "${state_opts[@]}" "$mac_root/ACTIVE_TASK.json" "capg:$remote_root/ACTIVE_TASK.json"
    rsync "${state_opts[@]}" "$mac_root/tasks/" "capg:$remote_root/tasks/"
    ;;
  pull-results)
    rsync "${opts[@]}" "capg:$remote_root/results/" "$mac_root/results/"
    ;;
  *)
    echo "用法: $0 {push-inputs|push-workflow|pull-results} [--apply]" >&2
    exit 2
    ;;
esac
