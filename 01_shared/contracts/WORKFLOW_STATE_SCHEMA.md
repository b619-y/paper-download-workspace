# 工作流状态与当前任务合同

总控 Agent 不得靠对话记忆推断进度。每个研究任务必须有独立目录，当前任务必须有显式指针。

## 1. 当前任务指针

路径：`runtime/ACTIVE_TASK.json`。`runtime/` 不进 Git。

```json
{
  "task_id": "training-data-20261013",
  "task_root": "runtime/tasks/training-data-20261013",
  "batch_ids": [],
  "updated_at": "2026-10-13T15:00:00+08:00"
}
```

- `task_id` 必须与任务目录名和简报一致。
- `task_root` 必须是工作区 `runtime/` 下的相对路径，不得指向外部目录。
- 筛选结果交接成功后，把新的 `batch_id` 追加到 `batch_ids`。
- 如果不存在指针且只有一个任务，状态检查可只读选中它；如果有多个，必须返回歧义并停止写入。

## 2. 任务简报

路径：`runtime/tasks/<task_id>/task_brief.json`。

```json
{
  "task_id": "training-data-20261013",
  "title": "寻找可用的训练数据",
  "research_question": "哪些论文可能提供目标训练数据？",
  "inclusion_criteria": ["..."],
  "exclusion_criteria": ["..."],
  "target_evidence": ["..."],
  "requires_fulltext_screening": true,
  "target_sources": ["openalex", "crossref", "europe_pmc"],
  "known_records": [],
  "limits": {},
  "created_at": "2026-10-13T15:00:00+08:00"
}
```

`task_id`、`title`、`research_question`、`inclusion_criteria`、`exclusion_criteria`、`target_evidence` 和 `requires_fulltext_screening` 为必填。“找方法”与“找数据”的纳排标准不同，应分成两个 `task_id`。

Mac 是这两类状态文件的协调权威端。在 capg 读取状态前先运行 `scripts/sync_runtime.sh push-workflow --apply`；capg 计算结果写入 `runtime/results/`，拉回 Mac 验收后才合并进任务状态。

## 3. 任务内产物

推荐固定布局：

```text
runtime/tasks/<task_id>/
├── task_brief.json
├── query/
│   ├── query_brief.json
│   ├── query_plan.json
│   ├── query_matrix.csv
│   ├── search_strategy.md
│   └── configs/
└── search/
    └── <run_id>/
        ├── run_manifest.json
        ├── all_records.csv
        ├── deduplicated_records.csv
        └── pre_screening.csv
```

同一阶段的文件必须来自同一个任务目录；禁止用全局“最新文件”拼接不同任务的状态。

## 4. 检索式可执行闸门

`query_plan.json` 只有同时满足以下条件，才表示可进入 API 检索：

```json
{
  "final_variant": "recall",
  "validation": {
    "live_counts_recorded": true,
    "known_record_check": "passed",
    "noise_sample_reviewed": true,
    "selection_rationale": "..."
  }
}
```

- `final_variant` 必须是 `recall` / `balanced` / `precision_pilot` 之一，且对应 `configs/<final_variant>.json` 存在。
- `known_record_check` 只允许 `passed` 或 `not_applicable`。
- `query_matrix.csv` 与 `search_strategy.md` 必须存在且非空。
- 工具默认生成的 `final_variant: null` 仍是候选检索式，不是可执行版。

## 5. 批次与全文筛选

- 只处理 `ACTIVE_TASK.json.batch_ids` 列出的批次；兼容旧任务时，可用批次中复制的 `run_manifest.json.query_id` 与当前任务匹配。
- 同时存在多个可执行批次时，先用 `--batch-id` 唯一选择。
- `BLOCKED` / `FAILED` / 索引不一致必须先修复，不得回落到初筛阶段重复入队。
- `REVIEW → ACCEPTED` 只能由 `scripts/accept_batch.py` 执行。它必须在同一批次锁临界区重读并核验 `MANIFEST.csv`、`download_results.csv`、实际 PDF/XML、`manual_backlog.csv`、论文索引和验收记录，然后同步更新 `STATUS.json` 与 `BATCH_INDEX.csv`。
- 只有批次 `ACCEPTED` 且 `requires_fulltext_screening=true` 时，才创建或继续全文筛选队列。
