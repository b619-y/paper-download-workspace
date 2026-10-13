# 论文下载工作区

本仓库组织一条可审计的论文流水线：检索式设计 → 多数据库全量导出 → 跨库去重 → 题名/摘要初筛 → 全文下载 → 全文证据筛选与数据提取。

## 强制入口流程

任何 Agent 每次进入本工作区，都必须先读 `01_shared/contracts/WORKFLOW_STATE_SCHEMA.md` 并执行：

```bash
python3 scripts/workflow_status.py --root . --json
```

按照报告顺序判断：任务简报 → 检索式 → API 检索/去重 → 题名摘要初筛 → 下载交接 → 全文下载/验收 → 全文证据筛选。先报告 `stage/reason/evidence/next_action`，再只执行 `next_action` 对应的最小安全步骤。不得靠聊天记忆猜进度，不得跳步，不得重做已有完整产物的阶段。每步完成后再运行一次状态检查。

如果同时存在多个任务或多个可执行批次，先通过 `runtime/ACTIVE_TASK.json` 或显式的 `--task-id` / `--batch-id` 确定唯一对象；无法从用户指令与状态文件唯一确定时，停止写入并请用户选择。

Mac 是 `runtime/ACTIVE_TASK.json` 和 `runtime/tasks/` 的协调权威端。capg Agent 开始前应由 Mac 运行 `scripts/sync_runtime.sh push-workflow --apply`；capg 只把计算结果写入 `runtime/results/`，由 Mac 验收后合并进任务状态。

## 总原则

- 先读 `README_总控.md`、`00_control/POLICIES.md` 和相关合同。
- API 密钥、Cookie、机构登录态、PDF/XML/补充材料、ASReview 项目库、PaperQA 索引均不得提交 Git。
- GitHub 只同步代码、提示词、合同和无敏感信息的配置模板。
- Mac 是检索和授权下载端；capg 是 ASReview、PaperQA2、Codex Luna 与计算端。
- `runtime/` 是跨机运行数据，按 `docs/同步方案.md` 单向同步；同一路径不得双向覆盖。
- 标题/摘要筛选只作优先级和下载决定。摘要未提及某项数据，不等于全文没有。
- 全文结论必须回到已核验 PDF/XML；模型回答只作候选定位，关键结论人工复核。

## 智能体入口

- `workflow_coordinator_agent`：先读取真实文件状态，判断断点并只调度下一阶段。
- `query_agent`：拆解问题，生成 recall / balanced / precision_pilot 检索式。
- `search_agent`：调用 OpenAlex、Crossref、Europe PMC、PubMed、Semantic Scholar、CORE、arXiv、WoS、Scopus，穷尽分页并保存原始响应。
- `dedup_agent`：按 DOI 优先、规范化题名+年份次之去重，冲突不自动吞并。
- `abstract_screening_agent`：使用 ASReview 的题名+摘要主动学习优先排序，并保留人工标签。
- `criteria_screening_agent`：按明确纳入/排除标准进行可解释复核，适合寻找优化方法；不得替代人工最终决定。
- `download_agent`：只下载有权访问的全文并核验身份、页数、哈希与来源。
- `fulltext_screening_agent`：依据全文、表格和图片证据判断是否含目标数据或方法。
- `paperqa_luna_agent`：PaperQA2 解析全文，独立 embedding/检索层提供候选，Codex CLI 中实测可用的 Luna 型号输出结构化证据判断。
- `sync_agent`：Git 同步代码；运行数据严格按方向同步并检查冲突。

各智能体只写其负责的批次或结果目录；总控区由协调者维护。
