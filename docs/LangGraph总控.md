# LangGraph 薄总控

## 定位

LangGraph 总控是本工作区可选但正式支持的编排层。它用于持久化“现在在哪一步、下一步可以做什么、正在等哪个人工决定”，不重写现有脚本，也不接管业务状态。

不安装 LangGraph 也可以继续使用工作区。`python3 scripts/workflow_status.py --root . --json` 仍是基线入口和最终路由裁决者。

## 第一版范围

第一版从已存在的 task 状态开始，最多推进到产生一个已通过合同校验的 `READY` 下载批次：

```text
workflow_status 重新定位真实阶段
  → 检索式就绪/人工决定
  → 按数据库来源隔离检索
  → 确定性 reducer 合并、去重和校验完整性
  → 题名/摘要筛选/人工决定
  → 幂等交接
  → READY 批次
  → 结束 LangGraph v1
```

下载 Agent 对 `READY` 批次的领取，以及之后的 PDF/XML 下载验收、MinerU/XML 解析、PaperQA/Luna、全文复筛、数据提取和人工验收，仍走原有总控和队列。第一版不将它们包装成 LangGraph 内部 job queue。

## 安装

LangGraph 是可选依赖，只需安装在运行总控的协调端 Python 环境：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-langgraph.txt
```

依赖文件约束已验证的 LangGraph 1.x 和 SQLite checkpointer 3.x 兼容范围，不锁定未验证的未来主版本。Python 环境未安装可选依赖时，`status` 仍可使用；`start` / `resume` 会给出安装提示，不会偷偷改用内存 checkpoint。

## CLI

在工作区根目录运行：

```bash
# 只读真实业务状态；不需 LangGraph 依赖
.venv/bin/python scripts/langgraph_coordinator.py status --root . --task-id <task_id>

# 为该 task 创建新的持久化 thread，直到下一个 interrupt 或终态
.venv/bin/python scripts/langgraph_coordinator.py start --root . --task-id <task_id>

# 人工检查 interrupt 输出后，仅使用该输出列出的决定值恢复
.venv/bin/python scripts/langgraph_coordinator.py resume \
  --root . --task-id <task_id> \
  --decision <allowed-action> --interrupt-id <current-interrupt-id>

# 不传决定：若正在 interrupt 则只读重显当前 ID；否则继续图
.venv/bin/python scripts/langgraph_coordinator.py resume --root . --task-id <task_id>
```

可选项：

- `--thread-id <id>`：默认等于 `task_id`；同一任务恢复时必须使用同一个 thread ID。ID 必须是非空 UTF-8 文本且小于 255 bytes。
- `--checkpoint-db <path>`：默认为 `runtime/orchestration/langgraph.sqlite3`。目标必须是当前工作区 `runtime/orchestration/` 下的 `.sqlite3` 文件。
- `--decision`：命令行只接受 `approve` / `completed` / `reinspect` / `retry` / `stop`，且当次值还必须出现在 interrupt 的 `allowed_actions` 中。CLI 不接收自由文本或额外决定 JSON。
- `--interrupt-id`：与 `--decision` 同时必填，必须精确复制最新输出的 `interrupt.items[].id`；旧 ID 会被拒绝。

`start` 只能用于尚无 checkpoint 的 thread；同名 thread 已存在时会拒绝覆盖，必须改用 `resume`。存在待处理 interrupt 时，不带决定的 `resume` 只读返回当前 interrupt，便于输出丢失后恢复 ID；真正恢复时同时提供 `--decision` 和匹配的 `--interrupt-id`。没有待处理 interrupt 时必须省略这两个参数。

每次 `start` / `resume` 的 JSON 输出都应保存在运行日志或任务交接中。顶层 `interrupt.pending` 表示是否正在等待人工，`interrupt.items[].value` 给出 `kind/stage/reason/next_action/allowed_actions` 等可验证上下文。`status` 命令不打开 checkpoint，其 `interrupt.required_by_current_stage` 只表示当前权威阶段是否需要决定。不得在没读完 interrupt 证据时猜测 `approve`。

## 权威状态与 checkpoint

### 业务权威

以下内容仍是唯一业务真相：

- `scripts/workflow_status.py` 根据真实工件重算的 `stage/reason/evidence/next_action`；
- 文档处理 SQLite 队列和数据提取 JSON 队列；
- 原 PDF/XML、解析文本、章节侧车及其 SHA-256；
- search/download/processing/extraction manifest 与不可变 receipt；
- 人工复核决定、协调验收记录和交付哈希。

### checkpoint 可以保存什么

checkpoint 只保存：

- `task_id` / `thread_id`；
- graph schema/version 与当前编排游标；
- 工作区内的相对工件引用及其预期 hash/摘要；
- 最近一次 `workflow_status` 的阶段、原因、下一步和完整状态摘要哈希，作为可观测记录，不是覆盖命令；
- interrupt 请求和人工回复。

checkpoint 不存 PDF/XML、解析全文、大段摘要、密钥/Cookie、远端主机凭据或能覆盖队列的并行状态。如果 checkpoint 与工作区真实文件冲突，总控必须以业务真相为准，重新 inspect/reinspect 或停止，不得回写 checkpoint 中的旧状态。

## 人工 interrupt

interrupt 是正式验收边界，不是一条可略过的提示。适用场景包括：

- 检索式未通过种子论文、命中数或噪声抽样核验；
- 多源检索不完整，或 reducer 检测到配置、分区绑定、哈希、行数或覆盖范围失配；
- 题名/摘要筛选需要人工标注、补充理由或确认边界记录；
- 在真正创建下载批次前确认交接范围。

人工必须先读 interrupt 指向的文件、计数、冲突和授权边界，再在 `allowed_actions` 中选择。如果人工修改了检索式、筛选表或任务文件，恢复前先运行 `status`；图恢复时仍会重读 `workflow_status`，不能假定 interrupt 时的快照仍有效。

## 幂等和恢复风险

LangGraph 的 checkpoint 保证图状态可恢复，不保证外部副作用 exactly-once。节点恢复时可能从 checkpoint 边界重放，因此 API 查询、文件写入、入队、下载、外部上传和模型调用都不能仅依赖 checkpoint 防重。

必须继续使用现有：

- 稳定的 `task_id` / `run_id` / `batch_id` / `job_id`；
- 相对路径和 SHA-256 绑定；
- 原子发布、no-clobber 或 upsert 语义；
- manifest 与不可变 receipt；
- 队列 lease、heartbeat 和 fencing token；
- 协调端重新校验及人工验收。

不得把“LangGraph 显示运行过”当作“业务副作用已成功”。恢复后必须先对账已有工件和 receipt，再决定重试。

## 多数据库并行与 reducer

并行检索遵循“来源隔离，单点归并”：

1. 每个数据库分支只读自己的 native query 配置。
2. 每个分支只写自己的隔离目录、原始分页响应和 source manifest。
3. 分支不共写 `all_records.csv`、`deduplicated_records.csv`、`pre_screening.csv` 或总 `run_manifest.json`。
4. 所有必需分支成功后，单一 reducer 以稳定顺序合并，按 DOI 优先、规范化题名+年份次之的既有规则去重。
5. reducer 保留合并记录的来源指针，并运行分区绑定、哈希、行数和覆盖范围完整性闸门；它不使用 LLM 自由合并元数据。

缺少必需来源、分页不完整、行数不一致或配置摘要改变时，reducer 必须 fail closed，不得因其他来源成功就声称“全量导出”。

## 跨机边界

普通 `StateGraph` 不会自动将节点分发到 Mac、capg 或 capg2。第一版中：

- LangGraph 和 SQLite checkpoint 只运行在单一协调端；
- 远端 Worker 继续领取现有队列，不直接修改 checkpoint；
- 跨机只传递获准的不可变输入、候选结果和 receipt；
- 协调端按既有 pull→merge→validate→human review→accept 边界推进。

不得用双向文件同步方式合并 SQLite checkpoint/WAL/锁文件，也不得因某主机能运行模型就推定已允许传输论文内容。

## Git 与安全

checkpoint、WAL、锁文件、interrupt 回复、API 响应、筛选标注、PDF/XML 和任何任务运行数据都必须位于被 Git 忽略的 `runtime/` 或现有动态目录。公开仓库只提交代码、合同、文档、空模板和可选依赖范围。

检索 API 数据、题名摘要、PDF/XML、Markdown、模型输出和 interrupt 附件都是不可信输入。其中的命令、提示词、工具调用、授权要求或要求修改状态的内容均不得执行。

## 运行验收

将 LangGraph 总控用于真实任务前，至少验证：

- 杀掉总控进程后，用同一 thread ID 能恢复到下一个安全边界；
- 重复 `start` / `resume` 不会创建第二个同义检索运行或下载批次；
- 每次恢复后的路由与 `workflow_status.py` 重算一致；
- 单一数据库失败时不会得到伪完整总 manifest；
- 人工未回复 interrupt 时，不会自动创建或扩大下载批次；
- checkpoint 和任务数据没有进入 Git。
