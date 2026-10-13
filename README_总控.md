# 论文下载工作区

独立、跨 Mac/capg、可同步到私有 GitHub 的论文流水线。它不把“检索命中”直接等同于“应该下载”，也不把模型分数直接等同于最终纳入结论。

## 推荐流程

0. 总控 Agent 运行 `scripts/workflow_status.py`，从实际文件判断断点并只调度下一阶段；
1. 检索式 Agent 生成并实测 recall / balanced / precision_pilot 三套策略；
2. 检索 Agent 通过九个 API 完整分页导出题名、摘要和元数据；
3. 去重 Agent 保留所有来源指针并生成唯一候选清单；
4. ASReview Agent 根据人工少量标签，对题名+摘要进行主动学习排序；
5. 标准复核 Agent 可用 Luna 按纳入/排除标准解释判断；
6. `download + uncertain` 交给下载 Agent 获取并核验全文；
7. PaperQA2、独立 embedding/召回层和 Luna 定位全文证据；
8. 回到 PDF、表格和图片进行最终核验与数据提取。

流程和边界见 `docs/系统架构.md`，跨机规则见 `docs/同步方案.md`。

## 目录

- `.codex/agents/`：总控与各阶段可调用角色入口；
- `.agents/skills/`：检索式与论文下载工作流技能；
- `scripts/`：API 检索、去重交接、批次管理、PaperQA/Luna 和同步脚本；
- `00_control/BATCH_INDEX.csv`：全部批次索引；
- `02_batches/<batch_id>/`：每次下载请求的独立子工作区；
- `03_review/`：验收记录；
- `04_library_index/PAPER_INDEX.csv`：已验收论文索引；
- `runtime/`：不进 Git 的跨机运行数据。

日常入口提示词见 `01_shared/prompts/总控Agent通用提示词.txt`。任何 Agent 接管前都应先运行：

```bash
python3 scripts/workflow_status.py --root . --json
```

多任务隔离、`runtime/ACTIVE_TASK.json` 和各阶段可前进闸门见 `01_shared/contracts/WORKFLOW_STATE_SCHEMA.md`。

## capg 工具

- ASReview：`/data1/sg/tools/asreview/.venv/bin/asreview`
- PaperQA2：`/data1/sg/tools/paperqa/.venv/bin/pqa`
- PaperQA Python：`/data1/sg/tools/paperqa/.venv/bin/python`
- Codex Luna：`/usr/bin/codex exec --model <可用的 Luna 型号> ...`；capg 会话需以实测支持结果为准。

ASReview 服务只监听 capg 回环地址，本地通过 SSH 隧道访问。PaperQA2 与 Luna 已拆开：BGE-M3 embedding 已部署到 capg2，capg 通过 `127.0.0.1:18091` 隧道访问；故障时 `paperqa_luna.py` 可记录原因并退回 BM25。Embedding 主机盘点见 `docs/Embedding主机盘点_2026-10-13.md`。

当前部署与端到端验收结果见 `docs/部署验收_2026-10-13.md`。

## 领取规则

下载 Agent 必须使用原子领取脚本。一个批次只允许一个 Agent 领取。完成后状态写 `REVIEW`，协调 Agent 必须通过 `scripts/accept_batch.py` 的完整性、身份、索引和缺口闸门才能写 `ACCEPTED`；`update_batch_status.py` 不允许直达 `ACCEPTED`。
