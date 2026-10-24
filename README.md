# 论文检索、下载与证据提取工作区

这是一条可审计的论文流水线：从研究问题、多库检索和题名/摘要初筛，到授权全文下载、MinerU/XML 解析、全文证据复筛，以及可选的结构化数据提取。

## 连续流程

| 步骤 | 阶段 | 主要产物 | 必要闸门 |
|---:|---|---|---|
| 0 | 任务定义 | `task_brief.json` | 明确纳入/排除标准与是否需要全文、解析、提取 |
| 1 | 检索式设计 | recall / balanced / precision-pilot 多库检索式 | 种子论文召回和噪声抽样 |
| 2 | API 检索与去重 | 完整分页响应、统一记录、`run_manifest.json` | 所有必需来源 `complete=true` |
| 3 | 题名/摘要初筛 | `pre_screening.csv` | ASReview 只排序；人工/规则记录 `download / exclude / uncertain` |
| 4 | 全文下载与验收 | PDF/XML、下载结果、SHA-256 | 身份、格式、完整性与访问权限通过 |
| 5 | 文档解析与验收 | `full.md`、`section_chunks.jsonl` | 原文/解析文本/章节侧车哈希与覆盖率通过 |
| 6 | 全文证据复筛 | 逐文判断、引文、页码/章节/图表位置 | 关键结论回到已验收原文人工复核 |
| 7 | 可选数据提取与交付 | JSON、CSV、SQLite、`datapackage.json` | 字段合同、证据绑定和人工接受 |

系统不把“检索命中”当作“应该下载”，不把模型分数当作最终纳入结论，也不把解析 Markdown 当作原始证据。

## 快速开始

```bash
python3 scripts/init_download_workspace.py --root .
python3 scripts/workflow_status.py --root . --json
```

每次只执行状态报告中 `next_action` 对应的最小安全步骤，完成后再运行一次状态检查。完整操作方法见 [总控操作手册](README_总控.md)。

## 目录结构

### 编号数据区（00–07）

| 目录 | 职责 | GitHub 是否保存真实运行数据 |
|---|---|---|
| [`00_control/`](00_control/) | 工作区身份、政策、批次索引与决定日志 | 仅合同与空示例 |
| [`01_shared/`](01_shared/) | 跨 Agent 合同、提示词和配置示例 | 是 |
| [`02_batches/`](02_batches/) | 全文下载批次和批次级处理队列 | 否 |
| [`03_review/`](03_review/) | 人工待办、验收和审核记录 | 否 |
| [`04_library_index/`](04_library_index/) | 已验收原文及解析产物索引 | 仅空示例 |
| [`05_archive/`](05_archive/) | 已结束运行材料的本地归档 | 否 |
| [`06_mineru/`](06_mineru/) | 按原文 SHA-256 寻址的 MinerU 派生物 | 否 |
| [`07_xml_markdown/`](07_xml_markdown/) | 按原文 SHA-256 寻址的 XML→Markdown 派生物 | 否 |

### 代码与文档区

- [`.codex/agents/`](.codex/agents/)：各角色入口和权限边界。
- [`.agents/skills/`](.agents/skills/)：可携带的工作区 Skill。
- [`scripts/`](scripts/)：确定性 CLI；按阶段的命令索引见 [scripts/README.md](scripts/README.md)。
- [`docs/`](docs/)：架构、操作、部署与故障恢复；见 [docs/README.md](docs/README.md)。
- [`tests/`](tests/)：状态机、队列、验收和安全边界回归测试。
- [`embedding_service/`](embedding_service/)：可选的独立 embedding 服务。
- `runtime/`：任务、检索、模型结果、队列和交付物；始终不进 Git。

## 文档导航

- [系统架构](docs/系统架构.md)
- [团队体系：角色分工与交接](docs/团队体系.md)
- [下载后文档处理与数据提取](docs/数据提取流程.md)
- [LangGraph 薄总控](docs/LangGraph总控.md)
- [故障恢复智能体](docs/故障恢复智能体.md)
- [跨机同步方案](docs/同步方案.md)

## 安全边界

- API 密钥、Cookie、机构登录态、PDF/XML、解析全文、人工标注、SQLite 队列和模型结果不进 Git。
- 全文下载、外部 MinerU/模型调用、embedding 传输和跨机同步分别需要明确授权。
- 模型可以排序、定位和生成候选；最终纳入、排除、关键数值和发布必须回到已验收原文和人工闸门。
