---
name: paper-download-workspace
description: 在本机组织“多数据库检索式设计 → API 检索与全量导出 → 题名/摘要初筛 → 全文下载 → 基于文本、表格和论文图的证据复筛”的论文工作流，并创建、领取、验收和交接下载批次；CNKI 条目按已授权的 Edge/WebVPN 流程逐篇核验。用于从研究问题生成检索式、按检索式导出论文、论文初筛、加入下载队列、让下载 Agent 处理批次、全文或图中数据筛选和查看进度；单篇即时下载交给 cnki-download。
---

# 论文下载工作区

默认工作区：`/Users/b/Desktop/论文下载`。

本技能把论文检索与下载需求转成可追踪的五段式流程。五种角色通过检索策略、原始响应、运行清单、CSV 和批次文件交接，不靠聊天记忆传递结论：

1. **检索式Agent**：从研究问题建立概念块和词汇层，生成高召回、平衡、高精度试探三套方案，翻译并实测各数据库语法。
2. **论文检索Agent**：通过 API 穷尽选定检索式的分页，保留原始响应并生成统一候选记录。
3. **论文筛选Agent（下载前初筛）**：仅依据题名和摘要做 `download / exclude / uncertain` 判断，产出下载候选 CSV。
4. **任务下载Agent**：只接收初筛后需要全文的记录，领取独立批次并下载、核验全文。
5. **任务筛选Agent（全文复筛）**：只对身份已核验且可读取的 PDF/XML 判断是否含目标数据或相关内容，逐条给出原文证据和位置。

筛选与下载阶段的详细输入、判定语义和交接表见[筛选与下载协议](references/screening-protocol.md)；检索来源、配置和完整性判定见[文献检索 API 适配器](references/literature-api-adapters.md)。原有下载批次、下载结果和验收机制保持不变。

在 `/Users/b/Desktop/论文下载` 工作时，调用本技能的当前智能体可直接充当下载Agent，使用已有脚本领取和执行批次；**不需要 Cursor CLI 或另一个Agent作为启动器**。若当前环境缺少可用的授权浏览器连接，记录工具障碍与未完成条目，不把它当成论文无全文。

## 用户口令

当用户只给研究问题、主题或自然语言目标并要求检索论文时，先调用 `literature-query-agent` 生成和验证多套检索式，再把选定的 `configs/<variant>.json` 交给本技能执行。生物医学 PubMed 检索由 `pubmed-search-builder-next` 先完成高召回式、MeSH 核验和 PRESS 结构化内部审查。用户已经提供数据库原生检索式时，可以从论文检索Agent阶段开始，但仍需记录其来源和适用数据库。

当用户要求“按检索式筛论文”“先筛选再下载”“找出需要下载的论文”时，当前智能体充当**论文筛选Agent**：先确认或整理纳入/排除标准，再按[筛选与下载协议](references/screening-protocol.md)执行题名/摘要初筛。没有摘要时不得假装已完成摘要筛选；可标记 `uncertain`，或补齐摘要后再判。初筛输出中只有 `download` 和因全文信息不足而保留的 `uncertain` 条目可以进入下载批次，明确 `exclude` 的条目不得下载。

当用户明确说“加入论文下载工作区”时，直接执行入队，不再询问是否创建。优先使用当前消息、最近明确提到的论文清单或用户给出的CSV/Markdown文件。若无法确定任何论文题名、DOI或清单路径，才询问具体要加入哪些论文。

当用户要求“检索后整理清单并交给下载Agent”“从检索一直做到下载队列”时，先完成 API 检索和题名/摘要初筛，再运行 `scripts/handoff_screening_to_download.py`。该脚本二次去重、验证筛选决定和检索完整性、生成 `download_manifest.csv`，并直接创建 `READY` 批次。创建 `READY` 表示已交给任务下载Agent的平台队列，不等于全文已经下载。

当用户要求“让下载 Agent 处理/继续已有批次”时，按下文领取 `READY` 批次并执行，不要重复入队。用户只要求即时下载一篇 CNKI 论文、没有工作区批次时，直接使用 `cnki-download`；不要为此强制建批次。

当用户要求“根据全文筛选”“判断是否包含想要的数据/内容”或下载批次完成后继续筛选时，当前智能体充当**任务筛选Agent**：先用 `scripts/make_fulltext_screening_queue.py <batch_id>` 生成全文筛选队列，再按协议逐项填写判断和证据。**若目标数据可能只在图中，必须先定位并下载目标图，阅读图像后才能完成该项判断；不得因 XML 或正文文本未列出数值就判为不存在。** 下载失败、只有摘要、身份未核验、全文不可读取或必需图像未取得时，标记全文/图像不可用或待人工处理；不得据此判为“不符合”。

## 五阶段交接

### -1. 检索式Agent：研究问题到多数据库策略

使用 `literature-query-agent` 把研究问题拆成 `search / screen / optional` 概念，生成 `recall / balanced / precision_pilot` 三套方案。通过种子论文召回、命中数和噪声抽样选择执行版；不得因为某套结果少就认定它更准确。保存 `query_brief.json`、`query_plan.json`、`query_matrix.csv`、`configs/` 和 `search_strategy.md`，再进入 API 检索。

### 0. 论文检索Agent：API 检索与全量导出

支持 OpenAlex、Crossref、Europe PMC、PubMed、Semantic Scholar、CORE、arXiv、Web of Science 和 Scopus。简单查询可直接运行：

```bash
PDW=~/.codex/skills/paper-download-workspace/scripts
python3 $PDW/search_literature.py \
  --query 'soil AND (lead OR cadmium)' \
  --source openalex --source crossref --source europe_pmc \
  --output-dir /path/to/search-run
```

复杂检索式必须为每个数据库提供原生查询，使用 `--config`；字段、环境变量和示例见[文献检索 API 适配器](references/literature-api-adapters.md)。生产运行不设 `--max-records`。只有 `run_manifest.json` 顶层 `complete=true`，且计划来源均为 `status=complete` 时，才可称为该次查询的完整导出；否则应修复重跑，或在用户明确接受后披露缺失范围再继续。不得把部分分页结果悄悄交给论文筛选Agent。

检索完成后使用生成的 `pre_screening.csv`；各来源原始分页响应和 `run_manifest.json` 必须与它一同保存。“全部论文条目”只指各来源在其索引覆盖、原生检索式和访问权限下返回的全部匹配项，不等于全世界所有论文。

### 1. 论文筛选Agent：下载前初筛

- 输入至少包括检索式或候选记录，以及用户想找的数据/内容或纳入标准。
- 保留原始检索式、数据库/来源、题名、摘要、DOI/URL和原始记录定位；不得凭记忆补造元数据。
- 输出使用[筛选与下载协议](references/screening-protocol.md)中的 `pre_screening.csv` 字段。判定为 `download` 时写明命中的标准；判定为 `exclude` 时写明排除理由；摘要不足时用 `uncertain`，不要强行二选一。
- 系统综述或高召回任务中，边界记录优先进入 `uncertain` 并下载全文复核，避免在摘要阶段误删。
- 将 `download` 与需要全文才能解决的 `uncertain` 条目投影成入队 CSV，再进入下述入队流程。初筛表本身随来源清单保存，保证能追溯“为什么下载”。
- 用户要求端到端交接时，完成初筛表后直接运行：

```bash
python3 $PDW/handoff_screening_to_download.py \
  --screening /path/to/search-run/pre_screening.csv \
  --purpose '为目标数据全文复筛取得原文' \
  --title '检索初筛后的全文下载批次'
```

脚本默认交接 `download + uncertain`，再次按 DOI、否则按规范化题名加年份去重，并拒绝空白决定、冲突决定或缺少筛选理由的记录。只交接明确 `download` 时加 `--download-only`。检索不完整时默认拒绝入队；只有用户已知情并接受缺口时才用 `--allow-incomplete-search`。

### 2. 任务下载Agent：获取并核验全文

只下载初筛交接的目标条目。下载完成并通过身份、格式和完整性闸门后，按原有流程提交 `download_results.csv`；网页、摘要、封面页和损坏文件不能进入全文筛选。

### 3. 任务筛选Agent：全文证据复筛

- 先生成 `outputs/fulltext_screening_queue.csv`；脚本会区分 `ready`、`fulltext_unavailable` 和 `identity_unverified`。
- 只对 `ready` 条目作内容判断。扫描件或无可靠文本层的 PDF 先 OCR；超长文档可分章节读取，但必须保留页码或章节定位。
- **图像前置闸门：**若筛选标准涉及曲线、散点、柱状图、热图、谱图、地图、显微图、多 panel 或“图示但未在正文列数值”的数据，先读图题和引用上下文选定候选图，再下载目标图。ScienceDirect TDM XML 调用 `zju-edge-paper-download/scripts/fetch_figures.py`，默认只取选中图的 `best`（矢量/高清）版；不默认全量下载。PDF 来源则渲染并裁取目标图所在页。
- 只有在目标图已取得并可读后，才能基于图中证据写 `include` 或 `exclude`。必需图下载失败、分辨率不足或 panel 无法确认时用 `uncertain`（全文本身不可读时才用 `not_assessed`），不得写 `exclude`。
- 对每个标准输出 `include / exclude / uncertain / not_assessed`，并记录原文证据、页码或章节、证据文件路径。找不到明确证据时用 `uncertain`，不得用常识补全。
- 另给每篇论文一个总体判断；关键纳入/排除决定和关键数据应由人工复核。LLM 的判断是辅助证据，不是不可复核的最终裁决。

## 入队流程

1. 确定批次级下载目的，例如“补齐综述P1全文”“下载附件和补充材料”或“为证据卡片准备PDF”。不要把目的写成泛泛的“下载论文”。
2. 读取用户指定清单；若论文只出现在对话中，先建立临时CSV。每篇至少保留题名或DOI，不得凭记忆补造元数据。**入队清单本身就用CSV**（`paper_id,title,doi,landing_url,requested_files,...`）；Markdown清单先转CSV，不要留下需要后续解析的格式。
3. 如工作区不存在，运行`scripts/init_download_workspace.py`。
4. 一般清单运行`scripts/enqueue_download_batch.py`；来自 `pre_screening.csv` 的清单优先运行 `scripts/handoff_screening_to_download.py`，由它先生成可审计的 `download_manifest.csv` 再调用入队脚本。入队脚本负责标准化、去重提示、创建批次和原子更新总索引，并**在建批次时就写好下载就绪投影`outputs/zju_papers.csv`**（无BOM，列`doi,paper_id,suggested_filename,title`，只含`queue_action=download`）。
5. 运行`scripts/validate_download_workspace.py`。向用户返回批次ID、批次目录、论文数、需实际下载数、重复/已存在数和投影路径。

入队时读取[工作区合同](references/workspace-contract.md)。若清单来自论文筛选Agent，同时保存初筛结果路径及筛选标准；不要只交接一个失去上下文的题名列表。不要在入队回合打开浏览器、登录机构账号或开始下载；用户另有明确要求时再使用相应下载技能。

## 下载Agent流程

下载Agent应先读取[下载执行协议](references/downloader-protocol.md)，用`scripts/list_download_batches.py`查看`READY`批次，再用`scripts/claim_download_batch.py`原子领取。它只能写自己的批次目录，逐篇填写`outputs/download_results.csv`，并在完成后用`scripts/update_batch_status.py`把状态改为`REVIEW`。

**知网条目走单独分支：**来源已核实为 CNKI 时，若只有题名或尚无可信详情页 URL，先读 `cnki-search/SKILL.md`，在国内知网定位实际记录；再按 `cnki-paper-detail/SKILL.md` 核对题名、作者、年份与文种。已有详情页 URL 也须核对身份。之后读 `cnki-download/SKILL.md` 及其浙大 Mac WebVPN 参考流程，在用户已授权的 Edge 页面使用原生 `PDF下载`。`zju-edge-paper-download/pipeline.py` 不是已验证的 CNKI 下载器；知网分支默认 PDF、逐篇核验，不套用 DOI 管线的 XML 优先或并行抓取规则。其余条目仍按既有 DOI 管线处理。无 DOI 不能作为“全文不可得”的依据；遇验证码或机构失权应停止并记录原因，不重试绕过。IP 登录失败、CAJ 误下、下载无回执或 PDF 校验异常，按[下载执行协议的 CNKI 实测故障表](references/downloader-protocol.md#cnki-实测故障与处理)判定；网页按钮点过不等于文件已交付。

**Mac 知网已验证路径与故障分流（2026-09-28）：** 用户授权的浙大 Edge/WebVPN 会话 → 门户打开页面上实际观察到的 CNKI 详情页 URL → 核对页面机构身份及论文身份 → 点击详情页原生 `PDF下载` → 核验本次下载的文件。该路径已取得并核验 67 页学位论文和 8 页期刊文，但不保证每篇都有 PDF 权限。直连提示 IP 不在机构范围时先检查 WebVPN 页面，不把 Mac 的 `10.*` 私网地址当作机构权限；通用“下载”若给 CAJ，回详情页找明确的 PDF 选项。页面停在“安全验证”时等待用户完成验证，保留原批次，验证后重核机构与论文身份再续下；这不是全文缺失。某个可选浏览器/CLI 工具因本机凭据或钥匙串问题无法启动时，先尝试当前可用的授权浏览器连接；若均不可用，则请用户在本机恢复会话或凭据，不能记为论文无全文，也不能索取密码。点击后无回执时先分辨传输是否仍在进行、浏览器控制脚本是否挂起，避免重复点击；`file` 的页数异常也不能单独判 PDF 损坏，应以 PDF 解析器及首末页核验为准。具体操作和停止条件仍以 `cnki-download` 的 [Mac WebVPN 流程](../cnki-download/references/zju-webvpn-edge.md) 与本技能的[故障表](references/downloader-protocol.md#cnki-实测故障与处理)为准。

**清单不要现场投影。** 批次自带的`outputs/zju_papers.csv`是 DOI/出版社管线的下载就绪输入；纯 CNKI 批次跳过该管线，混合批次中的 CNKI 条目另走上述分支。只有在投影缺失（老批次）或MANIFEST被改过时才重建：

```bash
PDW=~/.codex/skills/paper-download-workspace/scripts
python3 $PDW/make_download_csv.py <batch_id>       # 单个批次
python3 $PDW/make_download_csv.py --all            # 全部批次补齐
```

收尾用标准脚本（不要每批复制一份）：

```bash
python3 $PDW/build_download_results.py <batch_id>   # 逐篇过闸门+身份校验 → download_results.csv
                                                    # （顺带导出 manual_backlog.csv；默认不覆盖已有结果）
python3 $PDW/make_manual_backlog.py <batch_id>      # 只要待人工清单时单独跑
```

若开放获取来源提供 Europe PMC JATS 全文 XML，将经 DOI 与正文核验的文件放在批次 `outputs/europepmc-xml/<paper_id>.xml`；标准结果构建脚本会与 `tdm-xml/` 一并识别。保留来源 URL、SHA-256 和未取得 PDF 的事实。

可选输入：`outputs/identity_tokens.json`（正文不打印 DOI 的条目给硬标识）、`outputs/gap_reasons.csv`（人工核实后的缺口结论，优先于自动判定）。

交接前再跑一次待人工清单导出（字段与 `03_review/MANUAL_DOWNLOAD_BACKLOG.csv` 同构，协调侧只需拼接）：

```bash
python3 $PDW/make_manual_backlog.py <batch_id>     # 或 --all
```

投影**必须无BOM**：`resolve.py`用`encoding="utf-8"`读CSV，带BOM会让第一列名变成`\ufeffdoi`，`row.get("doi")`返回`None`，整批被静默判成“无DOI → manual”。`validate_download_workspace.py`会直接报这个错。

验收（`REVIEW → ACCEPTED`）由协调Agent跑 `scripts/accept_batch.py <batch_id> --actor-role coordinator`：
先校验（结果覆盖全部 item_id、成功件重过闸门、无分片残留），再持锁把成功件写进 `04_library_index/PAPER_INDEX.csv`、
把缺口并入 `03_review/MANUAL_DOWNLOAD_BACKLOG.csv`，写 `03_review/<batch_id>-acceptance.md`，最后置 `ACCEPTED`。
下载Agent不能跑这一步（脚本硬拦角色）。失败、无权限、付费墙、身份不匹配和仅获取摘要必须显式记录，不能用空文件冒充PDF。

验收后若任务还要求内容筛选，运行：

```bash
python3 $PDW/make_fulltext_screening_queue.py <batch_id>
```

然后把 `outputs/fulltext_screening_queue.csv` 交给任务筛选Agent。该脚本默认不覆盖已填写的筛选结论；需要重建空白队列时必须显式传 `--overwrite`。

## 已下载 PDF 的元信息核对

需要从 PDF 本身识别题名、摘要、作者、期刊或 DOI 时，可用 capg 上的 GROBID。先读 [GROBID 调用说明](references/grobid-metadata.md)，确认服务可达，再对任务相关的 PDF 逐篇调用。抽取结果用于核对论文身份或补充元信息；不能凭抽取结果把未取得的 PDF 记为下载成功，也不要自动覆盖原清单的可信字段。

## 核心边界

- 初筛结果不等于全文结论；摘要未报告某项数据，不代表全文没有。
- 检索运行不完整时不得声称已导出全部结果；必须披露失败、缺少授权或人为截断的来源。
- 自动生成的检索式只是候选；未经过目标数据库实测、种子召回和噪声抽样，不得称为最终检索策略。
- 全文不可得、无法解析或身份未核验不等于“不符合纳入标准”。
- 全文复筛必须附证据位置；没有明确证据时标记 `uncertain`。
- 入队不等于下载完成。
- `READY` 只表示已交给任务下载Agent队列；领取、下载和验收仍是后续状态。
- 清单中的下载目标必须可追溯到用户请求或源文件。
- 不把API密钥、Cookie、账号或`.env`写入工作区。
- 不覆盖现有PDF；同一DOI或题名已在活动/完成批次中时，记录引用关系，由下载Agent决定复用或核验。
- 一个批次目录只允许一个下载Agent领取；聊天记录不能代替`HANDOFF.md`。
