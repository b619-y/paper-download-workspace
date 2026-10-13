# 论文筛选与下载协议

本协议定义四个角色之间的可追溯交接。默认目标是减少无关下载，同时避免因为检索中断或摘要信息不完整而误删论文。

## 0. 检索前端：论文检索Agent

按[文献检索 API 适配器](literature-api-adapters.md)将概念检索式翻译为各来源原生语法，穷尽分页并保留原始响应。输出 `run_manifest.json`、规范化记录和 `pre_screening.csv`。

只有运行清单顶层 `complete=true`，且计划来源均为 `status=complete`，才能称为完整导出。失败、缺少授权或 `--max-records` 截断必须先处理，或经用户接受后在筛选报告中明确披露；不得静默缩小候选集合。

## 1. 下载前：论文筛选Agent

### 输入

- 检索式、检索来源和完整的 `run_manifest.json`；或用户提供的候选记录。
- 明确的纳入标准、排除标准，以及希望在全文中寻找的数据或内容。
- 题名和摘要。只有题名而没有摘要时，只能做题名级初筛，不能伪装成题名—摘要筛选。

若标准仍不明确，先把用户目标改写为可判定条目；不要自行引入会改变研究范围的新标准。

### 输出 `pre_screening.csv`

```text
record_id,search_query,source_database,source_record,title,abstract,authors,year,journal,doi,landing_url,screening_basis,decision,matched_criteria,exclusion_reason,needs_fulltext_reason,screened_by,screened_at,notes
```

判定语义：

- `download`：题名或摘要显示可能符合，且需要全文确认或抽取数据。
- `exclude`：题名或摘要提供了明确、可记录的排除依据。
- `uncertain`：摘要缺失、信息不足、标准冲突或只能从方法/结果全文判断。高召回任务中应保留并下载。

要求：

- 排除必须写 `exclusion_reason`，不得仅写“无关”。
- 下载或不确定必须写 `matched_criteria` 或 `needs_fulltext_reason`。
- 保留检索式、来源数据库和原始记录定位，便于去重和复核。
- 摘要未提及某项数据，不得推断全文没有该数据。
- 交给下载阶段的 CSV 只包含 `download`，以及用户目标要求高召回时需要全文解决的 `uncertain`；保留原 `record_id` 和初筛理由。

### 整理清单并交给下载平台

初筛完成后运行：

```bash
python3 scripts/handoff_screening_to_download.py \
  --screening /path/to/search-run/pre_screening.csv \
  --purpose '为目标数据全文复筛取得原文' \
  --title '检索初筛后的全文下载批次'
```

脚本会：

1. 检查 `run_manifest.json` 的完整性；
2. 验证每条记录已有合法筛选决定和入选理由；
3. 按 DOI，或规范化题名加年份二次去重；同一身份有冲突决定时停止，不擅自选择；
4. 默认保留 `download` 与 `uncertain`，明确 `exclude` 永不进入下载清单；
5. 生成 `download_manifest.csv` 并创建状态为 `READY` 的下载批次；
6. 把检索运行清单、去重清单、完整初筛表和最终下载清单复制到批次 `inputs/search_screening/`。

`READY` 只表示交接完成；任务下载Agent仍需原子领取批次并执行下载。

## 2. 中间：任务下载Agent

下载Agent只负责取得用户有权访问的全文、核验论文身份和文件完整性，并记录失败原因。不得把以下情况改写为内容排除：

- 只有摘要或落地页；
- 付费墙、验证码、机构权限或工具故障；
- PDF 损坏、扫描件尚未 OCR；
- 文件身份未核验。

下载成功的 PDF/XML 必须在 `download_results.csv` 中有来源、路径、哈希、身份核验和完整性信息。

## 3. 下载后：任务筛选Agent

先运行：

```bash
python3 scripts/make_fulltext_screening_queue.py <batch_id>
```

脚本生成 `outputs/fulltext_screening_queue.csv`。每个论文至少一行；如果用户给出多个标准，可用 `--criteria <criteria.csv>` 展开为“每篇论文 × 每项标准”的长表。

标准表字段：

```text
criterion_id,criterion_text
```

全文筛选队列字段：

```text
item_id,paper_id,title,doi,fulltext_path,fulltext_status,criterion_id,criterion_text,decision,evidence_quote,evidence_page,evidence_section,evidence_kind,evidence_figure,evidence_image_path,evidence_object_ref,reviewer,reviewed_at,notes
```

判定语义：

- `include`：全文有明确证据满足该标准。
- `exclude`：全文有明确证据不满足该标准。
- `uncertain`：全文已读取，但证据不足、表述冲突或仍需领域判断。
- `not_assessed`：全文尚不能可靠读取，或身份未核验。

证据要求：

- 判断只能基于 `fulltext_path` 指向的已核验全文，不用网络摘要或常识补充。
- 每个 `include` 或 `exclude` 都要记录最短充分原文证据，以及页码或章节。证据位置无法自动确定时，明确写“位置待人工核对”，不能捏造页码。
- 扫描 PDF 或无可靠文本层时先 OCR；长文可分块检索，但回到原 PDF 核对证据和页码。
- **图像前置闸门：**若目标数据可能存在于曲线、散点、柱状图、热图、谱图、地图、显微图、多 panel 或任何论文图中，必须先检查图题和正文引用上下文，选定目标图并取得可读图像，再作该项判断。不得因 XML/正文文本没有列出数值就推断图中不存在。
- ScienceDirect TDM XML 使用 `zju-edge-paper-download/scripts/fetch_figures.py` 先建 catalog，再按图号或 `ref` 只下载目标图的 `best` 版；图题无法区分时可先取 2–3 张普通版候选图。只有明确的完整归档任务才能下载全部图。PDF 来源渲染并裁取目标图所在页，保留原页作为溯源证据。
- 图像证据填 `evidence_kind=figure` 或 `mixed`，并记录 `evidence_figure`、`evidence_image_path`和可用时的 `evidence_object_ref`。必需图未取得、清晰度不足或 panel 对应无法核对时用 `uncertain`；全文本身不可读时才用 `not_assessed`。这些情况均不得判为 `exclude`。
- 找不到证据时用 `uncertain`，不把“未发现”写成“不存在”。
- 每篇论文另汇总总体结论，并保留逐标准记录。关键纳入/排除和关键数据由人工复核。

## 4. 推荐的人机边界

LLM 适合检索辅助、初筛、定位候选段落和结构化证据；不应单独决定不可逆的最终纳入/排除。批次交接必须保留原始记录、全文路径和证据定位，使人能够从结论回查原文。
