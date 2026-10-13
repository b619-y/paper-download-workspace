# 论文下载工作区合同

## 固定结构

```text
/Users/b/Desktop/论文下载/
├── README_总控.md
├── 00_control/
│   ├── WORKSPACE.json
│   ├── BATCH_INDEX.csv
│   ├── POLICIES.md
│   └── DECISIONS.md
├── 01_shared/contracts/
├── 02_batches/<batch_id>/
│   ├── REQUEST.json
│   ├── REQUEST.md
│   ├── MANIFEST.csv
│   ├── STATUS.json
│   ├── HANDOFF.md
│   ├── inputs/
│   │   └── search_screening/  # API检索、去重、初筛和最终下载清单证据（适用时）
│   ├── outputs/
│   │   ├── zju_papers.csv      # 入队时生成的「下载就绪投影」（无 BOM）
│   │   ├── download_results.csv
│   │   ├── fulltext_screening_queue.csv  # 下载后全文筛选队列与证据记录
│   │   └── files/
│   └── logs/
├── 03_review/
├── 04_library_index/PAPER_INDEX.csv
└── 05_archive/
```

总控和共享区仅由协调Agent修改。下载Agent只写自己领取的`02_batches/<batch_id>/`。

## 批次清单字段

`MANIFEST.csv`固定字段：

```text
item_id,paper_id,title,authors,year,journal,doi,landing_url,priority,download_reason,requested_files,target_filename,suggested_filename,source_record,local_existing_path,queue_action,existing_batch_id,notes
```

- `requested_files`可写`article_pdf`、`supplement`或用分号组合。
- `queue_action`为`download`、`verify_existing`或`reference_existing`。
- `download_reason`默认继承批次下载目的，但允许逐篇覆盖。
- `target_filename`只给建议文件名，下载Agent仍需验证文件身份。
- `suggested_filename`与`target_filename`同值，存在的唯一目的是**让`zju-edge-paper-download/scripts/resolve.py`能直接读MANIFEST**（它只认`doi/paper_id/suggested_filename/title`）。`suggested_filename`列是新增的；老批次没有该列时，`make_download_csv.py`会回退用`target_filename`。

## 下载就绪投影 `outputs/zju_papers.csv`

入队脚本在建批次时就写好这份投影，下载Agent**直接使用，不要现场再写投影代码**：

```text
doi,paper_id,suggested_filename,title
```

- 只含`queue_action=download`的条目（`reference_existing`/`verify_existing`按§3.4核验既有文件，不进投影）。
- **无DOI的行保留**：`resolve.py`会把它列为`manual`缺口，而不是静默丢弃。
- **必须无BOM**（utf-8）：`resolve.py`用`encoding="utf-8"`读CSV，带BOM会让第一列名变成`\ufeffdoi`，`row.get("doi")`返回`None`，整批退化为“无DOI → manual”。
- 缺失或MANIFEST改动后用`scripts/make_download_csv.py <batch_id>`重建；`validate_download_workspace.py`会校验投影与MANIFEST的`download`条目是否一致、是否带BOM。

## 下载结果字段

`outputs/download_results.csv`固定字段：

```text
item_id,paper_id,status,title,doi,source_url,pdf_path,supplement_paths,sha256,file_bytes,page_count,identity_verified,downloaded_at,notes
```

`status`可为：`downloaded`、`reused_verified`、`not_found`、`paywalled`、`access_denied`、`identity_mismatch`、`failed`或`skipped_duplicate`。

## 批次状态

`READY → CLAIMED → RUNNING → REVIEW → ACCEPTED`，执行失败可转为`BLOCKED`或`FAILED`。只有协调Agent可以写`ACCEPTED`。

## 去重语义

入队脚本按规范化DOI优先、规范化题名次之，搜索现有批次。发现重复不删除用户请求，而是把`queue_action`标为`reference_existing`并记录`existing_batch_id`。下载Agent须核验已有文件是否满足当前目的，例如是否还缺补充材料。

## 筛选交接文件

下载前的论文筛选Agent按 `references/screening-protocol.md` 产出 `pre_screening.csv`。入队时应把该文件或其稳定路径保存为来源材料，且只将 `download` 和需要全文才能判断的 `uncertain` 条目投影进 `MANIFEST.csv`。

来自 API 检索的筛选结果用 `scripts/handoff_screening_to_download.py` 交接。脚本生成 `download_manifest.csv`、创建 `READY` 批次，并把 `run_manifest.json`、`deduplicated_records.csv`（若存在）、`pre_screening.csv`、最终下载清单和 `handoff_summary.json` 保存到 `inputs/search_screening/`。同一论文的冲突筛选决定不能自动合并。

下载后运行 `scripts/make_fulltext_screening_queue.py <batch_id>`，生成：

```text
item_id,paper_id,title,doi,fulltext_path,fulltext_status,criterion_id,criterion_text,decision,evidence_quote,evidence_page,evidence_section,evidence_kind,evidence_figure,evidence_image_path,evidence_object_ref,reviewer,reviewed_at,notes
```

- `fulltext_status` 为 `ready`、`fulltext_unavailable` 或 `identity_unverified`。
- `decision` 为 `include`、`exclude`、`uncertain`、`not_assessed` 或空值。
- 只有 `ready` 可以作内容判断；其他状态不能等价为 `exclude`。
- 每个判定必须能追溯到 `fulltext_path`，并尽量提供原文、页码或章节。
- 若目标数据可能在论文图中，必须先取得并阅读目标图再判定；在 `evidence_kind`、`evidence_figure`、`evidence_image_path` 和 `evidence_object_ref` 中记录图像证据。必需图不可用时用 `uncertain`，不能据此写 `exclude`。

