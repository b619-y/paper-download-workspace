#!/usr/bin/env python3
"""Initialize the persistent local paper-download workspace."""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_ROOT = Path.cwd()
WORKSPACE_DIRS = (
    "00_control", "01_shared/contracts", "01_shared/prompts", "02_batches",
    "03_review", "04_library_index", "05_archive", "06_mineru",
    "07_xml_markdown", "runtime",
)
BATCH_FIELDS = [
    "batch_id", "created_at", "title", "purpose", "status", "paper_count",
    "download_count", "reference_existing_count", "verify_existing_count", "priority",
    "source_list", "workspace_path", "claimed_by", "updated_at", "accepted_at",
]
PAPER_FIELDS = [
    "paper_id", "title", "doi", "pdf_path", "supplement_paths", "sha256",
    "source_batch_id", "identity_verified", "accepted_at", "notes",
    "mineru_md_path", "xml_md_path", "source_item_id", "document_id",
    "source_kind", "processed_text_sha256", "processing_method",
    "processing_version", "processing_accepted_at", "section_chunks_path",
    "section_chunks_sha256", "section_chunks_count", "section_chunks_schema_version",
]
DECISIONS_TEMPLATE = (
    "# Decisions\n\n"
    "记录批次重分配、验收、合并、撤销和异常处理决定。\n"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def write_csv_header(path: Path, fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        csv.DictWriter(handle, fieldnames=fields).writeheader()


def reconcile_csv(path: Path, fields: list[str]) -> None:
    """Create a missing index or safely append newly introduced columns."""
    if not path.exists():
        write_csv_header(path, fields)
        return
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"workspace index is not a regular file: {path}")
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        current = list(reader.fieldnames or [])
        rows = list(reader)
    if current == fields:
        return
    if not current or fields[:len(current)] != current:
        raise ValueError(
            f"workspace index header is incompatible: {path}; "
            f"found {current!r}, expected prefix of {fields!r}"
        )
    if any(None in row for row in rows):
        raise ValueError(f"workspace index contains columns outside its header: {path}")
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="raise")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    args = parser.parse_args()
    root = Path(args.root).expanduser().resolve()
    identity = root / "00_control/WORKSPACE.json"
    existing_workspace = identity.exists()
    if not existing_workspace and root.exists() and any(root.iterdir()):
        parser.error(f"non-empty directory is not a paper-download workspace: {root}")

    for rel in WORKSPACE_DIRS:
        (root / rel).mkdir(parents=True, exist_ok=True)

    now = utc_now()
    if existing_workspace:
        if identity.is_symlink() or not identity.is_file():
            parser.error(f"workspace identity is not a regular file: {identity}")
        try:
            workspace = json.loads(identity.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            parser.error(f"workspace identity is unreadable: {identity}: {exc}")
        if not isinstance(workspace, dict) or workspace.get("workspace_id") != "paper-download-workspace":
            parser.error(f"directory has an incompatible WORKSPACE.json: {root}")
    else:
        identity.write_text(json.dumps({
            "workspace_id": "paper-download-workspace",
            "title": "论文下载工作区",
            "created_at": now,
            "workspace_version": 1,
            "coordinator": "local-coordinator",
            "default_status": "READY",
            "local_machine_only": True,
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    lock_path = root / "00_control/.queue.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            reconcile_csv(root / "00_control/BATCH_INDEX.csv", BATCH_FIELDS)
            reconcile_csv(root / "04_library_index/PAPER_INDEX.csv", PAPER_FIELDS)
            decisions = root / "00_control/DECISIONS.md"
            if not decisions.exists():
                decisions.write_text(DECISIONS_TEMPLATE, encoding="utf-8")
            elif decisions.is_symlink() or not decisions.is_file():
                parser.error(f"workspace decisions path is not a regular file: {decisions}")
        except (OSError, UnicodeError, csv.Error, ValueError) as exc:
            parser.error(str(exc))
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    if existing_workspace:
        print(root)
        return 0

    (root / "README_总控.md").write_text(
        "# 论文下载工作区\n\n"
        "这个工作区集中保存论文下载目的、下载清单、批次状态和下载结果。"
        "入队Agent只创建批次；下载Agent只写自己领取的批次目录；协调Agent负责验收和论文库索引。\n\n"
        "## 目录\n\n"
        "- `00_control/BATCH_INDEX.csv`：全部批次索引。\n"
        "- `02_batches/<batch_id>/`：每次下载请求的独立子工作区。\n"
        "- `03_review/`：验收记录。\n"
        "- `04_library_index/PAPER_INDEX.csv`：已验收论文文件索引。\n\n"
        "## 领取规则\n\n"
        "下载Agent必须使用Skill提供的原子领取脚本。一个批次只允许一个Agent领取。"
        "完成后状态写`REVIEW`，只有协调Agent可以写`ACCEPTED`。\n",
        encoding="utf-8",
    )
    (root / "00_control/POLICIES.md").write_text(
        "# 下载政策\n\n"
        "- 合法公开来源、出版社页面、机构已有权限和用户授权会话优先。\n"
        "- 不绕过付费墙、验证码或访问控制。\n"
        "- 网页、摘要、封面页和损坏文件不能冒充完整PDF。\n"
        "- 每篇成功文件必须核验身份并记录来源、哈希、大小和页数。\n"
        "- 工作区不得保存账号、Cookie、API密钥或`.env`。\n",
        encoding="utf-8",
    )
    (root / "01_shared/contracts/MANIFEST_SCHEMA.md").write_text(
        "# MANIFEST.csv\n\n"
        "字段：`item_id,paper_id,title,authors,year,journal,doi,landing_url,priority,download_reason,requested_files,target_filename,source_record,local_existing_path,queue_action,existing_batch_id,notes`。\n",
        encoding="utf-8",
    )
    (root / "01_shared/contracts/DOWNLOAD_RESULT_SCHEMA.md").write_text(
        "# download_results.csv\n\n"
        "字段：`item_id,paper_id,status,title,doi,source_url,pdf_path,supplement_paths,sha256,file_bytes,page_count,identity_verified,downloaded_at,notes`。结果必须覆盖MANIFEST全部item_id。\n",
        encoding="utf-8",
    )
    (root / "01_shared/contracts/PRE_SCREENING_SCHEMA.md").write_text(
        "# pre_screening.csv\n\n"
        "字段：`record_id,search_query,source_database,source_record,title,abstract,authors,year,journal,doi,landing_url,screening_basis,decision,matched_criteria,exclusion_reason,needs_fulltext_reason,screened_by,screened_at,notes`。\n\n"
        "`decision` 只用 `download`、`exclude`、`uncertain`。摘要未报告某项内容不能作为排除依据；需要全文判断时用 `uncertain`。\n",
        encoding="utf-8",
    )
    (root / "01_shared/contracts/FULLTEXT_SCREENING_SCHEMA.md").write_text(
        "# fulltext_screening_queue.csv\n\n"
        "字段：`item_id,paper_id,title,doi,fulltext_path,fulltext_status,criterion_id,criterion_text,decision,evidence_quote,evidence_page,evidence_section,evidence_kind,evidence_figure,evidence_image_path,evidence_object_ref,reviewer,reviewed_at,notes`。\n\n"
        "只有 `fulltext_status=ready` 才能作内容判断。`decision` 使用 `include`、`exclude`、`uncertain`、`not_assessed`；全文不可用或身份未核验不等于 `exclude`。若目标数据可能在论文图中，先下载并阅读目标图再判定，并记录图号、图片路径和可用时的对象 ref。\n",
        encoding="utf-8",
    )
    (root / "01_shared/contracts/LITERATURE_SEARCH_SCHEMA.md").write_text(
        "# 文献检索运行合同\n\n"
        "每次检索保留 `run_manifest.json`、各来源原始分页响应、`all_records.csv`、`deduplicated_records.csv` 和 `pre_screening.csv`。\n\n"
        "只有顶层 `complete=true` 且计划使用的来源均为 `status=complete`，才能称为完整导出。"
        "`reported_total` 是来源报告的命中数，`fetched_count` 是实际写入数。失败、缺少凭据或截断必须处理或披露。\n\n"
        "“全部”仅指该来源在本次原生检索式、索引覆盖范围和访问权限下返回的全部匹配记录。工作区不得保存 API 密钥。\n",
        encoding="utf-8",
    )
    (root / "01_shared/contracts/SCREENING_DOWNLOAD_HANDOFF.md").write_text(
        "# 检索筛选到下载批次的交接合同\n\n"
        "API 检索完成后先填写 `pre_screening.csv`，再运行 `handoff_screening_to_download.py`。"
        "脚本默认只交接 `download + uncertain`，按 DOI、否则按规范化题名加年份二次去重，并创建 `READY` 批次。\n\n"
        "空白决定、冲突决定、入选却无理由，以及未获接受的不完整检索都会阻止交接。"
        "批次 `inputs/search_screening/` 保留完整证据。`READY` 表示进入下载队列，不表示下载完成。\n",
        encoding="utf-8",
    )
    (root / "01_shared/contracts/QUERY_STRATEGY_SCHEMA.md").write_text(
        "# 检索式智能体交付合同\n\n"
        "检索前保存 `query_brief.json`、`query_plan.json`、`query_matrix.csv`、`configs/<variant>.json` 和 `search_strategy.md`。"
        "至少生成 recall、balanced、precision_pilot 三套候选；经过数据库实测、种子召回检查和噪声抽样后才能选定执行版。\n\n"
        "概念块内部 OR、块之间 AND。结局、比较、研究设计和狭窄场景默认留给筛选。"
        "OpenAlex/Crossref 自由文本翻译标为 approximate。内部模型审查不等于正式 PRESS 同行评审。\n",
        encoding="utf-8",
    )
    (root / "01_shared/search-config.example.json").write_text(json.dumps({
        "query_id": "example-search",
        "original_query": "soil AND (lead OR cadmium)",
        "source_order": ["openalex", "crossref", "europe_pmc", "pubmed"],
        "sources": {
            "openalex": {"query": "soil lead cadmium"},
            "crossref": {"query": "soil lead cadmium"},
            "europe_pmc": {"query": "soil AND (lead OR cadmium)"},
            "pubmed": {"query": "soil AND (lead OR cadmium)"},
        },
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (root / "01_shared/prompts/下载Agent通用提示词.txt").write_text(
        "你是论文下载执行Agent。打开本工作区00_control/BATCH_INDEX.csv，只选择状态为READY的批次。"
        "先使用paper-download-workspace Skill的claim_download_batch.py原子领取；领取失败则停止并选择其他READY批次。"
        "完整阅读该批次REQUEST.md、MANIFEST.csv和根目录下载政策，只写自己的批次目录。逐篇核验题名、DOI和PDF完整性，"
        "把文件放入outputs/files/并完整填写outputs/download_results.csv。不得绕过付费墙或访问控制。"
        "完成后填写HANDOFF.md，并用update_batch_status.py把状态改为REVIEW；不得自行写ACCEPTED或修改论文库总索引。\n",
        encoding="utf-8",
    )
    (root / "01_shared/prompts/论文初筛Agent通用提示词.txt").write_text(
        "你是论文筛选Agent。根据用户确认的检索式、纳入/排除标准，只使用候选记录的题名和摘要进行初筛。"
        "输出 pre_screening.csv：download、exclude 或 uncertain。排除必须给出明确的题名/摘要依据；摘要缺失、信息不足或必须读取方法与结果全文时标记 uncertain。"
        "高召回任务中保留边界记录进入全文下载。保留检索式、数据库、原始记录、DOI/URL和筛选理由，不得凭记忆补造元数据。"
        "只把 download 和需要全文解决的 uncertain 交给下载批次。\n",
        encoding="utf-8",
    )
    (root / "01_shared/prompts/论文检索Agent通用提示词.txt").write_text(
        "你是论文检索Agent。先把用户的概念检索式分别翻译成每个数据库的原生语法，再用 paper-download-workspace 的 "
        "search_literature.py 分页导出。保留每一页原始响应、来源报告命中数、实际写入数、原生检索式和运行清单。"
        "没有设置 max-records 且 run_manifest.json 顶层 complete=true、计划来源均为 complete 后，才可以声称完整导出。"
        "缺少密钥、限流、分页中断或数量不一致必须标成 incomplete，不能把部分记录静默交给初筛。"
        "API 密钥只从环境变量读取，不写入配置或工作区。检索完成后把 pre_screening.csv 交给论文筛选Agent。\n",
        encoding="utf-8",
    )
    (root / "01_shared/prompts/筛选结果交接Agent通用提示词.txt").write_text(
        "你负责把论文检索与初筛结果交给任务下载Agent。先确认 run_manifest.json 完整，再确认 pre_screening.csv 每行已有合法决定。"
        "使用 handoff_screening_to_download.py 二次去重并生成下载清单；默认保留 download 和 uncertain，排除 exclude。"
        "任何空白决定、身份重复但决定冲突、缺少入选理由或未披露的不完整检索都必须停止。"
        "脚本成功创建 READY 批次后才算交接完成；不得把 READY 说成已经下载。\n",
        encoding="utf-8",
    )
    (root / "01_shared/prompts/检索式Agent通用提示词.txt").write_text(
        "你是检索式智能体。不要直接凭直觉给出一个 Boolean 字符串。先明确研究问题和边界，选择合适框架，再把候选概念分成 search、screen、optional。"
        "为 search 概念建立锚词、同义词、缩写、拼写变体、扩展词和经核实的受控词，生成 recall、balanced、precision_pilot 三套方案并翻译成各数据库原生语法。"
        "用命中数、已知相关论文召回和噪声抽样迭代；记录每次修改。PubMed 生物医学检索优先使用 pubmed-search-builder-next。"
        "最终把选定配置交给论文检索Agent完整分页导出。\n",
        encoding="utf-8",
    )
    (root / "01_shared/prompts/任务筛选Agent通用提示词.txt").write_text(
        "你是任务筛选Agent。只依据 fulltext_screening_queue.csv 中 fulltext_status=ready 且身份已核验的全文判断是否包含用户需要的数据或内容。"
        "逐项输出 include、exclude、uncertain 或 not_assessed，并提供最短充分原文证据、页码或章节和全文路径。扫描件先 OCR。"
        "若目标数据可能在论文图中，必须先根据图题与引用上下文定位目标图，按需下载并阅读可读图像，再完成判断；不得因 XML/正文未列数值就判为不存在。"
        "图像证据记录 evidence_kind、图号、图片路径和可用时的对象 ref；必需图无法取得或辨读时标记 uncertain，不得判为 exclude。"
        "找不到明确证据时标记 uncertain，不得用常识或网络摘要补全。全文不可用、无法解析或身份未核验不得判为 exclude。"
        "关键纳入/排除和关键数据标记为需要人工复核。\n",
        encoding="utf-8",
    )
    print(root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
