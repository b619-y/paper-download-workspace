#!/usr/bin/env python3
"""Create a download batch and atomically add it to the shared index."""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import re
import secrets
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_ROOT = Path("/Users/b/Desktop/论文下载")
MANIFEST_FIELDS = [
    "item_id", "paper_id", "title", "authors", "year", "journal", "doi",
    "landing_url", "priority", "download_reason", "requested_files",
    "target_filename", "suggested_filename", "source_record", "local_existing_path",
    "queue_action", "existing_batch_id", "notes",
]
RESULT_FIELDS = [
    "item_id", "paper_id", "status", "title", "doi", "source_url", "pdf_path",
    "supplement_paths", "sha256", "file_bytes", "page_count", "identity_verified",
    "downloaded_at", "notes",
]
#: 入队时就写好的「下载就绪」投影：resolve.py 认 doi/paper_id/suggested_filename/title，
#: 因此下载Agent可直接 `pipeline.py --csv outputs/zju_papers.csv`，无需再自己写投影代码。
DOWNLOAD_CSV_FIELDS = ["doi", "paper_id", "suggested_filename", "title"]


ALIASES = {
    "paper_id": ["paper_id", "id", "论文id", "编号"],
    "title": ["title", "题名", "标题", "论文题目"],
    "authors": ["authors", "author", "作者"],
    "year": ["year", "年份"],
    "journal": ["journal", "期刊"],
    "doi": ["doi"],
    "landing_url": ["landing_url", "download_url", "url", "链接", "下载链接"],
    "priority": ["priority", "优先级"],
    "download_reason": ["download_reason", "reason", "screen_reasons", "下载目的", "原因"],
    "requested_files": ["requested_files", "文件类型", "下载内容"],
    "target_filename": ["target_filename", "suggested_filename", "文件名", "建议文件名"],
    "local_existing_path": ["local_existing_path", "pdf_paths", "pdf_path", "本地路径"],
    "notes": ["notes", "备注", "metadata_warning"],
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_value(row: dict[str, str], key: str) -> str:
    lowered = {str(k).strip().lower(): (v or "").strip() for k, v in row.items()}
    for alias in ALIASES[key]:
        value = lowered.get(alias.lower(), "")
        if value:
            return value
    return ""


def normalize_doi(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"^https?://(dx\.)?doi\.org/", "", value)
    value = re.sub(r"^doi\s*:\s*", "", value)
    return value.strip().strip(".;, ")


def normalize_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def safe_filename(paper_id: str, title: str, doi: str, item_id: str) -> str:
    base = paper_id or (doi.rsplit("/", 1)[-1] if doi else title[:80]) or item_id
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._-") or item_id
    return base[:120] + ".pdf"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]], *, bom: bool = True) -> None:
    """写 CSV。默认带 BOM（Excel 友好）。

    `bom=False` 专供下载就绪投影：`resolve.py` 用 `encoding="utf-8"` 打开 CSV，
    带 BOM 会让第一列名变成 `\\ufeffdoi`，`row.get("doi")` 返回 None，
    **整批被静默判成「无 DOI → manual」**，所以投影必须无 BOM。
    """
    with path.open("w", encoding="utf-8-sig" if bom else "utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def existing_records(root: Path) -> tuple[dict[str, str], dict[str, str]]:
    by_doi: dict[str, str] = {}
    by_title: dict[str, str] = {}
    for manifest in sorted((root / "02_batches").glob("*/MANIFEST.csv")):
        batch_id = manifest.parent.name
        for row in read_csv(manifest):
            doi = normalize_doi(row.get("doi", ""))
            title = normalize_title(row.get("title", ""))
            if doi:
                by_doi.setdefault(doi, batch_id)
            if title:
                by_title.setdefault(title, batch_id)
    return by_doi, by_title


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--purpose", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--priority", choices=["urgent", "high", "normal", "low"], default="normal")
    parser.add_argument("--source-context", default="")
    parser.add_argument("--requested-files", default="article_pdf")
    parser.add_argument("--force-download", action="store_true",
                        help="download even when the DOI/title appears in another batch; existing local paths still take precedence")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    manifest_source = Path(args.manifest).expanduser().resolve()
    if not manifest_source.is_file():
        parser.error(f"manifest not found: {manifest_source}")
    if not (root / "00_control/WORKSPACE.json").exists():
        initializer = Path(__file__).with_name("init_download_workspace.py")
        subprocess.run([sys.executable, str(initializer), "--root", str(root)], check=True)

    source_rows = read_csv(manifest_source)
    if not source_rows:
        parser.error("manifest contains no paper rows")
    for number, row in enumerate(source_rows, 1):
        if not get_value(row, "title") and not get_value(row, "doi"):
            parser.error(f"row {number} requires title or DOI")

    lock_path = root / "00_control/.queue.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        now = utc_now()
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        batch_id = f"DL-{stamp}-{secrets.token_hex(2)}"
        batch = root / "02_batches" / batch_id
        batch.mkdir(parents=False, exist_ok=False)
        for rel in ["inputs", "outputs/files", "logs"]:
            (batch / rel).mkdir(parents=True, exist_ok=True)

        by_doi, by_title = existing_records(root)
        normalized: list[dict[str, str]] = []
        counts = {"download": 0, "reference_existing": 0, "verify_existing": 0}
        for index, source in enumerate(source_rows, 1):
            item_id = f"D{index:03d}"
            paper_id = get_value(source, "paper_id")
            title = get_value(source, "title")
            doi = normalize_doi(get_value(source, "doi"))
            local_path = get_value(source, "local_existing_path")
            duplicate_batch = by_doi.get(doi, "") if doi else ""
            if not duplicate_batch and title:
                duplicate_batch = by_title.get(normalize_title(title), "")
            if local_path:
                action = "verify_existing"
            elif duplicate_batch and not args.force_download:
                action = "reference_existing"
            else:
                action = "download"
            counts[action] += 1
            requested = get_value(source, "requested_files") or args.requested_files
            reason = get_value(source, "download_reason") or args.purpose
            filename = get_value(source, "target_filename") or safe_filename(paper_id, title, doi, item_id)
            normalized.append({
                "item_id": item_id,
                "paper_id": paper_id,
                "title": title,
                "authors": get_value(source, "authors"),
                "year": get_value(source, "year"),
                "journal": get_value(source, "journal"),
                "doi": doi,
                "landing_url": get_value(source, "landing_url"),
                "priority": get_value(source, "priority") or args.priority,
                "download_reason": reason,
                "requested_files": requested,
                "target_filename": filename,
                "suggested_filename": filename,
                "source_record": f"{manifest_source}:{index + 1}",
                "local_existing_path": local_path,
                "queue_action": action,
                "existing_batch_id": duplicate_batch,
                "notes": get_value(source, "notes"),
            })

        write_csv(batch / "MANIFEST.csv", MANIFEST_FIELDS, normalized)
        # 下载就绪投影：入队即生成，下载Agent不再需要手写投影代码（也不需要改名）。
        # 只收 queue_action=download；无 DOI 的行保留（resolve.py 会显式判为 manual，
        # 不会静默消失），reference_existing/verify_existing 由下载Agent按 §3.4 核验既有文件。
        write_csv(
            batch / "outputs/zju_papers.csv",
            DOWNLOAD_CSV_FIELDS,
            [{
                "doi": row["doi"],
                "paper_id": row["paper_id"],
                "suggested_filename": row["suggested_filename"],
                "title": row["title"],
            } for row in normalized if row["queue_action"] == "download"],
            bom=False,   # resolve.py 用 encoding="utf-8" 读；带 BOM 会整批退化成 manual
        )
        shutil.copy2(manifest_source, batch / "inputs/source_manifest.csv")
        request = {
            "batch_id": batch_id,
            "title": args.title,
            "purpose": args.purpose,
            "priority": args.priority,
            "created_at": now,
            "source_context": args.source_context,
            "source_manifest": str(manifest_source),
            "paper_count": len(normalized),
            "requested_files_default": args.requested_files,
        }
        (batch / "REQUEST.json").write_text(json.dumps(request, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        (batch / "REQUEST.md").write_text(
            f"# {args.title}\n\n"
            f"- Batch ID: `{batch_id}`\n"
            f"- Priority: `{args.priority}`\n"
            f"- Created: `{now}`\n\n"
            f"## 下载目的\n\n{args.purpose}\n\n"
            f"## 来源说明\n\n{args.source_context or str(manifest_source)}\n\n"
            "## 执行边界\n\n按MANIFEST逐篇处理。下载Agent只写本批次目录，完成后提交HANDOFF并把状态改为REVIEW。\n\n"
            "## 下载就绪清单\n\n入队时已生成 `outputs/zju_papers.csv`（`doi,paper_id,suggested_filename,title`，"
            "仅含 `queue_action=download`），可直接传给 `zju-edge-paper-download/scripts/pipeline.py --csv`。"
            "清单若有改动，用 `paper-download-workspace/scripts/make_download_csv.py` 重建。\n",
            encoding="utf-8",
        )
        (batch / "STATUS.json").write_text(json.dumps({
            "batch_id": batch_id,
            "status": "READY",
            "claimed_by": None,
            "created_at": now,
            "updated_at": now,
            "attempt": 0,
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        (batch / "HANDOFF.md").write_text(
            f"# {batch_id} handoff\n\n## 结果摘要\n\n尚未开始。\n\n"
            "## 输出文件\n\n- None\n\n## 检查\n\n- None\n\n"
            "## 未完成和阻塞\n\n- None recorded\n",
            encoding="utf-8",
        )
        write_csv(batch / "outputs/download_results.csv", RESULT_FIELDS, [])

        index_path = root / "00_control/BATCH_INDEX.csv"
        with index_path.open("a", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=[
                "batch_id", "created_at", "title", "purpose", "status", "paper_count",
                "download_count", "reference_existing_count", "verify_existing_count", "priority",
                "source_list", "workspace_path", "claimed_by", "updated_at", "accepted_at",
            ])
            writer.writerow({
                "batch_id": batch_id,
                "created_at": now,
                "title": args.title,
                "purpose": args.purpose,
                "status": "READY",
                "paper_count": len(normalized),
                "download_count": counts["download"],
                "reference_existing_count": counts["reference_existing"],
                "verify_existing_count": counts["verify_existing"],
                "priority": args.priority,
                "source_list": str(manifest_source),
                "workspace_path": str(batch),
                "claimed_by": "",
                "updated_at": now,
                "accepted_at": "",
            })
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    print(json.dumps({
        "batch_id": batch_id,
        "batch_path": str(batch),
        "paper_count": len(normalized),
        "download_csv": str(batch / "outputs/zju_papers.csv"),
        "download_csv_rows": counts["download"],
        **counts,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
