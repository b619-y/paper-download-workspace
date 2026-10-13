#!/usr/bin/env python3
"""验收一个 `REVIEW` 批次：校验 → 写论文库索引/待人工清单 → 置 `ACCEPTED`。

这是**协调Agent**的动作（`--actor-role coordinator`），下载Agent不能跑它。
以前这步全靠手工：对着 `download_results.csv` 逐行抄进 `PAPER_INDEX.csv`、
再把缺口抄进 `MANUAL_DOWNLOAD_BACKLOG.csv`，既慢又容易抄错批次号/路径。

它做什么
--------
1. **校验**（任一失败即中止，不写任何东西）：
   * `outputs/download_results.csv` 覆盖 `MANIFEST.csv` 全部 `item_id`；
   * 每个成功条目的文件真实存在，且 PDF/XML 重过 §7 闸门（复用 `build_download_results`）；
   * `outputs/files/` 内没有 `.part` / `.partial` / `.partial.json` / `.crdownload` 残留；
   * 缺 `outputs/manual_backlog.csv` 时自动补生成。
2. **写入**（持 `00_control/.queue.lock`，按批次幂等；重复跑不会重复登记）：
   * `04_library_index/PAPER_INDEX.csv` ← 成功条目；
   * `03_review/MANUAL_DOWNLOAD_BACKLOG.csv` ← 缺口条目；
   * `03_review/<batch_id>-acceptance.md` ← 验收记录；
   * 在同一批次锁临界区内校验并置 `ACCEPTED`；`update_batch_status.py`
     不允许绕过本验收闸门直接写 `ACCEPTED`。

    python3 accept_batch.py <batch_id> --actor-role coordinator --dry-run
    python3 accept_batch.py <batch_id> --actor-role coordinator

已 ACCEPTED 但交付件后来被证明错配时，用 `--reaccept` 重过闸门，
替换本批次在论文库索引和待人工清单里的行，并重写验收记录。状态保持 ACCEPTED。
"""
from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import os
import re
import shutil
import importlib.util
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_ROOT = Path("/Users/b/Desktop/论文下载")
PAPER_INDEX = Path("04_library_index/PAPER_INDEX.csv")
MANUAL_BACKLOG = Path("03_review/MANUAL_DOWNLOAD_BACKLOG.csv")
#: 必须与 04_library_index/PAPER_INDEX.csv 的**列顺序**完全一致（追加行是按本列表写的；
#: 顺序不一致会把值写进错误的列）。新增列只能追加在末尾。
PAPER_FIELDS = ["paper_id", "title", "doi", "pdf_path", "supplement_paths", "sha256",
                "source_batch_id", "identity_verified", "accepted_at", "notes",
                "mineru_md_path", "xml_md_path"]

#: 派生全文（MinerU Markdown / TDM XML→Markdown）的默认落点（相对工作区根）
DERIVED_MD_DIRS = {"mineru_md_path": "06_mineru", "xml_md_path": "07_xml_markdown"}
BACKLOG_FIELDS = ["batch_id", "item_id", "paper_id", "doi", "title", "status", "reason_code",
                  "reason", "attempted_channels", "needs", "owner", "reported_at",
                  "resolved_at", "resolution_batch_id", "notes"]
DOWNLOAD_RESULT_FIELDS = [
    "item_id", "paper_id", "status", "title", "doi", "source_url", "pdf_path",
    "supplement_paths", "sha256", "file_bytes", "page_count", "identity_verified",
    "downloaded_at", "notes",
]
MANIFEST_FIELDS = [
    "item_id", "paper_id", "title", "authors", "year", "journal", "doi",
    "landing_url", "priority", "download_reason", "requested_files",
    "target_filename", "suggested_filename", "source_record", "local_existing_path",
    "queue_action", "existing_batch_id", "notes",
]
MANUAL_STATUSES = {"not_found", "paywalled", "access_denied", "identity_mismatch", "failed"}
COMMON_IDENTITY_TOKENS = {
    "abstract", "article", "conclusion", "download", "fulltext", "introduction",
    "journal", "method", "methods", "paper", "research", "result", "results",
    "study", "supplement", "water", "soil", "groundwater", "environmental",
}


def load_builder():
    """复用 build_download_results 的闸门实现，避免两套校验逻辑漂移。"""
    spec = importlib.util.spec_from_file_location(
        "build_download_results", HERE / "build_download_results.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def assert_header_matches(path: Path, fields: list[str]) -> None:
    """追加前必须确认磁盘上的列顺序与代码里的 fields 完全一致。

    2026-09-17 事故教训：CSV 加了列但代码没同步时，`DictWriter` 会按自己的顺序写值，
    **静默把内容写进错误的列**。这里宁可响亮失败。
    """
    if not path.is_file() or path.stat().st_size == 0:
        return
    with path.open(encoding="utf-8-sig", newline="") as fh:
        header = next(csv.reader(fh), [])
    if header != fields:
        raise SystemExit(
            f"{path.name} 的表头与代码不一致，拒绝追加（否则会写错列）：\n"
            f"  文件: {header}\n  代码: {fields}\n"
            f"请先同步 accept_batch.py 的字段列表或修正该 CSV。")


def backup_once(path: Path) -> None:
    """首次改动前留一份备份（索引被写坏的代价太高）。"""
    if path.is_file() and path.stat().st_size > 0:
        bak = path.with_suffix(path.suffix + ".bak")
        if not bak.exists():
            shutil.copy2(path, bak)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    assert_header_matches(path, fields)
    backup_once(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def normalized_title(value: str) -> str:
    """题名身份比对只容忍大小写和排版空白差异，不做模糊匹配。"""
    return " ".join((value or "").split()).casefold()


def normalized_doi(value: str) -> str:
    """将 DOI URL/标签写法统一后比对，仍要求 DOI 本体完全相同。"""
    value = (value or "").strip().casefold()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if value.startswith(prefix):
            value = value[len(prefix):].strip()
            break
    return value


def row_fingerprint(row: dict[str, str]) -> str:
    """给 CSV 行生成稳定快照，用于防止校验后、写回前被替换。"""
    payload = json.dumps(row, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def identity_token_errors(batch: Path,
                          tokens: dict[str, tuple[str, ...]]) -> list[str]:
    """Identity token 必须是硬标识，不能是空值、短词或普通学术词。"""
    errors: list[str] = []
    token_path = batch / "outputs/identity_tokens.json"
    if token_path.is_file():
        try:
            raw_doc = json.loads(token_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return [f"{batch.name} identity_tokens.json 无法解析：{exc}"]
        if not isinstance(raw_doc, dict) or not raw_doc:
            errors.append(f"{batch.name} identity_tokens.json 必须是非空对象")
        else:
            for paper_id, values in raw_doc.items():
                if not isinstance(values, list) or not values:
                    errors.append(
                        f"{batch.name} identity_tokens.json {paper_id or '<empty>'} "
                        "必须是非空 token 数组")
    for paper_id, values in tokens.items():
        if not values:
            errors.append(
                f"{batch.name} identity_tokens.json {paper_id or '<empty>'} token 列表为空")
        for raw in values:
            token = str(raw).strip()
            folded = token.casefold()
            if not token:
                reason = "为空"
            elif len(token) < 8:
                reason = "过短（少于 8 字符）"
            elif (folded in COMMON_IDENTITY_TOKENS
                  or re.fullmatch(r"[a-z\s._:/-]+", folded)):
                reason = "过常见，不是可唯一定位的硬标识"
            else:
                continue
            errors.append(
                f"{batch.name} identity_tokens.json {paper_id or '<empty>'} token {token!r} {reason}")
    return errors


def validate_manual_backlog(batch: Path, gaps: list[dict[str, str]]) \
        -> tuple[list[str], list[dict[str, str]]]:
    """校验批次待人工表与已验证的缺口一一对应。"""
    path = batch / "outputs/manual_backlog.csv"
    if not path.is_file():
        return [f"{batch.name} manual_backlog.csv 未生成"], []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames or []
        rows = list(reader)
    missing_fields = [field for field in BACKLOG_FIELDS if field not in fields]
    if missing_fields:
        return [f"{batch.name} manual_backlog.csv 缺少必需列：{missing_fields}"], rows

    expected_pairs = Counter((row.get("item_id", ""), row.get("status", "")) for row in gaps)
    actual_pairs = Counter((row.get("item_id", ""), row.get("status", "")) for row in rows)
    errors: list[str] = []
    if expected_pairs != actual_pairs:
        errors.append(
            f"{batch.name} manual_backlog.csv 与 gaps 的 item_id/status 覆盖不一致："
            f"期望 {dict(expected_pairs)}，实际 {dict(actual_pairs)}")
    gap_by_id = {row.get("item_id", ""): row for row in gaps}
    for row in rows:
        item_id = row.get("item_id", "")
        expected = gap_by_id.get(item_id)
        if row.get("batch_id", "") != batch.name:
            errors.append(f"{batch.name} manual_backlog.csv {item_id} batch_id 不一致")
        if expected is None:
            continue
        mismatches = []
        if (row.get("paper_id") or "").strip() != (expected.get("paper_id") or "").strip():
            mismatches.append("paper_id")
        if normalized_title(row.get("title") or "") != normalized_title(expected.get("title") or ""):
            mismatches.append("title")
        if normalized_doi(row.get("doi") or "") != normalized_doi(expected.get("doi") or ""):
            mismatches.append("doi")
        if mismatches:
            errors.append(
                f"{batch.name} manual_backlog.csv {item_id} 身份不一致：{','.join(mismatches)}")
    return errors, rows


def persist_validated_results(path: Path, success_rows: list[dict[str, str]],
                              validated_rows: list[dict[str, str]] | None = None) -> bool:
    """把验收时确认的全文路径原子写回结果表。

    XML-only 记录原先的 ``pdf_path`` 是空的；如果只把 XML 路径写进
    PAPER_INDEX，后续 fulltext queue 仍会读 download_results.csv 并误判为
    unavailable。因此在整个批次验证无误后，把已规范化的路径和
    ``[XML_FULL]`` 类型标记回写到权威结果表。
    """
    validated_rows = validated_rows if validated_rows is not None else success_rows
    if not validated_rows:
        return False
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames or []
        rows = list(reader)
    missing_fields = [field for field in DOWNLOAD_RESULT_FIELDS if field not in fields]
    if missing_fields:
        raise ValueError(
            f"{path.name} 缺少必需列，拒绝回写：{missing_fields}")
    current_ids = [row.get("item_id", "") for row in rows]
    validated_ids = [row.get("item_id", "") for row in validated_rows]
    if Counter(current_ids) != Counter(validated_ids):
        raise RuntimeError(f"{path.name} 在验收校验后发生了行集变化，拒绝写回")
    current_by_id = {row.get("item_id", ""): row for row in rows}
    for validated in validated_rows:
        item_id = validated.get("item_id", "")
        expected_fingerprint = validated.get("_validated_source_fingerprint", "")
        if not expected_fingerprint or row_fingerprint(current_by_id[item_id]) != expected_fingerprint:
            raise RuntimeError(
                f"{path.name} {item_id or '<unknown>'} 在验收校验后被修改，拒绝写回")

    replacements: dict[str, dict[str, str]] = {}
    for row in success_rows:
        item_id = row.get("item_id", "")
        payload = Path(row.get("pdf_path") or "")
        expected_sha256 = row.get("_validated_payload_sha256", "")
        if not payload.is_file() or payload.stat().st_size <= 0:
            raise RuntimeError(f"{item_id} 全文在验收校验后丢失或变空：{payload}")
        if not expected_sha256 or file_sha256(payload) != expected_sha256:
            raise RuntimeError(f"{item_id} 全文在验收校验后内容发生变化：{payload}")
        replacements[item_id] = {
            key: value for key, value in row.items() if not key.startswith("_validated_")
        }
    merged = [replacements.get(row.get("item_id", ""), row) for row in rows]
    if merged == rows:
        return False
    backup_once(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(merged)
    os.replace(tmp, path)
    return True


def validate(root: Path, batch: Path, builder) -> tuple[list[str], list[dict], list[dict]]:
    """返回 (errors, success_rows, gap_rows)。"""
    errors: list[str] = []
    manifest_path = batch / "MANIFEST.csv"
    manifest = read_csv(manifest_path)
    if not manifest:
        return [f"{batch.name} MANIFEST.csv 缺失或无数据行"], [], []
    with manifest_path.open(encoding="utf-8-sig", newline="") as handle:
        manifest_fields = csv.DictReader(handle).fieldnames or []
    missing_manifest_fields = [field for field in MANIFEST_FIELDS if field not in manifest_fields]
    if missing_manifest_fields:
        return [f"{batch.name} MANIFEST.csv 缺少必需列：{missing_manifest_fields}"], [], []
    blank_keys = [str(index) for index, row in enumerate(manifest, 2)
                  if not (row.get("item_id") or "").strip()
                  or not (row.get("paper_id") or "").strip()]
    if blank_keys:
        return [f"{batch.name} MANIFEST.csv 第 {blank_keys[:5]} 行 item_id/paper_id 不得为空"], [], []
    results_path = batch / "outputs/download_results.csv"
    results = read_csv(results_path)
    if not results:
        return [f"{batch.name} 缺 outputs/download_results.csv（先跑 build_download_results.py）"], [], []
    with results_path.open(encoding="utf-8-sig", newline="") as handle:
        result_fields = csv.DictReader(handle).fieldnames or []
    missing_fields = [field for field in DOWNLOAD_RESULT_FIELDS if field not in result_fields]
    if missing_fields:
        return [f"{batch.name} download_results.csv 缺少必需列：{missing_fields}"], [], []
    m_ids = [r.get("item_id", "") for r in manifest]
    r_ids = [r.get("item_id", "") for r in results]
    if set(m_ids) != set(r_ids):
        missing = sorted(set(m_ids) - set(r_ids))
        extra = sorted(set(r_ids) - set(m_ids))
        errors.append(f"{batch.name} download_results.csv 与 MANIFEST 不一致：缺 {missing[:5]} 多 {extra[:5]}")
    duplicate_manifest = sorted(item_id for item_id, count in Counter(m_ids).items() if count > 1)
    duplicate_results = sorted(item_id for item_id, count in Counter(r_ids).items() if count > 1)
    if duplicate_manifest:
        errors.append(f"{batch.name} MANIFEST.csv 有重复 item_id：{duplicate_manifest[:5]}")
    if duplicate_results:
        errors.append(f"{batch.name} download_results.csv 有重复 item_id：{duplicate_results[:5]}")

    files_dir = batch / "outputs/files"
    strays = [p.name for p in files_dir.glob("*")
              if p.suffix in (".part", ".crdownload") or ".partial" in p.name]
    if strays:
        errors.append(f"{batch.name} outputs/files 有分片残留：{strays[:5]}")

    tokens = builder.load_tokens(batch)
    errors.extend(identity_token_errors(batch, tokens))
    manifest_by_id = {r.get("item_id", ""): r for r in manifest}
    targets = {r.get("paper_id", ""): (r.get("suggested_filename") or r.get("target_filename") or "")
               for r in manifest}
    success, gaps = [], []
    for row in results:
        item_id = row.get("item_id", "") or "<unknown>"
        expected = manifest_by_id.get(row.get("item_id", ""))
        if expected is not None:
            mismatches = []
            if (row.get("paper_id") or "").strip() != (expected.get("paper_id") or "").strip():
                mismatches.append("paper_id")
            if normalized_title(row.get("title") or "") != normalized_title(expected.get("title") or ""):
                mismatches.append("title")
            if normalized_doi(row.get("doi") or "") != normalized_doi(expected.get("doi") or ""):
                mismatches.append("doi")
            if mismatches:
                errors.append(
                    f"{batch.name} {item_id} 与 MANIFEST 身份不一致：{','.join(mismatches)}")
                continue
        status = (row.get("status") or "").strip()
        allowed_statuses = set(builder.SUCCESS) | MANUAL_STATUSES
        if status not in allowed_statuses:
            errors.append(
                f"{batch.name} {item_id} status {status!r} 为空或不在允许集合 "
                f"{sorted(allowed_statuses)}")
            continue
        if status in builder.SUCCESS:
            path_text = (row.get("pdf_path") or "").strip()
            payload: Path | None = None
            payload_kind = "pdf"

            if path_text:
                payload = Path(path_text).expanduser()
                if not payload.is_absolute():
                    payload = root / payload
                if payload.suffix.lower() == ".xml":
                    payload_kind = "xml"
            else:
                # XML_FULL 条目在 download_results.csv 中没有 pdf_path；只有在
                # 标准目录确实找到全文 XML 并重过闸门时，才能视为成功。
                payload_kind = "xml"
                payload = builder.find_xml(
                    batch / "outputs/tdm-xml", row.get("paper_id", ""),
                    targets.get(row.get("paper_id", ""), ""), row.get("doi", ""))

            if not payload.is_file():
                detail = f"交付路径不存在：{payload}" if path_text else "未填写 pdf_path，且未找到全文 XML"
                errors.append(f"{batch.name} {item_id} {detail}")
                continue
            try:
                payload_size = payload.stat().st_size
            except OSError as exc:
                errors.append(f"{batch.name} {item_id} 无法读取交付件：{payload}（{exc}）")
                continue
            if payload_size <= 0:
                errors.append(f"{batch.name} {item_id} 交付件为空文件：{payload}")
                continue

            if payload_kind == "xml":
                gate = builder.gate_xml(payload, row.get("doi", ""), row.get("title", ""))
            else:
                gate = builder.gate_pdf(
                    payload, row.get("doi", ""), row.get("title", ""),
                    tokens.get(row.get("paper_id", ""), ()))
            if not (gate.get("ok") and gate.get("identity_verified")):
                errors.append(
                    f"{batch.name} {item_id} 交付件未过闸门/身份校验："
                    f"{gate.get('reason') or gate.get('how') or '未知原因'}")
                continue

            payload_sha256 = file_sha256(payload)
            accepted = dict(row)
            accepted["_validated_source_fingerprint"] = row_fingerprint(row)
            accepted["_validated_payload_sha256"] = payload_sha256
            accepted["pdf_path"] = str(payload)
            accepted["identity_verified"] = "true"
            if "file_bytes" in accepted:
                accepted["file_bytes"] = str(payload_size)
            if "sha256" in accepted:
                accepted["sha256"] = str(gate.get("sha256") or payload_sha256)
            if payload_kind == "xml":
                notes = row.get("notes") or ""
                if not notes.startswith("[XML_FULL]"):
                    notes = "[XML_FULL] 全文 XML（铁律 1），该条无 PDF；" + notes
                accepted["notes"] = notes[:300]
            success.append(accepted)
        else:
            gap = dict(row)
            gap["_validated_source_fingerprint"] = row_fingerprint(row)
            gaps.append(gap)
    return errors, success, gaps


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("batch_id")
    parser.add_argument("--actor-role", choices=["worker", "coordinator"], required=True)
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reaccept", action="store_true",
                        help="重验收已 ACCEPTED 的批次：替换本批次索引行和验收记录")
    args = parser.parse_args()

    if args.actor_role != "coordinator":
        raise SystemExit("只有协调Agent可以验收批次（--actor-role coordinator）")

    root = Path(args.root).expanduser().resolve()
    batch = root / "02_batches" / args.batch_id
    if not (batch / "MANIFEST.csv").is_file():
        raise SystemExit(f"批次不存在：{batch}")

    reaccept = bool(args.reaccept)
    lock_path = root / "00_control/.queue.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        index_path = root / "00_control/BATCH_INDEX.csv"
        with index_path.open(encoding="utf-8-sig", newline="") as handle:
            index_reader = csv.DictReader(handle)
            index_fields = index_reader.fieldnames or []
            index = list(index_reader)
        required_index_fields = {"batch_id", "status", "updated_at", "accepted_at"}
        if not required_index_fields <= set(index_fields):
            raise SystemExit(
                f"BATCH_INDEX.csv 缺少状态字段：{sorted(required_index_fields - set(index_fields))}")
        index_row = next((row for row in index if row.get("batch_id") == args.batch_id), None)
        if index_row is None:
            raise SystemExit(f"BATCH_INDEX.csv 里没有 {args.batch_id}")

        status_path = batch / "STATUS.json"
        if not status_path.is_file():
            raise SystemExit(f"{args.batch_id} 缺 STATUS.json")
        status_doc = json.loads(status_path.read_text(encoding="utf-8"))
        index_status = (index_row.get("status") or "").strip()
        batch_status = (status_doc.get("status") or "").strip()
        if index_status != batch_status:
            raise SystemExit(
                f"{args.batch_id} 状态不一致：BATCH_INDEX={index_status}, STATUS.json={batch_status}")

        existing_papers = read_csv(root / PAPER_INDEX)
        existing_backlog = read_csv(root / MANUAL_BACKLOG)
        already = any(row.get("source_batch_id") == args.batch_id for row in existing_papers)
        if already and batch_status == "ACCEPTED" and not reaccept:
            print(f"{args.batch_id}: 已是 ACCEPTED 且索引已登记，无需重复验收")
            return 0
        if reaccept and batch_status != "ACCEPTED":
            raise SystemExit(
                f"{args.batch_id} 当前状态 {batch_status}，--reaccept 只用于已 ACCEPTED 的批次")
        if not reaccept and batch_status != "REVIEW":
            raise SystemExit(f"{args.batch_id} 当前状态 {batch_status}，只有 REVIEW 可以验收")

        # 在同一 queue lock 临界区内重新读取并校验所有交付件。
        builder = load_builder()
        errors, success, gaps = validate(root, batch, builder)
        print(f"批次 {args.batch_id}")
        print(f"  条目: {len(success) + len(gaps)} | 成功 {len(success)} | 缺口 {len(gaps)}")
        for err in errors:
            print(f"  ERROR {err}")
        if errors:
            print("验收中止：先修掉上面的问题（不写任何文件）")
            return 1

        now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

        def derived_md(pid: str, subdir: str) -> str:
            md = root / subdir / pid / "full.md"
            return str(md) if md.is_file() and md.stat().st_size > 0 else ""

        paper_rows = [{
            "paper_id": row.get("paper_id", ""), "title": row.get("title", ""),
            "doi": row.get("doi", ""), "pdf_path": row.get("pdf_path", ""),
            "supplement_paths": row.get("supplement_paths", ""),
            "sha256": row.get("sha256", ""), "source_batch_id": args.batch_id,
            "identity_verified": row.get("identity_verified", ""), "accepted_at": now,
            "notes": (row.get("notes", "") or "")[:300],
            "mineru_md_path": derived_md(
                row.get("paper_id", ""), DERIVED_MD_DIRS["mineru_md_path"]),
            "xml_md_path": derived_md(
                row.get("paper_id", ""), DERIVED_MD_DIRS["xml_md_path"]),
        } for row in success]

        if args.dry_run:
            print(f"  [dry-run] 将登记 {len(paper_rows)} 条到 {PAPER_INDEX}")
            print(f"  [dry-run] 将并入 {len(gaps)} 条到 {MANUAL_BACKLOG}")
            print("  [dry-run] 将置为 ACCEPTED")
            return 0

        try:
            subprocess.run(
                [sys.executable, str(HERE / "make_manual_backlog.py"),
                 args.batch_id, "--root", str(root)],
                check=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            print(f"  ERROR manual_backlog.csv 生成失败：{exc}")
            print("验收中止：未更新索引或状态")
            return 1
        backlog_errors, backlog_rows = validate_manual_backlog(batch, gaps)
        if backlog_errors:
            for err in backlog_errors:
                print(f"  ERROR {err}")
            print("验收中止：待人工清单未覆盖全部缺口")
            return 1

        try:
            normalized_results = persist_validated_results(
                batch / "outputs/download_results.csv", success, success + gaps)
        except (OSError, ValueError, RuntimeError) as exc:
            print(f"  ERROR 校验后的结果/全文发生变化：{exc}")
            print("验收中止：未更新索引或状态")
            return 1

        known_papers = {(row.get("source_batch_id"), row.get("pdf_path") or row.get("doi"))
                        for row in existing_papers}
        fresh_papers = [r for r in paper_rows
                        if (r["source_batch_id"], r["pdf_path"] or r["doi"]) not in known_papers]
        known_backlog = {(row.get("batch_id"), row.get("item_id"))
                         for row in existing_backlog}
        fresh_backlog = [r for r in backlog_rows
                         if (r["batch_id"], r["item_id"]) not in known_backlog]
        if reaccept:
            paper_kept = [r for r in existing_papers
                          if r.get("source_batch_id") != args.batch_id]
            backlog_kept = [r for r in existing_backlog
                            if r.get("batch_id") != args.batch_id]
            final_papers = paper_kept + paper_rows
            final_backlog = backlog_kept + backlog_rows
            n_papers, n_backlog = len(paper_rows), len(backlog_rows)
        else:
            final_papers = existing_papers + fresh_papers
            final_backlog = existing_backlog + fresh_backlog
            n_papers, n_backlog = len(fresh_papers), len(fresh_backlog)
        # 全局 CSV 不再 append：统一临时文件 + os.replace，避免半行/半批写入。
        write_csv(root / PAPER_INDEX, PAPER_FIELDS, final_papers)
        write_csv(root / MANUAL_BACKLOG, BACKLOG_FIELDS, final_backlog)

        record = root / "03_review" / f"{args.batch_id}-acceptance.md"
        record.parent.mkdir(parents=True, exist_ok=True)
        tmp_record = record.with_suffix(".md.tmp")
        tmp_record.write_text(
            f"# {args.batch_id} 验收记录\n\n"
            f"- 验收时间：{now}\n- 验收人角色：协调Agent（accept_batch.py）\n"
            f"- 交付成功：{len(success)} 条（其中身份已证实 "
            f"{sum(1 for r in success if r.get('identity_verified') == 'true')} 条）\n"
            f"- 缺口：{len(gaps)} 条（已并入 MANUAL_DOWNLOAD_BACKLOG.csv）\n"
            f"- 本次{'替换登记' if reaccept else '新登记'}：索引 {n_papers} 条 / 待人工 {n_backlog} 条\n"
            + ("- 重新验收：按修正后的 download_results.csv 重过身份、完整性和正文闸门。\n"
               if reaccept else "")
            + "\n"
            "## 缺口明细\n\n"
            + ("\n".join(f"- `{r['item_id']}` {r.get('paper_id','')} — {r.get('status','')}"
                         f"：{(r.get('notes','') or '')[:160]}" for r in gaps)
               or "- 无缺口") + "\n",
            encoding="utf-8")
        os.replace(tmp_record, record)

        status_now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        note = (f"accept_batch.py {'重新' if reaccept else ''}验收："
                f"成功 {len(success)} / 缺口 {len(gaps)}")
        status_doc["status"] = "ACCEPTED"
        status_doc["updated_at"] = status_now
        status_doc["last_note"] = note
        tmp_status = status_path.with_suffix(".json.tmp")
        tmp_status.write_text(
            json.dumps(status_doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        for row in index:
            if row.get("batch_id") == args.batch_id:
                row["status"] = "ACCEPTED"
                row["updated_at"] = status_now
                row["accepted_at"] = status_now
        tmp_index = index_path.with_suffix(".csv.tmp")
        with tmp_index.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=index_fields)
            writer.writeheader()
            writer.writerows(index)
        os.replace(tmp_status, status_path)
        os.replace(tmp_index, index_path)

    print(f"  已登记索引 {n_papers} 条、待人工 {n_backlog} 条")
    if normalized_results:
        print("  已将验证后的全文路径/类型写回 outputs/download_results.csv")
    print(f"  验收记录：{record.relative_to(root)}")
    if reaccept:
        print(f"  {args.batch_id} 保持 ACCEPTED（索引已按修正结果替换）")
        return 0
    print(f"  {args.batch_id} → ACCEPTED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
