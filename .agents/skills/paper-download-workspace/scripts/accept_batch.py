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
   * 最后用 `update_batch_status.py --actor-role coordinator` 置 `ACCEPTED`。

    python3 accept_batch.py <batch_id> --actor-role coordinator --dry-run
    python3 accept_batch.py <batch_id> --actor-role coordinator

已 ACCEPTED 但交付件后来被证明错配时，用 `--reaccept` 重过闸门，
替换本批次在论文库索引和待人工清单里的行，并重写验收记录。状态保持 ACCEPTED。
"""
from __future__ import annotations

import argparse
import csv
import fcntl
import json
import os
import shutil
import importlib.util
import subprocess
import sys
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


def append_rows(path: Path, fields: list[str], rows: list[dict[str, str]]) -> int:
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    assert_header_matches(path, fields)
    backup_once(path)
    exists = path.is_file() and path.stat().st_size > 0
    with path.open("a", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def validate(root: Path, batch: Path, builder) -> tuple[list[str], list[dict], list[dict]]:
    """返回 (errors, success_rows, gap_rows)。"""
    errors: list[str] = []
    manifest = read_csv(batch / "MANIFEST.csv")
    results = read_csv(batch / "outputs/download_results.csv")
    if not results:
        return [f"{batch.name} 缺 outputs/download_results.csv（先跑 build_download_results.py）"], [], []
    m_ids = [r["item_id"] for r in manifest]
    r_ids = [r["item_id"] for r in results]
    if set(m_ids) != set(r_ids):
        missing = sorted(set(m_ids) - set(r_ids))
        extra = sorted(set(r_ids) - set(m_ids))
        errors.append(f"{batch.name} download_results.csv 与 MANIFEST 不一致：缺 {missing[:5]} 多 {extra[:5]}")

    files_dir = batch / "outputs/files"
    strays = [p.name for p in files_dir.glob("*")
              if p.suffix in (".part", ".crdownload") or ".partial" in p.name]
    if strays:
        errors.append(f"{batch.name} outputs/files 有分片残留：{strays[:5]}")

    tokens = builder.load_tokens(batch)
    targets = {r.get("paper_id", ""): (r.get("suggested_filename") or r.get("target_filename") or "")
               for r in manifest}
    success, gaps = [], []
    for row in results:
        status = (row.get("status") or "").strip()
        if status in builder.SUCCESS:
            pdf = Path(row.get("pdf_path") or "") if row.get("pdf_path") else None
            if pdf and pdf.is_file():
                gate = builder.gate_pdf(pdf, row.get("doi", ""), row.get("title", ""),
                                        tokens.get(row.get("paper_id", ""), ()))
                if not (gate["ok"] and gate["identity_verified"]):
                    errors.append(f"{batch.name} {row['item_id']} 交付件未过闸门/身份校验："
                                  f"{gate['reason'] or gate['how']}")
            if not row.get("pdf_path"):
                # XML_FULL 条目：索引有路径才找得到文件；这里指向 XML 并在 notes 里说明（铁律 1）
                xml = builder.find_xml(batch / "outputs/tdm-xml", row.get("paper_id", ""),
                                       targets.get(row.get("paper_id", ""), ""),
                                       row.get("doi", ""))
                if xml.is_file():
                    row = dict(row)
                    row["pdf_path"] = str(xml)
                    row["notes"] = ("[XML_FULL] 全文 XML（铁律 1），该条无 PDF；"
                                    + (row.get("notes") or ""))[:300]
            success.append(row)
        else:
            gaps.append(row)
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

    index = read_csv(root / "00_control/BATCH_INDEX.csv")
    row = next((r for r in index if r.get("batch_id") == args.batch_id), None)
    if row is None:
        raise SystemExit(f"BATCH_INDEX.csv 里没有 {args.batch_id}")
    status = (row.get("status") or "").strip()
    already = any(r.get("source_batch_id") == args.batch_id
                  for r in read_csv(root / PAPER_INDEX))
    reaccept = bool(args.reaccept)
    if already and status == "ACCEPTED" and not reaccept:
        print(f"{args.batch_id}: 已是 ACCEPTED 且索引已登记，无需重复验收")
        return 0
    if reaccept and status != "ACCEPTED":
        raise SystemExit(f"{args.batch_id} 当前状态 {status}，--reaccept 只用于已 ACCEPTED 的批次")
    if not reaccept and status != "REVIEW":
        raise SystemExit(f"{args.batch_id} 当前状态 {status}，只有 REVIEW 可以验收")

    builder = load_builder()
    errors, success, gaps = validate(root, batch, builder)
    if not (batch / "outputs/manual_backlog.csv").is_file() and not args.dry_run:
        subprocess.run([sys.executable, str(HERE / "make_manual_backlog.py"),
                        args.batch_id, "--root", str(root)], check=False)

    print(f"批次 {args.batch_id}")
    print(f"  条目: {len(success) + len(gaps)} | 成功 {len(success)} | 缺口 {len(gaps)}")
    unverified = [r["item_id"] for r in success if r.get("identity_verified") != "true"]
    if unverified:
        print(f"  ⚠ 成功但身份未证实: {unverified}")
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
        "paper_id": r.get("paper_id", ""), "title": r.get("title", ""), "doi": r.get("doi", ""),
        "pdf_path": r.get("pdf_path", ""), "supplement_paths": r.get("supplement_paths", ""),
        "sha256": r.get("sha256", ""), "source_batch_id": args.batch_id,
        "identity_verified": r.get("identity_verified", ""), "accepted_at": now,
        "notes": (r.get("notes", "") or "")[:300],
        "mineru_md_path": derived_md(r.get("paper_id", ""), DERIVED_MD_DIRS["mineru_md_path"]),
        "xml_md_path": derived_md(r.get("paper_id", ""), DERIVED_MD_DIRS["xml_md_path"]),
    } for r in success]
    backlog_rows = read_csv(batch / "outputs/manual_backlog.csv")

    if args.dry_run:
        print(f"  [dry-run] 将登记 {len(paper_rows)} 条到 {PAPER_INDEX}")
        print(f"  [dry-run] 将并入 {len(backlog_rows)} 条到 {MANUAL_BACKLOG}")
        print("  [dry-run] 将置为 ACCEPTED")
        return 0

    lock_path = root / "00_control/.queue.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        known_papers = {(r.get("source_batch_id"), r.get("pdf_path") or r.get("doi"))
                        for r in read_csv(root / PAPER_INDEX)}
        fresh_papers = [r for r in paper_rows
                        if (r["source_batch_id"], r["pdf_path"] or r["doi"]) not in known_papers]
        known_backlog = {(r.get("batch_id"), r.get("item_id"))
                         for r in read_csv(root / MANUAL_BACKLOG)}
        fresh_backlog = [r for r in backlog_rows
                         if (r["batch_id"], r["item_id"]) not in known_backlog]
        if reaccept:
            paper_kept = [r for r in read_csv(root / PAPER_INDEX)
                          if r.get("source_batch_id") != args.batch_id]
            backlog_kept = [r for r in read_csv(root / MANUAL_BACKLOG)
                            if r.get("batch_id") != args.batch_id]
            write_csv(root / PAPER_INDEX, PAPER_FIELDS, paper_kept + paper_rows)
            write_csv(root / MANUAL_BACKLOG, BACKLOG_FIELDS, backlog_kept + backlog_rows)
            n_papers, n_backlog = len(paper_rows), len(backlog_rows)
        else:
            n_papers = append_rows(root / PAPER_INDEX, PAPER_FIELDS, fresh_papers)
            n_backlog = append_rows(root / MANUAL_BACKLOG, BACKLOG_FIELDS, fresh_backlog)
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
        if reaccept:
            note = (f"accept_batch.py 重新验收：成功 {len(success)} / 缺口 {len(gaps)}")
            status_path = batch / "STATUS.json"
            status_doc = json.loads(status_path.read_text(encoding="utf-8"))
            status_doc["status"] = "ACCEPTED"
            status_doc["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            status_doc["last_note"] = note
            tmp_status = status_path.with_suffix(".json.tmp")
            tmp_status.write_text(json.dumps(status_doc, ensure_ascii=False, indent=2) + "\n",
                                  encoding="utf-8")
            os.replace(tmp_status, status_path)
            index_path = root / "00_control/BATCH_INDEX.csv"
            with index_path.open(encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                fields = reader.fieldnames or []
                index_rows = list(reader)
            for index_row in index_rows:
                if index_row.get("batch_id") == args.batch_id:
                    index_row["status"] = "ACCEPTED"
                    index_row["updated_at"] = status_doc["updated_at"]
                    index_row["accepted_at"] = status_doc["updated_at"]
            tmp_index = index_path.with_suffix(".csv.tmp")
            with tmp_index.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerows(index_rows)
            os.replace(tmp_index, index_path)
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    print(f"  已登记索引 {n_papers} 条、待人工 {n_backlog} 条")
    print(f"  验收记录：{record.relative_to(root)}")
    if reaccept:
        print(f"  {args.batch_id} 保持 ACCEPTED（索引已按修正结果替换）")
        return 0
    code = subprocess.run([sys.executable, str(HERE / "update_batch_status.py"),
                           "--actor-role", "coordinator", "--worker", "coordinator",
                           args.batch_id, "ACCEPTED", "--note",
                           f"accept_batch.py 验收：成功 {len(success)} / 缺口 {len(gaps)}",
                           "--root", str(root)], check=False).returncode
    if code != 0:
        raise SystemExit("状态更新失败（索引已写入，请检查 STATUS.json 后重跑）")
    print(f"  {args.batch_id} → ACCEPTED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
