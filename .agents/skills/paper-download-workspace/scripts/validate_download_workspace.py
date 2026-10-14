#!/usr/bin/env python3
"""Validate the persistent paper-download workspace and batch index."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


DEFAULT_ROOT = Path.cwd()
REQUIRED_DIRS = ["00_control", "01_shared/contracts", "01_shared/prompts", "02_batches", "03_review", "04_library_index", "05_archive"]
REQUIRED_FILES = [
    "README_总控.md", "00_control/WORKSPACE.json", "00_control/BATCH_INDEX.csv",
    "00_control/POLICIES.md", "00_control/DECISIONS.md", "04_library_index/PAPER_INDEX.csv",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    args = parser.parse_args()
    root = Path(args.root).expanduser().resolve()
    errors: list[str] = []
    warnings: list[str] = []
    for rel in REQUIRED_DIRS:
        if not (root / rel).is_dir():
            errors.append(f"missing directory: {rel}")
    for rel in REQUIRED_FILES:
        if not (root / rel).is_file():
            errors.append(f"missing file: {rel}")
    if errors:
        for item in errors:
            print(f"ERROR {item}")
        return 1

    try:
        identity = json.loads((root / "00_control/WORKSPACE.json").read_text(encoding="utf-8"))
        if identity.get("workspace_id") != "paper-download-workspace":
            errors.append("unexpected workspace_id")
    except Exception as exc:
        errors.append(f"invalid WORKSPACE.json: {exc}")

    index = read_csv(root / "00_control/BATCH_INDEX.csv")
    ids = [row.get("batch_id", "") for row in index]
    if len(ids) != len(set(ids)):
        errors.append("duplicate batch_id in BATCH_INDEX.csv")
    for row in index:
        batch_id = row.get("batch_id", "")
        batch = root / "02_batches" / batch_id
        for rel in ["REQUEST.json", "REQUEST.md", "MANIFEST.csv", "STATUS.json", "HANDOFF.md", "inputs", "outputs", "outputs/files", "logs"]:
            if not (batch / rel).exists():
                errors.append(f"{batch_id} missing {rel}")
        try:
            status = json.loads((batch / "STATUS.json").read_text(encoding="utf-8"))
            if status.get("batch_id") != batch_id:
                errors.append(f"{batch_id} STATUS batch_id mismatch")
            if status.get("status") != row.get("status"):
                errors.append(f"{batch_id} status mismatch: index={row.get('status')} local={status.get('status')}")
        except Exception as exc:
            errors.append(f"{batch_id} invalid STATUS.json: {exc}")
        try:
            manifest = read_csv(batch / "MANIFEST.csv")
            if len(manifest) != int(row.get("paper_count") or 0):
                errors.append(f"{batch_id} paper_count mismatch")
            item_ids = [item.get("item_id", "") for item in manifest]
            if len(item_ids) != len(set(item_ids)):
                errors.append(f"{batch_id} duplicate item_id")

            # 下载就绪投影：新批次由入队脚本生成；老批次用 make_download_csv.py 补齐。
            with (batch / "MANIFEST.csv").open(encoding="utf-8-sig", newline="") as handle:
                header = next(csv.reader(handle), [])
            modern = "suggested_filename" in header
            if not modern:
                warnings.append(f"{batch_id} MANIFEST.csv 无 suggested_filename 列（老批次，"
                                f"下载前先跑 scripts/make_download_csv.py {batch_id}）")
            expected = {
                ((item.get("doi") or "").strip(),
                 (item.get("paper_id") or "").strip(),
                 (item.get("suggested_filename") or item.get("target_filename") or "").strip())
                for item in manifest if (item.get("queue_action") or "download") == "download"
            }
            projection = batch / "outputs/zju_papers.csv"
            if not projection.is_file():
                (errors if modern else warnings).append(
                    f"{batch_id} 缺 outputs/zju_papers.csv（下载就绪投影）")
            else:
                if projection.read_bytes()[:3] == b"\xef\xbb\xbf":
                    errors.append(
                        f"{batch_id} outputs/zju_papers.csv 带 BOM —— resolve.py 用 "
                        f"encoding='utf-8' 读取，会把整批判成「无 DOI → manual」；"
                        f"跑 scripts/make_download_csv.py {batch_id} 重建（无 BOM）")
                actual = {
                    ((item.get("doi") or "").strip(),
                     (item.get("paper_id") or "").strip(),
                     (item.get("suggested_filename") or "").strip())
                    for item in read_csv(projection)
                }
                if actual != expected:
                    errors.append(
                        f"{batch_id} outputs/zju_papers.csv 与 MANIFEST 的 download 条目不一致"
                        f"（投影 {len(actual)} 行 vs MANIFEST {len(expected)} 行；"
                        f"跑 scripts/make_download_csv.py {batch_id} 重建）")

            if str(row.get("status") or "") in ("REVIEW", "ACCEPTED"):
                results = batch / "outputs/download_results.csv"
                if not results.is_file():
                    errors.append(f"{batch_id} status={row.get('status')} 但缺 outputs/download_results.csv")
                else:
                    covered = {r.get("item_id", "") for r in read_csv(results) if r.get("item_id")}
                    missing = [i for i in item_ids if i and i not in covered]
                    if missing:
                        errors.append(f"{batch_id} download_results.csv 未覆盖 "
                                      f"{len(missing)} 个 item_id: {missing[:5]}")
        except Exception as exc:
            errors.append(f"{batch_id} invalid MANIFEST.csv: {exc}")

    for batch in (root / "02_batches").iterdir():
        if batch.is_dir() and batch.name not in set(ids):
            warnings.append(f"orphan batch directory: {batch.name}")
    for path in root.rglob("*"):
        lowered = path.name.lower()
        if path.is_file() and (lowered == ".env" or path.suffix == ".key" or "secret" in lowered or "credential" in lowered):
            errors.append(f"possible secret file: {path.relative_to(root)}")

    for item in warnings:
        print(f"WARN {item}")
    for item in errors:
        print(f"ERROR {item}")
    if errors:
        return 1
    print(f"OK batches={len(index)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
