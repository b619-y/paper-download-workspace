#!/usr/bin/env python3
"""Regenerate a batch's download-ready CSV (`outputs/zju_papers.csv`) from its MANIFEST.

入队脚本（`enqueue_download_batch.py`）现在会在建批次时就写好这份投影，下载Agent直接拿来喂
`zju-edge-paper-download/scripts/pipeline.py --csv` 即可。本脚本用于两种情况：

  * 老批次（在投影内置之前建立的）需要补齐；
  * 批次建立后 MANIFEST 被协调Agent改动过（例如 queue_action 从 download 改成
    reference_existing），需要重建投影，避免下载Agent下到已经有的论文。

用法：
    python3 make_download_csv.py <batch_id> [--root /path/to/paper-workspace]
    python3 make_download_csv.py --all

只读 MANIFEST.csv，只写该批次的 `outputs/zju_papers.csv`；不动任何其他文件。
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


DEFAULT_ROOT = Path.cwd()
FIELDS = ["doi", "paper_id", "suggested_filename", "title"]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    """写下载就绪投影。

    **必须用无 BOM 的 utf-8**：`zju-edge-paper-download/scripts/resolve.py` 用
    `open(csv, encoding="utf-8")` 读取，若带 BOM，第一列名会变成 `\\ufeffdoi`，
    `row.get("doi")` 返回 None，于是**整批被静默判成「无 DOI → manual」**。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build(batch: Path) -> dict[str, int | str]:
    manifest = batch / "MANIFEST.csv"
    if not manifest.is_file():
        raise SystemExit(f"no MANIFEST.csv in {batch}")
    rows = read_csv(manifest)
    out: list[dict[str, str]] = []
    counts = {"download": 0, "reference_existing": 0, "verify_existing": 0, "skipped": 0}
    no_doi = 0
    for row in rows:
        action = (row.get("queue_action") or "download").strip() or "download"
        if action != "download":
            counts[action if action in counts else "skipped"] += 1
            continue
        doi = (row.get("doi") or "").strip()
        filename = (row.get("suggested_filename") or row.get("target_filename") or "").strip()
        if not filename:
            filename = f'{row.get("paper_id") or row.get("item_id") or "paper"}.pdf'
        if not doi:
            # 无 DOI 的行仍然保留：resolve.py 会把它显式列为 manual 缺口，
            # 而不是静默丢弃（见 zju skill §9）。
            no_doi += 1
        out.append({
            "doi": doi,
            "paper_id": (row.get("paper_id") or "").strip(),
            "suggested_filename": filename,
            "title": (row.get("title") or "").strip(),
        })
        counts["download"] += 1
    write_csv(batch / "outputs/zju_papers.csv", out)
    return {
        "batch_id": batch.name,
        "manifest_rows": len(rows),
        "written_rows": len(out),
        "no_doi_rows": no_doi,
        **counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_id", nargs="?")
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--all", action="store_true", help="rebuild every batch under 02_batches")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if args.all:
        batches = sorted(p for p in (root / "02_batches").iterdir() if p.is_dir())
    elif args.batch_id:
        batches = [root / "02_batches" / args.batch_id]
    else:
        parser.error("give a batch_id or --all")

    for batch in batches:
        info = build(batch)
        print(f"{info['batch_id']}: wrote {info['written_rows']} rows "
              f"(manifest {info['manifest_rows']}, "
              f"reference_existing {info['reference_existing']}, "
              f"verify_existing {info['verify_existing']}, no-doi {info['no_doi_rows']}) "
              f"-> {batch / 'outputs/zju_papers.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
