#!/usr/bin/env python3
"""Build an evidence-oriented full-text screening queue for one download batch."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


DEFAULT_ROOT = Path.cwd()
SUCCESS = {"downloaded", "reused_verified"}
FIELDS = [
    "item_id", "paper_id", "title", "doi", "fulltext_path", "fulltext_status",
    "criterion_id", "criterion_text", "decision", "evidence_quote", "evidence_page",
    "evidence_section", "evidence_kind", "evidence_figure", "evidence_image_path",
    "evidence_object_ref", "reviewer", "reviewed_at", "notes",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def fulltext_status(result: dict[str, str]) -> tuple[str, str]:
    status = (result.get("status") or "").strip()
    identity = (result.get("identity_verified") or "").strip().lower()
    path_text = (result.get("pdf_path") or "").strip()
    path = Path(path_text).expanduser() if path_text else None
    if status not in SUCCESS or path is None or not path.is_file():
        return "fulltext_unavailable", path_text
    if identity != "true":
        return "identity_unverified", path_text
    return "ready", str(path.resolve())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch_id")
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--criteria", help="CSV with criterion_id,criterion_text")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    batch = root / "02_batches" / args.batch_id
    manifest_path = batch / "MANIFEST.csv"
    results_path = batch / "outputs/download_results.csv"
    output = batch / "outputs/fulltext_screening_queue.csv"
    if not manifest_path.is_file():
        parser.error(f"batch not found: {batch}")
    if not results_path.is_file():
        parser.error(f"missing download results: {results_path}")
    if output.exists() and not args.overwrite:
        parser.error(f"refusing to overwrite existing screening work: {output}; use --overwrite to rebuild")

    criteria = [{"criterion_id": "OVERALL", "criterion_text": ""}]
    if args.criteria:
        criteria_path = Path(args.criteria).expanduser().resolve()
        criteria = read_csv(criteria_path)
        if not criteria:
            parser.error(f"criteria file is empty: {criteria_path}")
        for number, row in enumerate(criteria, 1):
            if not (row.get("criterion_id") or "").strip() or not (row.get("criterion_text") or "").strip():
                parser.error(f"criteria row {number} requires criterion_id and criterion_text")

    manifest = read_csv(manifest_path)
    results = {row.get("item_id", ""): row for row in read_csv(results_path)}
    rows: list[dict[str, str]] = []
    counts = {"ready": 0, "fulltext_unavailable": 0, "identity_unverified": 0}
    for item in manifest:
        result = results.get(item.get("item_id", ""), {})
        state, fulltext_path = fulltext_status(result)
        counts[state] += 1
        for criterion in criteria:
            rows.append({
                "item_id": item.get("item_id", ""),
                "paper_id": item.get("paper_id", ""),
                "title": item.get("title", ""),
                "doi": item.get("doi", ""),
                "fulltext_path": fulltext_path,
                "fulltext_status": state,
                "criterion_id": (criterion.get("criterion_id") or "").strip(),
                "criterion_text": (criterion.get("criterion_text") or "").strip(),
                "decision": "" if state == "ready" else "not_assessed",
                "evidence_quote": "",
                "evidence_page": "",
                "evidence_section": "",
                "evidence_kind": "",
                "evidence_figure": "",
                "evidence_image_path": "",
                "evidence_object_ref": "",
                "reviewer": "",
                "reviewed_at": "",
                "notes": (result.get("notes") or "") if state != "ready" else "",
            })

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"{output} rows={len(rows)} ready={counts['ready']} unavailable={counts['fulltext_unavailable']} unverified={counts['identity_unverified']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
