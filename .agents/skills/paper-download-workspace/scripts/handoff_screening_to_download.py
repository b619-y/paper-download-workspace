#!/usr/bin/env python3
"""Validate a screened search result and create a READY download batch."""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_ROOT = Path("/Users/b/Desktop/论文下载")
OUTPUT_FIELDS = [
    "paper_id", "title", "authors", "year", "journal", "doi", "landing_url",
    "priority", "download_reason", "requested_files", "source_database",
    "source_record", "screening_decision", "notes",
]
VALID_DECISIONS = {"download", "exclude", "uncertain"}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return [{str(k): (v or "").strip() for k, v in row.items()} for row in csv.DictReader(handle)]


def normalize_doi(value: str) -> str:
    value = value.strip().casefold()
    value = re.sub(r"^https?://(dx\.)?doi\.org/", "", value)
    value = re.sub(r"^doi\s*:\s*", "", value)
    return value.strip().strip(".,; ")


def normalize_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def identity_key(row: dict[str, str]) -> str:
    doi = normalize_doi(row.get("doi", ""))
    if doi:
        return f"doi:{doi}"
    title = normalize_title(row.get("title", ""))
    if title:
        return f"title:{title}:{row.get('year', '').strip()}"
    return ""


def merge_values(left: str, right: str) -> str:
    values = [value for value in left.split(";") if value]
    for value in right.split(";"):
        if value and value not in values:
            values.append(value)
    return ";".join(values)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def validate_search_manifest(path: Path, allow_incomplete: bool) -> dict:
    if not path.is_file():
        raise ValueError(f"search manifest not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    source_states = {name: info.get("status") for name, info in (data.get("sources") or {}).items()}
    bad = {name: state for name, state in source_states.items() if state != "complete"}
    if (not data.get("complete") or bad) and not allow_incomplete:
        raise ValueError(
            "search run is incomplete; rerun failed/blocked sources or pass "
            f"--allow-incomplete-search after disclosing the gap: {bad or source_states}"
        )
    return data


def build_download_rows(rows: list[dict[str, str]], include_uncertain: bool,
                        priority: str, requested_files: str) -> tuple[list[dict[str, str]], dict[str, int]]:
    counts = {"input": len(rows), "download": 0, "uncertain": 0, "exclude": 0, "duplicates_removed": 0}
    grouped: dict[str, dict[str, str]] = {}
    decisions: dict[str, str] = {}
    for line, row in enumerate(rows, 2):
        decision = row.get("decision", "").strip().casefold()
        if not decision:
            raise ValueError(f"row {line} has no screening decision")
        if decision not in VALID_DECISIONS:
            raise ValueError(f"row {line} has invalid decision: {decision}")
        counts[decision] += 1
        key = identity_key(row)
        if not key:
            raise ValueError(f"row {line} requires DOI or title")
        if key in decisions and decisions[key] != decision:
            raise ValueError(f"duplicate record has conflicting decisions ({decisions[key]} vs {decision}): {key}")
        if key in grouped:
            counts["duplicates_removed"] += 1
            current = grouped[key]
            for field in ("source_database", "source_record", "record_id", "landing_url"):
                current[field] = merge_values(current.get(field, ""), row.get(field, ""))
            continue
        grouped[key] = dict(row)
        decisions[key] = decision

    selected: list[dict[str, str]] = []
    for key, row in grouped.items():
        decision = decisions[key]
        if decision == "exclude" or (decision == "uncertain" and not include_uncertain):
            continue
        reason = row.get("matched_criteria") or row.get("needs_fulltext_reason")
        if not reason:
            raise ValueError(f"selected record lacks matched_criteria/needs_fulltext_reason: {key}")
        notes = "; ".join(filter(None, (
            f"screening_decision={decision}",
            f"screening_basis={row.get('screening_basis', '')}" if row.get("screening_basis") else "",
            row.get("notes", ""),
        )))
        selected.append({
            "paper_id": row.get("record_id", ""),
            "title": row.get("title", ""),
            "authors": row.get("authors", ""),
            "year": row.get("year", ""),
            "journal": row.get("journal", ""),
            "doi": normalize_doi(row.get("doi", "")),
            "landing_url": row.get("landing_url", ""),
            "priority": priority,
            "download_reason": reason,
            "requested_files": requested_files,
            "source_database": row.get("source_database", ""),
            "source_record": row.get("source_record", ""),
            "screening_decision": decision,
            "notes": notes,
        })
    if not selected:
        raise ValueError("screening produced no records eligible for download")
    return selected, counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--screening", required=True, help="completed pre_screening.csv")
    parser.add_argument("--search-manifest", help="defaults to run_manifest.json beside screening CSV")
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--purpose", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--priority", choices=["urgent", "high", "normal", "low"], default="normal")
    parser.add_argument("--requested-files", default="article_pdf")
    parser.add_argument("--download-only", action="store_true", help="exclude uncertain records from handoff")
    parser.add_argument("--allow-incomplete-search", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="write the cleaned list but do not create a batch")
    parser.add_argument("--output", help="default: download_manifest.csv beside screening CSV")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    screening = Path(args.screening).expanduser().resolve()
    if not screening.is_file():
        parser.error(f"screening CSV not found: {screening}")
    search_manifest = Path(args.search_manifest).expanduser().resolve() if args.search_manifest else screening.with_name("run_manifest.json")
    try:
        search_info = validate_search_manifest(search_manifest, args.allow_incomplete_search)
        selected, counts = build_download_rows(
            read_csv(screening), not args.download_only, args.priority, args.requested_files,
        )
    except (ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))

    output = Path(args.output).expanduser().resolve() if args.output else screening.with_name("download_manifest.csv")
    protected = {screening, search_manifest, screening.with_name("deduplicated_records.csv")}
    if output in protected:
        parser.error(f"output must not overwrite search evidence: {output}")
    if output.exists() and not args.overwrite:
        parser.error(f"output already exists; pass --overwrite to replace: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    write_csv(output, selected)
    summary: dict[str, object] = {
        "created_at": utc_now(), "screening": str(screening), "search_manifest": str(search_manifest),
        "download_manifest": str(output), "selected_count": len(selected), **counts,
    }
    if args.dry_run:
        summary["status"] = "dry_run"
        print(json.dumps(summary, ensure_ascii=False))
        return 0

    source_context = (
        f"API search query_id={search_info.get('query_id', '')}; "
        f"query={search_info.get('original_query', '')}; screening={screening}; "
        f"search_complete={bool(search_info.get('complete'))}"
    )
    command = [
        sys.executable, str(Path(__file__).with_name("enqueue_download_batch.py")),
        "--root", str(Path(args.root).expanduser().resolve()),
        "--manifest", str(output), "--purpose", args.purpose, "--title", args.title,
        "--priority", args.priority, "--requested-files", args.requested_files,
        "--source-context", source_context,
    ]
    try:
        completed = subprocess.run(command, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or str(exc)).strip()
        parser.error(f"failed to create download batch: {detail}")
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("enqueue script returned no batch result")
    batch_info = json.loads(lines[-1])
    batch = Path(batch_info["batch_path"])
    evidence = batch / "inputs/search_screening"
    evidence.mkdir(parents=True, exist_ok=False)
    evidence_files = (
        (screening, "pre_screening.csv"),
        (search_manifest, "run_manifest.json"),
        (output, "download_manifest.csv"),
        (screening.with_name("deduplicated_records.csv"), "deduplicated_records.csv"),
    )
    for source, target_name in evidence_files:
        if source.is_file():
            shutil.copy2(source, evidence / target_name)
    summary.update({"status": "READY", "batch": batch_info})
    (evidence / "handoff_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
