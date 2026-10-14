#!/usr/bin/env python3
"""Create an auditable PDF/XML processing queue for an ACCEPTED download batch."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import processing_queue as queue


SUCCESS = {"downloaded", "reused_verified"}


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        raise ValueError(f"missing required CSV: {path}")
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def resolve_source(root: Path, batch: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    candidates = (root / path, batch / path, batch / "outputs" / path)
    return next((item.resolve() for item in candidates if item.is_file()), (root / path).resolve())


def source_kind(path: Path, notes: str) -> str:
    suffix = path.suffix.casefold()
    if suffix == ".pdf":
        return "pdf"
    if suffix in {".xml", ".nxml"} or "[xml_full]" in notes.casefold():
        return "xml"
    raise ValueError(f"unsupported accepted full-text type: {path}")


def build_queue(root: Path, batch_id: str, *, max_attempts: int = 3) -> dict[str, Any]:
    root = root.expanduser().resolve()
    batch = root / "02_batches" / batch_id
    status_path = batch / "STATUS.json"
    status = load_json(status_path)
    if status.get("batch_id") not in (None, "", batch_id):
        raise ValueError(f"{status_path} belongs to another batch")
    if str(status.get("status") or "").upper() != "ACCEPTED":
        raise ValueError(f"batch {batch_id} is not ACCEPTED")
    _, manifest = read_csv(batch / "MANIFEST.csv")
    result_fields, results = read_csv(batch / "outputs" / "download_results.csv")
    required_result_fields = {"item_id", "paper_id", "status", "title", "doi", "pdf_path", "sha256", "identity_verified"}
    missing = sorted(required_result_fields - set(result_fields))
    if missing:
        raise ValueError(f"download_results.csv is missing fields: {missing}")
    manifest_by_id: dict[str, dict[str, str]] = {}
    for item in manifest:
        item_id = (item.get("item_id") or "").strip()
        if not item_id or item_id in manifest_by_id:
            raise ValueError(f"MANIFEST.csv has blank or duplicate item_id: {item_id!r}")
        manifest_by_id[item_id] = item

    index_fields, index_rows = read_csv(root / "04_library_index" / "PAPER_INDEX.csv")
    for field in ("source_batch_id", "source_item_id", "sha256", "identity_verified"):
        if field not in index_fields:
            raise ValueError(
                f"PAPER_INDEX.csv is missing {field}; run the document-processing index migration first")

    planned: list[dict[str, Any]] = []
    seen_results: set[str] = set()
    for result in results:
        item_id = (result.get("item_id") or "").strip()
        if not item_id or item_id in seen_results:
            raise ValueError(f"download_results.csv has blank or duplicate item_id: {item_id!r}")
        seen_results.add(item_id)
        if (result.get("status") or "").strip() not in SUCCESS:
            continue
        item = manifest_by_id.get(item_id)
        if item is None:
            raise ValueError(f"successful result {item_id} is absent from MANIFEST.csv")
        if (result.get("identity_verified") or "").strip().casefold() != "true":
            raise ValueError(f"successful result {item_id} is not identity_verified=true")
        for field in ("paper_id", "title", "doi"):
            left = " ".join((result.get(field) or "").split()).casefold()
            right = " ".join((item.get(field) or "").split()).casefold()
            if left != right:
                raise ValueError(f"successful result {item_id} does not match MANIFEST {field}")
        raw_path = (result.get("pdf_path") or "").strip()
        if not raw_path:
            raise ValueError(f"successful result {item_id} has no full-text path")
        source = resolve_source(root, batch, raw_path)
        if not source.is_file() or source.stat().st_size == 0:
            raise ValueError(f"successful result {item_id} source is missing or empty: {source}")
        actual_sha = queue.file_sha256(source)
        expected_sha = (result.get("sha256") or "").strip().casefold()
        if not expected_sha or expected_sha != actual_sha:
            raise ValueError(f"successful result {item_id} source SHA-256 mismatch")
        registered = [
            row for row in index_rows
            if (row.get("source_batch_id") or "").strip() == batch_id
            and (row.get("source_item_id") or "").strip() == item_id
        ]
        if len(registered) != 1:
            raise ValueError(
                f"successful result {item_id} must map to exactly one PAPER_INDEX row; found {len(registered)}")
        indexed = registered[0]
        if ((indexed.get("sha256") or "").strip().casefold() != actual_sha
                or (indexed.get("identity_verified") or "").strip().casefold() != "true"):
            raise ValueError(f"successful result {item_id} is not a hash-bound, identity-verified PAPER_INDEX source")
        kind = source_kind(source, result.get("notes") or "")
        processor = queue.PROCESSORS[kind]
        document_id = queue.make_document_id(item.get("doi") or "", actual_sha)
        job_id = queue.make_job_id(batch_id, item_id, processor)
        output = queue.artifact_output_dir(root, kind, actual_sha, job_id)
        try:
            priority = int((item.get("priority") or "0").strip() or 0)
        except ValueError:
            priority = 0
        planned.append({
            "job_id": job_id,
            "batch_id": batch_id,
            "source_item_id": item_id,
            "paper_id": (item.get("paper_id") or "").strip(),
            "document_id": document_id,
            "title": (item.get("title") or "").strip(),
            "doi": (item.get("doi") or "").strip(),
            "source_kind": kind,
            "source_path": queue.portable_path(root, source),
            "source_sha256": actual_sha,
            "processor": processor,
            "processor_version": queue.PROCESSOR_VERSIONS[processor],
            "output_dir": queue.portable_path(root, output),
            "priority": priority,
            "max_attempts": max_attempts,
        })

    db_path = queue.database_path(root, batch_id)
    db = queue.connect(db_path)
    try:
        queue.initialize(db)
        existing = {row["job_id"]: row for row in queue.rows(db)}
        planned_ids = {job["job_id"] for job in planned}
        extra = sorted(set(existing) - planned_ids)
        if extra:
            raise ValueError(
                "existing queue contains jobs no longer present in the accepted result set: "
                + ", ".join(extra)
            )
        created = sum(queue.insert_job(db, job) for job in planned)
        queue.export_projection(db, queue.projection_path(root, batch_id))
        counts = queue.status_counts(db)
    finally:
        db.close()
    return {
        "batch_id": batch_id, "database": str(db_path),
        "projection": str(queue.projection_path(root, batch_id)),
        "jobs": len(planned), "created": created, "status_counts": counts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch_id")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--max-attempts", type=int, default=3)
    args = parser.parse_args()
    if args.max_attempts <= 0:
        parser.error("--max-attempts must be positive")
    try:
        report = build_queue(Path(args.root), args.batch_id, max_attempts=args.max_attempts)
    except (ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
