#!/usr/bin/env python3
"""Coordinator acceptance for parsed Markdown and PAPER_INDEX backfill."""
from __future__ import annotations

import argparse
import csv
import fcntl
import json
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import fulltext_parsers as parsers
import paper_section_chunking as section_chunking
import processing_queue as queue


INDEX_REQUIRED = {
    "paper_id", "title", "doi", "pdf_path", "sha256", "source_batch_id",
    "identity_verified", "mineru_md_path", "xml_md_path", "source_item_id",
    "document_id", "source_kind", "processed_text_sha256", "processing_method",
    "processing_version", "processing_accepted_at", "section_chunks_path",
    "section_chunks_sha256", "section_chunks_count", "section_chunks_schema_version",
}
PROCESSING_FIELDS = (
    "source_item_id", "document_id", "source_kind", "processed_text_sha256",
    "processing_method", "processing_version", "processing_accepted_at",
    "section_chunks_path", "section_chunks_sha256", "section_chunks_count",
    "section_chunks_schema_version",
)


@contextmanager
def workspace_lock(root: Path) -> Iterator[None]:
    path = root / "00_control" / ".queue.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_index(path: Path) -> tuple[list[str], list[dict[str, str]], bytes]:
    if not path.is_file():
        raise ValueError(f"PAPER_INDEX.csv is missing: {path}")
    original = path.read_bytes()
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = list(reader.fieldnames or [])
        rows = list(reader)
    if "source_item_id" not in fields:
        raise ValueError(
            "PAPER_INDEX.csv has no source_item_id column. Run the document-processing index "
            "migration/initializer first; refusing to guess by paper_id or DOI.")
    missing = sorted(INDEX_REQUIRED - set(fields))
    if missing:
        raise ValueError(
            f"PAPER_INDEX.csv needs the document-processing schema migration; missing columns: {missing}")
    return fields, rows, original


def _read_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is not valid JSON: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain a JSON object: {path}")
    return value


def validate_review_job(root: Path, job: dict[str, Any]) -> dict[str, Path]:
    source = queue.resolve_stored_path(root, job["source_path"])
    parsed = queue.resolve_stored_path(root, job["parsed_text_path"])
    sections = queue.resolve_stored_path(root, job["sections_path"])
    state_path = queue.resolve_stored_path(root, job["state_path"])
    result_path = queue.resolve_stored_path(root, job["result_path"])
    output = queue.resolve_stored_path(root, job["output_dir"])
    expected_processor = queue.PROCESSORS.get(job["source_kind"])
    if expected_processor != job["processor"]:
        raise ValueError(f"{job['job_id']} source kind and processor disagree")
    expected_output = queue.artifact_output_dir(
        root, job["source_kind"], job["source_sha256"], job["job_id"]).resolve()
    if output.resolve() != expected_output:
        raise ValueError(f"{job['job_id']} output directory is not the required content-addressed path")
    for label, path in (
        ("source", source), ("parsed text", parsed), ("sections", sections),
        ("state", state_path), ("result", result_path),
    ):
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f"{job['job_id']} {label} is missing or empty: {path}")
    if parsed.resolve() != output.resolve() / "full.md" or output.resolve() not in parsed.resolve().parents:
        raise ValueError(f"{job['job_id']} parsed text is outside its content-addressed output directory")
    if sections.resolve() != output.resolve() / parsers.SECTION_CHUNKS_FILENAME:
        raise ValueError(f"{job['job_id']} sections are outside the required output directory")
    if state_path.resolve() != output.resolve() / "state.json":
        raise ValueError(f"{job['job_id']} state is outside the required output directory")
    if result_path.resolve() != output.resolve() / "result.json":
        raise ValueError(f"{job['job_id']} result is outside the required output directory")
    checks = (
        ("source", queue.file_sha256(source), job["source_sha256"]),
        ("parsed text", queue.file_sha256(parsed), job["parsed_text_sha256"]),
        ("sections", queue.file_sha256(sections), job["sections_sha256"]),
        ("state", queue.file_sha256(state_path), job["state_sha256"]),
        ("result", queue.file_sha256(result_path), job["result_sha256"]),
    )
    for label, actual, expected in checks:
        if not expected or actual != expected:
            raise ValueError(f"{job['job_id']} {label} SHA-256 mismatch")
    valid, reason = parsers.valid_markdown(parsed)
    if not valid:
        raise ValueError(f"{job['job_id']} parsed Markdown failed validation: {reason}")
    sections_count = job.get("sections_count")
    if (isinstance(sections_count, bool) or not isinstance(sections_count, int)
            or sections_count <= 0):
        raise ValueError(f"{job['job_id']} sections_count must be a positive integer")
    try:
        section_records = section_chunking.validate_sections_jsonl(
            sections,
            parsed,
            source_sha256=job["source_sha256"],
            parsed_text_sha256=job["parsed_text_sha256"],
            expected_count=sections_count,
            paper_id=job["paper_id"],
            source_file=source.name,
            source_format=job["source_kind"],
        )
    except (OSError, ValueError) as exc:
        raise ValueError(f"{job['job_id']} sections validation failed: {exc}") from exc
    expected_section_binding = {
        "paper_id": job["paper_id"],
        "source_file": source.name,
        "source_format": job["source_kind"],
    }
    for number, record in enumerate(section_records, 1):
        mismatches = [
            key for key, value in expected_section_binding.items()
            if record.get(key) != value
        ]
        if mismatches:
            raise ValueError(
                f"{job['job_id']} section record {number} binding mismatch: {mismatches}")
    canonical_records = section_chunking.section_records(
        parsed.read_bytes().decode("utf-8"),
        paper_id=job["paper_id"],
        source_file=source.name,
        source_format=job["source_kind"],
        source_sha256=job["source_sha256"],
        parsed_text_sha256=job["parsed_text_sha256"],
    )
    if section_records != canonical_records:
        raise ValueError(
            f"{job['job_id']} sections are not the canonical deterministic projection")
    state = _read_object(state_path, "state")
    result = _read_object(result_path, "result")
    expected_common = {
        "job_id": job["job_id"],
        "document_id": job["document_id"],
        "paper_id": job["paper_id"],
        "source_file": source.name,
        "source_format": job["source_kind"],
        "source_sha256": job["source_sha256"],
        "parsed_text_sha256": job["parsed_text_sha256"],
        "sections_sha256": job["sections_sha256"],
        "sections_count": sections_count,
        "processor": job["processor"],
        "processor_version": job["processor_version"],
    }
    for label, document in (("state", state), ("result", result)):
        mismatches = [key for key, value in expected_common.items() if document.get(key) != value]
        if mismatches:
            raise ValueError(f"{job['job_id']} {label} binding mismatch: {mismatches}")
    if job["processor"] == "mineru":
        remote_batch_id = str(state.get("remote_batch_id") or "")
        if not remote_batch_id or state.get("remote_data_id") != job["job_id"]:
            raise ValueError(f"{job['job_id']} state lacks the exact MinerU remote identity")
        if (result.get("remote_batch_id") != remote_batch_id
                or result.get("remote_data_id") != job["job_id"]):
            raise ValueError(f"{job['job_id']} result MinerU remote identity mismatch")
    if state.get("status") != "done" or result.get("status") != "succeeded":
        raise ValueError(f"{job['job_id']} state/result is not successful")
    if result.get("state_sha256") != job["state_sha256"]:
        raise ValueError(f"{job['job_id']} result does not bind the accepted state hash")
    if (result.get("parsed_text_path") != "full.md"
            or result.get("sections_path") != parsers.SECTION_CHUNKS_FILENAME
            or result.get("state_path") != "state.json"):
        raise ValueError(f"{job['job_id']} result uses unexpected artifact paths")
    serialized = state_path.read_text(encoding="utf-8", errors="ignore") + result_path.read_text(
        encoding="utf-8", errors="ignore")
    for name in parsers.MINERU_TOKEN_ENV_WHITELIST:
        token = os.environ.get(name, "")
        if token and token in serialized:
            raise ValueError(f"{job['job_id']} state/result leaked the value of {name}")
    return {
        "source": source, "parsed": parsed, "sections": sections,
        "state": state_path, "result": result_path,
    }


def write_index_temp(path: Path, fields: list[str], rows: list[dict[str, str]]) -> Path:
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="raise")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        return Path(name)
    except Exception:
        Path(name).unlink(missing_ok=True)
        raise


def restore_bytes(path: Path, content: bytes) -> None:
    fd, name = tempfile.mkstemp(prefix=path.name + ".restore-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def accept(root: Path, batch_id: str, *, actor: str) -> dict[str, Any]:
    root = root.expanduser().resolve()
    db_path = queue.database_path(root, batch_id)
    if not db_path.is_file():
        raise ValueError(f"document-processing queue is missing: {db_path}")
    index_path = root / "04_library_index" / "PAPER_INDEX.csv"
    accepted_ids: list[str] = []
    with workspace_lock(root):
        db = queue.connect(db_path)
        index_replaced = False
        original_index = b""
        temporary: Path | None = None
        try:
            queue.initialize(db)
            db.execute("BEGIN IMMEDIATE")
            review = [dict(row) for row in db.execute(
                "SELECT * FROM jobs WHERE status='review' ORDER BY created_at,job_id")]
            if not review:
                db.rollback()
                queue.export_projection(db, queue.projection_path(root, batch_id))
                return {
                    "batch_id": batch_id, "accepted": 0,
                    "status_counts": queue.status_counts(db),
                    "message": "no jobs are awaiting review",
                }
            for job in review:
                if job["batch_id"] != batch_id:
                    raise ValueError(f"{job['job_id']} belongs to batch {job['batch_id']!r}, not {batch_id!r}")
                validate_review_job(root, job)
            fields, index_rows, original_index = load_index(index_path)
            updated_rows = [dict(row) for row in index_rows]
            accepted_at = queue.utc_now()
            for job in review:
                matches = [
                    row for row in updated_rows
                    if (row.get("source_batch_id") or "").strip() == batch_id
                    and (row.get("source_item_id") or "").strip() == job["source_item_id"]
                ]
                if len(matches) != 1:
                    raise ValueError(
                        f"{job['job_id']} expected exactly one PAPER_INDEX row for "
                        f"source_batch_id={batch_id!r}, source_item_id={job['source_item_id']!r}; "
                        f"found {len(matches)}")
                row = matches[0]
                if (row.get("sha256") or "").strip().casefold() != job["source_sha256"]:
                    raise ValueError(f"{job['job_id']} PAPER_INDEX source SHA-256 mismatch")
                if (row.get("identity_verified") or "").strip().casefold() != "true":
                    raise ValueError(f"{job['job_id']} PAPER_INDEX identity is not verified")
                parsed_portable = job["parsed_text_path"]
                if job["source_kind"] == "pdf":
                    row["mineru_md_path"] = parsed_portable
                else:
                    row["xml_md_path"] = parsed_portable
                row.update({
                    "source_item_id": job["source_item_id"],
                    "document_id": job["document_id"],
                    "source_kind": job["source_kind"],
                    "processed_text_sha256": job["parsed_text_sha256"],
                    "processing_method": job["processor"],
                    "processing_version": job["processor_version"],
                    "processing_accepted_at": accepted_at,
                    "section_chunks_path": job["sections_path"],
                    "section_chunks_sha256": job["sections_sha256"],
                    "section_chunks_count": str(job["sections_count"]),
                    "section_chunks_schema_version": section_chunking.SECTION_SCHEMA_VERSION,
                })
            temporary = write_index_temp(index_path, fields, updated_rows)
            for job in review:
                changed = db.execute(
                    """UPDATE jobs SET status='accepted', reviewed_by=?, reviewed_at=?,
                              accepted_at=?, updated_at=? WHERE job_id=? AND status='review'""",
                    (actor, accepted_at, accepted_at, accepted_at, job["job_id"]),
                )
                if changed.rowcount != 1:
                    raise RuntimeError(f"concurrent status change for {job['job_id']}")
                queue._event(  # same transaction; public events are immutable audit records
                    db, job["job_id"], "accepted", "review", "accepted", actor,
                    {
                        "parsed_text_sha256": job["parsed_text_sha256"],
                        "sections_sha256": job["sections_sha256"],
                        "sections_count": job["sections_count"],
                    }, at=accepted_at,
                )
                accepted_ids.append(job["job_id"])
            # Replace while SQLite is still locked.  A crash here rolls SQLite back; a
            # repeated run is then idempotent because these same index values are written.
            os.replace(temporary, index_path)
            temporary = None
            index_replaced = True
            db.commit()
        except Exception:
            if db.in_transaction:
                db.rollback()
            if index_replaced and original_index:
                restore_bytes(index_path, original_index)
            raise
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            if not db.in_transaction:
                queue.export_projection(db, queue.projection_path(root, batch_id))
            counts = queue.status_counts(db)
            db.close()
    return {"batch_id": batch_id, "accepted": len(accepted_ids), "job_ids": accepted_ids,
            "status_counts": counts, "paper_index": str(index_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch_id")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--actor-role", required=True, choices=("coordinator",))
    parser.add_argument("--actor", default="coordinator")
    args = parser.parse_args()
    try:
        report = accept(Path(args.root), args.batch_id, actor=args.actor)
    except (ValueError, RuntimeError, sqlite3.Error) as exc:
        parser.error(str(exc))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
