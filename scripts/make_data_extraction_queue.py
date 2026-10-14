#!/usr/bin/env python3
"""Create a task-scoped extraction queue from accepted parsed documents."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import re
from pathlib import Path
from typing import Any

import paper_section_chunking

from extraction_common import (
    QUEUE_VERSION, exclusive_lock, job_id_for, load_queue, load_spec,
    paperqa_question_for, portable_path, resolve_path,
    sha256_file, task_paths, utc_now, write_queue,
)


STAGED_SCHEMA_VERSION = 3
STAGED_MANIFEST_REQUIRED = {
    "schema_version", "task_id", "document_id", "batch_id", "source_item_id",
    "item_id", "paper_id", "title", "doi", "parser", "parser_version", "status",
    "source_sha256", "parsed_text_sha256", "source_kind", "source_ref",
    "source_staged_ref", "source_path", "parsed_text_source_ref", "parsed_text_ref",
    "parsed_text_path", "section_chunks_source_ref", "section_chunks_ref",
    "section_chunks_path", "section_chunks_sha256", "section_chunks_count",
    "section_chunks_schema_version", "fulltext_copied", "source_bytes",
    "parsed_text_bytes", "section_chunks_bytes",
}
STAGED_MANIFEST_OPTIONAL: set[str] = set()


def _json_rows(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(value, list):
        rows = value
    elif isinstance(value, dict):
        if "documents" in value:
            rows = value["documents"]
        elif "jobs" in value:
            rows = value["jobs"]
        elif {"schema_version", "task_id", "document_id"} <= set(value):
            # ``stage_processing_inputs.py`` publishes one immutable document
            # object rather than wrapping it in a list.
            rows = [value]
        else:
            raise ValueError(f"unrecognized JSON document manifest object: {path}")
    else:
        rows = []
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError(f"document manifest must contain object rows: {path}")
    return rows


def _csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _sqlite_rows(path: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        tables = {
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "jobs" not in tables:
            raise ValueError(f"SQLite document queue has no jobs table: {path}")
        return [dict(row) for row in connection.execute("SELECT * FROM jobs")]
    finally:
        connection.close()


def load_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix.casefold() in {".sqlite", ".sqlite3", ".db"}:
        return _sqlite_rows(path)
    if path.suffix.casefold() == ".csv":
        return _csv_rows(path)
    return _json_rows(path)


def discover_manifests(root: Path, task_id: str) -> list[Path]:
    active = root / "runtime" / "ACTIVE_TASK.json"
    if not active.is_file():
        return []
    try:
        document = json.loads(active.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return []
    if document.get("task_id") != task_id:
        return []
    paths = []
    for batch_id in document.get("batch_ids", []):
        if not isinstance(batch_id, str) or not batch_id:
            continue
        base = root / "02_batches" / batch_id / "outputs" / "document_processing"
        for name in ("queue.sqlite3", "accepted_documents.json", "results.json"):
            candidate = base / name
            if candidate.is_file():
                paths.append(candidate)
                break
    return paths


def value(row: dict[str, Any], *names: str) -> str:
    for name in names:
        current = row.get(name)
        if current is not None and str(current).strip():
            return str(current).strip()
    return ""


def screening_decisions(root: Path) -> dict[tuple[str, str], str]:
    """Read unique OVERALL full-text decisions without treating Markdown as authority."""
    output: dict[tuple[str, str], str] = {}
    for path in sorted((root / "02_batches").glob("*/outputs/fulltext_screening_queue.csv")):
        batch_id = path.parents[1].name
        with path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                if (row.get("criterion_id") or "").strip() != "OVERALL":
                    continue
                decision = (row.get("decision") or "").strip()
                for key in ((batch_id, (row.get("item_id") or "").strip()),
                            (batch_id, (row.get("paper_id") or "").strip())):
                    if not key[1]:
                        continue
                    if key in output and output[key] != decision:
                        raise ValueError(f"conflicting OVERALL decisions for {key}")
                    output[key] = decision
    return output


def _workspace_relative_ref(value: Any, label: str) -> str:
    text = str(value or "")
    path = Path(text)
    if (not text or text != text.strip() or path.is_absolute() or "\\" in text
            or any(part in {"", ".", ".."} for part in text.split("/"))):
        raise ValueError(f"staged {label} must be a normalized workspace-relative path")
    return text


def _validate_staged_artifact_ref(
    root: Path, row: dict[str, Any], ref: str, *, directory: str,
    digest: str, exact_suffix: str | None = None,
) -> Path:
    lexical = root / ref
    expected_parent = (
        root / "runtime" / "inputs" / row["task_id"] / "documents"
        / row["document_id"] / directory
    ).resolve()
    resolved = lexical.resolve()
    if resolved.parent != expected_parent:
        raise ValueError(f"staged {directory} artifact is outside its document identity directory")
    cursor = lexical
    while cursor != root:
        if cursor.is_symlink():
            raise ValueError(f"staged {directory} artifact must not traverse symlinks")
        if cursor.parent == cursor:
            raise ValueError(f"staged {directory} artifact escapes the workspace")
        cursor = cursor.parent
    if not resolved.is_file() or not resolved.name.startswith(digest):
        raise ValueError(f"staged {directory} artifact filename does not bind its SHA-256")
    if exact_suffix is not None and resolved.name != digest + exact_suffix:
        raise ValueError(f"staged {directory} artifact has an unexpected filename")
    return resolved


def _validate_staged_manifest(
    row: dict[str, Any], task_id: str | None, root: Path | None = None,
) -> None:
    keys = set(row)
    allowed = STAGED_MANIFEST_REQUIRED | STAGED_MANIFEST_OPTIONAL
    missing, unknown = sorted(STAGED_MANIFEST_REQUIRED - keys), sorted(keys - allowed)
    if missing or unknown:
        raise ValueError(f"staged manifest keys mismatch (missing={missing}, unknown={unknown})")
    if (isinstance(row.get("schema_version"), bool)
            or row.get("schema_version") != STAGED_SCHEMA_VERSION):
        raise ValueError(f"staged manifest schema_version must be {STAGED_SCHEMA_VERSION}")
    if task_id is not None and row.get("task_id") != task_id:
        raise ValueError("staged manifest task_id does not match selected task")
    if row.get("status") != "accepted":
        raise ValueError("staged manifest status must be accepted")
    identifier_re = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
    for key in ("task_id", "document_id"):
        if not isinstance(row.get(key), str) or not identifier_re.fullmatch(row[key]):
            raise ValueError(f"staged manifest {key} is missing or invalid")
    for key in ("batch_id", "source_item_id", "item_id"):
        if (not isinstance(row.get(key), str)
                or (row[key] and not identifier_re.fullmatch(row[key]))):
            raise ValueError(f"staged manifest {key} is invalid")
    if row["item_id"] != row["source_item_id"]:
        raise ValueError("staged manifest item_id/source_item_id disagree")
    for key in ("paper_id", "parser", "parser_version"):
        if not isinstance(row.get(key), str) or not row[key].strip():
            raise ValueError(f"staged manifest {key} must be a non-empty string")
    if row.get("source_kind") not in {"pdf", "xml"}:
        raise ValueError("staged manifest source_kind must be pdf or xml")
    for key in ("source_sha256", "parsed_text_sha256"):
        if not isinstance(row.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", row[key]):
            raise ValueError(f"staged manifest {key} must be a lowercase SHA-256")
    if not isinstance(row.get("fulltext_copied"), bool):
        raise ValueError("staged manifest fulltext_copied must be boolean")
    for key in ("source_ref", "source_path", "parsed_text_source_ref", "parsed_text_ref", "parsed_text_path"):
        _workspace_relative_ref(row.get(key), key)
    staged = row.get("source_staged_ref")
    if staged is not None:
        _workspace_relative_ref(staged, "source_staged_ref")
    if bool(staged) is not row["fulltext_copied"]:
        raise ValueError("staged manifest source_staged_ref/fulltext_copied disagree")
    if row["source_path"] != (staged or row["source_ref"]):
        raise ValueError("staged manifest source_path does not match the selected source reference")
    if row["parsed_text_path"] != row["parsed_text_ref"]:
        raise ValueError("staged manifest parsed_text_path/parsed_text_ref disagree")
    for key in ("source_bytes", "parsed_text_bytes"):
        value = row.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"staged manifest {key} must be a positive integer")
    section_values = {
        key: row.get(key) for key in (
            "section_chunks_source_ref", "section_chunks_ref", "section_chunks_path",
            "section_chunks_sha256", "section_chunks_schema_version",
        )
    }
    section_count, section_bytes = row.get("section_chunks_count"), row.get("section_chunks_bytes")
    has_sections = any(value is not None for value in section_values.values())
    if has_sections:
        if any(not isinstance(value, str) or not value.strip() for value in section_values.values()):
            raise ValueError("staged manifest has partial section chunk references")
        for key in ("section_chunks_source_ref", "section_chunks_ref", "section_chunks_path"):
            _workspace_relative_ref(row[key], key)
        if row["section_chunks_path"] != row["section_chunks_ref"]:
            raise ValueError("staged manifest section_chunks_path/ref disagree")
        if not re.fullmatch(r"[0-9a-f]{64}", row["section_chunks_sha256"]):
            raise ValueError("staged manifest section_chunks_sha256 must be lowercase SHA-256")
        if row["section_chunks_schema_version"] != "1.0":
            raise ValueError("staged manifest section chunk schema is unsupported")
        for key, value in (("section_chunks_count", section_count), ("section_chunks_bytes", section_bytes)):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"staged manifest {key} must be a positive integer")
    elif (section_count != 0 or section_bytes != 0
          or any(value is not None for value in section_values.values())):
        raise ValueError("staged manifest empty section chunk binding is inconsistent")
    if root is not None:
        _validate_staged_artifact_ref(
            root, row, row["parsed_text_ref"], directory="parsed",
            digest=row["parsed_text_sha256"],
        )
        if staged:
            _validate_staged_artifact_ref(
                root, row, staged, directory="source", digest=row["source_sha256"],
            )
        if has_sections:
            _validate_staged_artifact_ref(
                root, row, row["section_chunks_ref"], directory="parsed",
                digest=row["section_chunks_sha256"], exact_suffix=".section_chunks.jsonl",
            )


def canonical_document(root: Path, row: dict[str, Any], source_name: str,
                       allow_hash_only_source: bool = False,
                       task_id: str | None = None) -> dict[str, Any]:
    status = value(row, "status", "processing_status")
    staged_manifest = {"schema_version", "task_id", "document_id"} <= set(row)
    if staged_manifest:
        _validate_staged_manifest(row, task_id, root)
    elif task_id and value(row, "task_id") and value(row, "task_id") != task_id:
        raise ValueError(f"document task_id does not match selected task: {source_name}")
    if not status and not staged_manifest:
        raise ValueError(f"document missing accepted processing status: {source_name}")
    if status and status.casefold() not in {"accepted", "skipped"}:
        raise ValueError(f"document is not processing-accepted ({status}): {source_name}")
    paper_id = value(row, "paper_id", "document_id")
    if not paper_id:
        raise ValueError(f"document missing paper_id: {source_name}")
    staged_source = value(row, "source_staged_ref")
    source_text = staged_source or value(row, "source_path", "source_ref", "original_path", "fulltext_path")
    parsed_text = value(row, "parsed_text_path", "parsed_text_ref", "markdown_path", "mineru_md_path", "xml_md_path")
    if not source_text or not parsed_text:
        raise ValueError(f"document missing source_path or parsed_text_path: {paper_id}")
    source = resolve_path(root, source_text)
    parsed = resolve_path(root, parsed_text)
    if not parsed.is_file() or parsed.stat().st_size == 0:
        raise ValueError(f"parsed Markdown is missing or empty: {parsed}")
    actual_parsed = sha256_file(parsed)
    declared_source = value(row, "source_sha256", "sha256")
    declared_parsed = value(row, "parsed_text_sha256", "markdown_sha256")
    source_available = source.is_file() and source.stat().st_size > 0
    if staged_source and not source_available:
        raise ValueError(f"staged source is missing or empty: {source}")
    if source_available:
        actual_source = sha256_file(source)
        if declared_source and declared_source != actual_source:
            raise ValueError(f"stale source SHA-256 for {paper_id}")
        source_revalidation = "verified_local"
    else:
        if not allow_hash_only_source:
            raise ValueError(f"source is missing or empty: {source}")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", declared_source):
            raise ValueError(f"hash-only source requires a declared SHA-256 for {paper_id}")
        actual_source = declared_source.casefold()
        source_revalidation = "deferred_to_mac"
    if declared_parsed and declared_parsed != actual_parsed:
        raise ValueError(f"stale parsed-text SHA-256 for {paper_id}")
    if staged_manifest:
        if parsed.stat().st_size != row["parsed_text_bytes"]:
            raise ValueError(f"staged parsed-text byte count changed for {paper_id}")
        # A reference-only source may be absent on the compute host, but if it
        # is available its immutable manifest byte count must still match.
        if source_available and source.stat().st_size != row["source_bytes"]:
            raise ValueError(f"staged source byte count changed for {paper_id}")
    sections_text = value(row, "sections_path", "section_chunks_path", "section_chunks_ref")
    sections_sha = value(row, "sections_sha256", "section_chunks_sha256")
    sections_schema = value(row, "sections_schema_version", "section_chunks_schema_version")
    raw_sections_count = row.get("sections_count", row.get("section_chunks_count", 0))
    if sections_text or sections_sha or sections_schema or raw_sections_count:
        if not sections_text or not re.fullmatch(r"[0-9a-f]{64}", sections_sha):
            raise ValueError(f"document has partial/invalid section chunk binding: {paper_id}")
        try:
            sections_count = int(raw_sections_count)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"document has invalid sections_count: {paper_id}") from exc
        if sections_count <= 0 or sections_schema != paper_section_chunking.SECTION_SCHEMA_VERSION:
            raise ValueError(f"document has invalid section count/schema: {paper_id}")
        sections = resolve_path(root, sections_text)
        if not sections.is_file() or sections.stat().st_size <= 0:
            raise ValueError(f"section chunk sidecar is missing or empty: {sections}")
        if sha256_file(sections) != sections_sha:
            raise ValueError(f"stale section chunk SHA-256 for {paper_id}")
        if staged_manifest and sections.stat().st_size != row["section_chunks_bytes"]:
            raise ValueError(f"staged section chunk byte count changed for {paper_id}")
        paper_section_chunking.validate_sections_jsonl(
            sections, parsed, source_sha256=actual_source,
            parsed_text_sha256=actual_parsed, expected_count=sections_count,
            paper_id=paper_id,
            source_file=Path(value(row, "source_ref", "source_path")).name or None,
            source_format=value(row, "source_kind") or None,
        )
        portable_sections = portable_path(root, sections)
    else:
        sections_count, sections_schema, sections_sha, portable_sections = 0, "", "", ""
    parser = value(row, "parser", "processor")
    parser_version = value(row, "parser_version", "processor_version")
    if not parser or not parser_version:
        raise ValueError(f"document missing parser/version binding: {paper_id}")
    return {
        "batch_id": value(row, "batch_id"),
        "item_id": value(row, "item_id", "source_item_id"),
        "paper_id": paper_id,
        "title": value(row, "title"),
        "doi": value(row, "doi"),
        "source_kind": value(row, "source_kind") or source.suffix.lstrip(".").casefold(),
        "source_path": portable_path(root, source),
        "source_sha256": actual_source,
        "source_revalidation": source_revalidation,
        "parsed_text_path": portable_path(root, parsed),
        "parsed_text_sha256": actual_parsed,
        "parser": parser,
        "parser_version": parser_version,
        "sections_path": portable_sections,
        "sections_sha256": sections_sha,
        "sections_count": sections_count,
        "sections_schema_version": sections_schema,
        "fulltext_decision": value(row, "fulltext_decision", "decision"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--task-id", required=True)
    parser.add_argument(
        "--documents", action="append", type=Path,
        help="Accepted document manifest (JSON/CSV) or document-processing SQLite queue",
    )
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--allow-unscreened", action="store_true")
    parser.add_argument(
        "--allow-hash-only-source", action="store_true",
        help="Allow an uncopied staged source reference on a compute host; Mac acceptance revalidates it",
    )
    args = parser.parse_args()
    if not 1 <= args.max_attempts <= 100:
        parser.error("--max-attempts must be between 1 and 100")

    paths = task_paths(Path(args.root), args.task_id)
    try:
        spec, spec_hash = load_spec(paths["spec"], args.task_id)
    except ValueError as exc:
        parser.error(str(exc))
    sources = [path.expanduser().resolve() for path in (args.documents or [])]
    if not sources:
        sources = discover_manifests(paths["root"], args.task_id)
    if not sources:
        parser.error("no document manifest found; pass --documents")
    allowed_decisions = set(spec.get("source_scope", {}).get("fulltext_decisions", ["include", "uncertain"]))
    documents: list[dict[str, Any]] = []
    try:
        decisions = screening_decisions(paths["root"])
    except (OSError, ValueError, csv.Error) as exc:
        parser.error(str(exc))
    try:
        for source in sources:
            if not source.is_file():
                raise ValueError(f"document manifest not found: {source}")
            for row in load_rows(source):
                if not value(row, "batch_id"):
                    try:
                        relative = source.relative_to(paths["root"] / "02_batches")
                        row = {**row, "batch_id": relative.parts[0]}
                    except ValueError:
                        pass
                row_status = value(row, "status", "processing_status").casefold()
                if row_status == "skipped":
                    continue
                document = canonical_document(
                    paths["root"], row, str(source), args.allow_hash_only_source,
                    args.task_id,
                )
                if not document["fulltext_decision"]:
                    document["fulltext_decision"] = (
                        decisions.get((document["batch_id"], document["item_id"]))
                        or decisions.get((document["batch_id"], document["paper_id"]))
                        or ""
                    )
                if not document["fulltext_decision"] and not args.allow_unscreened:
                    raise ValueError(
                        f"missing unique OVERALL full-text decision for {document['paper_id']}; "
                        "finish screening or use --allow-unscreened explicitly"
                    )
                documents.append(document)
    except (OSError, ValueError, sqlite3.Error, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    if not documents:
        parser.error("no accepted parsed documents in selected manifest(s)")

    now = utc_now()
    paperqa_question = paperqa_question_for(spec)
    import hashlib
    paperqa_question_sha256 = hashlib.sha256(paperqa_question.encode("utf-8")).hexdigest()
    jobs = []
    seen: set[str] = set()
    for document in documents:
        job_id = job_id_for(args.task_id, document, spec_hash)
        if job_id in seen:
            raise ValueError(
                f"multiple manifest rows collapse to the same extraction job_id: {job_id}; "
                "deduplicate documents before queue creation"
            )
        seen.add(job_id)
        decision = document.pop("fulltext_decision", "")
        selected = not decision or decision in allowed_decisions
        jobs.append({
            "job_id": job_id,
            "task_id": args.task_id,
            **document,
            "fulltext_decision": decision,
            "spec_version": spec["spec_version"],
            "spec_sha256": spec_hash,
            "paperqa_question": paperqa_question,
            "paperqa_question_sha256": paperqa_question_sha256,
            "status": "queued" if selected else "skipped",
            "skip_reason": "" if selected else f"fulltext decision {decision!r} outside extraction scope",
            "attempt_count": 0,
            "max_attempts": args.max_attempts,
            "available_at": now,
            "claimed_by": "",
            "run_id": "",
            "attempt_id": "",
            "lease_token": "",
            "lease_expires_at": "",
            "heartbeat_at": "",
            "created_at": now,
            "updated_at": now,
            "error_code": "",
            "error_message": "",
            "receipt_history": [],
        })
    queue = {
        "queue_version": QUEUE_VERSION,
        "task_id": args.task_id,
        "spec_version": spec["spec_version"],
        "spec_sha256": spec_hash,
        "created_at": now,
        "updated_at": now,
        "jobs": sorted(jobs, key=lambda row: row["job_id"]),
    }
    paths["extraction"].mkdir(parents=True, exist_ok=True)
    with exclusive_lock(paths["queue_lock"]):
        if paths["queue"].exists() and not args.overwrite:
            parser.error(f"refusing to overwrite extraction queue: {paths['queue']}")
        if paths["queue"].exists() and args.overwrite:
            try:
                existing = load_queue(paths["queue"])
            except ValueError as exc:
                parser.error(f"refusing to overwrite unreadable existing queue: {exc}")
            live = [job["job_id"] for job in existing["jobs"] if job["status"] not in {"failed", "skipped"}]
            if live:
                parser.error(f"refusing to overwrite live/reviewed extraction jobs: {live}")
            if paths["delivery"].exists() and any(paths["delivery"].iterdir()):
                parser.error("refusing to overwrite a queue with published delivery artifacts")
        write_queue(paths, queue)
    print(f"{paths['queue']} jobs={len(jobs)} queued={sum(j['status'] == 'queued' for j in jobs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
