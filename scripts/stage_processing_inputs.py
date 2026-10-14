#!/usr/bin/env python3
"""Stage one verified document for downstream parsing or data extraction.

The accepted parsed text and its source-bound ``section_chunks.jsonl`` sidecar
are always copied under SHA-256-derived names.  The source PDF/XML is
referenced by default and is copied only when ``--allow-copy-fulltext`` is
supplied.  All paths recorded in ``manifest.json`` are workspace-relative
POSIX paths.

Example::

    python3 scripts/stage_processing_inputs.py \\
      --root . \\
      --task-id training-data-20261013 \\
      --document-id P-0001 \\
      --batch-id DL-20261014-001 \\
      --source-item-id D001 \\
      --paper-id doi:10.1000/example \\
      --parser mineru \\
      --parser-version vlm \\
      --source-ref 02_batches/DL-001/outputs/files/P-0001.pdf \\
      --source-sha256 <sha256> \\
      --parsed-text-ref 06_mineru/by-sha256/ab/<source-sha>/<job-id>/full.md \\
      --parsed-text-sha256 <sha256>

The caller is responsible for enforcing the accepted/parsed workflow gate
before invoking this single-document staging tool.  Staging is immutable: an
existing document directory is never overwritten.  A complete existing stage
whose manifest and staged-file hashes are identical is treated as an
idempotent success; otherwise use a new ``document_id`` after deliberately
changing either input.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sqlite3
import tempfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any


ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
SHA256_RE = re.compile(r"[0-9a-fA-F]{64}\Z")
SOURCE_SUFFIXES = {".pdf", ".xml"}
PARSED_TEXT_SUFFIXES = {".md", ".markdown", ".txt"}
SECTION_CHUNK_SUFFIXES = {".jsonl"}
MANIFEST_NAME = "manifest.json"
SCHEMA_VERSION = 3
INDEX_REQUIRED_FIELDS = {
    "paper_id",
    "title",
    "doi",
    "pdf_path",
    "sha256",
    "source_batch_id",
    "identity_verified",
    "mineru_md_path",
    "xml_md_path",
    "source_item_id",
    "document_id",
    "source_kind",
    "processed_text_sha256",
    "processing_method",
    "processing_version",
    "processing_accepted_at",
    "section_chunks_path",
    "section_chunks_sha256",
    "section_chunks_count",
    "section_chunks_schema_version",
}
QUEUE_REQUIRED_FIELDS = {
    "batch_id",
    "source_item_id",
    "paper_id",
    "document_id",
    "title",
    "doi",
    "source_kind",
    "source_path",
    "source_sha256",
    "processor",
    "processor_version",
    "parsed_text_path",
    "parsed_text_sha256",
    "sections_path",
    "sections_sha256",
    "sections_count",
    "status",
    "accepted_at",
}


class StageError(ValueError):
    """Raised when an input cannot be staged safely."""


def _load_sibling_module(filename: str, module_name: str):
    """Load a sibling script even when this file is imported via spec in tests."""
    path = Path(__file__).resolve().with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise StageError(f"cannot load required sibling module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_processing_queue = _load_sibling_module(
    "processing_queue.py", "paper_workspace_processing_queue_for_staging"
)
_section_chunking = _load_sibling_module(
    "paper_section_chunking.py", "paper_workspace_section_chunking_for_staging"
)
CURRENT_QUEUE_SCHEMA_VERSION = str(_processing_queue.SCHEMA_VERSION)


@contextmanager
def accepted_state_read_lock(root: Path):
    """Hold the workspace acceptance lock without creating or modifying it."""
    lock_path = root.expanduser().resolve(strict=True) / "00_control" / ".queue.lock"
    if lock_path.is_symlink() or not lock_path.is_file():
        raise StageError(f"authoritative workspace lock is missing: {lock_path}")
    with lock_path.open("rb") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def validate_id(value: str, label: str) -> str:
    """Return a filesystem-safe task/document identifier."""
    if not ID_RE.fullmatch(value or "") or value in {".", ".."}:
        raise StageError(
            f"{label} must match {ID_RE.pattern!r} (ASCII letters, digits, '.', '_', '-')"
        )
    return value


def validate_sha256(value: str, label: str) -> str:
    """Validate and normalize an expected SHA-256 digest."""
    if not SHA256_RE.fullmatch(value or ""):
        raise StageError(f"{label} must be exactly 64 hexadecimal characters")
    return value.lower()


def validate_manifest_text(value: str, label: str, *, default: str = "") -> str:
    """Validate non-path metadata before it is written to a transfer manifest."""
    normalized = value if value else default
    if not normalized or normalized != normalized.strip() or len(normalized) > 256:
        raise StageError(f"{label} must be non-empty, trimmed, and at most 256 characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in normalized):
        raise StageError(f"{label} must not contain control characters")
    return normalized


def validate_optional_manifest_text(value: str, label: str, *, limit: int) -> str:
    if not value:
        return ""
    if value != value.strip() or len(value) > limit:
        raise StageError(f"{label} must be trimmed and at most {limit} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise StageError(f"{label} must not contain control characters")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_within(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
    except ValueError:
        return False
    return True


def workspace_relative(path: Path, root: Path) -> str:
    """Render an already-validated path as a workspace-relative POSIX ref."""
    try:
        return path.relative_to(root).as_posix()
    except ValueError as exc:  # defensive: callers validate before manifest creation
        raise StageError(f"path escapes workspace: {path}") from exc


def resolve_workspace_ref(root: Path, raw_ref: str, label: str) -> tuple[Path, str]:
    """Resolve a user ref without allowing absolute, parent, or symlink escape."""
    if not raw_ref or not raw_ref.strip():
        raise StageError(f"{label} must be a non-empty workspace-relative path")
    if raw_ref != raw_ref.strip():
        raise StageError(f"{label} must not contain leading or trailing whitespace")
    if "\\" in raw_ref:
        raise StageError(f"{label} must use '/' separators")

    raw_parts = raw_ref.split("/")
    if any(part in {"", ".", ".."} for part in raw_parts):
        raise StageError(f"{label} must not contain empty, '.', or '..' components: {raw_ref!r}")
    posix_ref = PurePosixPath(raw_ref)
    windows_ref = PureWindowsPath(raw_ref)
    if posix_ref.is_absolute() or windows_ref.is_absolute() or windows_ref.drive:
        raise StageError(f"{label} must be workspace-relative, not absolute: {raw_ref!r}")
    candidate = root.joinpath(*posix_ref.parts)
    try:
        resolved = candidate.resolve(strict=True)
    except (FileNotFoundError, RuntimeError, OSError) as exc:
        raise StageError(f"{label} does not resolve to an existing file: {raw_ref!r}") from exc
    if not is_within(resolved, root):
        raise StageError(f"{label} escapes workspace (including via symlink): {raw_ref!r}")
    if not resolved.is_file():
        raise StageError(f"{label} is not a regular file: {raw_ref!r}")
    if resolved.stat().st_size <= 0:
        raise StageError(f"{label} is empty: {raw_ref!r}")

    # Record the lexical ref, not the resolved target, so the manifest preserves
    # the accepted hand-off path while remaining workspace-relative.
    return resolved, posix_ref.as_posix()


def validate_authoritative_acceptance(
    *,
    root: Path,
    batch_id: str,
    source_item_id: str,
    document_id: str,
    paper_id: str,
    title: str,
    doi: str,
    parser: str,
    parser_version: str,
    source_ref: str,
    source_sha256: str,
    parsed_text_ref: str,
    parsed_text_sha256: str,
) -> dict[str, str]:
    """Prove that CLI inputs are the coordinator-accepted processing binding.

    This function is intentionally read-only.  It does not infer acceptance
    from file existence or caller-supplied hashes: the batch processing DB and
    the corresponding PAPER_INDEX row must agree exactly.
    """
    try:
        root = root.expanduser().resolve(strict=True)
    except (FileNotFoundError, RuntimeError, OSError) as exc:
        raise StageError(f"workspace root does not exist: {root}") from exc
    if not root.is_dir():
        raise StageError(f"workspace root is not a directory: {root}")

    batch_id = validate_id(batch_id, "batch_id")
    source_item_id = validate_id(source_item_id, "source_item_id")
    document_id = validate_id(document_id, "document_id")
    expected_source_sha = validate_sha256(source_sha256, "source_sha256")
    expected_parsed_sha = validate_sha256(parsed_text_sha256, "parsed_text_sha256")

    queue_path = (
        root
        / "02_batches"
        / batch_id
        / "outputs"
        / "document_processing"
        / "queue.sqlite3"
    )
    if queue_path.is_symlink() or not queue_path.is_file():
        raise StageError(f"authoritative document-processing queue is missing: {queue_path}")
    queue_resolved = queue_path.resolve(strict=True)
    if not is_within(queue_resolved, root):
        raise StageError("authoritative document-processing queue escapes workspace")

    try:
        connection = sqlite3.connect(
            f"{queue_resolved.as_uri()}?mode=ro", uri=True, timeout=5
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA query_only=ON")
            schema_row = connection.execute(
                "SELECT value FROM metadata WHERE key='schema_version'"
            ).fetchone()
            actual_schema = "" if schema_row is None else str(schema_row[0])
            if actual_schema != CURRENT_QUEUE_SCHEMA_VERSION:
                raise StageError(
                    "document-processing queue schema needs migration: "
                    f"found {actual_schema!r}, expected {CURRENT_QUEUE_SCHEMA_VERSION!r}; "
                    "open the queue with the current processing_queue.initialize before staging"
                )
            columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(jobs)").fetchall()
            }
            missing_columns = sorted(QUEUE_REQUIRED_FIELDS - columns)
            if missing_columns:
                raise StageError(
                    f"document-processing queue is missing required fields: {missing_columns}"
                )
            rows = connection.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND source_item_id=?",
                (batch_id, source_item_id),
            ).fetchall()
        finally:
            connection.close()
    except sqlite3.Error as exc:
        raise StageError(f"cannot read authoritative processing queue: {exc}") from exc
    if len(rows) != 1:
        raise StageError(
            "staging requires exactly one document-processing job for "
            f"batch_id={batch_id!r}, source_item_id={source_item_id!r}; found {len(rows)}"
        )
    job = {key: str(rows[0][key] or "").strip() for key in rows[0].keys()}
    if job.get("status") != "accepted" or not job.get("accepted_at"):
        raise StageError("document-processing job is not coordinator-accepted")
    expected_processor = {"pdf": "mineru", "xml": "xml_to_markdown"}.get(
        job.get("source_kind", "")
    )
    if not expected_processor or job.get("processor") != expected_processor:
        raise StageError("accepted processing job has an invalid source-kind/parser binding")

    required_job_values = {
        "document_id": document_id,
        "source_sha256": expected_source_sha,
        "parsed_text_sha256": expected_parsed_sha,
    }
    supplied_optional = {
        "paper_id": paper_id,
        "title": title,
        "doi": doi,
        "processor": parser,
        "processor_version": parser_version,
    }
    mismatches = [
        key for key, expected in required_job_values.items()
        if job.get(key, "").casefold() != expected.casefold()
    ]
    mismatches.extend(
        key for key, supplied in supplied_optional.items()
        if supplied and job.get(key, "") != supplied
    )
    if mismatches:
        raise StageError(
            "CLI arguments do not match the accepted processing job: "
            + ", ".join(sorted(set(mismatches)))
        )

    _, source_cli_ref = resolve_workspace_ref(root, source_ref, "source_ref")
    source_path, source_job_ref = resolve_workspace_ref(
        root, job.get("source_path", ""), "queue source_path"
    )
    _, parsed_cli_ref = resolve_workspace_ref(root, parsed_text_ref, "parsed_text_ref")
    _, parsed_job_ref = resolve_workspace_ref(
        root, job.get("parsed_text_path", ""), "queue parsed_text_path"
    )
    sections_path, sections_job_ref = resolve_workspace_ref(
        root, job.get("sections_path", ""), "queue sections_path"
    )
    if source_cli_ref != source_job_ref:
        raise StageError("source_ref does not match the accepted processing job")
    if parsed_cli_ref != parsed_job_ref:
        raise StageError("parsed_text_ref does not match the accepted processing job")
    if sections_path.suffix.casefold() not in SECTION_CHUNK_SUFFIXES:
        raise StageError("accepted section chunks must be a JSONL file")
    parsed_path, _ = resolve_workspace_ref(root, parsed_job_ref, "queue parsed_text_path")
    if (
        sections_path.parent != parsed_path.parent
        or sections_path.name != "section_chunks.jsonl"
    ):
        raise StageError(
            "accepted section chunks must be section_chunks.jsonl beside the parsed Markdown"
        )
    sections_sha = validate_sha256(job.get("sections_sha256", ""), "sections_sha256")
    if sha256_file(sections_path) != sections_sha:
        raise StageError("accepted section chunks SHA-256 mismatch")
    try:
        sections_count = int(job.get("sections_count", ""))
    except ValueError as exc:
        raise StageError("accepted section chunks count is invalid") from exc
    if sections_count <= 0 or str(sections_count) != job.get("sections_count", ""):
        raise StageError("accepted section chunks count must be a canonical positive integer")
    try:
        section_records = _section_chunking.validate_sections_jsonl(
            sections_path,
            parsed_path,
            source_sha256=job["source_sha256"].casefold(),
            parsed_text_sha256=job["parsed_text_sha256"].casefold(),
            expected_count=sections_count,
            paper_id=job.get("paper_id") or job["document_id"],
            source_file=source_path.name,
            source_format=job["source_kind"],
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise StageError(f"accepted section chunks failed validation: {exc}") from exc
    expected_section_paper_id = job.get("paper_id") or job["document_id"]
    if any(record.get("paper_id") != expected_section_paper_id for record in section_records):
        raise StageError("accepted section chunks paper_id binding mismatch")
    section_schema_versions = {
        str(record.get("schema_version") or "") for record in section_records
    }
    if section_schema_versions != {str(_section_chunking.SECTION_SCHEMA_VERSION)}:
        raise StageError("accepted section chunks schema version mismatch")

    index_path = root / "04_library_index" / "PAPER_INDEX.csv"
    if index_path.is_symlink() or not index_path.is_file():
        raise StageError(f"authoritative PAPER_INDEX.csv is missing: {index_path}")
    try:
        with index_path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fields = set(reader.fieldnames or [])
            missing = sorted(INDEX_REQUIRED_FIELDS - fields)
            if missing:
                raise StageError(f"PAPER_INDEX.csv is missing required fields: {missing}")
            index_rows = [
                row
                for row in reader
                if (row.get("source_batch_id") or "").strip() == batch_id
                and (row.get("source_item_id") or "").strip() == source_item_id
            ]
    except (OSError, UnicodeError, csv.Error) as exc:
        raise StageError(f"cannot read authoritative PAPER_INDEX.csv: {exc}") from exc
    if len(index_rows) != 1:
        raise StageError(
            "staging requires exactly one PAPER_INDEX row for "
            f"batch_id={batch_id!r}, source_item_id={source_item_id!r}; "
            f"found {len(index_rows)}"
        )
    index = {key: (value or "").strip() for key, value in index_rows[0].items()}
    parsed_index_field = "mineru_md_path" if job.get("source_kind") == "pdf" else "xml_md_path"
    index_expected = {
        "paper_id": job.get("paper_id", ""),
        "title": job.get("title", ""),
        "doi": job.get("doi", ""),
        "sha256": job.get("source_sha256", ""),
        "document_id": job.get("document_id", ""),
        "source_kind": job.get("source_kind", ""),
        "processed_text_sha256": job.get("parsed_text_sha256", ""),
        "processing_method": job.get("processor", ""),
        "processing_version": job.get("processor_version", ""),
        "processing_accepted_at": job.get("accepted_at", ""),
        "section_chunks_path": job.get("sections_path", ""),
        "section_chunks_sha256": job.get("sections_sha256", ""),
        "section_chunks_count": str(sections_count),
        "section_chunks_schema_version": str(_section_chunking.SECTION_SCHEMA_VERSION),
        parsed_index_field: job.get("parsed_text_path", ""),
    }
    index_mismatches = [
        key for key, expected in index_expected.items()
        if index.get(key, "") != expected
    ]
    for hash_field in (
        "sha256", "processed_text_sha256", "section_chunks_sha256"
    ):
        if index.get(hash_field, "").casefold() == index_expected[hash_field].casefold():
            try:
                index_mismatches.remove(hash_field)
            except ValueError:
                pass
    if index.get("identity_verified", "").casefold() != "true":
        index_mismatches.append("identity_verified")
    _, index_source_ref = resolve_workspace_ref(root, index.get("pdf_path", ""), "PAPER_INDEX pdf_path")
    _, index_parsed_ref = resolve_workspace_ref(
        root, index.get(parsed_index_field, ""), f"PAPER_INDEX {parsed_index_field}"
    )
    _, index_sections_ref = resolve_workspace_ref(
        root, index.get("section_chunks_path", ""),
        "PAPER_INDEX section_chunks_path",
    )
    if index_source_ref != source_job_ref:
        index_mismatches.append("pdf_path")
    if index_parsed_ref != parsed_job_ref:
        index_mismatches.append(parsed_index_field)
    if index_sections_ref != sections_job_ref:
        index_mismatches.append("section_chunks_path")
    if index_mismatches:
        raise StageError(
            "PAPER_INDEX.csv does not match the accepted processing job: "
            + ", ".join(sorted(set(index_mismatches)))
        )

    return {
        "batch_id": batch_id,
        "source_item_id": source_item_id,
        "document_id": job["document_id"],
        "paper_id": job["paper_id"],
        "title": job["title"],
        "doi": job["doi"],
        "parser": job["processor"],
        "parser_version": job["processor_version"],
        "source_ref": source_job_ref,
        "source_sha256": job["source_sha256"].casefold(),
        "parsed_text_ref": parsed_job_ref,
        "parsed_text_sha256": job["parsed_text_sha256"].casefold(),
        "sections_ref": sections_job_ref,
        "sections_sha256": sections_sha,
        "sections_count": sections_count,
        "sections_schema_version": str(_section_chunking.SECTION_SCHEMA_VERSION),
    }


def checked_output_parent(root: Path, task_id: str) -> Path:
    parent = root / "runtime" / "inputs" / task_id / "documents"
    resolved_parent = parent.resolve(strict=False)
    if not is_within(resolved_parent, root):
        raise StageError(
            "runtime/inputs destination escapes workspace (including via symlink)"
        )
    return parent


def copy_verified(source: Path, destination: Path, expected_sha256: str) -> None:
    """Copy bytes and re-hash the copy to close the source-mutation window."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as input_handle, destination.open("xb") as output_handle:
        shutil.copyfileobj(input_handle, output_handle, length=1 << 20)
        output_handle.flush()
        os.fsync(output_handle.fileno())
    copied_sha256 = sha256_file(destination)
    if copied_sha256 != expected_sha256:
        raise StageError(
            f"input changed while being staged: expected {expected_sha256}, got {copied_sha256}"
        )


def claim_destination(destination: Path) -> None:
    """Claim a document id with the kernel's exclusive directory creation."""
    os.mkdir(destination, 0o700)


def validate_existing_stage(
    destination: Path, manifest: dict[str, Any], root: Path
) -> Path:
    """Accept only an existing stage that is byte-for-byte equivalent in meaning."""
    if destination.is_symlink() or not destination.is_dir():
        raise StageError(f"refusing to overwrite existing staged document: {destination}")
    manifest_path = destination / MANIFEST_NAME
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise StageError(
            f"existing staged document is incomplete (missing regular {MANIFEST_NAME}): "
            f"{destination}"
        )
    try:
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise StageError(f"existing stage manifest is unreadable: {manifest_path}") from exc
    if existing != manifest:
        raise StageError(
            f"refusing to overwrite existing staged document with different manifest: {destination}"
        )

    refs_and_hashes = [(manifest["parsed_text_ref"], manifest["parsed_text_sha256"])]
    if manifest.get("section_chunks_ref") is not None:
        refs_and_hashes.append(
            (manifest["section_chunks_ref"], manifest["section_chunks_sha256"])
        )
    if manifest["source_staged_ref"] is not None:
        refs_and_hashes.append((manifest["source_staged_ref"], manifest["source_sha256"]))
    for raw_ref, expected_sha in refs_and_hashes:
        # Manifest equality above guarantees these are the refs generated by
        # this invocation; resolve them again to detect later symlink tampering.
        staged_path, canonical_ref = resolve_workspace_ref(root, raw_ref, "staged_ref")
        if canonical_ref != raw_ref or not is_within(staged_path, destination.resolve()):
            raise StageError(f"existing staged file escapes its document directory: {raw_ref}")
        actual_sha = sha256_file(staged_path)
        if actual_sha != expected_sha:
            raise StageError(
                f"existing staged file hash mismatch for {raw_ref}: "
                f"expected {expected_sha}, got {actual_sha}"
            )
    return manifest_path


def prepare_stage(
    *,
    root: Path,
    task_id: str,
    document_id: str,
    batch_id: str = "",
    source_item_id: str = "",
    paper_id: str = "",
    title: str = "",
    doi: str = "",
    parser: str = "",
    parser_version: str = "",
    source_ref: str,
    source_sha256: str,
    parsed_text_ref: str,
    parsed_text_sha256: str,
    sections_ref: str = "",
    sections_sha256: str = "",
    sections_count: int = 0,
    sections_schema_version: str = "",
    allow_copy_fulltext: bool = False,
) -> dict[str, Any]:
    """Validate inputs and return a complete, deterministic staging plan."""
    try:
        root = root.expanduser().resolve(strict=True)
    except (FileNotFoundError, RuntimeError, OSError) as exc:
        raise StageError(f"workspace root does not exist: {root}") from exc
    if not root.is_dir():
        raise StageError(f"workspace root is not a directory: {root}")

    task_id = validate_id(task_id, "task_id")
    document_id = validate_id(document_id, "document_id")
    if bool(batch_id) != bool(source_item_id):
        raise StageError("batch_id and source_item_id must be supplied together")
    if batch_id:
        batch_id = validate_id(batch_id, "batch_id")
        source_item_id = validate_id(source_item_id, "source_item_id")
    expected_source_sha = validate_sha256(source_sha256, "source_sha256")
    expected_parsed_sha = validate_sha256(parsed_text_sha256, "parsed_text_sha256")
    source_path, source_lexical_ref = resolve_workspace_ref(root, source_ref, "source_ref")
    parsed_path, parsed_lexical_ref = resolve_workspace_ref(
        root, parsed_text_ref, "parsed_text_ref"
    )
    has_sections = any(
        (sections_ref, sections_sha256, sections_count, sections_schema_version)
    )
    sections_path: Path | None = None
    sections_lexical_ref = ""
    expected_sections_sha = ""
    if has_sections:
        if not all((sections_ref, sections_sha256, sections_count, sections_schema_version)):
            raise StageError(
                "sections_ref, sections_sha256, sections_count and "
                "sections_schema_version must be supplied together"
            )
        if isinstance(sections_count, bool) or not isinstance(sections_count, int) or sections_count <= 0:
            raise StageError("sections_count must be a positive integer")
        sections_path, sections_lexical_ref = resolve_workspace_ref(
            root, sections_ref, "sections_ref"
        )
        if sections_path.suffix.casefold() not in SECTION_CHUNK_SUFFIXES:
            raise StageError("sections_ref must identify a JSONL file")
        expected_sections_sha = validate_sha256(sections_sha256, "sections_sha256")
        if sections_schema_version != str(_section_chunking.SECTION_SCHEMA_VERSION):
            raise StageError("unsupported sections_schema_version")

    source_suffix = source_path.suffix.lower()
    parsed_suffix = parsed_path.suffix.lower()
    if source_suffix not in SOURCE_SUFFIXES:
        raise StageError(
            f"source_ref must identify PDF or XML ({sorted(SOURCE_SUFFIXES)}), got {source_suffix!r}"
        )
    if parsed_suffix not in PARSED_TEXT_SUFFIXES:
        raise StageError(
            "parsed_text_ref must identify Markdown or plain text "
            f"({sorted(PARSED_TEXT_SUFFIXES)}), got {parsed_suffix!r}"
        )
    paper_id = validate_manifest_text(paper_id, "paper_id", default=document_id)
    title = validate_optional_manifest_text(title, "title", limit=2048)
    doi = validate_optional_manifest_text(doi, "doi", limit=512)
    parser = validate_manifest_text(
        parser,
        "parser",
        default="mineru" if source_suffix == ".pdf" else "xml_to_markdown",
    )
    parser_version = validate_manifest_text(
        parser_version, "parser_version", default="unspecified"
    )

    actual_source_sha = sha256_file(source_path)
    if actual_source_sha != expected_source_sha:
        raise StageError(
            "stale source_sha256: "
            f"expected {expected_source_sha}, current file is {actual_source_sha}"
        )
    actual_parsed_sha = sha256_file(parsed_path)
    if actual_parsed_sha != expected_parsed_sha:
        raise StageError(
            "stale parsed_text_sha256: "
            f"expected {expected_parsed_sha}, current file is {actual_parsed_sha}"
        )
    if sections_path is not None:
        actual_sections_sha = sha256_file(sections_path)
        if actual_sections_sha != expected_sections_sha:
            raise StageError(
                "stale sections_sha256: "
                f"expected {expected_sections_sha}, current file is {actual_sections_sha}"
            )
        try:
            section_records = _section_chunking.validate_sections_jsonl(
                sections_path,
                parsed_path,
                source_sha256=expected_source_sha,
                parsed_text_sha256=expected_parsed_sha,
                expected_count=sections_count,
                paper_id=paper_id,
                source_file=source_path.name,
                source_format=source_suffix.removeprefix("."),
            )
        except (OSError, UnicodeError, ValueError) as exc:
            raise StageError(f"section chunks failed validation: {exc}") from exc
        if any(record.get("paper_id") != paper_id for record in section_records):
            raise StageError("section chunks paper_id binding mismatch")

    documents_parent = checked_output_parent(root, task_id)
    destination = documents_parent / document_id
    # resolve(strict=False) also follows any existing parent symlink.
    if not is_within(destination.resolve(strict=False), root):
        raise StageError("document staging destination escapes workspace")
    parsed_staged = destination / "parsed" / f"{expected_parsed_sha}{parsed_suffix}"
    sections_staged = (
        destination / "parsed" / f"{expected_sections_sha}.section_chunks.jsonl"
        if sections_path is not None
        else None
    )
    source_staged = (
        destination / "source" / f"{expected_source_sha}{source_suffix}"
        if allow_copy_fulltext
        else None
    )
    staged_source_ref = (
        workspace_relative(source_staged, root) if source_staged is not None else None
    )
    staged_parsed_ref = workspace_relative(parsed_staged, root)
    staged_sections_ref = (
        workspace_relative(sections_staged, root)
        if sections_staged is not None
        else None
    )
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task_id": task_id,
        "document_id": document_id,
        "batch_id": batch_id,
        "source_item_id": source_item_id,
        "item_id": source_item_id,
        "paper_id": paper_id,
        "title": title,
        "doi": doi,
        "parser": parser,
        "parser_version": parser_version,
        "status": "accepted",
        "source_sha256": expected_source_sha,
        "parsed_text_sha256": expected_parsed_sha,
        "source_kind": source_suffix.removeprefix("."),
        "source_ref": source_lexical_ref,
        "source_staged_ref": staged_source_ref,
        "source_path": staged_source_ref or source_lexical_ref,
        "parsed_text_source_ref": parsed_lexical_ref,
        "parsed_text_ref": staged_parsed_ref,
        "parsed_text_path": staged_parsed_ref,
        "section_chunks_source_ref": sections_lexical_ref or None,
        "section_chunks_ref": staged_sections_ref,
        "section_chunks_path": staged_sections_ref,
        "section_chunks_sha256": expected_sections_sha or None,
        "section_chunks_count": sections_count if sections_path is not None else 0,
        "section_chunks_schema_version": (
            sections_schema_version if sections_path is not None else None
        ),
        "fulltext_copied": source_staged is not None,
        "source_bytes": source_path.stat().st_size,
        "parsed_text_bytes": parsed_path.stat().st_size,
        "section_chunks_bytes": (
            sections_path.stat().st_size if sections_path is not None else 0
        ),
    }
    already_staged = False
    if destination.exists() or destination.is_symlink():
        validate_existing_stage(destination, manifest, root)
        already_staged = True

    return {
        "root": root,
        "source_path": source_path,
        "parsed_path": parsed_path,
        "sections_path": sections_path,
        "documents_parent": documents_parent,
        "destination": destination,
        "parsed_staged": parsed_staged,
        "sections_staged": sections_staged,
        "source_staged": source_staged,
        "manifest": manifest,
        "already_staged": already_staged,
    }


def stage_document(plan: dict[str, Any]) -> Path:
    """Publish without replacement; ``manifest.json`` is the atomic commit marker."""
    parent: Path = plan["documents_parent"]
    destination: Path = plan["destination"]
    manifest: dict[str, Any] = plan["manifest"]
    if plan.get("already_staged"):
        return validate_existing_stage(destination, manifest, plan["root"])
    parent.mkdir(parents=True, exist_ok=True)
    if not is_within(parent.resolve(strict=True), plan["root"]):
        raise StageError("runtime/inputs destination escapes workspace (including via symlink)")

    # The exclusive reservation serializes this script's writers.  A later
    # exclusive mkdir is the kernel-enforced no-replace claim.
    lock_path = parent / f".{destination.name}.stage.lock"
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise StageError(f"another staging process owns {lock_path}") from exc

    temporary: Path | None = None
    try:
        os.close(lock_fd)
        if destination.exists() or destination.is_symlink():
            plan["already_staged"] = True
            return validate_existing_stage(destination, manifest, plan["root"])
        temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.tmp-", dir=parent))

        parsed_relative = Path("parsed") / Path(plan["parsed_staged"]).name
        copy_verified(
            plan["parsed_path"],
            temporary / parsed_relative,
            manifest["parsed_text_sha256"],
        )
        if (temporary / parsed_relative).stat().st_size != manifest["parsed_text_bytes"]:
            raise StageError("parsed text size changed after validation")
        sections_relative: Path | None = None
        if plan["sections_staged"] is not None:
            sections_relative = Path("parsed") / Path(plan["sections_staged"]).name
            copy_verified(
                plan["sections_path"],
                temporary / sections_relative,
                manifest["section_chunks_sha256"],
            )
            if (temporary / sections_relative).stat().st_size != manifest["section_chunks_bytes"]:
                raise StageError("section chunks size changed after validation")
        if plan["source_staged"] is not None:
            source_relative = Path("source") / Path(plan["source_staged"]).name
            copy_verified(
                plan["source_path"],
                temporary / source_relative,
                manifest["source_sha256"],
            )

        # The source may be reference-only.  Re-check it immediately before
        # publication so a plan prepared earlier cannot publish a stale hash.
        current_source_sha = sha256_file(plan["source_path"])
        if current_source_sha != manifest["source_sha256"]:
            raise StageError(
                "source changed after validation: "
                f"expected {manifest['source_sha256']}, got {current_source_sha}"
            )
        if plan["source_path"].stat().st_size != manifest["source_bytes"]:
            raise StageError("source size changed after validation")
        current_parsed_sha = sha256_file(plan["parsed_path"])
        if current_parsed_sha != manifest["parsed_text_sha256"]:
            raise StageError(
                "parsed text changed after validation: "
                f"expected {manifest['parsed_text_sha256']}, got {current_parsed_sha}"
            )
        if plan["parsed_path"].stat().st_size != manifest["parsed_text_bytes"]:
            raise StageError("parsed text size changed after validation")
        if plan["sections_path"] is not None:
            current_sections_sha = sha256_file(plan["sections_path"])
            if current_sections_sha != manifest["section_chunks_sha256"]:
                raise StageError(
                    "section chunks changed after validation: "
                    f"expected {manifest['section_chunks_sha256']}, got {current_sections_sha}"
                )
            if plan["sections_path"].stat().st_size != manifest["section_chunks_bytes"]:
                raise StageError("section chunks size changed after validation")

        manifest_path = temporary / MANIFEST_NAME
        with manifest_path.open("x", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        try:
            claim_destination(destination)
        except FileExistsError:
            plan["already_staged"] = True
            return validate_existing_stage(destination, manifest, plan["root"])

        # The destination is now exclusively ours.  Link verified immutable
        # files into it, then publish manifest.json last as the commit marker.
        # Hard-link creation fails rather than replacing an existing path.
        destination_committed = False
        try:
            (destination / "parsed").mkdir(mode=0o700)
            os.link(
                temporary / parsed_relative,
                destination / parsed_relative,
                follow_symlinks=False,
            )
            if sections_relative is not None:
                os.link(
                    temporary / sections_relative,
                    destination / sections_relative,
                    follow_symlinks=False,
                )
            if plan["source_staged"] is not None:
                (destination / "source").mkdir(mode=0o700)
                os.link(
                    temporary / source_relative,
                    destination / source_relative,
                    follow_symlinks=False,
                )
            os.link(manifest_path, destination / MANIFEST_NAME, follow_symlinks=False)
            destination_committed = True
        finally:
            if not destination_committed:
                shutil.rmtree(destination, ignore_errors=True)
        return destination / MANIFEST_NAME
    finally:
        if temporary is not None:
            shutil.rmtree(temporary, ignore_errors=True)
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--root", default=".", help="workspace root (default: current directory)")
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--document-id", required=True)
    parser.add_argument(
        "--batch-id",
        required=True,
        help="accepted download batch identifier",
    )
    parser.add_argument(
        "--source-item-id",
        required=True,
        help="accepted MANIFEST item identifier",
    )
    parser.add_argument(
        "--paper-id",
        default="",
        help="logical paper identifier (default: document_id)",
    )
    parser.add_argument("--title", default="", help="optional paper title")
    parser.add_argument("--doi", default="", help="optional DOI")
    parser.add_argument(
        "--parser",
        default="",
        help="parser name (default: mineru for PDF, xml_to_markdown for XML)",
    )
    parser.add_argument(
        "--parser-version",
        default="",
        help="parser version recorded in the manifest (default: unspecified)",
    )
    parser.add_argument(
        "--source-ref", required=True,
        help="workspace-relative accepted PDF/XML path",
    )
    parser.add_argument("--source-sha256", required=True, help="expected source file SHA-256")
    parser.add_argument(
        "--parsed-text-ref", required=True,
        help="workspace-relative parsed Markdown/text path",
    )
    parser.add_argument(
        "--parsed-text-sha256", required=True,
        help="expected parsed text SHA-256",
    )
    parser.add_argument(
        "--allow-copy-fulltext", action="store_true",
        help="explicitly allow copying source PDF/XML into runtime/inputs",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="validate refs, hashes, IDs, and destination without writing",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        with accepted_state_read_lock(Path(args.root)):
            binding = validate_authoritative_acceptance(
                root=Path(args.root),
                batch_id=args.batch_id,
                source_item_id=args.source_item_id,
                document_id=args.document_id,
                paper_id=args.paper_id,
                title=args.title,
                doi=args.doi,
                parser=args.parser,
                parser_version=args.parser_version,
                source_ref=args.source_ref,
                source_sha256=args.source_sha256,
                parsed_text_ref=args.parsed_text_ref,
                parsed_text_sha256=args.parsed_text_sha256,
            )
            plan = prepare_stage(
                root=Path(args.root),
                task_id=args.task_id,
                document_id=binding["document_id"],
                batch_id=binding["batch_id"],
                source_item_id=binding["source_item_id"],
                paper_id=binding["paper_id"],
                title=binding["title"],
                doi=binding["doi"],
                parser=binding["parser"],
                parser_version=binding["parser_version"],
                source_ref=binding["source_ref"],
                source_sha256=binding["source_sha256"],
                parsed_text_ref=binding["parsed_text_ref"],
                parsed_text_sha256=binding["parsed_text_sha256"],
                sections_ref=binding["sections_ref"],
                sections_sha256=binding["sections_sha256"],
                sections_count=binding["sections_count"],
                sections_schema_version=binding["sections_schema_version"],
                allow_copy_fulltext=args.allow_copy_fulltext,
            )
            if args.dry_run:
                result = {
                    "status": "validated",
                    "dry_run": True,
                    "already_staged": bool(plan["already_staged"]),
                    "destination": workspace_relative(plan["destination"], plan["root"]),
                    "manifest": plan["manifest"],
                }
            else:
                manifest_path = stage_document(plan)
                result = {
                    "status": "already_staged" if plan["already_staged"] else "staged",
                    "dry_run": False,
                    "already_staged": bool(plan["already_staged"]),
                    "destination": workspace_relative(plan["destination"], plan["root"]),
                    "manifest_ref": workspace_relative(manifest_path, plan["root"]),
                    "manifest": plan["manifest"],
                }
    except (StageError, OSError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
