#!/usr/bin/env python3
"""Shared, dependency-free contracts for task-scoped data extraction.

Runtime files deliberately live below ``runtime/`` so source documents, parsed
paper text, and model output cannot be committed with the public codebase.
"""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import math
import os
import re
import tempfile
import unicodedata
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


QUEUE_STATES = {
    "queued", "claimed", "running", "review", "accepted",
    "retry_wait", "blocked", "failed", "skipped",
}
CLAIMABLE_STATES = {"queued", "retry_wait"}
DATA_TYPES = {"string", "number", "integer", "boolean", "category", "date"}
VALUE_SEMANTICS = {
    "measured", "reported", "derived", "categorical", "identifier",
    "date", "free_text",
}
QUEUE_VERSION = "1.0"
RESULT_VERSION = "1.0"
MAX_CHUNK_CHARS = 100_000
MAX_CHUNK_OVERLAP = 20_000
MAX_SPEC_BYTES = 1_048_576
MAX_SPEC_FIELDS = 500
MAX_SPEC_TEXT = 20_000
MAX_SPEC_ENUM_VALUES = 1_000
MODEL_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
ATTEMPT_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,191}\Z")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_json(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def atomic_write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


@contextmanager
def exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return value


def ensure_task_id(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value):
        raise ValueError("task_id must be a path-safe identifier")
    return value


def task_paths(root: Path, task_id: str) -> dict[str, Path]:
    task_id = ensure_task_id(task_id)
    root = root.expanduser().resolve()
    task = root / "runtime" / "tasks" / task_id
    extraction = task / "extraction"
    results = root / "runtime" / "results" / task_id
    return {
        "root": root,
        "task": task,
        "extraction": extraction,
        "spec": extraction / "extraction_spec.json",
        "queue": extraction / "data_extraction_queue.json",
        "queue_csv": extraction / "data_extraction_queue.csv",
        "queue_lock": extraction / ".data_extraction_queue.lock",
        "review_queue": extraction / "review_queue.csv",
        "review_lock": extraction / ".review_queue.lock",
        "delivery": extraction / "delivery",
        "results": results,
        "paperqa_results": results / "paperqa",
        "extraction_results": results / "data_extraction",
    }


def inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def resolve_path(root: Path, value: str, *, require_runtime: bool = False) -> Path:
    candidate = Path(value).expanduser()
    path = (candidate if candidate.is_absolute() else root / candidate).resolve()
    if require_runtime and not inside(path, root / "runtime"):
        raise ValueError(f"runtime artifact escapes runtime/: {value}")
    return path


def portable_path(root: Path, path: Path) -> str:
    path = path.resolve()
    try:
        return str(path.relative_to(root.resolve()))
    except ValueError:
        # Inputs may be external when a user explicitly stages a local source.
        # Runtime outputs always use workspace-relative paths.
        return str(path)


def normalize_text(value: Any) -> str:
    return " ".join(unicodedata.normalize("NFKC", str(value or "")).split())


def has_visible_text(value: Any) -> bool:
    """Return true only when text contains a visible, non-format character."""
    if not isinstance(value, str):
        return False
    return any(
        not character.isspace()
        and unicodedata.category(character)[0] not in {"C", "Z"}
        for character in unicodedata.normalize("NFKC", value)
    )


def normalize_for_id(value: Any) -> str:
    return normalize_text(value).casefold()


def chunk_document(text: str, limit: int, overlap: int) -> list[dict[str, Any]]:
    # Keep one authoritative chunking algorithm.  The structure-aware helper
    # preserves heading paths and table/figure routing metadata while proving
    # every chunk is an exact substring and that no non-whitespace text is lost.
    from paper_section_chunking import chunk_markdown

    return chunk_markdown(text, limit=limit, overlap=overlap)


def load_spec(path: Path, task_id: str | None = None) -> tuple[dict[str, Any], str]:
    spec = read_json(path)
    if len(json.dumps(spec, ensure_ascii=False).encode("utf-8")) > MAX_SPEC_BYTES:
        raise ValueError(f"extraction spec exceeds {MAX_SPEC_BYTES} bytes")
    allowed = {
        "spec_version", "task_id", "title", "purpose", "fields",
        "record_identity_fields", "source_scope", "review_policy", "model",
    }
    unknown = sorted(set(spec) - allowed)
    if unknown:
        raise ValueError(f"unknown extraction spec keys: {unknown}")
    required = {"spec_version", "task_id", "title", "purpose", "fields"}
    missing = sorted(key for key in required if key not in spec)
    if missing:
        raise ValueError(f"missing extraction spec keys: {missing}")
    for key in ("spec_version", "title", "purpose"):
        if not isinstance(spec.get(key), str) or not spec[key].strip():
            raise ValueError(f"extraction spec {key} must be a non-empty string")
        if len(spec[key]) > MAX_SPEC_TEXT:
            raise ValueError(f"extraction spec {key} exceeds {MAX_SPEC_TEXT} characters")
    if task_id and spec.get("task_id") != task_id:
        raise ValueError("extraction spec task_id does not match selected task")
    ensure_task_id(str(spec["task_id"]))
    if not isinstance(spec.get("fields"), list) or not spec["fields"]:
        raise ValueError("extraction spec fields must be a non-empty array")
    if len(spec["fields"]) > MAX_SPEC_FIELDS:
        raise ValueError(f"extraction spec exceeds {MAX_SPEC_FIELDS} fields")
    field_ids: set[str] = set()
    for number, field in enumerate(spec["fields"], 1):
        if not isinstance(field, dict):
            raise ValueError(f"field {number} must be an object")
        field_allowed = {
            "field_id", "label", "description", "data_type", "required",
            "multiple", "value_semantics", "unit", "allowed_values", "notes",
        }
        extra = sorted(set(field) - field_allowed)
        if extra:
            raise ValueError(f"field {number} has unknown keys: {extra}")
        for key in ("field_id", "label", "description", "data_type", "value_semantics"):
            if key not in field:
                raise ValueError(f"field {number} missing {key}")
        for key in ("label", "description"):
            if not isinstance(field[key], str) or not field[key].strip():
                raise ValueError(f"field {number} {key} must be a non-empty string")
            if len(field[key]) > MAX_SPEC_TEXT:
                raise ValueError(f"field {number} {key} exceeds {MAX_SPEC_TEXT} characters")
        field_id = str(field["field_id"])
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,127}", field_id):
            raise ValueError(f"invalid field_id: {field_id!r}")
        if field_id in field_ids:
            raise ValueError(f"duplicate field_id: {field_id}")
        field_ids.add(field_id)
        if field["data_type"] not in DATA_TYPES:
            raise ValueError(f"unsupported data_type for {field_id}")
        if field["value_semantics"] not in VALUE_SEMANTICS:
            raise ValueError(f"unsupported value_semantics for {field_id}")
        if "required" in field and not isinstance(field["required"], bool):
            raise ValueError(f"required must be boolean for {field_id}")
        if "multiple" in field and not isinstance(field["multiple"], bool):
            raise ValueError(f"multiple must be boolean for {field_id}")
        unit = field.get("unit", {"required": False, "allowed": []})
        if not isinstance(unit, dict) or set(unit) - {"required", "allowed"}:
            raise ValueError(f"invalid unit contract for {field_id}")
        if not isinstance(unit.get("required", False), bool):
            raise ValueError(f"unit.required must be boolean for {field_id}")
        if not isinstance(unit.get("allowed", []), list):
            raise ValueError(f"unit.allowed must be an array for {field_id}")
        allowed_units = unit.get("allowed", [])
        if len(allowed_units) > MAX_SPEC_ENUM_VALUES:
            raise ValueError(f"unit.allowed has too many values for {field_id}")
        if any(not isinstance(value, str) for value in allowed_units):
            raise ValueError(f"unit.allowed values must be strings for {field_id}")
        if any(not value.strip() or len(value) > MAX_SPEC_TEXT for value in allowed_units):
            raise ValueError(f"unit.allowed values must be non-empty and bounded for {field_id}")
        if len(allowed_units) != len(set(allowed_units)):
            raise ValueError(f"unit.allowed values must be unique for {field_id}")
        if "allowed_values" in field:
            allowed_values = field["allowed_values"]
            if not isinstance(allowed_values, list):
                raise ValueError(f"allowed_values must be an array for {field_id}")
            if len(allowed_values) > MAX_SPEC_ENUM_VALUES:
                raise ValueError(f"allowed_values has too many values for {field_id}")
            if any(not validate_value_type(value, field["data_type"])
                   for value in allowed_values):
                raise ValueError(f"allowed_values do not match data_type for {field_id}")
            if len({sha256_json(value) for value in allowed_values}) != len(allowed_values):
                raise ValueError(f"allowed_values must be unique for {field_id}")
        if "notes" in field and not isinstance(field["notes"], str):
            raise ValueError(f"notes must be a string for {field_id}")
        if len(field.get("notes", "")) > MAX_SPEC_TEXT:
            raise ValueError(f"notes exceeds {MAX_SPEC_TEXT} characters for {field_id}")
    identities = spec.get("record_identity_fields", [])
    if not isinstance(identities, list) or any(value not in field_ids for value in identities):
        raise ValueError("record_identity_fields must reference declared fields")
    if len(identities) != len(set(identities)):
        raise ValueError("record_identity_fields must be unique")
    scope = spec.get("source_scope", {})
    if not isinstance(scope, dict) or set(scope) - {"fulltext_decisions", "notes"}:
        raise ValueError("source_scope has unknown keys or is not an object")
    decisions = scope.get("fulltext_decisions", ["include", "uncertain"])
    if (not isinstance(decisions, list)
            or any(value not in {"include", "uncertain"} for value in decisions)
            or len(decisions) != len(set(decisions))):
        raise ValueError("source_scope.fulltext_decisions must contain unique include/uncertain values")
    if "notes" in scope and not isinstance(scope["notes"], str):
        raise ValueError("source_scope.notes must be a string")
    if len(scope.get("notes", "")) > MAX_SPEC_TEXT:
        raise ValueError("source_scope.notes exceeds the size limit")
    review = spec.get("review_policy", {"human_review_required": True})
    if (not isinstance(review, dict)
            or set(review) - {"human_review_required", "instructions"}
            or ("review_policy" in spec and "human_review_required" not in review)
            or review.get("human_review_required", True) is not True):
        raise ValueError("human review cannot be disabled")
    if "instructions" in review and not isinstance(review["instructions"], str):
        raise ValueError("review_policy.instructions must be a string")
    if len(review.get("instructions", "")) > MAX_SPEC_TEXT:
        raise ValueError("review_policy.instructions exceeds the size limit")
    model = spec.get("model", {})
    if not isinstance(model, dict) or set(model) - {"name", "chunk_chars", "chunk_overlap"}:
        raise ValueError("model has unknown keys or is not an object")
    if "name" in model and (
        not isinstance(model["name"], str)
        or not MODEL_NAME_RE.fullmatch(model["name"])
    ):
        raise ValueError("model.name must be a path-safe model identifier of at most 128 characters")
    chunk_chars = model.get("chunk_chars", 12000)
    chunk_overlap = model.get("chunk_overlap", 800)
    if (isinstance(chunk_chars, bool) or not isinstance(chunk_chars, int)
            or not 1000 <= chunk_chars <= MAX_CHUNK_CHARS):
        raise ValueError(
            f"model.chunk_chars must be an integer between 1000 and {MAX_CHUNK_CHARS}"
        )
    if (isinstance(chunk_overlap, bool) or not isinstance(chunk_overlap, int)
            or not 0 <= chunk_overlap <= MAX_CHUNK_OVERLAP):
        raise ValueError(
            f"model.chunk_overlap must be an integer between 0 and {MAX_CHUNK_OVERLAP}"
        )
    if chunk_overlap >= chunk_chars:
        raise ValueError("model.chunk_overlap must be smaller than model.chunk_chars")
    if chunk_overlap > chunk_chars // 2:
        raise ValueError("model.chunk_overlap must not exceed half of model.chunk_chars")
    return spec, sha256_json(spec)


def paperqa_question_for(spec: dict[str, Any]) -> str:
    """Build the only PaperQA question accepted for an extraction spec."""
    return spec["purpose"] + "\n" + "\n".join(
        f"- {field['field_id']}: {field['description']}" for field in spec["fields"]
    )


def field_map(spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(field["field_id"]): field for field in spec["fields"]}


_QUEUE_JOB_REQUIRED_KEYS = {
    "job_id", "task_id", "batch_id", "item_id", "paper_id", "title", "doi",
    "source_kind", "source_path", "source_sha256", "source_revalidation",
    "parsed_text_path", "parsed_text_sha256", "parser", "parser_version",
    "sections_path", "sections_sha256", "sections_count", "sections_schema_version",
    "fulltext_decision", "spec_version", "spec_sha256", "paperqa_question",
    "paperqa_question_sha256", "status", "skip_reason", "attempt_count",
    "max_attempts", "available_at", "claimed_by", "run_id", "attempt_id",
    "lease_token", "lease_expires_at", "heartbeat_at", "created_at", "updated_at",
    "error_code", "error_message", "receipt_history",
}
_QUEUE_JOB_OPTIONAL_KEYS = {
    "receipt_path", "receipt_sha256", "accepted_at", "acceptance_path",
}
_QUEUE_JOB_STRING_KEYS = (_QUEUE_JOB_REQUIRED_KEYS | _QUEUE_JOB_OPTIONAL_KEYS) - {
    "attempt_count", "max_attempts", "sections_count", "receipt_history",
}
_QUEUE_JOB_NONEMPTY_STRINGS = {
    "job_id", "task_id", "paper_id", "source_kind", "source_path",
    "source_sha256", "source_revalidation", "parsed_text_path",
    "parsed_text_sha256", "parser", "parser_version", "spec_version",
    "spec_sha256", "paperqa_question", "paperqa_question_sha256", "status",
    "created_at", "updated_at",
}


def _validate_queue_job_shape(job: dict[str, Any]) -> None:
    """Validate the complete persisted job schema before state-specific checks."""
    keys = set(job)
    missing = sorted(_QUEUE_JOB_REQUIRED_KEYS - keys)
    unknown = sorted(keys - _QUEUE_JOB_REQUIRED_KEYS - _QUEUE_JOB_OPTIONAL_KEYS)
    if missing or unknown:
        raise ValueError(
            f"queue job keys mismatch (missing={missing}, unknown={unknown})"
        )
    for key in _QUEUE_JOB_STRING_KEYS & keys:
        if not isinstance(job[key], str):
            raise ValueError(f"queue job {key} must be a string")
    for key in _QUEUE_JOB_NONEMPTY_STRINGS:
        if not job[key].strip():
            raise ValueError(f"queue job {key} must be a non-empty string")
    if job["source_revalidation"] not in {"verified_local", "deferred_to_mac"}:
        raise ValueError("queue job source_revalidation is invalid")
    if len(job["error_code"]) > 128 or len(job["error_message"]) > 1000:
        raise ValueError("queue job error fields exceed their size limits")
    for key in ("created_at", "updated_at"):
        if parse_time(job[key]) is None:
            raise ValueError(f"queue job {key} must be an ISO timestamp")
    if job["status"] in CLAIMABLE_STATES:
        if parse_time(job["available_at"]) is None:
            raise ValueError("claimable queue job available_at must be an ISO timestamp")
    elif job["available_at"] and parse_time(job["available_at"]) is None:
        raise ValueError("queue job available_at must be empty or an ISO timestamp")
    if job["status"] == "skipped":
        if not job["skip_reason"].strip():
            raise ValueError("skipped queue job must have a skip_reason")
    elif job["skip_reason"]:
        raise ValueError("non-skipped queue job must not have a skip_reason")
    acceptance_keys = {"accepted_at", "acceptance_path"}
    if job["status"] == "accepted":
        if not acceptance_keys <= keys:
            raise ValueError("accepted queue job lacks acceptance bindings")
        if parse_time(job["accepted_at"]) is None or not job["acceptance_path"].strip():
            raise ValueError("accepted queue job has invalid acceptance bindings")
    elif keys & acceptance_keys:
        raise ValueError("non-accepted queue job carries acceptance bindings")
    receipt_keys = {"receipt_path", "receipt_sha256"}
    if keys & receipt_keys and not receipt_keys <= keys:
        raise ValueError("queue job has partial receipt projection bindings")
    if receipt_keys <= keys:
        if (not job["receipt_path"].strip()
                or not SHA256_RE.fullmatch(job["receipt_sha256"])):
            raise ValueError("queue job has invalid receipt projection bindings")


def _validate_queue_value(
    queue: dict[str, Any], *, expected_task_id: str | None = None,
    expected_spec_version: str | None = None,
    expected_spec_sha256: str | None = None,
) -> dict[str, Any]:
    queue_keys = {
        "queue_version", "task_id", "spec_version", "spec_sha256",
        "created_at", "updated_at", "jobs",
    }
    if set(queue) != queue_keys:
        missing = sorted(queue_keys - set(queue))
        unknown = sorted(set(queue) - queue_keys)
        raise ValueError(f"extraction queue keys mismatch (missing={missing}, unknown={unknown})")
    if queue.get("queue_version") != QUEUE_VERSION:
        raise ValueError("unsupported extraction queue version")
    task_id = ensure_task_id(str(queue.get("task_id") or ""))
    if expected_task_id is not None and task_id != ensure_task_id(expected_task_id):
        raise ValueError("queue.task_id does not match the selected task")
    if not isinstance(queue.get("spec_version"), str) or not queue["spec_version"].strip():
        raise ValueError("queue.spec_version must be a non-empty string")
    if not isinstance(queue.get("spec_sha256"), str) or not SHA256_RE.fullmatch(queue["spec_sha256"]):
        raise ValueError("queue.spec_sha256 must be a lowercase SHA-256")
    if expected_spec_version is not None and queue["spec_version"] != expected_spec_version:
        raise ValueError("queue.spec_version does not match the current extraction spec")
    if expected_spec_sha256 is not None and queue["spec_sha256"] != expected_spec_sha256:
        raise ValueError("queue.spec_sha256 does not match the current extraction spec")
    for key in ("created_at", "updated_at"):
        if parse_time(queue.get(key, "")) is None:
            raise ValueError(f"queue.{key} must be an ISO timestamp")
    if not isinstance(queue.get("jobs"), list):
        raise ValueError("queue.jobs must be an array")
    seen: set[str] = set()
    for job in queue["jobs"]:
        if not isinstance(job, dict):
            raise ValueError("queue job is invalid")
        _validate_queue_job_shape(job)
        if job["job_id"] in seen:
            raise ValueError(f"duplicate job_id: {job['job_id']}")
        seen.add(job["job_id"])
        if job.get("status") not in QUEUE_STATES:
            raise ValueError(f"unknown queue status for {job['job_id']}")
        if job.get("task_id") != task_id:
            raise ValueError(f"queue job task_id mismatch: {job['job_id']}")
        if job.get("spec_version") != queue["spec_version"]:
            raise ValueError(f"queue job spec_version mismatch: {job['job_id']}")
        if job.get("spec_sha256") != queue["spec_sha256"]:
            raise ValueError(f"queue job spec_sha256 mismatch: {job['job_id']}")
        question = job.get("paperqa_question")
        question_sha = job.get("paperqa_question_sha256")
        if (not isinstance(question, str) or not question.strip()
                or not isinstance(question_sha, str)
                or not SHA256_RE.fullmatch(question_sha)
                or hashlib.sha256(question.encode("utf-8")).hexdigest() != question_sha):
            raise ValueError(f"queue job has an invalid PaperQA question binding: {job['job_id']}")
        attempt = job.get("attempt_count")
        maximum = job.get("max_attempts")
        if (isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0
                or isinstance(maximum, bool) or not isinstance(maximum, int)
                or maximum < 1 or attempt > maximum):
            raise ValueError(f"queue job attempt counters are invalid: {job['job_id']}")
        for key in ("source_sha256", "parsed_text_sha256"):
            if not isinstance(job.get(key), str) or not SHA256_RE.fullmatch(job[key]):
                raise ValueError(f"queue job has invalid {key}: {job['job_id']}")
        sections_sha = job.get("sections_sha256", "")
        if sections_sha:
            if not isinstance(sections_sha, str) or not SHA256_RE.fullmatch(sections_sha):
                raise ValueError(f"queue job has invalid sections_sha256: {job['job_id']}")
            count = job.get("sections_count")
            if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
                raise ValueError(f"queue job has invalid sections_count: {job['job_id']}")
            if not str(job.get("sections_path") or "").strip() or not str(job.get("sections_schema_version") or "").strip():
                raise ValueError(f"queue job lacks section sidecar bindings: {job['job_id']}")
        elif any(job.get(key) for key in ("sections_path", "sections_count", "sections_schema_version")):
            raise ValueError(f"queue job has partial section sidecar bindings: {job['job_id']}")
        if not isinstance(job.get("paper_id"), str) or not job["paper_id"].strip():
            raise ValueError(f"queue job has invalid paper_id: {job['job_id']}")
        try:
            expected_id = job_id_for(task_id, job, str(job["spec_sha256"]))
        except KeyError as exc:
            raise ValueError(f"queue job {job['job_id']} lacks binding {exc.args[0]}") from exc
        if job["job_id"] != expected_id:
            raise ValueError(f"queue job_id is stale for current bindings: {job['job_id']}")
        _validate_job_runtime_state(job)
    return queue


def load_queue(
    path: Path, *, expected_task_id: str | None = None,
    expected_spec_version: str | None = None,
    expected_spec_sha256: str | None = None,
) -> dict[str, Any]:
    return _validate_queue_value(
        read_json(path), expected_task_id=expected_task_id,
        expected_spec_version=expected_spec_version,
        expected_spec_sha256=expected_spec_sha256,
    )


_RECEIPT_HISTORY_KEYS = {
    "attempt_id", "run_id", "attempt_count", "status", "receipt_sha256",
}


def _validate_job_runtime_state(job: dict[str, Any]) -> None:
    """Reject internally impossible mutable queue state before it is acted on."""
    job_id = str(job["job_id"])
    status = str(job["status"])
    attempt = int(job["attempt_count"])
    run_id = job.get("run_id", "")
    attempt_id = job.get("attempt_id", "")
    active = status in {"claimed", "running"}
    attempted = status in {"claimed", "running", "retry_wait", "review", "accepted", "failed"} \
        or (status == "blocked" and attempt > 0)
    if attempted:
        if (attempt < 1 or not isinstance(run_id, str)
                or not re.fullmatch(r"[0-9a-f]{32}", run_id)
                or attempt_id != attempt_id_for(job_id, attempt, run_id)):
            raise ValueError(f"queue job has an invalid attempt binding: {job_id}")
    elif attempt == 0:
        if run_id or attempt_id:
            raise ValueError(f"unattempted queue job carries an attempt binding: {job_id}")
    if active:
        if (not str(job.get("claimed_by") or "").strip()
                or not str(job.get("lease_token") or "").strip()
                or parse_time(str(job.get("lease_expires_at") or "")) is None
                or parse_time(str(job.get("heartbeat_at") or "")) is None):
            raise ValueError(f"active queue job has an invalid lease: {job_id}")
    elif any(job.get(key) for key in ("claimed_by", "lease_token", "lease_expires_at")):
        raise ValueError(f"inactive queue job retains a lease: {job_id}")
    if status == "retry_wait":
        if attempt >= int(job["max_attempts"]):
            raise ValueError(f"retry_wait job exhausted max_attempts: {job_id}")
        if parse_time(str(job.get("available_at") or "")) is None:
            raise ValueError(f"retry_wait job has invalid available_at: {job_id}")
    if status == "failed" and attempt != int(job["max_attempts"]):
        raise ValueError(f"failed job has not exhausted max_attempts: {job_id}")
    history = job.get("receipt_history", [])
    if not isinstance(history, list):
        raise ValueError(f"queue job receipt_history must be an array: {job_id}")
    seen: set[str] = set()
    counts: list[int] = []
    for entry in history:
        if not isinstance(entry, dict) or set(entry) != _RECEIPT_HISTORY_KEYS:
            raise ValueError(f"queue job has a malformed receipt history entry: {job_id}")
        count = entry.get("attempt_count")
        if (isinstance(count, bool) or not isinstance(count, int) or count < 1
                or count > attempt or entry.get("status") not in {"review", "retry_wait", "failed"}
                or not isinstance(entry.get("run_id"), str)
                or not re.fullmatch(r"[0-9a-f]{32}", entry["run_id"])
                or entry.get("attempt_id") != attempt_id_for(job_id, count, entry["run_id"])
                or not isinstance(entry.get("receipt_sha256"), str)
                or not SHA256_RE.fullmatch(entry["receipt_sha256"])):
            raise ValueError(f"queue job has an invalid receipt history entry: {job_id}")
        if entry["attempt_id"] in seen:
            raise ValueError(f"queue job repeats a receipt attempt: {job_id}")
        seen.add(entry["attempt_id"])
        counts.append(count)
    expected_receipts = attempt if status in {
        "retry_wait", "review", "accepted", "failed"
    } else max(0, attempt - 1) if (active or status == "blocked") else len(counts)
    if counts != list(range(1, expected_receipts + 1)):
        raise ValueError(f"queue job receipt history is not contiguous: {job_id}")
    if counts:
        latest = history[-1]
        if (job.get("receipt_sha256") != latest["receipt_sha256"]
                or job.get("receipt_path", "").split("/")[-1]
                != f"{latest['attempt_id']}.json"):
            raise ValueError(f"queue job latest receipt projection is stale: {job_id}")
    elif job.get("receipt_path") or job.get("receipt_sha256"):
        raise ValueError(f"queue job has a receipt projection without history: {job_id}")


QUEUE_CSV_FIELDS = [
    "job_id", "task_id", "batch_id", "item_id", "paper_id", "title", "doi",
    "source_kind", "source_path", "source_sha256", "parsed_text_path",
    "source_revalidation", "parsed_text_sha256", "parser", "parser_version", "spec_version",
    "sections_path", "sections_sha256", "sections_count", "sections_schema_version",
    "spec_sha256", "status", "attempt_count", "max_attempts", "available_at",
    "claimed_by", "run_id", "attempt_id", "lease_expires_at", "heartbeat_at",
    "updated_at", "error_code", "error_message", "receipt_path", "receipt_sha256",
]


def write_queue(paths: dict[str, Path], queue: dict[str, Any]) -> None:
    queue["updated_at"] = utc_now()
    # Apply the exact same top-level, immutable-binding, question, job-id and
    # runtime-state validation used on reads before either projection changes.
    spec, spec_sha = load_spec(paths["spec"], paths["task"].name)
    _validate_queue_value(
        queue, expected_task_id=paths["task"].name,
        expected_spec_version=str(spec["spec_version"]),
        expected_spec_sha256=spec_sha,
    )
    expected_question = paperqa_question_for(spec)
    expected_question_sha = hashlib.sha256(expected_question.encode("utf-8")).hexdigest()
    for job in queue["jobs"]:
        if (job.get("paperqa_question") != expected_question
                or job.get("paperqa_question_sha256") != expected_question_sha):
            raise ValueError(
                f"queue job PaperQA question differs from current spec: {job.get('job_id')}"
            )
    atomic_write_json(paths["queue"], queue)
    rows = []
    for job in queue["jobs"]:
        rows.append({key: job.get(key, "") for key in QUEUE_CSV_FIELDS})
    from io import StringIO
    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=QUEUE_CSV_FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    atomic_write_text(paths["queue_csv"], stream.getvalue())


def get_job(queue: dict[str, Any], job_id: str) -> dict[str, Any]:
    jobs = [job for job in queue["jobs"] if job.get("job_id") == job_id]
    if len(jobs) != 1:
        raise ValueError(f"job_id must resolve exactly once: {job_id}")
    return jobs[0]


def job_id_for(task_id: str, document: dict[str, Any], spec_sha256: str) -> str:
    binding = {
        "task_id": task_id,
        "paper_id": document["paper_id"],
        "source_sha256": document["source_sha256"],
        "parsed_text_sha256": document["parsed_text_sha256"],
        "sections_sha256": str(document.get("sections_sha256") or ""),
        "spec_sha256": spec_sha256,
    }
    return "extract-" + sha256_json(binding)[:24]


def document_identity(job: dict[str, Any]) -> dict[str, Any]:
    """Return the cross-host identity binding without machine-specific paths."""
    return {
        "batch_id": str(job.get("batch_id") or ""),
        "item_id": str(job.get("item_id") or ""),
        "paper_id": str(job.get("paper_id") or ""),
        "source_sha256": str(job.get("source_sha256") or ""),
        "parsed_text_sha256": str(job.get("parsed_text_sha256") or ""),
        "sections_sha256": str(job.get("sections_sha256") or ""),
        "sections_count": int(job.get("sections_count") or 0),
        "sections_schema_version": str(job.get("sections_schema_version") or ""),
        "parser": str(job.get("parser") or ""),
        "parser_version": str(job.get("parser_version") or ""),
    }


def attempt_id_for(job_id: str, attempt_count: int, run_id: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{32}", run_id):
        raise ValueError("run_id must be 32 lowercase hexadecimal characters")
    if isinstance(attempt_count, bool) or not isinstance(attempt_count, int) or attempt_count < 1:
        raise ValueError("attempt_count must be a positive integer")
    value = f"{job_id}-attempt-{attempt_count:04d}-{run_id}"
    if not ATTEMPT_ID_RE.fullmatch(value):
        raise ValueError("generated attempt_id is not path-safe")
    return value


def record_id_for(paper_id: str, fields: list[dict[str, Any]],
                  identity_fields: list[str] | None = None) -> str:
    paper_key = hashlib.sha256(paper_id.encode("utf-8")).hexdigest()[:12]
    identities = set(identity_fields or [])
    use_identity = bool(identities) and identities <= {
        str(item.get("field_id", "")) for item in fields
    }
    normalized = []
    for item in sorted(fields, key=lambda row: str(row.get("field_id", ""))):
        if use_identity and item.get("field_id") not in identities:
            continue
        normalized.append({
            "field_id": item.get("field_id"),
            "value": item.get("value"),
            "unit": item.get("unit"),
            "value_semantics": item.get("value_semantics"),
            # Declared identity fields define logical observations across chunks.
            # Without such a contract, quotes keep equal-valued observations apart.
            "quotes": [] if use_identity else sorted(
                normalize_for_id(evidence.get("exact_quote", ""))
                for evidence in item.get("evidence", []) if isinstance(evidence, dict)
            ),
        })
    return f"rec-{paper_key}-{sha256_json(normalized)[:16]}"


def validate_value_type(value: Any, data_type: str) -> bool:
    if data_type == "string" or data_type == "category" or data_type == "date":
        return isinstance(value, str) and bool(value.strip())
    if data_type == "number":
        return (isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(float(value)))
    if data_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if data_type == "boolean":
        return isinstance(value, bool)
    return False


def result_path(paths: dict[str, Path], job_id: str) -> Path:
    return paths["extraction_results"] / job_id / "result.json"


def attempt_result_path(paths: dict[str, Path], job_id: str, attempt_id: str) -> Path:
    if not ATTEMPT_ID_RE.fullmatch(attempt_id):
        raise ValueError("attempt_id is not path-safe")
    return paths["extraction_results"] / job_id / "attempts" / attempt_id / "result.json"


def validation_path(paths: dict[str, Path], job_id: str) -> Path:
    return paths["extraction_results"] / job_id / "validation.json"


def acceptance_path(paths: dict[str, Path], job_id: str) -> Path:
    return paths["extraction_results"] / job_id / "ACCEPTANCE.json"


def completion_receipt_path(
    paths: dict[str, Path], job_id: str, attempt_id: str
) -> Path:
    """Return an immutable per-attempt cross-host completion receipt path."""
    if not ATTEMPT_ID_RE.fullmatch(attempt_id):
        raise ValueError("attempt_id is not path-safe")
    return paths["extraction_results"] / job_id / "receipts" / f"{attempt_id}.json"
