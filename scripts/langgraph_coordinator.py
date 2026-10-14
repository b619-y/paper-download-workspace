#!/usr/bin/env python3
"""A thin, durable LangGraph control plane for the search-screen-handoff pilot.

The business state machine remains :mod:`scripts.workflow_status`.  This module
only checkpoints orchestration decisions and immutable artifact references.  It
does not replace any download, processing, or extraction queue.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any, Callable, Iterable

try:  # Python 3.11-compatible TypedDict behavior for LangGraph.
    from typing_extensions import TypedDict
except ImportError:  # pragma: no cover - LangGraph installs typing_extensions
    from typing import TypedDict

try:  # package import in tests; direct script execution from scripts/
    from . import workflow_status
except ImportError:  # pragma: no cover - exercised by direct CLI use
    import workflow_status


STATE_SCHEMA_VERSION = 1
CONTROL_PLANE_VERSION = "paper-download-langgraph-thin-v1"
DEFAULT_CHECKPOINT = "runtime/orchestration/langgraph.sqlite3"
DEPENDENCY_HINT = (
    "Install the optional control-plane dependencies with: "
    "python3 -m pip install 'langgraph>=1.2,<2' "
    "'langgraph-checkpoint-sqlite>=3.1,<4'"
)
DECISION_WHITELIST = frozenset({"approve", "retry", "stop", "completed", "reinspect"})
PARTITION_BINDING_VERSION = "search-partition-binding-v1"
PARTITION_STATUSES = frozenset({
    "complete", "incomplete", "blocked_missing_credentials", "failed",
})
PARTITION_BINDING_FIELDS = frozenset({
    "version", "source", "config_sha256", "max_records", "manifest_sha256",
    "source_tree_sha256", "records_sha256", "native_query_sha256",
})
MAX_SEARCH_SOURCES = 64

REPAIR_STAGES = frozenset({
    "NOT_STARTED",
    "TASK_STATE_ERROR",
    "AMBIGUOUS",
    "STAGE_MINUS_1_TASK_BRIEF",
    "STAGE_MINUS_1_QUERY_DESIGN",
    "NO_RESULTS",
})
PILOT_STAGES = frozenset({
    *REPAIR_STAGES,
    "STAGE_0_API_SEARCH",
    "STAGE_0_SEARCH_INCOMPLETE",
    "STAGE_1_TITLE_ABSTRACT_SCREENING",
    "STAGE_1_HANDOFF_TO_DOWNLOAD",
})

PATH_STATE_FIELDS = (
    "task_root",
    "task_brief_path",
    "query_config_path",
    "search_manifest_path",
    "screening_path",
    "partition_work_root",
    "search_output_dir",
)
STATE_FIELDS = frozenset({
    "schema_version",
    "control_plane_version",
    "task_id",
    "thread_id",
    "orchestration_id",
    "stage",
    "reason",
    "next_action",
    "status_sha256",
    "task_root",
    "task_brief_path",
    "task_brief_sha256",
    "query_config_path",
    "query_config_sha256",
    "search_manifest_path",
    "search_manifest_sha256",
    "screening_path",
    "screening_sha256",
    "planned_sources",
    "partition_source",
    "search_attempt",
    "partition_work_root",
    "search_output_dir",
    "partition_results",
    "published_search",
    "handoff_result",
    "approval_decisions",
    "search_authorization_sha256",
    "handoff_authorization_sha256",
    "last_decision",
    "control_status",
})

PARTITION_RESULT_FIELDS = frozenset({
    "source",
    "status",
    "complete",
    "partition_dir",
    "config_sha256",
    "manifest_path",
    "manifest_sha256",
    "records_path",
    "records_sha256",
    "source_tree_sha256",
    "query_id",
    "fetched_count",
    "pages",
    "max_records",
    "binding",
    "orchestration_id",
    "search_attempt",
})
PUBLISHED_RESULT_FIELDS = frozenset({
    "output_dir",
    "reused",
    "complete",
    "raw_record_count",
    "deduplicated_record_count",
    "abstract_present_count",
    "manifest_path",
    "manifest_sha256",
    "source_status",
    "config_sha256",
    "orchestration_id",
    "search_attempt",
})


class CoordinatorError(RuntimeError):
    """A safe, user-facing control-plane error."""


class MissingLangGraph(CoordinatorError):
    """Raised only by start/resume when optional LangGraph packages are absent."""


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _is_sha256(value: Any, *, allow_empty: bool = False) -> bool:
    text = str(value or "")
    if allow_empty and not text:
        return True
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _non_negative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _relative_path(root: Path, path: Path) -> str:
    root = root.resolve()
    resolved = path.expanduser().resolve()
    try:
        return str(resolved.relative_to(root))
    except ValueError as exc:
        raise CoordinatorError(f"artifact path escapes the workspace: {resolved}") from exc


def resolve_workspace_ref(root: Path, value: str, *, must_exist: bool = False) -> Path:
    """Resolve a checkpointed relative path while enforcing workspace confinement."""
    text = str(value or "").strip()
    if not text:
        raise CoordinatorError("an expected workspace-relative artifact path is empty")
    raw = Path(text).expanduser()
    if raw.is_absolute():
        raise CoordinatorError(f"checkpoint artifact path must be relative: {text}")
    root = root.expanduser().resolve()
    candidate = (root / raw).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise CoordinatorError(f"checkpoint artifact path escapes the workspace: {text}") from exc
    if must_exist and not candidate.exists():
        raise CoordinatorError(f"checkpoint artifact is missing: {text}")
    return candidate


def _secure_workspace_path(
    root: Path,
    path: Path,
    *,
    must_exist: bool = False,
) -> Path:
    """Resolve a workspace path without following any workspace-owned symlink.

    This stricter helper is used for control-plane files and handoff evidence.
    It deliberately checks every existing lexical component before callers
    create or read anything, so rejecting a link never creates data through it.
    """
    lexical_root = root.expanduser().absolute()
    root = lexical_root.resolve()
    raw = path.expanduser()
    lexical_candidate = raw if raw.is_absolute() else lexical_root / raw
    try:
        # macOS exposes /var through /private/var.  Derive the relative path
        # before resolving that system-owned alias, then inspect every
        # workspace-owned component below the canonical root ourselves.
        relative = lexical_candidate.relative_to(lexical_root)
    except ValueError:
        try:
            relative = lexical_candidate.relative_to(root)
        except ValueError as exc:
            raise CoordinatorError(
                f"path escapes the workspace: {lexical_candidate}"
            ) from exc
    candidate = root / relative
    if any(part in {"", ".", ".."} for part in relative.parts):
        raise CoordinatorError(f"path contains an unsafe component: {candidate}")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise CoordinatorError(f"workspace control path must not contain symlinks: {current}")
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise CoordinatorError(f"path escapes the workspace: {candidate}") from exc
    if must_exist and not resolved.exists():
        raise CoordinatorError(f"required workspace path is missing: {relative}")
    return resolved


def resolve_checkpoint_path(root: Path, value: str | os.PathLike[str] = DEFAULT_CHECKPOINT) -> Path:
    """Resolve a checkpoint DB below the isolated orchestration directory."""
    root = root.expanduser().resolve()
    raw = Path(value).expanduser()
    lexical = raw if raw.is_absolute() else root / raw
    try:
        candidate = _secure_workspace_path(root, lexical)
    except CoordinatorError as exc:
        raise CoordinatorError(
            "checkpoint DB must remain inside workspace runtime/orchestration/"
        ) from exc
    orchestration = _secure_workspace_path(root, root / "runtime/orchestration")
    try:
        relative = candidate.relative_to(orchestration)
    except ValueError as exc:
        raise CoordinatorError(
            "checkpoint DB must remain inside workspace runtime/orchestration/"
        ) from exc
    if not relative.parts or candidate.suffix != ".sqlite3":
        raise CoordinatorError(
            "checkpoint DB must be a .sqlite3 file inside runtime/orchestration/"
        )
    return candidate


def _secure_runtime_directory(root: Path, relative: str) -> Path:
    relative_path = Path(relative)
    if relative_path.is_absolute() or any(
        part in {"", ".", ".."} for part in relative_path.parts
    ):
        raise CoordinatorError("runtime directory must be a safe relative path")
    directory = root.expanduser().resolve() / "runtime" / relative_path
    # Validate before mkdir: a rejected link must not receive external writes.
    resolved = _secure_workspace_path(root, directory)
    resolved.mkdir(parents=True, exist_ok=True)
    return _secure_workspace_path(root, directory, must_exist=True)


@contextmanager
def _locked_file(path: Path, *, nonblocking: bool = False) -> Any:
    """Lock one regular file without ever following a final symlink."""
    if path.is_symlink():
        raise CoordinatorError(f"lock file must not be a symlink: {path.name}")
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise CoordinatorError(
            f"cannot safely open orchestration lock {path.name} ({exc.__class__.__name__})"
        ) from exc
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise CoordinatorError(f"orchestration lock is not a regular file: {path.name}")
        operation = fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0)
        try:
            fcntl.flock(descriptor, operation)
        except BlockingIOError as exc:
            raise CoordinatorError("orchestration lock is already held") from exc
        try:
            yield descriptor
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def validate_thread_id(value: str) -> str:
    thread_id = str(value or "")
    if not thread_id or thread_id != thread_id.strip():
        raise CoordinatorError("thread_id must be a non-empty trimmed string")
    if any(ord(character) < 32 or ord(character) == 127 for character in thread_id):
        raise CoordinatorError("thread_id must not contain control characters")
    if len(thread_id.encode("utf-8")) >= 255:
        raise CoordinatorError("thread_id must be shorter than 255 UTF-8 bytes")
    return thread_id


def default_thread_id(task_id: str) -> str:
    """Map a task ID to a stable checkpoint thread ID below the 255-byte limit."""
    candidate = str(task_id or "").strip()
    try:
        return validate_thread_id(candidate)
    except CoordinatorError:
        if not candidate:
            raise CoordinatorError("cannot derive thread_id without a selected task_id")
        return "task-" + hashlib.sha256(candidate.encode("utf-8")).hexdigest()


def orchestration_id_for(task_id: str, thread_id: str) -> str:
    value = f"{CONTROL_PLANE_VERSION}\0{task_id}\0{thread_id}"
    return "lg-" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:20]


def normalize_resume_decision(value: Any, allowed: Iterable[str]) -> str:
    if isinstance(value, str):
        action = value.strip().casefold()
    elif isinstance(value, dict):
        action = str(value.get("action") or "").strip().casefold()
    else:
        action = ""
    permitted = frozenset(allowed)
    if action not in DECISION_WHITELIST or action not in permitted:
        expected = ", ".join(sorted(permitted))
        raise CoordinatorError(f"resume decision must be one of: {expected}")
    return action


def _validate_binding(binding: Any) -> dict[str, Any]:
    if not isinstance(binding, dict):
        raise CoordinatorError("partition binding must be a JSON object")
    if set(binding) != PARTITION_BINDING_FIELDS:
        raise CoordinatorError("partition binding fields do not match the search adapter contract")
    if binding.get("version") != PARTITION_BINDING_VERSION:
        raise CoordinatorError("partition binding version is unsupported")
    source = binding.get("source")
    if not isinstance(source, str) or not source or len(source) > 100:
        raise CoordinatorError("partition binding source is invalid")
    if not _non_negative_int(binding.get("max_records")):
        raise CoordinatorError("partition binding max_records must be a non-negative integer")
    for field in (
        "config_sha256", "manifest_sha256", "source_tree_sha256", "native_query_sha256",
    ):
        if not _is_sha256(binding.get(field)):
            raise CoordinatorError(f"partition binding {field} is invalid")
    if not _is_sha256(binding.get("records_sha256"), allow_empty=True):
        raise CoordinatorError("partition binding records_sha256 is invalid")
    return dict(binding)


def _validate_partition_scalars(raw: dict[str, Any]) -> dict[str, Any]:
    """Validate the complete, reference-only partition adapter contract."""
    if not isinstance(raw, dict):
        raise CoordinatorError("search partition returned a non-object result")
    source = raw.get("source")
    if not isinstance(source, str) or not source or len(source) > 100:
        raise CoordinatorError("partition source is invalid")
    status = raw.get("status")
    if not isinstance(status, str) or status not in PARTITION_STATUSES:
        raise CoordinatorError("partition status is invalid")
    complete = raw.get("complete")
    if not isinstance(complete, bool) or complete is not (status == "complete"):
        raise CoordinatorError("partition complete flag is inconsistent")
    if raw.get("manifest_path") != "run_manifest.json":
        raise CoordinatorError("partition manifest_path is invalid")
    if raw.get("records_path") != f"{source}/records.csv":
        raise CoordinatorError("partition records_path is invalid")
    query_id = raw.get("query_id")
    if not isinstance(query_id, str) or not query_id or len(query_id) > 512:
        raise CoordinatorError("partition query_id is invalid")
    for field in ("fetched_count", "pages", "max_records"):
        if not _non_negative_int(raw.get(field)):
            raise CoordinatorError(f"partition {field} must be a non-negative integer")
    for field in ("config_sha256", "manifest_sha256", "source_tree_sha256"):
        if not _is_sha256(raw.get(field)):
            raise CoordinatorError(f"partition {field} is invalid")
    if not _is_sha256(raw.get("records_sha256"), allow_empty=True):
        raise CoordinatorError("partition records_sha256 is invalid")
    binding = _validate_binding(raw.get("binding"))
    expected = {
        "source": source,
        "config_sha256": raw["config_sha256"],
        "manifest_sha256": raw["manifest_sha256"],
        "records_sha256": raw["records_sha256"],
        "source_tree_sha256": raw["source_tree_sha256"],
        "max_records": raw["max_records"],
    }
    if any(binding.get(field) != value for field, value in expected.items()):
        raise CoordinatorError("partition binding does not match its outer reference")
    return binding


def partition_result_reference(root: Path, raw: dict[str, Any], *,
                               orchestration_id: str, search_attempt: int) -> dict[str, Any]:
    """Reduce a search result to the reference-only form allowed in checkpoints."""
    binding = _validate_partition_scalars(raw)
    missing = [
        field for field in ("source", "status", "partition_dir", "config_sha256")
        if not str(raw.get(field) or "").strip()
    ]
    if missing:
        raise CoordinatorError("search partition result lacks: " + ", ".join(missing))
    partition_dir = Path(str(raw["partition_dir"])).expanduser()
    if not partition_dir.is_absolute():
        partition_dir = root / partition_dir
    if not isinstance(orchestration_id, str) or not orchestration_id or len(orchestration_id) > 100:
        raise CoordinatorError("partition orchestration_id is invalid")
    if not _non_negative_int(search_attempt) or search_attempt == 0:
        raise CoordinatorError("partition search_attempt must be a positive integer")
    partition_dir = _secure_workspace_path(root, partition_dir, must_exist=True)
    if not partition_dir.is_dir():
        raise CoordinatorError("search partition directory is not a directory")
    reference: dict[str, Any] = {}
    for key in PARTITION_RESULT_FIELDS:
        if key in raw and key not in {"partition_dir", "binding"}:
            reference[key] = raw[key]
    reference.update({
        "partition_dir": _relative_path(root, partition_dir),
        "binding": binding,
        "orchestration_id": orchestration_id,
        "search_attempt": search_attempt,
    })
    # A JSON round trip rejects bytes, Path objects, and arbitrary model output.
    try:
        return json.loads(json.dumps(reference, ensure_ascii=False))
    except (TypeError, ValueError) as exc:
        raise CoordinatorError("search partition reference is not JSON serializable") from exc


def materialize_partition_reference(root: Path, reference: dict[str, Any]) -> dict[str, Any]:
    materialized = dict(reference)
    materialized.pop("orchestration_id", None)
    materialized.pop("search_attempt", None)
    materialized["partition_dir"] = str(
        resolve_workspace_ref(root, str(reference.get("partition_dir") or ""), must_exist=True)
    )
    return materialized


def merge_partition_results(
    left: list[dict[str, Any]] | None,
    right: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Deterministically reduce parallel partition references without silent conflicts."""
    merged: dict[tuple[str, int, str], dict[str, Any]] = {}
    for item in list(left or []) + list(right or []):
        if not isinstance(item, dict):
            raise CoordinatorError("partition result reducer received a non-object item")
        key = (
            str(item.get("orchestration_id") or ""),
            int(item.get("search_attempt") or 0),
            str(item.get("source") or ""),
        )
        if not all((key[0], key[1] > 0, key[2])):
            raise CoordinatorError("partition result reducer received an incomplete identity")
        previous = merged.get(key)
        if previous is not None and _canonical_sha256(previous) != _canonical_sha256(item):
            raise CoordinatorError(
                f"conflicting partition results for attempt={key[1]} source={key[2]}"
            )
        merged[key] = dict(item)
    return [merged[key] for key in sorted(merged)]


class CoordinatorState(TypedDict, total=False):
    schema_version: int
    control_plane_version: str
    task_id: str
    thread_id: str
    orchestration_id: str
    stage: str
    reason: str
    next_action: str
    status_sha256: str
    task_root: str
    task_brief_path: str
    task_brief_sha256: str
    query_config_path: str
    query_config_sha256: str
    search_manifest_path: str
    search_manifest_sha256: str
    screening_path: str
    screening_sha256: str
    planned_sources: list[str]
    partition_source: str
    search_attempt: int
    partition_work_root: str
    search_output_dir: str
    partition_results: Annotated[list[dict[str, Any]], merge_partition_results]
    published_search: dict[str, Any]
    handoff_result: dict[str, Any]
    approval_decisions: dict[str, str]
    search_authorization_sha256: str
    handoff_authorization_sha256: str
    last_decision: str
    control_status: str


def _assert_state_version(state: dict[str, Any]) -> None:
    if state.get("schema_version") != STATE_SCHEMA_VERSION:
        raise CoordinatorError(
            f"checkpoint schema_version={state.get('schema_version')!r} is unsupported; "
            f"expected {STATE_SCHEMA_VERSION}"
        )
    if state.get("control_plane_version") != CONTROL_PLANE_VERSION:
        raise CoordinatorError(
            f"checkpoint control_plane_version={state.get('control_plane_version')!r} "
            f"is unsupported; expected {CONTROL_PLANE_VERSION}"
        )


def _expected_search_locations(
    root: Path,
    *,
    task_root: str,
    orchestration_id: str,
    search_attempt: int,
) -> tuple[Path, Path]:
    if not _non_negative_int(search_attempt) or search_attempt == 0:
        raise CoordinatorError("search attempt must be a positive integer")
    task_directory = _secure_workspace_path(
        root, resolve_workspace_ref(root, task_root, must_exist=True), must_exist=True
    )
    partition_root = (
        root / "runtime/orchestration/search" / orchestration_id
        / f"attempt-{search_attempt:04d}" / "partitions"
    )
    output_dir = task_directory / "search" / (
        f"{orchestration_id}-attempt-{search_attempt:04d}"
    )
    return (
        _secure_workspace_path(root, partition_root),
        _secure_workspace_path(root, output_dir),
    )


def validate_checkpoint_state(root: Path, state: dict[str, Any], *,
                              task_id: str, thread_id: str) -> None:
    if not isinstance(state, dict) or not state:
        raise CoordinatorError("checkpoint has no coordinator state")
    _assert_state_version(state)
    unexpected = sorted(set(state) - STATE_FIELDS)
    if unexpected:
        raise CoordinatorError("checkpoint contains unsupported state fields: " + ", ".join(unexpected))
    if state.get("task_id") != task_id or state.get("thread_id") != thread_id:
        raise CoordinatorError("checkpoint task/thread binding does not match this resume request")
    validate_thread_id(str(state.get("thread_id") or ""))
    for field, limit in (
        ("task_id", 1024), ("orchestration_id", 100), ("stage", 100),
        ("reason", 4096), ("next_action", 4096), ("last_decision", 100),
        ("control_status", 100), ("partition_source", 100),
    ):
        value = state.get(field, "")
        if not isinstance(value, str) or len(value) > limit:
            raise CoordinatorError(f"checkpoint {field} must be a bounded string")
    for field in PATH_STATE_FIELDS:
        raw_value = state.get(field, "")
        if not isinstance(raw_value, str) or len(raw_value) > 2048:
            raise CoordinatorError(f"checkpoint {field} must be a bounded string path")
        value = raw_value.strip()
        if value:
            resolve_workspace_ref(root, value)
    for field in (
        "status_sha256", "task_brief_sha256", "query_config_sha256",
        "search_manifest_sha256", "screening_sha256",
        "search_authorization_sha256", "handoff_authorization_sha256",
    ):
        if not _is_sha256(state.get(field), allow_empty=True):
            raise CoordinatorError(f"checkpoint {field} must be an empty value or SHA-256")
    approvals = state.get("approval_decisions", {})
    if not isinstance(approvals, dict) or len(approvals) > 256 or any(
        not isinstance(key, str) or not isinstance(value, str)
        or not key or len(key) > 100 or value not in DECISION_WHITELIST
        for key, value in approvals.items()
    ):
        raise CoordinatorError("checkpoint approval_decisions violates the state contract")
    if str(state.get("orchestration_id") or "") != orchestration_id_for(task_id, thread_id):
        raise CoordinatorError("checkpoint orchestration_id does not match its task/thread binding")
    attempt = state.get("search_attempt", 0)
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 0:
        raise CoordinatorError("checkpoint search_attempt must be a non-negative integer")
    planned = state.get("planned_sources", [])
    if not isinstance(planned, list) or any(
        not isinstance(source, str) or not source.strip() or len(source) > 100
        for source in planned
    ) or len(planned) > MAX_SEARCH_SOURCES or len(set(planned)) != len(planned):
        raise CoordinatorError("checkpoint planned_sources must be a unique string list")
    partition_results = state.get("partition_results", [])
    if not isinstance(partition_results, list) or len(partition_results) > (
        MAX_SEARCH_SOURCES * max(1, attempt)
    ):
        raise CoordinatorError("checkpoint partition_results must be a bounded list")
    for result in partition_results:
        if not isinstance(result, dict) or set(result) != PARTITION_RESULT_FIELDS:
            raise CoordinatorError("checkpoint contains an invalid partition result reference")
        _validate_partition_scalars(result)
        result_orchestration = result.get("orchestration_id")
        result_attempt = result.get("search_attempt")
        if result_orchestration != state.get("orchestration_id"):
            raise CoordinatorError("checkpoint partition orchestration binding is invalid")
        if not _non_negative_int(result_attempt) or not 1 <= result_attempt <= attempt:
            raise CoordinatorError("checkpoint partition attempt binding is invalid")
        partition_path_value = result.get("partition_dir")
        if not isinstance(partition_path_value, str) or len(partition_path_value) > 2048:
            raise CoordinatorError("checkpoint partition_dir is invalid")
        partition_path = _secure_workspace_path(
            root, resolve_workspace_ref(root, partition_path_value)
        )
        expected_partition_root, _ = _expected_search_locations(
            root,
            task_root=str(state.get("task_root") or ""),
            orchestration_id=str(state.get("orchestration_id") or ""),
            search_attempt=result_attempt,
        )
        try:
            partition_path.relative_to(expected_partition_root)
        except ValueError as exc:
            raise CoordinatorError("checkpoint partition_dir is outside its attempt root") from exc
        if str(result.get("source") or "") not in planned:
            raise CoordinatorError("checkpoint partition source is not planned")
        if result.get("config_sha256") != state.get("query_config_sha256"):
            raise CoordinatorError("checkpoint partition config binding is invalid")
    if attempt == 0:
        if state.get("partition_work_root") or state.get("search_output_dir"):
            raise CoordinatorError("unstarted checkpoint contains search attempt paths")
    else:
        expected_partition_root, expected_output = _expected_search_locations(
            root,
            task_root=str(state.get("task_root") or ""),
            orchestration_id=str(state.get("orchestration_id") or ""),
            search_attempt=attempt,
        )
        if resolve_workspace_ref(root, str(state.get("partition_work_root") or "")) != expected_partition_root:
            raise CoordinatorError("checkpoint partition_work_root is not deterministic")
        if resolve_workspace_ref(root, str(state.get("search_output_dir") or "")) != expected_output:
            raise CoordinatorError("checkpoint search_output_dir is not deterministic")
    published = state.get("published_search", {})
    if not isinstance(published, dict) or (published and set(published) != PUBLISHED_RESULT_FIELDS):
        raise CoordinatorError("checkpoint contains an invalid published-search reference")
    if published.get("output_dir"):
        resolve_workspace_ref(root, str(published["output_dir"]))
    if published:
        if published.get("manifest_path") != "run_manifest.json":
            raise CoordinatorError("checkpoint published manifest_path is invalid")
        if not _is_sha256(published.get("manifest_sha256")):
            raise CoordinatorError("checkpoint published manifest_sha256 is invalid")
        if not isinstance(published.get("complete"), bool) or not isinstance(
            published.get("reused"), bool
        ):
            raise CoordinatorError("checkpoint published flags must be boolean")
        for field in (
            "raw_record_count", "deduplicated_record_count", "abstract_present_count",
            "search_attempt",
        ):
            if field in published and not _non_negative_int(published[field]):
                raise CoordinatorError(f"checkpoint published {field} must be non-negative")
        if published.get("orchestration_id") != state.get("orchestration_id"):
            raise CoordinatorError("checkpoint published orchestration binding is invalid")
        if published.get("search_attempt") != attempt:
            raise CoordinatorError("checkpoint published attempt binding is invalid")
        if published.get("config_sha256") != state.get("query_config_sha256"):
            raise CoordinatorError("checkpoint published config binding is invalid")
        if resolve_workspace_ref(root, str(published.get("output_dir") or "")) != expected_output:
            raise CoordinatorError("checkpoint published output_dir is not deterministic")
        statuses = published.get("source_status")
        if (
            not isinstance(statuses, dict)
            or len(statuses) > 64
            or any(
                not isinstance(source, str)
                or not source
                or len(source) > 100
                or not isinstance(value, str)
                or not value
                or len(value) > 100
                for source, value in statuses.items()
            )
            or set(statuses) != set(planned)
            or any(value not in PARTITION_STATUSES for value in statuses.values())
        ):
            raise CoordinatorError("checkpoint published source_status is invalid")
    handoff = state.get("handoff_result", {})
    allowed_handoff = {
        "batch_id", "batch_path", "status", "selected_count", "screening_sha256", "reused",
    }
    if not isinstance(handoff, dict) or (handoff and set(handoff) != allowed_handoff):
        raise CoordinatorError("checkpoint contains an invalid handoff reference")
    if handoff.get("batch_path"):
        resolve_workspace_ref(root, str(handoff["batch_path"]))
    if handoff:
        batch_id = _valid_batch_id(str(handoff.get("batch_id") or ""))
        if handoff.get("batch_path") != f"02_batches/{batch_id}":
            raise CoordinatorError("checkpoint handoff batch path is invalid")
        if handoff.get("status") not in workflow_status.BATCH_STATUSES:
            raise CoordinatorError("checkpoint handoff status is invalid")
        count = handoff.get("selected_count")
        if count is not None and not _non_negative_int(count):
            raise CoordinatorError("checkpoint handoff selected_count is invalid")
        if not _is_sha256(handoff.get("screening_sha256")):
            raise CoordinatorError("checkpoint handoff screening_sha256 is invalid")
        if "reused" in handoff and not isinstance(handoff["reused"], bool):
            raise CoordinatorError("checkpoint handoff reused must be boolean")


def _safe_file_ref(root: Path, value: Any) -> tuple[str, str]:
    text = str(value or "").strip()
    if not text:
        return "", ""
    raw = Path(text).expanduser()
    path = raw if raw.is_absolute() else root / raw
    resolved = path.resolve()
    relative = _relative_path(root, resolved)
    return relative, _file_sha256(resolved) if resolved.is_file() else ""


def _handoff_binding(state: dict[str, Any]) -> str:
    return _canonical_sha256({
        "task_id": str(state.get("task_id") or ""),
        "task_root": str(state.get("task_root") or ""),
        "task_brief_sha256": str(state.get("task_brief_sha256") or ""),
        "search_manifest_sha256": str(state.get("search_manifest_sha256") or ""),
        "screening_sha256": str(state.get("screening_sha256") or ""),
    })


def _search_binding(state: dict[str, Any]) -> str:
    return _canonical_sha256({
        "query_config_path": str(state.get("query_config_path") or ""),
        "query_config_sha256": str(state.get("query_config_sha256") or ""),
        "planned_sources": list(state.get("planned_sources") or []),
    })


def _valid_batch_id(batch_id: str) -> str:
    value = str(batch_id or "").strip()
    if (
        not value
        or value in {".", ".."}
        or Path(value).name != value
        or any(ord(character) < 32 for character in value)
    ):
        raise CoordinatorError("handoff returned an unsafe batch_id")
    return value


def record_active_task_batch(root: Path, *, task_id: str, task_root: str,
                             batch_id: str) -> bool:
    """Atomically and idempotently append one handoff batch to ACTIVE_TASK."""
    batch_id = _valid_batch_id(batch_id)
    expected_task_root = _secure_workspace_path(
        root, resolve_workspace_ref(root, task_root, must_exist=True), must_exist=True
    )
    batch_dir = _secure_workspace_path(
        root, root / "02_batches" / batch_id, must_exist=True
    )
    if not batch_dir.is_dir():
        raise CoordinatorError(f"handoff batch directory is missing: {batch_id}")
    runtime = _secure_runtime_directory(root, "")
    pointer_path = runtime / "ACTIVE_TASK.json"
    lock_path = runtime / ".active_task.lock"
    with _locked_file(lock_path):
        if pointer_path.is_symlink():
            raise CoordinatorError("ACTIVE_TASK.json must not be a symlink")
        if pointer_path.is_file():
            try:
                pointer = json.loads(pointer_path.read_text(encoding="utf-8-sig"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise CoordinatorError("ACTIVE_TASK.json is unreadable during handoff") from exc
            if not isinstance(pointer, dict):
                raise CoordinatorError("ACTIVE_TASK.json must be a JSON object")
            if str(pointer.get("task_id") or "").strip() != task_id:
                raise CoordinatorError(
                    "ACTIVE_TASK changed to another task during handoff; refusing to overwrite it"
                )
            pointer_root_text = str(pointer.get("task_root") or "").strip()
            if not pointer_root_text:
                raise CoordinatorError("ACTIVE_TASK.task_root is missing during handoff")
            raw_pointer_root = Path(pointer_root_text).expanduser()
            pointer_root = (
                raw_pointer_root if raw_pointer_root.is_absolute()
                else root / raw_pointer_root
            ).resolve()
            if pointer_root != expected_task_root:
                raise CoordinatorError("ACTIVE_TASK.task_root changed during handoff")
            batches = pointer.get("batch_ids")
            if not isinstance(batches, list) or any(
                not isinstance(item, str) or not item.strip() for item in batches
            ):
                raise CoordinatorError("ACTIVE_TASK.batch_ids is invalid during handoff")
            if len(set(batches)) != len(batches):
                raise CoordinatorError("ACTIVE_TASK.batch_ids already contains duplicates")
        else:
            pointer = {
                "task_id": task_id,
                "task_root": _relative_path(root, expected_task_root),
                "batch_ids": [],
            }
            batches = pointer["batch_ids"]
        if batch_id in batches:
            return False
        batches.append(batch_id)
        pointer["batch_ids"] = batches
        pointer["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        temporary_name = ""
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=runtime,
                prefix=".ACTIVE_TASK.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary_name = temporary.name
                json.dump(pointer, temporary, ensure_ascii=False, indent=2)
                temporary.write("\n")
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_name, pointer_path)
            directory_fd = os.open(runtime, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary_name and Path(temporary_name).exists():
                Path(temporary_name).unlink()
        return True


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            json.dump(value, temporary, ensure_ascii=False, indent=2)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary_name and Path(temporary_name).exists():
            Path(temporary_name).unlink()


@contextmanager
def task_execution_lock(root: Path, task_id: str) -> Any:
    """Prevent concurrent coordinator runs for the same task, across thread IDs."""
    lock_dir = _secure_runtime_directory(root, "orchestration/locks")
    identity = hashlib.sha256(task_id.encode("utf-8")).hexdigest()
    lock_path = lock_dir / f"task-{identity}.lock"
    try:
        with _locked_file(lock_path, nonblocking=True):
            yield
    except CoordinatorError as exc:
        if str(exc) == "orchestration lock is already held":
            raise CoordinatorError(
                f"another coordinator is already running task_id={task_id!r}"
            ) from exc
        raise


@contextmanager
def checkpoint_thread_lock(
    root: Path,
    checkpoint_path: Path,
    thread_id: str,
) -> Any:
    """Serialize one SQLite/thread identity even when callers name different tasks."""
    lock_dir = _secure_runtime_directory(root, "orchestration/locks")
    identity = _canonical_sha256({
        "checkpoint": _relative_path(root, checkpoint_path),
        "thread_id": thread_id,
    })
    lock_path = lock_dir / f"checkpoint-thread-{identity}.lock"
    try:
        with _locked_file(lock_path, nonblocking=True):
            yield
    except CoordinatorError as exc:
        if str(exc) == "orchestration lock is already held":
            raise CoordinatorError(
                f"another coordinator is already using thread_id={thread_id!r}"
            ) from exc
        raise


@contextmanager
def handoff_execution_lock(root: Path, binding: str) -> Any:
    directory = _secure_runtime_directory(root, "orchestration/handoffs")
    # Batch creation uses random IDs.  A global coordinator-side handoff lock
    # makes crash reconciliation distinguishable from another task's enqueue.
    lock_path = directory / "global.lock"
    with _locked_file(lock_path):
        intent_path = directory / f"{binding}.json"
        if intent_path.is_symlink():
            raise CoordinatorError("handoff intent must not be a symlink")
        yield intent_path


def _batch_directory_ids(root: Path) -> list[str]:
    directory = _secure_workspace_path(root, root / "02_batches")
    if not directory.is_dir():
        return []
    result: list[str] = []
    for path in directory.iterdir():
        if path.is_symlink():
            raise CoordinatorError(f"batch directory must not be a symlink: {path.name}")
        if path.is_dir():
            result.append(_valid_batch_id(path.name))
    return sorted(result)


def _new_handoff_intent(root: Path, state: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    screening = _secure_workspace_path(
        root,
        resolve_workspace_ref(root, str(update.get("screening_path") or ""), must_exist=True),
        must_exist=True,
    )
    return {
        "version": "paper-download-handoff-intent-v1",
        "binding": _handoff_binding(update),
        "task_id": str(state.get("task_id") or ""),
        "task_root": str(update.get("task_root") or ""),
        "task_brief_sha256": str(update.get("task_brief_sha256") or ""),
        "search_manifest_sha256": str(update.get("search_manifest_sha256") or ""),
        "screening_path": str(update.get("screening_path") or ""),
        "screening_sha256": str(update.get("screening_sha256") or ""),
        "download_manifest_path": _relative_path(root, screening.with_name("download_manifest.csv")),
        "batch_ids_before": _batch_directory_ids(root),
        "status": "started",
        "batch_id": "",
    }


def _load_handoff_intent(path: Path, expected: dict[str, Any]) -> dict[str, Any]:
    if not path.is_file():
        _atomic_write_json(path, expected)
        return dict(expected)
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CoordinatorError("handoff intent is unreadable; refusing to enqueue again") from exc
    if not isinstance(value, dict):
        raise CoordinatorError("handoff intent is not a JSON object")
    immutable = (
        "version", "binding", "task_id", "task_root", "task_brief_sha256",
        "search_manifest_sha256", "screening_path", "screening_sha256",
        "download_manifest_path",
    )
    if any(value.get(field) != expected.get(field) for field in immutable):
        raise CoordinatorError("handoff intent binding changed; refusing to enqueue again")
    if value.get("status") not in {"started", "completed"}:
        raise CoordinatorError("handoff intent has an unsupported status")
    return value


def _batch_enqueue_matches_intent(root: Path, batch_id: str,
                                  intent: dict[str, Any]) -> bool:
    batch_id = _valid_batch_id(batch_id)
    batch = _secure_workspace_path(root, root / "02_batches" / batch_id)
    if not batch.is_dir():
        return False
    request_path = _secure_workspace_path(root, batch / "REQUEST.json")
    source_copy = _secure_workspace_path(root, batch / "inputs/source_manifest.csv")
    expected_manifest = _secure_workspace_path(
        root,
        resolve_workspace_ref(root, str(intent.get("download_manifest_path") or "")),
    )
    if any(not path.is_file() for path in (request_path, source_copy)):
        return False
    try:
        request = json.loads(request_path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    raw_source = Path(str(request.get("source_manifest") or "")).expanduser()
    request_source = raw_source if raw_source.is_absolute() else root / raw_source
    if request_source.resolve() != expected_manifest.resolve() or not expected_manifest.is_file():
        return False
    if _file_sha256(source_copy) != _file_sha256(expected_manifest):
        return False
    status = workflow_status.read_json(batch / "STATUS.json")
    index, index_issues = workflow_status.batch_index(root)
    return (
        not index_issues
        and str(status.get("batch_id") or "") == batch_id
        and batch_id in index
        and str(index[batch_id].get("status") or "") == str(status.get("status") or "")
    )


def _batch_request_targets_intent(root: Path, batch_id: str,
                                  intent: dict[str, Any]) -> bool | None:
    """True=ours, False=known unrelated, None=too partial to classify safely."""
    batch_id = _valid_batch_id(batch_id)
    batch = _secure_workspace_path(root, root / "02_batches" / batch_id)
    if not batch.is_dir():
        return None
    request_path = _secure_workspace_path(root, batch / "REQUEST.json")
    if not request_path.is_file():
        return None
    try:
        request = json.loads(request_path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    raw_text = str(request.get("source_manifest") or "").strip()
    if not raw_text:
        return None
    raw_source = Path(raw_text).expanduser()
    request_source = raw_source if raw_source.is_absolute() else root / raw_source
    expected = _secure_workspace_path(
        root, resolve_workspace_ref(root, str(intent.get("download_manifest_path") or ""))
    )
    return request_source.resolve() == expected.resolve()


def _complete_handoff_evidence(root: Path, batch_id: str,
                               intent: dict[str, Any]) -> bool:
    if not _batch_enqueue_matches_intent(root, batch_id, intent):
        return False
    evidence = _secure_workspace_path(
        root, root / "02_batches" / batch_id / "inputs/search_screening"
    )
    required = (
        _secure_workspace_path(root, evidence / "pre_screening.csv"),
        _secure_workspace_path(root, evidence / "run_manifest.json"),
        _secure_workspace_path(root, evidence / "download_manifest.csv"),
        _secure_workspace_path(root, evidence / "deduplicated_records.csv"),
        _secure_workspace_path(root, evidence / "handoff_summary.json"),
    )
    if any(not path.is_file() for path in required):
        return False
    if (
        _file_sha256(evidence / "pre_screening.csv") != intent["screening_sha256"]
        or _file_sha256(evidence / "run_manifest.json") != intent["search_manifest_sha256"]
        or _file_sha256(evidence / "download_manifest.csv")
        != _file_sha256(_secure_workspace_path(
            root,
            resolve_workspace_ref(root, intent["download_manifest_path"], must_exist=True),
            must_exist=True,
        ))
    ):
        return False
    summary = workflow_status.read_json(evidence / "handoff_summary.json")
    batch = summary.get("batch") if isinstance(summary.get("batch"), dict) else {}
    return str(batch.get("batch_id") or "") == batch_id


def _recover_handoff_batch(root: Path, intent: dict[str, Any]) -> str | None:
    completed_id = str(intent.get("batch_id") or "").strip()
    if intent.get("status") == "completed":
        if completed_id and _complete_handoff_evidence(root, completed_id, intent):
            return completed_id
        raise CoordinatorError("completed handoff intent no longer matches its batch evidence")
    before = set(str(value) for value in intent.get("batch_ids_before", []))
    new_ids = [value for value in _batch_directory_ids(root) if value not in before]
    if not new_ids:
        return None
    classified = {
        batch_id: _batch_request_targets_intent(root, batch_id, intent)
        for batch_id in new_ids
    }
    unknown = [batch_id for batch_id, owner in classified.items() if owner is None]
    related = [batch_id for batch_id, owner in classified.items() if owner is True]
    if unknown:
        raise CoordinatorError(
            "handoff outcome contains unclassifiable partial batches; inspect before retrying: "
            f"{unknown}"
        )
    if len(related) > 1:
        raise CoordinatorError(
            "multiple batches match one handoff intent; repair duplicates manually: "
            f"{related}"
        )
    matching = [
        batch_id for batch_id in related
        if _batch_enqueue_matches_intent(root, batch_id, intent)
    ]
    complete = [
        batch_id for batch_id in matching
        if _complete_handoff_evidence(root, batch_id, intent)
    ]
    if len(complete) == 1 and len(related) == 1:
        return complete[0]
    if not related:
        return None
    raise CoordinatorError(
        "handoff outcome is partially written; inspect this batch before retrying: "
        f"{related}"
    )


def state_update_from_status(root: Path, status: dict[str, Any]) -> dict[str, Any]:
    evidence = status.get("evidence") if isinstance(status.get("evidence"), dict) else {}
    query = evidence.get("query") if isinstance(evidence.get("query"), dict) else {}
    search = evidence.get("search") if isinstance(evidence.get("search"), dict) else {}
    search_paths = search.get("paths") if isinstance(search.get("paths"), dict) else {}
    task_brief_path, task_brief_sha = _safe_file_ref(root, evidence.get("task_brief"))
    config_path, config_sha = _safe_file_ref(root, query.get("config"))
    manifest_path, manifest_sha = _safe_file_ref(root, search.get("manifest"))
    screening_path, screening_sha = _safe_file_ref(root, search_paths.get("pre_screening"))
    planned = query.get("planned_sources")
    update: dict[str, Any] = {
        "stage": str(status.get("stage") or "UNKNOWN"),
        "reason": str(status.get("reason") or ""),
        "next_action": str(status.get("next_action") or ""),
        "status_sha256": _canonical_sha256(status),
        "task_root": str(evidence.get("task_root") or ""),
        "task_brief_path": task_brief_path,
        "task_brief_sha256": task_brief_sha,
        "query_config_path": config_path,
        "query_config_sha256": config_sha,
        "search_manifest_path": manifest_path,
        "search_manifest_sha256": manifest_sha,
        "screening_path": screening_path,
        "screening_sha256": screening_sha,
        "planned_sources": [str(source) for source in planned] if isinstance(planned, list) else [],
    }
    selected_task = str(evidence.get("task_id") or "").strip()
    if selected_task:
        update["task_id"] = selected_task
    return update


def route_for_stage(stage: str) -> str:
    if stage in REPAIR_STAGES:
        return "repair"
    if stage == "STAGE_0_API_SEARCH":
        return "approve_search"
    if stage == "STAGE_0_SEARCH_INCOMPLETE":
        return "search_incomplete"
    if stage == "STAGE_1_TITLE_ABSTRACT_SCREENING":
        return "screening"
    if stage == "STAGE_1_HANDOFF_TO_DOWNLOAD":
        return "approve_handoff"
    return "end"


def expected_interrupt(status: dict[str, Any]) -> dict[str, Any]:
    stage = str(status.get("stage") or "UNKNOWN")
    route = route_for_stage(stage)
    mapping = {
        "repair": ("repair_required", ["reinspect", "stop"]),
        "approve_search": ("search_approval", ["approve", "stop"]),
        "search_incomplete": ("search_incomplete", ["retry", "stop"]),
        "screening": ("screening_required", ["completed", "reinspect", "stop"]),
        "approve_handoff": ("handoff_approval", ["approve", "stop"]),
    }
    if route not in mapping:
        return {"required": False, "kind": None, "allowed_actions": []}
    kind, allowed = mapping[route]
    return {"required": True, "kind": kind, "allowed_actions": allowed}


def _load_langgraph() -> dict[str, Any]:
    try:
        from langgraph.checkpoint.sqlite import SqliteSaver
        from langgraph.graph import END, START, StateGraph
        from langgraph.types import Command, Send, interrupt
    except ImportError as exc:
        raise MissingLangGraph(f"LangGraph is required for start/resume. {DEPENDENCY_HINT}") from exc
    return {
        "SqliteSaver": SqliteSaver,
        "StateGraph": StateGraph,
        "START": START,
        "END": END,
        "Command": Command,
        "Send": Send,
        "interrupt": interrupt,
    }


def _load_search_api() -> Any:
    try:
        from . import langgraph_search
    except ImportError:
        try:
            import langgraph_search  # type: ignore[no-redef]
        except ImportError as exc:
            raise CoordinatorError(
                "scripts.langgraph_search is unavailable; the search fan-out adapter is required"
            ) from exc
    return langgraph_search


def _interrupt_payload(kind: str, state: dict[str, Any], allowed: list[str]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": kind,
        "stage": str(state.get("stage") or "UNKNOWN"),
        "reason": str(state.get("reason") or ""),
        "next_action": str(state.get("next_action") or ""),
        "allowed_actions": allowed,
        "side_effects_before_interrupt": False,
    }
    if kind == "search_approval":
        payload.update({
            "query_config_path": str(state.get("query_config_path") or ""),
            "query_config_sha256": str(state.get("query_config_sha256") or ""),
            "planned_sources": list(state.get("planned_sources") or []),
            "proposed_search_attempt": int(state.get("search_attempt") or 0) + 1,
        })
    if kind == "handoff_approval":
        payload.update({
            "screening_path": str(state.get("screening_path") or ""),
            "screening_sha256": str(state.get("screening_sha256") or ""),
        })
    return payload


def build_graph(
    root: Path,
    checkpointer: Any,
    *,
    inspect_fn: Callable[..., dict[str, Any]] = workflow_status.inspect,
    search_api: Any | None = None,
    command_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> Any:
    """Build the pilot graph.  All business routing is delegated to inspect_fn."""
    lg = _load_langgraph()
    search_api = search_api or _load_search_api()
    interrupt = lg["interrupt"]
    Send = lg["Send"]
    root = root.expanduser().resolve()
    adapter_error = getattr(search_api, "SearchOrchestrationError", None)

    def call_search_adapter(label: str, function: Callable[..., Any], *args: Any,
                            **kwargs: Any) -> Any:
        try:
            return function(*args, **kwargs)
        except CoordinatorError:
            raise
        except OSError as exc:
            raise CoordinatorError(
                f"{label} failed ({exc.__class__.__name__})"
            ) from exc
        except Exception as exc:
            if isinstance(adapter_error, type) and isinstance(exc, adapter_error):
                raise CoordinatorError(f"{label} failed: {exc}") from exc
            raise

    def fresh_status(state: dict[str, Any]) -> dict[str, Any]:
        _assert_state_version(state)
        return inspect_fn(root, task_id=str(state["task_id"]))

    def inspect_node(state: CoordinatorState) -> dict[str, Any]:
        status = fresh_status(state)
        return {**state_update_from_status(root, status), "control_status": "running"}

    def repair_node(state: CoordinatorState) -> dict[str, Any]:
        decision = interrupt(_interrupt_payload(
            "repair_required", state, ["reinspect", "stop"]
        ))
        action = normalize_resume_decision(decision, {"reinspect", "stop"})
        return {"last_decision": action, "control_status": "stopped" if action == "stop" else "running"}

    def approve_search_node(state: CoordinatorState) -> dict[str, Any]:
        decision = interrupt(_interrupt_payload(
            "search_approval", state, ["approve", "stop"]
        ))
        action = normalize_resume_decision(decision, {"approve", "stop"})
        approvals = dict(state.get("approval_decisions") or {})
        key = f"search_attempt_{int(state.get('search_attempt') or 0) + 1}"
        approvals[key] = action
        return {
            "approval_decisions": approvals,
            "search_authorization_sha256": _search_binding(state),
            "last_decision": action,
            "control_status": "stopped" if action == "stop" else "running",
        }

    def search_incomplete_node(state: CoordinatorState) -> dict[str, Any]:
        decision = interrupt(_interrupt_payload(
            "search_incomplete", state, ["retry", "stop"]
        ))
        action = normalize_resume_decision(decision, {"retry", "stop"})
        approvals = dict(state.get("approval_decisions") or {})
        approvals[f"search_retry_after_{int(state.get('search_attempt') or 0)}"] = action
        return {
            "approval_decisions": approvals,
            "search_authorization_sha256": _search_binding(state),
            "last_decision": action,
            "control_status": "stopped" if action == "stop" else "running",
        }

    def screening_node(state: CoordinatorState) -> dict[str, Any]:
        decision = interrupt(_interrupt_payload(
            "screening_required", state, ["completed", "reinspect", "stop"]
        ))
        action = normalize_resume_decision(decision, {"completed", "reinspect", "stop"})
        return {"last_decision": action, "control_status": "stopped" if action == "stop" else "running"}

    def approve_handoff_node(state: CoordinatorState) -> dict[str, Any]:
        decision = interrupt(_interrupt_payload(
            "handoff_approval", state, ["approve", "stop"]
        ))
        action = normalize_resume_decision(decision, {"approve", "stop"})
        approvals = dict(state.get("approval_decisions") or {})
        approvals["download_handoff"] = action
        return {
            "approval_decisions": approvals,
            "handoff_authorization_sha256": _handoff_binding(state),
            "last_decision": action,
            "control_status": "stopped" if action == "stop" else "running",
        }

    def prepare_search_node(state: CoordinatorState) -> dict[str, Any]:
        status = fresh_status(state)  # stale-checkpoint fence before any filesystem/API work
        stage = str(status.get("stage") or "")
        current_attempt = int(state.get("search_attempt") or 0)
        approvals = dict(state.get("approval_decisions") or {})
        initial_authorized = (
            stage == "STAGE_0_API_SEARCH"
            and approvals.get(f"search_attempt_{current_attempt + 1}") == "approve"
        )
        retry_authorized = (
            stage == "STAGE_0_SEARCH_INCOMPLETE"
            and approvals.get(f"search_retry_after_{current_attempt}") == "retry"
        )
        if not (initial_authorized or retry_authorized):
            return {
                **state_update_from_status(root, status),
                "last_decision": "stale_checkpoint_skipped",
            }
        update = state_update_from_status(root, status)
        if _search_binding(update) != str(
            state.get("search_authorization_sha256") or ""
        ):
            return {
                **update,
                "last_decision": "stale_checkpoint_skipped",
            }
        config_path = str(update.get("query_config_path") or "")
        sources = list(update.get("planned_sources") or [])
        if not config_path or not sources:
            raise CoordinatorError("validated search status lacks config path or planned sources")
        attempt = current_attempt + 1
        orchestration_id = str(state["orchestration_id"])
        partition_root, output_dir = _expected_search_locations(
            root,
            task_root=str(update.get("task_root") or ""),
            orchestration_id=orchestration_id,
            search_attempt=attempt,
        )
        return {
            **update,
            "search_attempt": attempt,
            "partition_work_root": _relative_path(root, partition_root),
            "search_output_dir": _relative_path(root, output_dir),
            "published_search": {},
            "last_decision": "prepared",
        }

    def dispatch_partitions(state: CoordinatorState) -> list[Any] | str:
        if state.get("last_decision") == "stale_checkpoint_skipped":
            return "inspect"
        payloads: list[Any] = []
        for source in state.get("planned_sources") or []:
            payloads.append(Send("search_partition", {
                "schema_version": state["schema_version"],
                "control_plane_version": state["control_plane_version"],
                "task_id": state["task_id"],
                "thread_id": state["thread_id"],
                "orchestration_id": state["orchestration_id"],
                "query_config_path": state["query_config_path"],
                "query_config_sha256": state["query_config_sha256"],
                "partition_work_root": state["partition_work_root"],
                "search_attempt": state["search_attempt"],
                "planned_sources": list(state.get("planned_sources") or []),
                "partition_source": source,
                "approval_decisions": dict(state.get("approval_decisions") or {}),
                "search_authorization_sha256": str(
                    state.get("search_authorization_sha256") or ""
                ),
            }))
        if not payloads:
            raise CoordinatorError("search fan-out has no source partitions")
        return payloads

    def search_partition_node(state: CoordinatorState) -> dict[str, Any]:
        status = fresh_status(state)  # required immediately before the external API side effect
        fresh_update = state_update_from_status(root, status)
        stage = str(status.get("stage") or "")
        attempt = int(state.get("search_attempt") or 0)
        approvals = dict(state.get("approval_decisions") or {})
        authorized = (
            (stage == "STAGE_0_API_SEARCH"
             and approvals.get(f"search_attempt_{attempt}") == "approve")
            or
            (stage == "STAGE_0_SEARCH_INCOMPLETE"
             and approvals.get(f"search_retry_after_{attempt - 1}") == "retry")
        )
        if not authorized:
            return {"partition_results": []}
        if (
            _search_binding(fresh_update)
            != str(state.get("search_authorization_sha256") or "")
            or _search_binding(state)
            != str(state.get("search_authorization_sha256") or "")
        ):
            return {"partition_results": []}
        config_path = resolve_workspace_ref(root, str(state["query_config_path"]), must_exist=True)
        if _search_binding(state) != str(
            state.get("search_authorization_sha256") or ""
        ):
            raise CoordinatorError("search authorization is not bound to the selected config")
        if _file_sha256(config_path) != str(state.get("query_config_sha256") or ""):
            raise CoordinatorError("selected query config changed after search approval")
        expected_work_root, _ = _expected_search_locations(
            root,
            task_root=str(fresh_update.get("task_root") or ""),
            orchestration_id=str(state["orchestration_id"]),
            search_attempt=attempt,
        )
        work_root = resolve_workspace_ref(root, str(state["partition_work_root"]))
        if work_root != expected_work_root:
            raise CoordinatorError("partition work root differs from the deterministic attempt path")
        runtime = _secure_workspace_path(root, root / "runtime")
        work_root = _secure_runtime_directory(root, str(expected_work_root.relative_to(runtime)))
        source = str(state.get("partition_source") or "")
        if source not in (state.get("planned_sources") or []):
            raise CoordinatorError("search partition source is not in the approved source list")
        raw = call_search_adapter(
            f"search partition {source}",
            search_api.run_source_partition,
            config_path,
            source,
            work_root,
            max_records=0,
            python_executable=sys.executable,
        )
        reference = partition_result_reference(
            root,
            raw,
            orchestration_id=str(state["orchestration_id"]),
            search_attempt=int(state["search_attempt"]),
        )
        return {"partition_results": [reference]}

    def publish_search_node(state: CoordinatorState) -> dict[str, Any]:
        status = fresh_status(state)  # stale-checkpoint fence before atomic publication
        fresh_update = state_update_from_status(root, status)
        stage = str(status.get("stage") or "")
        attempt = int(state.get("search_attempt") or 0)
        approvals = dict(state.get("approval_decisions") or {})
        authorized = (
            (stage == "STAGE_0_API_SEARCH"
             and approvals.get(f"search_attempt_{attempt}") == "approve")
            or
            (stage == "STAGE_0_SEARCH_INCOMPLETE"
             and approvals.get(f"search_retry_after_{attempt - 1}") == "retry")
        )
        if not authorized:
            return {
                **fresh_update,
                "last_decision": "stale_checkpoint_skipped",
            }
        if (
            _search_binding(fresh_update)
            != str(state.get("search_authorization_sha256") or "")
            or _search_binding(state)
            != str(state.get("search_authorization_sha256") or "")
        ):
            return {
                **fresh_update,
                "last_decision": "stale_checkpoint_skipped",
            }
        config_path = resolve_workspace_ref(root, str(state["query_config_path"]), must_exist=True)
        if _search_binding(state) != str(
            state.get("search_authorization_sha256") or ""
        ):
            raise CoordinatorError("search authorization is not bound to the selected config")
        if _file_sha256(config_path) != str(state.get("query_config_sha256") or ""):
            raise CoordinatorError("selected query config changed before search publication")
        _, expected_output_dir = _expected_search_locations(
            root,
            task_root=str(fresh_update.get("task_root") or ""),
            orchestration_id=str(state["orchestration_id"]),
            search_attempt=attempt,
        )
        output_dir = resolve_workspace_ref(root, str(state["search_output_dir"]))
        if output_dir != expected_output_dir:
            raise CoordinatorError("search output differs from the deterministic attempt path")
        _secure_workspace_path(root, output_dir)
        orchestration_id = str(state["orchestration_id"])
        references = [
            result for result in state.get("partition_results", [])
            if result.get("orchestration_id") == orchestration_id
            and result.get("search_attempt") == attempt
        ]
        expected = list(state.get("planned_sources") or [])
        actual = [str(result.get("source") or "") for result in references]
        if sorted(actual) != sorted(expected) or len(actual) != len(set(actual)):
            raise CoordinatorError(
                f"partition coverage mismatch: expected={expected!r}, actual={actual!r}"
            )
        raw = call_search_adapter(
            "search publication",
            search_api.publish_search_run,
            config_path,
            [materialize_partition_reference(root, item) for item in references],
            output_dir,
            orchestration_id,
            attempt,
        )
        if not isinstance(raw, dict):
            raise CoordinatorError("search publisher returned a non-object result")
        required_published = PUBLISHED_RESULT_FIELDS - {"output_dir"}
        # orchestration_id/search_attempt are still verified from the raw
        # adapter result, then overwritten with the approved state values.
        if not required_published.issubset(raw):
            missing = sorted(required_published - set(raw))
            raise CoordinatorError("search publisher result lacks: " + ", ".join(missing))
        if raw.get("orchestration_id") != orchestration_id or raw.get("search_attempt") != attempt:
            raise CoordinatorError("search publisher returned a mismatched attempt binding")
        if raw.get("config_sha256") != state.get("query_config_sha256"):
            raise CoordinatorError("search publisher returned a mismatched config binding")
        published = {key: raw[key] for key in PUBLISHED_RESULT_FIELDS if key in raw}
        raw_output = Path(str(raw.get("output_dir") or output_dir)).expanduser()
        if not raw_output.is_absolute():
            raw_output = root / raw_output
        published.update({
            "output_dir": _relative_path(root, raw_output),
            "orchestration_id": orchestration_id,
            "search_attempt": attempt,
        })
        try:
            published = json.loads(json.dumps(published, ensure_ascii=False))
        except (TypeError, ValueError) as exc:
            raise CoordinatorError("published search reference is not JSON serializable") from exc
        return {"published_search": published, "last_decision": "published"}

    def handoff_node(state: CoordinatorState) -> dict[str, Any]:
        status = fresh_status(state)  # stale-checkpoint fence before enqueue side effect
        if status.get("stage") != "STAGE_1_HANDOFF_TO_DOWNLOAD":
            update = state_update_from_status(root, status)
            if (
                str(state.get("approval_decisions", {}).get("download_handoff") or "") == "approve"
                and str(state.get("handoff_authorization_sha256") or "") == _handoff_binding(update)
            ):
                binding = str(state.get("handoff_authorization_sha256") or "")
                expected_intent = _new_handoff_intent(root, state, update)
                with handoff_execution_lock(root, binding) as intent_path:
                    if not intent_path.is_file():
                        raise CoordinatorError(
                            "workflow advanced but no bound handoff intent exists; inspect the batch manually"
                        )
                    intent = _load_handoff_intent(intent_path, expected_intent)
                    recovered = _recover_handoff_batch(root, intent)
                    if recovered is None:
                        raise CoordinatorError(
                            "workflow advanced but the approved handoff batch cannot be recovered"
                        )
                    batch_id = _valid_batch_id(recovered)
                    if intent.get("status") != "completed":
                        _atomic_write_json(
                            intent_path,
                            {**intent, "status": "completed", "batch_id": batch_id},
                        )
                actual_status = str(
                    workflow_status.read_json(
                        root / "02_batches" / batch_id / "STATUS.json"
                    ).get("status") or ""
                )
                if actual_status not in workflow_status.BATCH_STATUSES:
                    raise CoordinatorError("recovered handoff batch has an invalid status")
                summary = workflow_status.read_json(
                    root / "02_batches" / batch_id
                    / "inputs/search_screening/handoff_summary.json"
                )
                selected_count = summary.get("selected_count")
                changed = record_active_task_batch(
                    root,
                    task_id=str(state["task_id"]),
                    task_root=str(update.get("task_root") or state.get("task_root") or ""),
                    batch_id=batch_id,
                )
                return {
                    **update,
                    "handoff_result": {
                        "batch_id": batch_id,
                        "batch_path": f"02_batches/{batch_id}",
                        "status": actual_status,
                        "selected_count": (
                            selected_count if _non_negative_int(selected_count) else None
                        ),
                        "screening_sha256": str(update.get("screening_sha256") or ""),
                        "reused": True,
                    },
                    "last_decision": "handoff_pointer_recorded" if changed else "handoff_reused",
                }
            return {
                **update,
                "last_decision": "stale_checkpoint_skipped",
            }
        update = state_update_from_status(root, status)
        if (
            str(state.get("approval_decisions", {}).get("download_handoff") or "") != "approve"
            or str(state.get("handoff_authorization_sha256") or "") != _handoff_binding(update)
        ):
            return {
                **update,
                "last_decision": "stale_checkpoint_skipped",
            }
        screening = _secure_workspace_path(
            root,
            resolve_workspace_ref(root, str(update.get("screening_path") or ""), must_exist=True),
            must_exist=True,
        )
        if _file_sha256(screening) != str(update.get("screening_sha256") or ""):
            raise CoordinatorError("screening CSV changed during handoff preflight")
        brief_path = _secure_workspace_path(
            root,
            resolve_workspace_ref(root, str(update.get("task_brief_path") or ""), must_exist=True),
            must_exist=True,
        )
        try:
            brief = json.loads(brief_path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CoordinatorError("task brief became unreadable before handoff") from exc
        title = str(brief.get("title") or state["task_id"]).strip()
        question = str(brief.get("research_question") or "").strip()
        purpose = f"为任务 {state['task_id']} 的全文复筛取得原文：{question}".strip("：")
        command = [
            sys.executable,
            str(root / "scripts/handoff_screening_to_download.py"),
            "--root", str(root),
            "--screening", str(screening),
            "--purpose", purpose,
            "--title", title,
            "--overwrite",
        ]
        binding = str(state.get("handoff_authorization_sha256") or "")
        expected_intent = _new_handoff_intent(root, state, update)
        if expected_intent["binding"] != binding:
            return {**update, "last_decision": "stale_checkpoint_skipped"}
        with handoff_execution_lock(root, binding) as intent_path:
            intent = _load_handoff_intent(intent_path, expected_intent)
            batch_id = _recover_handoff_batch(root, intent)
            raw: dict[str, Any] = {}
            reused = batch_id is not None
            if batch_id is None:
                runner_error: BaseException | None = None
                completed: subprocess.CompletedProcess[str] | None = None
                try:
                    completed = command_runner(
                        command, check=True, text=True, capture_output=True
                    )
                except (subprocess.CalledProcessError, OSError) as exc:
                    runner_error = exc
                # The child may have committed enqueue/evidence before reporting
                # failure.  Reconcile the deterministic intent before deciding
                # whether another attempt is safe.
                batch_id = _recover_handoff_batch(root, intent)
                if runner_error is not None and batch_id is None:
                    if isinstance(runner_error, subprocess.CalledProcessError):
                        raise CoordinatorError(
                            "download handoff subprocess failed before creating a recoverable batch "
                            f"(exit={runner_error.returncode})"
                        ) from runner_error
                    raise CoordinatorError(
                        f"download handoff could not start ({runner_error.__class__.__name__})"
                    ) from runner_error
                if batch_id is None:
                    lines = [
                        line for line in str(completed.stdout if completed else "").splitlines()
                        if line.strip()
                    ]
                    if not lines:
                        raise CoordinatorError("download handoff returned no audit result")
                    try:
                        parsed = json.loads(lines[-1])
                    except json.JSONDecodeError as exc:
                        recovered = _recover_handoff_batch(root, intent)
                        if recovered is None:
                            raise CoordinatorError("download handoff returned invalid JSON") from exc
                        batch_id = recovered
                    else:
                        raw = parsed if isinstance(parsed, dict) else {}
                        batch = raw.get("batch") if isinstance(raw.get("batch"), dict) else {}
                        batch_id = _valid_batch_id(str(batch.get("batch_id") or ""))
                        batch_path = str(batch.get("batch_path") or "").strip()
                        batch_ref = _relative_path(root, Path(batch_path)) if batch_path else ""
                        if batch_ref != f"02_batches/{batch_id}":
                            raise CoordinatorError("download handoff returned a mismatched batch path")
                        if not _complete_handoff_evidence(root, batch_id, intent):
                            raise CoordinatorError(
                                "download handoff batch lacks complete evidence bound to the approval"
                            )
                reused = runner_error is not None
            if batch_id is None:  # pragma: no cover - guarded by branches above
                raise CoordinatorError("download handoff did not resolve a batch")
            if not _complete_handoff_evidence(root, batch_id, intent):
                raise CoordinatorError(
                    "download handoff recovery found a partial batch; repair it before retrying"
                )
            completed_intent = {**intent, "status": "completed", "batch_id": batch_id}
            _atomic_write_json(intent_path, completed_intent)
            changed = record_active_task_batch(
                root,
                task_id=str(state["task_id"]),
                task_root=str(update.get("task_root") or ""),
                batch_id=batch_id,
            )
            evidence_summary = workflow_status.read_json(
                root / "02_batches" / batch_id / "inputs/search_screening/handoff_summary.json"
            )
            selected_count = evidence_summary.get("selected_count")
            batch_status = str(
                workflow_status.read_json(root / "02_batches" / batch_id / "STATUS.json").get("status")
                or ""
            )
            handoff_ref = {
                "batch_id": batch_id,
                "batch_path": f"02_batches/{batch_id}",
                "status": batch_status,
                "selected_count": selected_count if _non_negative_int(selected_count) else None,
                "screening_sha256": str(update.get("screening_sha256") or ""),
                "reused": bool(reused),
            }
            return {
                "handoff_result": handoff_ref,
                "last_decision": (
                    "handoff_pointer_recorded" if changed and reused
                    else "handoff_reused" if reused
                    else "handoff_created"
                ),
            }

    builder = lg["StateGraph"](CoordinatorState)
    builder.add_node("inspect", inspect_node)
    builder.add_node("repair", repair_node)
    builder.add_node("approve_search", approve_search_node)
    builder.add_node("prepare_search", prepare_search_node)
    builder.add_node("search_partition", search_partition_node)
    builder.add_node("publish_search", publish_search_node)
    builder.add_node("search_incomplete", search_incomplete_node)
    builder.add_node("screening", screening_node)
    builder.add_node("approve_handoff", approve_handoff_node)
    builder.add_node("handoff", handoff_node)
    builder.add_edge(lg["START"], "inspect")
    builder.add_conditional_edges("inspect", lambda state: route_for_stage(str(state.get("stage") or "")), {
        "repair": "repair",
        "approve_search": "approve_search",
        "search_incomplete": "search_incomplete",
        "screening": "screening",
        "approve_handoff": "approve_handoff",
        "end": lg["END"],
    })
    builder.add_conditional_edges(
        "repair",
        lambda state: lg["END"] if state.get("last_decision") == "stop" else "inspect",
        {"inspect": "inspect", lg["END"]: lg["END"]},
    )
    builder.add_conditional_edges(
        "approve_search",
        lambda state: lg["END"] if state.get("last_decision") == "stop" else "prepare_search",
        {"prepare_search": "prepare_search", lg["END"]: lg["END"]},
    )
    builder.add_conditional_edges(
        "prepare_search",
        dispatch_partitions,
        ["search_partition", "inspect"],
    )
    builder.add_edge("search_partition", "publish_search")
    builder.add_edge("publish_search", "inspect")
    builder.add_conditional_edges(
        "search_incomplete",
        lambda state: lg["END"] if state.get("last_decision") == "stop" else "prepare_search",
        {"prepare_search": "prepare_search", lg["END"]: lg["END"]},
    )
    builder.add_conditional_edges(
        "screening",
        lambda state: lg["END"] if state.get("last_decision") == "stop" else "inspect",
        {"inspect": "inspect", lg["END"]: lg["END"]},
    )
    builder.add_conditional_edges(
        "approve_handoff",
        lambda state: lg["END"] if state.get("last_decision") == "stop" else "handoff",
        {"handoff": "handoff", lg["END"]: lg["END"]},
    )
    builder.add_edge("handoff", "inspect")
    return builder.compile(checkpointer=checkpointer, name="paper-download-thin-coordinator")


def _selected_task_id(status: dict[str, Any], requested: str | None) -> str:
    selected = str(status.get("evidence", {}).get("task_id") or "").strip()
    task_id = str(requested or selected).strip()
    if not task_id:
        raise CoordinatorError(
            "no unique workflow task is selected; create/fix ACTIVE_TASK.json or pass --task-id"
        )
    return task_id


def initial_state(task_id: str, thread_id: str) -> CoordinatorState:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "control_plane_version": CONTROL_PLANE_VERSION,
        "task_id": task_id,
        "thread_id": thread_id,
        "orchestration_id": orchestration_id_for(task_id, thread_id),
        "stage": "BOOTSTRAP",
        "reason": "Awaiting authoritative workflow inspection.",
        "next_action": "Run workflow_status.inspect.",
        "status_sha256": "",
        "task_root": "",
        "task_brief_path": "",
        "task_brief_sha256": "",
        "query_config_path": "",
        "query_config_sha256": "",
        "search_manifest_path": "",
        "search_manifest_sha256": "",
        "screening_path": "",
        "screening_sha256": "",
        "planned_sources": [],
        "search_attempt": 0,
        "partition_work_root": "",
        "search_output_dir": "",
        "partition_results": [],
        "published_search": {},
        "handoff_result": {},
        "approval_decisions": {},
        "search_authorization_sha256": "",
        "handoff_authorization_sha256": "",
        "last_decision": "",
        "control_status": "running",
    }


def _interrupts_from_snapshot(snapshot: Any | None) -> dict[str, Any]:
    interrupts = list(getattr(snapshot, "interrupts", ()) or ()) if snapshot is not None else []
    items: list[dict[str, Any]] = []
    for item in interrupts:
        value = getattr(item, "value", None)
        items.append({
            "id": str(getattr(item, "id", "") or ""),
            "value": value if isinstance(value, dict) else {"message": str(value)},
        })
    return {"pending": bool(items), "items": items}


def audit_report(
    *,
    command: str,
    root: Path,
    task_id: str | None,
    thread_id: str | None,
    status: dict[str, Any],
    checkpoint_path: Path | None = None,
    snapshot: Any | None = None,
) -> dict[str, Any]:
    checkpoint_interrupt = _interrupts_from_snapshot(snapshot)
    predicted = expected_interrupt(status)
    state = dict(getattr(snapshot, "values", {}) or {}) if snapshot is not None else {}
    safe_state = {
        key: state[key] for key in (
            "schema_version", "control_plane_version", "orchestration_id", "search_attempt",
            "approval_decisions", "published_search", "handoff_result", "control_status",
        ) if key in state
    }
    if "partition_results" in state:
        safe_state["partition_results"] = state["partition_results"]
    next_nodes = list(getattr(snapshot, "next", ()) or ()) if snapshot is not None else []
    active_interrupt = checkpoint_interrupt if snapshot is not None else {
        "pending": False,
        "items": [],
        "required_by_current_stage": predicted,
    }
    top_next = str(status.get("next_action") or "")
    if checkpoint_interrupt["pending"]:
        payload = checkpoint_interrupt["items"][0].get("value", {})
        top_next = str(payload.get("next_action") or top_next)
    report: dict[str, Any] = {
        "command": command,
        "workspace": str(root),
        "task_id": task_id,
        "thread_id": thread_id,
        "stage": status.get("stage"),
        "interrupt": active_interrupt,
        "next_action": top_next,
        "workflow_status": status,
    }
    if checkpoint_path is not None:
        report["checkpoint"] = {
            "path": _relative_path(root, checkpoint_path),
            "next_nodes": next_nodes,
            "state": safe_state,
        }
    return report


def status_command(root: Path, task_id: str | None, thread_id: str | None) -> dict[str, Any]:
    status = workflow_status.inspect(root, task_id=task_id)
    selected = str(status.get("evidence", {}).get("task_id") or task_id or "").strip() or None
    resolved_thread = validate_thread_id(thread_id) if thread_id else (
        default_thread_id(selected) if selected else None
    )
    return audit_report(
        command="status",
        root=root,
        task_id=selected,
        thread_id=resolved_thread,
        status=status,
    )


def run_graph_command(
    command: str,
    *,
    root: Path,
    task_id: str | None,
    thread_id: str | None,
    checkpoint_db: str,
    decision: str | None = None,
    interrupt_id: str | None = None,
) -> dict[str, Any]:
    lg = _load_langgraph()
    status_before = workflow_status.inspect(root, task_id=task_id)
    selected_task = _selected_task_id(status_before, task_id)
    selected_thread = validate_thread_id(thread_id) if thread_id else default_thread_id(selected_task)
    checkpoint_path = resolve_checkpoint_path(root, checkpoint_db)
    config = {"configurable": {"thread_id": selected_thread}}
    try:
        runtime = _secure_workspace_path(root, root / "runtime")
        _secure_runtime_directory(root, str(checkpoint_path.parent.relative_to(runtime)))
        with task_execution_lock(root, selected_task), checkpoint_thread_lock(
            root, checkpoint_path, selected_thread
        ):
            with lg["SqliteSaver"].from_conn_string(str(checkpoint_path)) as saver:
                graph = build_graph(root, saver)
                existing = graph.get_state(config)
                values = dict(existing.values or {})
                if command == "start":
                    if values:
                        raise CoordinatorError(
                            f"thread_id={selected_thread!r} already has a checkpoint; use resume"
                        )
                    if decision is not None:
                        raise CoordinatorError("start does not accept a resume decision")
                    if interrupt_id is not None:
                        raise CoordinatorError("start does not accept an interrupt ID")
                    graph.invoke(
                        initial_state(selected_task, selected_thread),
                        config,
                        durability="sync",
                    )
                elif command == "resume":
                    if not values:
                        raise CoordinatorError(
                            f"thread_id={selected_thread!r} has no checkpoint; use start"
                        )
                    validate_checkpoint_state(
                        root, values, task_id=selected_task, thread_id=selected_thread
                    )
                    pending = list(existing.interrupts or ())
                    if pending and decision is None:
                        if interrupt_id is not None:
                            raise CoordinatorError("--interrupt-id requires --decision")
                        status_after = workflow_status.inspect(root, task_id=selected_task)
                        return audit_report(
                            command=command,
                            root=root,
                            task_id=selected_task,
                            thread_id=selected_thread,
                            status=status_after,
                            checkpoint_path=checkpoint_path,
                            snapshot=existing,
                        )
                    if not pending and decision is not None:
                        raise CoordinatorError(
                            "this checkpoint has no pending interrupt; omit --decision"
                        )
                    if decision is not None:
                        expected_interrupt_id = str(getattr(pending[0], "id", "") or "")
                        supplied_interrupt_id = str(interrupt_id or "").strip()
                        if not supplied_interrupt_id:
                            raise CoordinatorError(
                                "resume with --decision also requires --interrupt-id from start/resume output"
                            )
                        if supplied_interrupt_id != expected_interrupt_id:
                            raise CoordinatorError(
                                "interrupt ID is stale; inspect current status before deciding"
                            )
                    elif interrupt_id is not None:
                        raise CoordinatorError("--interrupt-id requires --decision")
                    if decision is None:
                        graph.invoke(None, config, durability="sync")
                    else:
                        allowed = []
                        payload = getattr(pending[0], "value", {}) if pending else {}
                        if isinstance(payload, dict) and isinstance(
                            payload.get("allowed_actions"), list
                        ):
                            allowed = [str(value) for value in payload["allowed_actions"]]
                        normalized = normalize_resume_decision(decision, allowed)
                        graph.invoke(
                            lg["Command"](resume={expected_interrupt_id: {"action": normalized}}),
                            config,
                            durability="sync",
                        )
                else:  # pragma: no cover - argparse constrains this
                    raise CoordinatorError(f"unsupported graph command: {command}")
                snapshot = graph.get_state(config)
                validate_checkpoint_state(
                    root,
                    dict(snapshot.values or {}),
                    task_id=selected_task,
                    thread_id=selected_thread,
                )
                status_after = workflow_status.inspect(root, task_id=selected_task)
                return audit_report(
                    command=command,
                    root=root,
                    task_id=selected_task,
                    thread_id=selected_thread,
                    status=status_after,
                    checkpoint_path=checkpoint_path,
                    snapshot=snapshot,
                )
    except CoordinatorError:
        raise
    except (OSError, sqlite3.Error) as exc:
        raise CoordinatorError(
            f"orchestration storage failed ({exc.__class__.__name__})"
        ) from exc


def _add_common_arguments(parser: argparse.ArgumentParser, *, checkpoint: bool) -> None:
    parser.add_argument("--root", default=".", help="paper-download workspace root")
    parser.add_argument("--task-id", help="select one workflow task explicitly")
    parser.add_argument("--thread-id", help="checkpoint thread; defaults to the selected task_id")
    if checkpoint:
        parser.add_argument(
            "--checkpoint-db",
            default=DEFAULT_CHECKPOINT,
            help=(
                "SQLite checkpoint path inside runtime/orchestration/ "
                f"(default: {DEFAULT_CHECKPOINT})"
            ),
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    status_parser = commands.add_parser("status", help="inspect authoritative state without LangGraph")
    _add_common_arguments(status_parser, checkpoint=False)
    start_parser = commands.add_parser("start", help="start a new persistent coordinator thread")
    _add_common_arguments(start_parser, checkpoint=True)
    resume_parser = commands.add_parser("resume", help="resume an interrupted or failed coordinator thread")
    _add_common_arguments(resume_parser, checkpoint=True)
    resume_parser.add_argument("--decision", choices=sorted(DECISION_WHITELIST))
    resume_parser.add_argument(
        "--interrupt-id",
        help=(
            "required with --decision; copy the current "
            "interrupt.items[].id from start/resume output"
        ),
    )
    args = parser.parse_args(argv)
    root = Path(args.root).expanduser().resolve()
    if not (root / "00_control/WORKSPACE.json").is_file():
        parser.error(f"not a paper download workspace: {root}")
    try:
        if args.command == "status":
            report = status_command(root, args.task_id, args.thread_id)
        else:
            report = run_graph_command(
                args.command,
                root=root,
                task_id=args.task_id,
                thread_id=args.thread_id,
                checkpoint_db=args.checkpoint_db,
                decision=getattr(args, "decision", None),
                interrupt_id=getattr(args, "interrupt_id", None),
            )
    except CoordinatorError as exc:
        print(json.dumps({
            "command": args.command,
            "workspace": str(root),
            "error": str(exc),
            "dependency_hint": DEPENDENCY_HINT if isinstance(exc, MissingLangGraph) else None,
        }, ensure_ascii=False, indent=2), file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
