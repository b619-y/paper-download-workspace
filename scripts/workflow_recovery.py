#!/usr/bin/env python3
"""Diagnose and apply bounded, deterministic paper-workflow repairs.

The recovery Agent may call this module, but the module itself is deliberately
not an LLM.  It only executes a small allowlist of reversible maintenance
actions and re-runs ``workflow_status`` after every action.  Anything involving
research judgement, authorization, source identity, hashes, acceptance, or an
external model remains manual.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote

try:  # Direct script execution.
    import accept_batch
    import extraction_common
    import processing_queue
    import workflow_status
except ModuleNotFoundError:  # pragma: no cover - package-style imports in callers.
    from scripts import (  # type: ignore[no-redef]
        accept_batch,
        extraction_common,
        processing_queue,
        workflow_status,
    )


RECOVERY_VERSION = "safe-workflow-recovery-v2"
BUDGET_NAMESPACE = "workflow-recovery-budget-v1"
MAX_ACTIONS_HARD_LIMIT = 3
ABNORMAL_STAGE_MARKERS = (
    "INVALID", "BLOCKED", "FAILED", "ERROR", "AMBIGUOUS", "INCOMPLETE",
    "RECOVERY",
)
DANGEROUS_STAGE_MARKERS = ("INVALID", "BLOCKED", "FAILED", "ERROR", "AMBIGUOUS")
DOCUMENT_PROCESSING_RECOVERY_STAGES = {
    "STAGE_3_DOCUMENT_PROCESSING",
    "STAGE_3_DOCUMENT_PROCESSING_RECOVERY",
    "STAGE_3_DOCUMENT_PROCESSING_INVALID",
}
DOCUMENT_PROJECTION_ISSUE_PREFIXES = (
    "queue.csv missing",
    "queue.csv unreadable",
    "queue.csv missing_header",
    "queue.csv header does not match",
    "queue.csv job_id values",
    "queue.csv does not project exactly",
    "queue.csv differs from jobs table",
)
EXTRACTION_PROJECTION_ISSUE_PREFIXES = (
    "data_extraction_queue.csv ",
)


class RecoveryError(RuntimeError):
    """Raised when a proposed safe repair cannot be proven safe."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_json(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _relative(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise RecoveryError(f"recovery path escapes workspace: {path}") from exc


def _task_key(task_id: str) -> str:
    return "task-" + hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:20]


@contextmanager
def recovery_lock(root: Path) -> Iterator[extraction_common.PinnedDirectory]:
    with extraction_common.PinnedDirectory(
        root, "runtime/recovery", create=True,
    ) as directory:
        with directory.locked_file(".recovery.lock"):
            yield directory


def _read_ledger(directory: extraction_common.PinnedDirectory) -> list[dict[str, Any]]:
    try:
        raw = directory.read_bytes("events.jsonl", maximum_bytes=16 * 1024 * 1024)
    except FileNotFoundError:
        return []
    events: list[dict[str, Any]] = []
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError as exc:
        raise RecoveryError("recovery ledger is not UTF-8") from exc
    for number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RecoveryError(f"recovery ledger line {number} is invalid") from exc
        if not isinstance(value, dict):
            raise RecoveryError(f"recovery ledger line {number} is not an object")
        events.append(value)
    return events


def _append_ledger(
    directory: extraction_common.PinnedDirectory, event: dict[str, Any],
) -> None:
    payload = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    directory.append_line("events.jsonl", payload + "\n")


def _selected_task(status: dict[str, Any], requested: str | None) -> str:
    evidence = status.get("evidence") if isinstance(status.get("evidence"), dict) else {}
    observed = str(evidence.get("task_id") or "").strip()
    requested = str(requested or "").strip()
    if observed and requested and observed != requested:
        raise RecoveryError("workflow status task_id differs from the requested task")
    task_id = observed or requested
    if not task_id:
        raise RecoveryError("no unique task is selected")
    try:
        return extraction_common.ensure_task_id(task_id)
    except ValueError as exc:
        raise RecoveryError("selected task_id is unsafe") from exc


def _selected_batch(status: dict[str, Any], requested: str | None) -> str | None:
    evidence = status.get("evidence") if isinstance(status.get("evidence"), dict) else {}
    observed = str(evidence.get("selected_batch_id") or "").strip()
    requested = str(requested or "").strip()
    if observed and requested and observed != requested:
        raise RecoveryError("workflow status batch_id differs from the requested batch")
    selected = observed or requested
    if not selected:
        return None
    if (not accept_batch.SAFE_BATCH_ID_RE.fullmatch(selected)
            or selected in {".", ".."}
            or ".." in selected
            or selected.casefold().endswith(".committed")
            or Path(selected).name != selected):
        raise RecoveryError("selected batch_id is unsafe")
    return selected


def _batch_row(status: dict[str, Any], batch_id: str | None) -> dict[str, Any]:
    if not batch_id:
        return {}
    evidence = status.get("evidence") if isinstance(status.get("evidence"), dict) else {}
    for row in evidence.get("batches", []):
        if isinstance(row, dict) and str(row.get("batch_id") or "") == batch_id:
            return row
    return {}


def _batch_gate(status: dict[str, Any], batch_id: str | None) -> dict[str, Any]:
    row = _batch_row(status, batch_id)
    gate = row.get("document_processing")
    return gate if isinstance(gate, dict) else {}


def _projection_only(issues: Any, prefixes: tuple[str, ...]) -> bool:
    return (
        isinstance(issues, list)
        and bool(issues)
        and all(
            isinstance(issue, str) and issue.startswith(prefixes)
            for issue in issues
        )
    )


def _trusted_document_recovery_gate(
    status: dict[str, Any], batch_id: str | None,
) -> tuple[bool, dict[str, Any]]:
    """Return whether status proves this is the active accepted parse queue."""
    row = _batch_row(status, batch_id)
    gate = _batch_gate(status, batch_id)
    stage = str(status.get("stage") or "")
    issues = gate.get("issues")
    if (
        not row
        or str(row.get("status") or "") != "ACCEPTED"
        or stage not in DOCUMENT_PROCESSING_RECOVERY_STAGES
        or not gate
        or gate.get("exists") is not True
    ):
        return False, gate
    if stage in {"STAGE_3_DOCUMENT_PROCESSING", "STAGE_3_DOCUMENT_PROCESSING_RECOVERY"}:
        return gate.get("valid") is True and issues == [], gate
    return _projection_only(issues, DOCUMENT_PROJECTION_ISSUE_PREFIXES), gate


def _safe_document_paths(root: Path, batch_id: str) -> tuple[Path, Path]:
    batch = extraction_common.secure_workspace_path(
        root, root / "02_batches" / batch_id, within=root / "02_batches",
        must_exist=True,
    )
    database = extraction_common.secure_workspace_path(
        root, processing_queue.database_path(root, batch_id), within=batch,
        must_exist=True,
    )
    projection = extraction_common.secure_workspace_path(
        root, processing_queue.projection_path(root, batch_id), within=batch,
    )
    return database, projection


@contextmanager
def _working_directory(descriptor: int) -> Iterator[None]:
    original = os.open(".", os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fchdir(descriptor)
        yield
    finally:
        os.fchdir(original)
        os.close(original)


def _export_document_projection_pinned(root: Path, batch_id: str) -> None:
    relative = Path("02_batches") / batch_id / "outputs/document_processing"
    with extraction_common.PinnedDirectory(root, relative) as directory:
        with directory.locked_file("queue.csv.lock"):
            # SQLite needs its WAL/SHM siblings.  Changing cwd to the pinned
            # directory keeps all read-only opens on the pinned inode even if
            # the lexical directory is replaced concurrently.
            with _working_directory(directory.fd):
                db = _open_processing_readonly(Path("queue.sqlite3"))
                try:
                    _assert_processing_database(db)
                    rows = processing_queue.rows(db)
                finally:
                    db.close()
            stream = io.StringIO(newline="")
            writer = csv.DictWriter(
                stream,
                fieldnames=processing_queue.JOB_COLUMNS,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(rows)
            directory.atomic_write_text("queue.csv", stream.getvalue())


def _safe_extraction_paths(root: Path, task_id: str) -> dict[str, Path]:
    paths = extraction_common.task_paths(root, task_id)
    safe = dict(paths)
    safe["task"] = extraction_common.secure_workspace_path(
        root, paths["task"], within=root / "runtime/tasks", must_exist=True,
    )
    safe["extraction"] = extraction_common.secure_workspace_path(
        root, paths["extraction"], within=safe["task"], must_exist=True,
    )
    for key in ("spec", "queue"):
        safe[key] = extraction_common.secure_workspace_path(
            root, paths[key], within=safe["extraction"], must_exist=True,
        )
    for key in ("queue_csv", "queue_lock"):
        safe[key] = extraction_common.secure_workspace_path(
            root, paths[key], within=safe["extraction"],
        )
    return safe


def _open_processing_readonly(path: Path) -> sqlite3.Connection:
    uri = "file:" + quote(str(path.resolve())) + "?mode=ro"
    db = sqlite3.connect(uri, uri=True, timeout=30)
    db.row_factory = sqlite3.Row
    return db


def _assert_processing_database(db: sqlite3.Connection) -> None:
    quick = [str(row[0]) for row in db.execute("PRAGMA quick_check")]
    if quick != ["ok"]:
        raise RecoveryError("document-processing SQLite quick_check failed")
    row = db.execute(
        "SELECT value FROM metadata WHERE key='schema_version'"
    ).fetchone()
    if row is None or str(row[0]) != processing_queue.SCHEMA_VERSION:
        raise RecoveryError("document-processing schema version is unsupported")
    columns = [str(row[1]) for row in db.execute("PRAGMA table_info(jobs)")]
    if columns != processing_queue.JOB_COLUMNS:
        raise RecoveryError("document-processing jobs schema differs from the contract")


def _budget_keys_for(
    kind: str, *, task_id: str, batch_id: str | None, target: str,
) -> list[str]:
    subjects = ["target"]
    keys = []
    for subject in subjects:
        binding = {
            "namespace": BUDGET_NAMESPACE,
            "kind": kind,
            "task_id": task_id,
            "batch_id": batch_id or "",
            "target": target,
            "subject": subject,
        }
        keys.append("budget-" + sha256_json(binding)[:24])
    return keys


def _action(
    kind: str, *, status: dict[str, Any], task_id: str,
    target: str, batch_id: str | None = None, details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_sha = sha256_json(status)
    action_details = details or {}
    binding = {
        "version": RECOVERY_VERSION,
        "task_id": task_id,
        "batch_id": batch_id or "",
        "stage": str(status.get("stage") or ""),
        "evidence_sha256": evidence_sha,
        "kind": kind,
        "target": target,
        "details": action_details,
    }
    budget_keys = _budget_keys_for(
        kind,
        task_id=task_id,
        batch_id=batch_id,
        target=target,
    )
    return {
        **binding,
        "action_id": "repair-" + sha256_json(binding)[:24],
        "budget_keys": budget_keys,
        "details": action_details,
    }


def _attempted_bindings(
    events: list[dict[str, Any]],
) -> tuple[set[str], set[str]]:
    action_ids: set[str] = set()
    budget_keys: set[str] = set()
    for event in events:
        if event.get("phase") != "started":
            continue
        action_id = str(event.get("action_id") or "")
        if action_id:
            action_ids.add(action_id)
        values = event.get("budget_keys")
        if isinstance(values, list):
            budget_keys.update(str(value) for value in values if str(value))
    return action_ids, budget_keys


def _eligible_actions(
    actions: list[dict[str, Any]], *, attempted_action_ids: set[str],
    spent_budget_keys: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    eligible: list[dict[str, Any]] = []
    exhausted: list[dict[str, Any]] = []
    for action in actions:
        keys = set(action.get("budget_keys") or [])
        if action["action_id"] in attempted_action_ids or keys & spent_budget_keys:
            exhausted.append(action)
        else:
            eligible.append(action)
    return eligible, exhausted


def plan_safe_actions(
    root: Path, status: dict[str, Any], *, task_id: str, batch_id: str | None,
) -> list[dict[str, Any]]:
    """Return deterministic safe actions in priority order without writing."""
    actions: list[dict[str, Any]] = []
    selected_batch = _selected_batch(status, batch_id)
    trusted_gate, gate = _trusted_document_recovery_gate(status, selected_batch)
    if selected_batch and trusted_gate:
        try:
            database, projection = _safe_document_paths(root, selected_batch)
            with _open_processing_readonly(database) as db:
                _assert_processing_database(db)
            database_valid = True
        except (OSError, sqlite3.Error, RecoveryError, ValueError):
            database_valid = False
        # Repair a bad projection first so the next status observation proves
        # that the authoritative DB and its public projection agree.
        if database_valid and _projection_only(
            gate.get("issues"), DOCUMENT_PROJECTION_ISSUE_PREFIXES
        ):
            actions.append(_action(
                "rebuild_document_processing_projection",
                status=status,
                task_id=task_id,
                batch_id=selected_batch,
                target=_relative(root, projection),
            ))

    evidence = status.get("evidence") if isinstance(status.get("evidence"), dict) else {}
    extraction = evidence.get("data_extraction_queue")
    if isinstance(extraction, dict):
        if _projection_only(
            extraction.get("issues"), EXTRACTION_PROJECTION_ISSUE_PREFIXES
        ):
            try:
                extraction_paths = _safe_extraction_paths(root, task_id)
            except (OSError, RecoveryError, ValueError):
                extraction_paths = {}
            if extraction_paths:
                actions.append(_action(
                    "rebuild_extraction_projection",
                    status=status,
                    task_id=task_id,
                    target=_relative(root, extraction_paths["queue_csv"]),
                ))
    return actions


def _assert_action_still_current(root: Path, action: dict[str, Any]) -> dict[str, Any]:
    """Re-read workflow evidence and require the exact repair plan to survive."""
    task_id = str(action["task_id"])
    batch_id = str(action.get("batch_id") or "") or None
    status = workflow_status.inspect(root, task_id=task_id, batch_id=batch_id)
    if _selected_task(status, task_id) != task_id:
        raise RecoveryError("selected task changed before recovery")
    candidates = plan_safe_actions(
        root, status, task_id=task_id, batch_id=batch_id,
    )
    current = next(
        (item for item in candidates if item["action_id"] == action["action_id"]),
        None,
    )
    if current is None or current.get("budget_keys") != action.get("budget_keys"):
        raise RecoveryError("recovery evidence changed before execution")
    return status


def _execute_action(root: Path, action: dict[str, Any]) -> dict[str, Any]:
    _assert_action_still_current(root, action)
    kind = action["kind"]
    task_id = str(action["task_id"])
    batch_id = str(action.get("batch_id") or "")
    if kind == "rebuild_document_processing_projection":
        _, projection = _safe_document_paths(root, batch_id)
        if _relative(root, projection) != action["target"]:
            raise RecoveryError("document-processing repair target changed")
        _export_document_projection_pinned(root, batch_id)
        return {"projection_rebuilt": True}

    if kind == "rebuild_extraction_projection":
        paths = _safe_extraction_paths(root, task_id)
        if _relative(root, paths["queue_csv"]) != action["target"]:
            raise RecoveryError("extraction projection target changed")
        extraction_common.export_queue_projection(paths)
        return {"projection_rebuilt": True}

    raise RecoveryError(f"unsupported recovery action: {kind}")


def _classification(status: dict[str, Any], actions: list[dict[str, Any]]) -> str:
    if actions:
        return "safe_repair_available"
    stage = str(status.get("stage") or "")
    if stage.startswith("COMPLETE"):
        return "healthy"
    if any(marker in stage for marker in ABNORMAL_STAGE_MARKERS):
        return "manual_required"
    return "workflow_action_required"


def _new_dangerous_stage(before: dict[str, Any], after: dict[str, Any]) -> bool:
    before_stage = str(before.get("stage") or "")
    after_stage = str(after.get("stage") or "")
    before_markers = {value for value in DANGEROUS_STAGE_MARKERS if value in before_stage}
    after_markers = {value for value in DANGEROUS_STAGE_MARKERS if value in after_stage}
    return bool(after_markers - before_markers)


def _same_defect_remains(
    action: dict[str, Any], after_actions: list[dict[str, Any]],
) -> bool:
    expected = set(action.get("budget_keys") or [])
    return any(expected & set(item.get("budget_keys") or []) for item in after_actions)


def inspect_recovery(
    root: Path, *, task_id: str | None, batch_id: str | None,
) -> dict[str, Any]:
    status = workflow_status.inspect(root, task_id=task_id, batch_id=batch_id)
    selected_task = _selected_task(status, task_id)
    actions = plan_safe_actions(
        root, status, task_id=selected_task, batch_id=batch_id,
    )
    ledger_relative = Path("runtime/recovery") / _task_key(selected_task)
    try:
        with extraction_common.PinnedDirectory(root, ledger_relative) as ledger:
            events = _read_ledger(ledger)
    except FileNotFoundError:
        events = []
    attempted_ids, spent_keys = _attempted_bindings(events)
    eligible, exhausted = _eligible_actions(
        actions,
        attempted_action_ids=attempted_ids,
        spent_budget_keys=spent_keys,
    )
    classification = _classification(status, eligible)
    if exhausted and not eligible:
        classification = "manual_required"
    return {
        "version": RECOVERY_VERSION,
        "command": "status",
        "task_id": selected_task,
        "batch_id": _selected_batch(status, batch_id),
        "classification": classification,
        "safe_actions": eligible,
        "automatic_budget_exhausted": exhausted,
        "workflow_status": status,
    }


def _run_recovery_with_ledger(
    root: Path, *, selected_task: str, batch_id: str | None,
    max_actions: int, ledger: extraction_common.PinnedDirectory,
) -> dict[str, Any]:
    events = _read_ledger(ledger)
    attempted, spent_budget_keys = _attempted_bindings(events)
    attempts: list[dict[str, Any]] = []
    stop_reason = "no_safe_action"
    for ordinal in range(1, max_actions + 1):
        before = workflow_status.inspect(root, task_id=selected_task, batch_id=batch_id)
        candidates = plan_safe_actions(
            root, before, task_id=selected_task, batch_id=batch_id,
        )
        fresh, exhausted = _eligible_actions(
            candidates,
            attempted_action_ids=attempted,
            spent_budget_keys=spent_budget_keys,
        )
        if not fresh:
            stop_reason = (
                "automatic_repair_budget_exhausted" if exhausted else "no_safe_action"
            )
            break
        action = fresh[0]
        attempted.add(action["action_id"])
        spent_budget_keys.update(action["budget_keys"])
        started = {
            "version": RECOVERY_VERSION,
            "phase": "started",
            "timestamp": utc_now(),
            "ordinal": ordinal,
            "action_id": action["action_id"],
            "budget_keys": action["budget_keys"],
            "action": action["kind"],
            "task_id": selected_task,
            "batch_id": action.get("batch_id") or "",
            "stage_before": before.get("stage"),
            "evidence_sha256": action["evidence_sha256"],
            "target": action["target"],
        }
        _append_ledger(ledger, started)
        try:
            details = _execute_action(root, action)
            after = workflow_status.inspect(
                root, task_id=selected_task, batch_id=batch_id
            )
            after_actions = plan_safe_actions(
                root, after, task_id=selected_task, batch_id=batch_id,
            )
            changed = sha256_json(before) != sha256_json(after)
            defect_remains = _same_defect_remains(action, after_actions)
            worsened = _new_dangerous_stage(before, after)
            if defect_remains or worsened:
                outcome = "unresolved_or_worsened"
            else:
                outcome = "changed" if changed else "no_effect"
            finished = {
                **started,
                "phase": "finished",
                "timestamp": utc_now(),
                "result": outcome,
                "stage_after": after.get("stage"),
                "details": details,
                "postcondition": {
                    "defect_remains": defect_remains,
                    "new_dangerous_stage": worsened,
                    "status_changed": changed,
                },
            }
            _append_ledger(ledger, finished)
            attempts.append(finished)
            if outcome == "unresolved_or_worsened":
                stop_reason = "repair_unresolved_or_worsened"
                break
            if not changed:
                stop_reason = "repair_had_no_observable_effect"
                break
            stop_reason = "max_actions_reached"
        except Exception as exc:
            message = str(exc).replace(str(root), ".")[:1000]
            failed = {
                **started,
                "phase": "finished",
                "timestamp": utc_now(),
                "result": "failed",
                "error_type": exc.__class__.__name__,
                "error": message,
            }
            _append_ledger(ledger, failed)
            attempts.append(failed)
            stop_reason = "repair_failed_closed"
            break
    final = workflow_status.inspect(root, task_id=selected_task, batch_id=batch_id)
    remaining = plan_safe_actions(
        root, final, task_id=selected_task, batch_id=batch_id,
    )
    eligible_remaining, exhausted_remaining = _eligible_actions(
        remaining,
        attempted_action_ids=attempted,
        spent_budget_keys=spent_budget_keys,
    )
    classification = _classification(final, eligible_remaining)
    if exhausted_remaining and not eligible_remaining:
        classification = "manual_required"
    return {
        "version": RECOVERY_VERSION,
        "command": "run",
        "task_id": selected_task,
        "batch_id": _selected_batch(final, batch_id),
        "policy": "safe",
        "max_actions": max_actions,
        "attempts": attempts,
        "stop_reason": stop_reason,
        "classification": classification,
        "remaining_safe_actions": eligible_remaining,
        "automatic_budget_exhausted": exhausted_remaining,
        "ledger": (
            Path("runtime/recovery") / _task_key(selected_task) / "events.jsonl"
        ).as_posix(),
        "workflow_status": final,
    }


def run_recovery(
    root: Path, *, task_id: str | None, batch_id: str | None,
    max_actions: int,
) -> dict[str, Any]:
    if not 1 <= max_actions <= MAX_ACTIONS_HARD_LIMIT:
        raise RecoveryError(
            f"max_actions must be between 1 and {MAX_ACTIONS_HARD_LIMIT}"
        )
    with recovery_lock(root) as recovery_directory:
        initial = workflow_status.inspect(root, task_id=task_id, batch_id=batch_id)
        selected_task = _selected_task(initial, task_id)
        task_key = _task_key(selected_task)
        recovery_directory.mkdir(task_key)
        with extraction_common.PinnedDirectory(
            root, Path("runtime/recovery") / task_key,
        ) as ledger:
            return _run_recovery_with_ledger(
                root,
                selected_task=selected_task,
                batch_id=batch_id,
                max_actions=max_actions,
                ledger=ledger,
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "run"):
        command = commands.add_parser(name)
        command.add_argument("--root", default=".")
        command.add_argument("--task-id")
        command.add_argument("--batch-id")
        if name == "run":
            command.add_argument("--policy", choices=["safe"], default="safe")
            command.add_argument("--max-actions", type=int, default=3)
    args = parser.parse_args(argv)
    root = Path(args.root).expanduser().resolve()
    if not (root / "00_control/WORKSPACE.json").is_file():
        parser.error(f"not a paper download workspace: {root}")
    try:
        if args.command == "status":
            report = inspect_recovery(
                root, task_id=args.task_id, batch_id=args.batch_id,
            )
        else:
            report = run_recovery(
                root,
                task_id=args.task_id,
                batch_id=args.batch_id,
                max_actions=args.max_actions,
            )
    except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
        print(json.dumps({
            "version": RECOVERY_VERSION,
            "command": args.command,
            "error": str(exc).replace(str(root), ".")[:1000],
            "error_type": exc.__class__.__name__,
        }, ensure_ascii=False, indent=2))
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
