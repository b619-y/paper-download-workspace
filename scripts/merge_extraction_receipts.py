#!/usr/bin/env python3
"""Merge immutable cross-host extraction attempt receipts into the Mac queue.

Workers write ``runtime/results/<task>/data_extraction/<job>/receipts/
<attempt_id>.json``. Paths on the worker are deliberately not receipt
bindings: coordinator mapping uses document identity plus content hashes.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from extraction_common import (
    atomic_write_text, attempt_id_for, attempt_result_path, completion_receipt_path,
    document_identity, exclusive_lock,
    get_job, load_queue, parse_time, result_path, sha256_file, task_paths,
    secure_workspace_path, utc_now, write_queue,
)
from validate_extraction_result import write_validation


RECEIPT_KEYS = {
    "receipt_version", "task_id", "job_id", "paper_id", "batch_id", "item_id",
    "status", "source_sha256", "source_revalidation", "parsed_text_sha256",
    "sections_sha256", "sections_count", "sections_schema_version",
    "parser", "parser_version", "spec_version", "spec_sha256", "result_sha256",
    "model", "attempt_id", "run_id", "attempt_count", "max_attempts", "worker",
    "available_at", "error_code", "error_message", "completed_at",
}
RECEIPT_STATUSES = {"review", "retry_wait", "failed"}
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
RUN_ID_RE = re.compile(r"[0-9a-f]{32}\Z")


def read_receipt(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unreadable completion receipt {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"completion receipt must be an object: {path}")
    if set(value) != RECEIPT_KEYS:
        missing = sorted(RECEIPT_KEYS - set(value))
        unknown = sorted(set(value) - RECEIPT_KEYS)
        raise ValueError(f"completion receipt keys mismatch (missing={missing}, unknown={unknown})")
    if value.get("receipt_version") != "2.0" or value.get("status") not in RECEIPT_STATUSES:
        raise ValueError("unsupported completion receipt version/status")
    if parse_time(str(value.get("completed_at") or "")) is None:
        raise ValueError("completion receipt has invalid completed_at")
    run_id, attempt, maximum = value.get("run_id"), value.get("attempt_count"), value.get("max_attempts")
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise ValueError("completion receipt run_id is invalid")
    if (isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1
            or isinstance(maximum, bool) or not isinstance(maximum, int)
            or maximum < 1 or attempt > maximum):
        raise ValueError("completion receipt attempt counters are invalid")
    if value.get("attempt_id") != attempt_id_for(str(value.get("job_id") or ""), attempt, run_id):
        raise ValueError("completion receipt attempt_id does not bind job/run/attempt")
    for key in ("source_sha256", "parsed_text_sha256", "spec_sha256"):
        if not isinstance(value.get(key), str) or not SHA256_RE.fullmatch(value[key]):
            raise ValueError(f"completion receipt has invalid {key}")
    sections_sha = value.get("sections_sha256")
    sections_count = value.get("sections_count")
    sections_schema = value.get("sections_schema_version")
    if sections_sha:
        if (not isinstance(sections_sha, str) or not SHA256_RE.fullmatch(sections_sha)
                or isinstance(sections_count, bool) or not isinstance(sections_count, int)
                or sections_count <= 0 or not isinstance(sections_schema, str)
                or not sections_schema.strip()):
            raise ValueError("completion receipt section sidecar binding is invalid")
    elif sections_count != 0 or sections_schema != "":
        raise ValueError("completion receipt has a partial empty section sidecar binding")
    if value.get("source_revalidation") not in {"verified_local", "deferred_to_mac"}:
        raise ValueError("completion receipt has invalid source_revalidation")
    if not isinstance(value.get("worker"), str) or not value["worker"].strip():
        raise ValueError("completion receipt worker is missing")

    status = value["status"]
    if status == "review":
        if (not SHA256_RE.fullmatch(str(value.get("result_sha256") or ""))
                or not isinstance(value.get("model"), str) or not value["model"].strip()
                or value.get("available_at") or value.get("error_code") or value.get("error_message")):
            raise ValueError("review receipt result/model/error fields are inconsistent")
    else:
        if value.get("result_sha256") or value.get("model"):
            raise ValueError("failure receipt must not claim a result/model")
        if not isinstance(value.get("error_code"), str) or not value["error_code"].strip():
            raise ValueError("failure receipt must carry error_code")
        if status == "retry_wait":
            if parse_time(str(value.get("available_at") or "")) is None:
                raise ValueError("retry receipt has invalid available_at")
            if attempt >= maximum:
                raise ValueError("retry receipt has exhausted max_attempts")
        elif value.get("available_at"):
            raise ValueError("failed receipt must not carry available_at")
        if status == "failed" and attempt != maximum:
            raise ValueError("failed receipt has not exhausted max_attempts")
    return value


def check_bindings(receipt: dict[str, Any], job: dict[str, Any]) -> None:
    expected = {
        "task_id": job["task_id"], "job_id": job["job_id"],
        "spec_version": job["spec_version"], "spec_sha256": job["spec_sha256"],
        "max_attempts": job["max_attempts"], **document_identity(job),
    }
    for key, expected_value in expected.items():
        if receipt.get(key) != expected_value:
            raise ValueError(f"completion receipt binding mismatch: {key}")
    remote_state = receipt.get("source_revalidation")
    local_state = job.get("source_revalidation", "verified_local")
    if remote_state != local_state and not (
        remote_state == "deferred_to_mac" and local_state == "verified_local"
    ):
        raise ValueError("completion receipt binding mismatch: source_revalidation")


def _same_merged_receipt(job: dict[str, Any], receipt: dict[str, Any], digest: str) -> bool:
    return (
        job.get("receipt_sha256") == digest
        and job.get("attempt_id") == receipt["attempt_id"]
        and job.get("run_id") == receipt["run_id"]
        and job.get("attempt_count") == receipt["attempt_count"]
        and job.get("status") == receipt["status"]
    )


def _history_entry(receipt: dict[str, Any], digest: str) -> dict[str, Any]:
    return {
        "attempt_id": receipt["attempt_id"], "run_id": receipt["run_id"],
        "attempt_count": receipt["attempt_count"], "status": receipt["status"],
        "receipt_sha256": digest,
    }


def merge_one(paths: dict[str, Path], receipt_path: Path) -> str:
    root = paths["root"]
    safe = dict(paths)
    safe["task"] = secure_workspace_path(
        root, paths["task"], within=root / "runtime/tasks"
    )
    for key in ("extraction", "spec", "queue", "queue_csv", "queue_lock"):
        safe[key] = secure_workspace_path(root, paths[key], within=safe["task"])
    safe["results"] = secure_workspace_path(
        root, paths["results"], within=root / "runtime/results"
    )
    safe["extraction_results"] = secure_workspace_path(
        root, paths["extraction_results"], within=safe["results"]
    )
    receipt_path = secure_workspace_path(
        root, receipt_path, within=safe["extraction_results"], must_exist=True,
    )
    initial_hash = sha256_file(receipt_path)
    receipt = read_receipt(receipt_path)
    job_id = str(receipt["job_id"])
    canonical = secure_workspace_path(
        root,
        completion_receipt_path(safe, job_id, str(receipt["attempt_id"])),
        within=safe["extraction_results"],
        must_exist=True,
    )
    if receipt_path != canonical:
        raise ValueError(f"receipt is outside canonical attempt directory: {receipt_path}")

    with exclusive_lock(safe["queue_lock"]):
        # Recheck all mutable control paths after acquiring the queue lock.
        for key in ("spec", "queue", "queue_csv", "queue_lock"):
            safe[key] = secure_workspace_path(root, safe[key], within=safe["task"])
        receipt_path = secure_workspace_path(
            root, receipt_path, within=safe["extraction_results"], must_exist=True,
        )
        queue = load_queue(safe["queue"], expected_task_id=safe["task"].name)
        if queue["task_id"] != receipt["task_id"]:
            raise ValueError("completion receipt task_id does not match selected queue")
        job = get_job(queue, job_id)
        check_bindings(receipt, job)
        if sha256_file(receipt_path) != initial_hash:
            raise ValueError(f"completion receipt changed during merge: {job_id}")
        history = job.setdefault("receipt_history", [])
        prior = next(
            (row for row in history if row.get("attempt_id") == receipt["attempt_id"]),
            None,
        )
        expected_history = _history_entry(receipt, initial_hash)
        if prior is not None:
            if prior != expected_history:
                raise ValueError(f"immutable receipt conflicts with applied history: {job_id}")
            if receipt["status"] == "review":
                immutable_result = secure_workspace_path(
                    root,
                    attempt_result_path(safe, job_id, str(receipt["attempt_id"])),
                    within=safe["extraction_results"], must_exist=True,
                )
                result = secure_workspace_path(
                    root, result_path(safe, job_id), within=safe["extraction_results"]
                )
                if (not immutable_result.is_file()
                        or sha256_file(immutable_result) != receipt["result_sha256"]):
                    raise ValueError(f"already-merged review result is stale: {job_id}")
                if receipt["attempt_id"] == job.get("attempt_id") and (
                    not result.is_file() or sha256_file(result) != receipt["result_sha256"]
                ):
                    raise ValueError(f"already-merged canonical review result is stale: {job_id}")
            return "unchanged"
        if _same_merged_receipt(job, receipt, initial_hash):
            raise ValueError(f"latest receipt is missing from receipt_history: {job_id}")
        if job["status"] in {"review", "accepted", "failed", "skipped"}:
            raise ValueError(
                f"refusing a different/replayed receipt for terminal job {job_id} ({job['status']})"
            )
        active_recovery = False
        if job["status"] in {"claimed", "running"}:
            expiry = parse_time(str(job.get("lease_expires_at") or ""))
            active_recovery = (
                expiry is not None and expiry <= datetime.now(timezone.utc)
                and receipt["attempt_count"] == job["attempt_count"]
                and receipt["attempt_id"] == job.get("attempt_id")
                and receipt["run_id"] == job.get("run_id")
            )
            if not active_recovery:
                raise ValueError(f"local job is active/not mergeable: {job_id} ({job['status']})")
        elif job["status"] not in {"queued", "retry_wait"}:
            raise ValueError(f"local job is not mergeable: {job_id} ({job['status']})")
        expected_attempt = job["attempt_count"] if active_recovery else job["attempt_count"] + 1
        if receipt["attempt_count"] != expected_attempt:
            raise ValueError(
                f"receipt attempt is stale or skips history for {job_id}: "
                f"local={job['attempt_count']} receipt={receipt['attempt_count']}"
            )

        target = str(receipt["status"])
        attempt_binding = {
            "attempt_id": receipt["attempt_id"], "run_id": receipt["run_id"],
            "attempt_count": receipt["attempt_count"],
        }
        if target == "review":
            immutable_result = secure_workspace_path(
                root,
                attempt_result_path(safe, job_id, str(receipt["attempt_id"])),
                within=safe["extraction_results"], must_exist=True,
            )
            expected_hash = str(receipt["result_sha256"])
            if not immutable_result.is_file() or sha256_file(immutable_result) != expected_hash:
                raise ValueError(f"review receipt result SHA-256 mismatch: {job_id}")
            result_value = json.loads(immutable_result.read_text(encoding="utf-8"))
            if result_value.get("model") != receipt.get("model"):
                raise ValueError(f"review receipt model mismatch: {job_id}")
            from validate_extraction_result import validate
            errors, _, _ = validate(
                safe, job_id, require_source=True, attempt_binding=attempt_binding,
                result_file=immutable_result,
            )
            if errors:
                raise ValueError(f"remote result failed coordinator validation: {'; '.join(errors)}")
            if sha256_file(immutable_result) != expected_hash or sha256_file(receipt_path) != initial_hash:
                raise ValueError(f"remote result/receipt changed during coordinator validation: {job_id}")
            atomic_write_text(
                secure_workspace_path(
                    root, result_path(safe, job_id), within=safe["extraction_results"]
                ),
                immutable_result.read_text(encoding="utf-8"),
            )
            valid, errors = write_validation(
                safe, job_id, require_source=True, attempt_binding=attempt_binding
            )
            if not valid:
                raise ValueError(f"promoted result failed coordinator validation: {'; '.join(errors)}")
            if sha256_file(result_path(safe, job_id)) != expected_hash:
                raise ValueError(f"promoted canonical result hash mismatch: {job_id}")
            job["status"] = "review"
            job["error_code"] = ""
            job["error_message"] = ""
        else:
            if target == "retry_wait":
                job["available_at"] = str(receipt["available_at"])
            job["status"] = target
            job["error_code"] = str(receipt["error_code"])[:128]
            job["error_message"] = str(receipt["error_message"])[:1000]

        job.update(attempt_binding)
        job["claimed_by"] = ""
        job["lease_token"] = ""
        job["lease_expires_at"] = ""
        job["heartbeat_at"] = ""
        job["receipt_path"] = str(receipt_path.relative_to(safe["root"]))
        job["receipt_sha256"] = initial_hash
        history.append(expected_history)
        job["updated_at"] = utc_now()
        # Validation and canonical promotion may be slow.  Fence the immutable
        # inputs again at the final queue-commit boundary so a late sync/edit
        # cannot make the queue attest to bytes we did not validate.
        if (receipt_path.is_symlink() or not receipt_path.is_file()
                or sha256_file(receipt_path) != initial_hash):
            raise ValueError(f"completion receipt changed before queue commit: {job_id}")
        if target == "review":
            canonical_result = secure_workspace_path(
                root, result_path(safe, job_id), within=safe["extraction_results"],
                must_exist=True,
            )
            if (immutable_result.is_symlink() or not immutable_result.is_file()
                    or sha256_file(immutable_result) != expected_hash
                    or canonical_result.is_symlink() or not canonical_result.is_file()
                    or sha256_file(canonical_result) != expected_hash):
                raise ValueError(f"review result changed before queue commit: {job_id}")
        write_queue(safe, queue)
        return target


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--job-id", default="")
    parser.add_argument("--actor-role", required=True, choices=["coordinator"])
    args = parser.parse_args()
    paths = task_paths(Path(args.root), args.task_id)
    if args.job_id:
        receipts = list((paths["extraction_results"] / args.job_id / "receipts").glob("*.json"))
    else:
        receipts = list(paths["extraction_results"].glob("*/receipts/*.json"))
    if not receipts:
        parser.error("no immutable extraction attempt receipts found")
    counts: dict[str, int] = {}
    try:
        ordered = sorted(
            ((int(read_receipt(path)["attempt_count"]), path) for path in receipts),
            key=lambda item: (item[0], str(item[1])),
        )
        for _, path in ordered:
            status = merge_one(paths, path)
            counts[status] = counts.get(status, 0) + 1
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    print(f"{paths['queue']} " + " ".join(f"{key}={counts[key]}" for key in sorted(counts)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
