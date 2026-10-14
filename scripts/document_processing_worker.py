#!/usr/bin/env python3
"""Claim and advance PDF/XML document-processing jobs without long polling."""
from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import threading
from pathlib import Path

import fulltext_parsers as parsers
import processing_queue as queue


class LeaseHeartbeat:
    """Renew a job lease during blocking I/O and expose a fencing guard."""

    def __init__(
        self, db_path: Path, job_id: str, lease_token: str, worker_id: str,
        lease_seconds: int, *, interval_seconds: float | None = None,
    ) -> None:
        self.db_path = db_path
        self.job_id = job_id
        self.lease_token = lease_token
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.interval_seconds = (
            interval_seconds if interval_seconds is not None
            else max(1.0, min(30.0, lease_seconds / 3.0))
        )
        self._stop = threading.Event()
        self._failure: BaseException | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "LeaseHeartbeat":
        self._thread = threading.Thread(
            target=self._run,
            name=f"document-processing-heartbeat-{self.job_id}",
            daemon=True,
        )
        self._thread.start()
        return self

    def _run(self) -> None:
        db = queue.connect(self.db_path)
        try:
            queue.initialize(db)
            while not self._stop.wait(self.interval_seconds):
                try:
                    queue.heartbeat(
                        db, self.job_id, self.lease_token, self.worker_id,
                        lease_seconds=self.lease_seconds,
                    )
                except BaseException as exc:  # surfaced synchronously by guard()
                    self._failure = exc
                    self._stop.set()
                    return
        finally:
            db.close()

    def guard(self) -> None:
        if self._failure is not None:
            raise RuntimeError(
                f"lease heartbeat lost for {self.job_id}: {self._failure}") from self._failure
        db = queue.connect(self.db_path)
        try:
            queue.assert_lease(
                db, self.job_id, self.lease_token, self.worker_id,
            )
        finally:
            db.close()

    def publish(self, temporary: Path, destination: Path) -> None:
        """Fence final artifact promotion inside the queue write transaction."""
        if self._failure is not None:
            raise RuntimeError(
                f"lease heartbeat lost for {self.job_id}: {self._failure}") from self._failure
        db = queue.connect(self.db_path)
        try:
            db.execute("BEGIN IMMEDIATE")
            queue.assert_lease(db, self.job_id, self.lease_token, self.worker_id)
            os.replace(temporary, destination)
            db.commit()
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise
        finally:
            db.close()

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_seconds + 1.0))
        if exc_type is None:
            self.guard()


def run_worker(
    root: Path, batch_id: str, *, worker_id: str, lease_seconds: int = 900,
    max_jobs: int = 0, external_upload_authorized: bool = False,
    model_version: str = "vlm", enable_table: bool = True,
    enable_formula: bool = True, language: str = "en",
) -> dict[str, object]:
    root = root.expanduser().resolve()
    db_path = queue.database_path(root, batch_id)
    if not db_path.is_file():
        raise ValueError(
            f"document-processing queue is missing: {db_path}; run make_document_processing_queue.py first")
    db = queue.connect(db_path)
    processed = 0
    outcomes: dict[str, int] = {}
    try:
        queue.initialize(db)
        resolved_codes = set()
        if external_upload_authorized:
            resolved_codes.add("external_upload_not_authorized")
        if any((os.environ.get(name) or "").strip() for name in parsers.MINERU_TOKEN_ENV_WHITELIST):
            resolved_codes.add("missing_mineru_token")
        queue.requeue_blocked_by_code(db, resolved_codes, worker_id)
        while max_jobs <= 0 or processed < max_jobs:
            job = queue.claim_one(
                db, worker_id, host=socket.gethostname(), lease_seconds=lease_seconds)
            if job is None:
                break
            processed += 1
            job_id = job["job_id"]
            lease_token = job["lease_token"]
            try:
                queue.mark_running(db, job_id, lease_token, worker_id)
                queue.heartbeat(
                    db, job_id, lease_token, worker_id, lease_seconds=lease_seconds)
                source = queue.resolve_stored_path(root, job["source_path"])
                output = queue.resolve_stored_path(root, job["output_dir"])
                with LeaseHeartbeat(
                    db_path, job_id, lease_token, worker_id, lease_seconds,
                ) as heartbeat:
                    if job["processor"] == "mineru":
                        outcome = parsers.mineru_step(
                            source=source,
                            expected_sha256=job["source_sha256"],
                            output_dir=output,
                            job_id=job_id,
                            document_id=job["document_id"],
                            paper_id=job["paper_id"],
                            processor_version=job["processor_version"],
                            external_upload_authorized=external_upload_authorized,
                            model_version=model_version,
                            enable_table=enable_table,
                            enable_formula=enable_formula,
                            language=language,
                            lease_guard=heartbeat.guard,
                        )
                    elif job["processor"] == "xml_to_markdown":
                        outcome = parsers.xml_to_markdown_step(
                            source=source,
                            expected_sha256=job["source_sha256"],
                            output_dir=output,
                            job_id=job_id,
                            document_id=job["document_id"],
                            paper_id=job["paper_id"],
                            processor_version=job["processor_version"],
                            lease_guard=heartbeat.guard,
                        )
                    else:
                        outcome = parsers.ParserOutcome(
                            "blocked", "unsupported_processor",
                            f"unsupported processor {job['processor']!r}",
                        )
                    heartbeat.guard()

                if outcome.status == "waiting_remote":
                    queue.mark_waiting_remote(
                        db, job_id, lease_token, worker_id,
                        poll_after_seconds=outcome.poll_after_seconds,
                        code=outcome.code,
                        message=parsers.redact(outcome.message),
                    )
                elif outcome.status == "review":
                    queue.mark_review(
                        db, job_id, lease_token, worker_id,
                        parsed_text_path=queue.portable_path(root, Path(outcome.parsed_text_path)),
                        parsed_text_sha256=outcome.parsed_text_sha256,
                        sections_path=queue.portable_path(root, Path(outcome.sections_path)),
                        sections_sha256=outcome.sections_sha256,
                        sections_count=outcome.sections_count,
                        state_path=queue.portable_path(root, Path(outcome.state_path)),
                        state_sha256=outcome.state_sha256,
                        result_path=queue.portable_path(root, Path(outcome.result_path)),
                        result_sha256=outcome.result_sha256,
                    )
                elif outcome.status == "blocked":
                    queue.mark_blocked(
                        db, job_id, lease_token, worker_id,
                        code=outcome.code, message=parsers.redact(outcome.message),
                    )
                elif outcome.status == "failed":
                    queue.mark_failure(
                        db, job_id, lease_token, worker_id,
                        code=outcome.code or "parser_failed",
                        message=parsers.redact(outcome.message),
                        retryable=outcome.retryable,
                    )
                else:
                    raise RuntimeError(f"unknown parser outcome: {outcome.status}")
                outcomes[outcome.status] = outcomes.get(outcome.status, 0) + 1
                print(json.dumps({"job_id": job_id, "outcome": outcome.status}, ensure_ascii=False))
            except Exception as exc:  # one broken document must not stop other jobs
                message = parsers.redact(f"{type(exc).__name__}: {exc}")
                try:
                    if job.get("claim_origin") == "waiting_remote":
                        # Polling an already-submitted remote job is not a new
                        # parse attempt.  Preserve both its checkpoint state and
                        # attempt budget when local/network code fails unexpectedly.
                        queue.mark_waiting_remote(
                            db, job_id, lease_token, worker_id,
                            poll_after_seconds=60,
                            code="worker_exception", message=message,
                        )
                    else:
                        queue.mark_failure(
                            db, job_id, lease_token, worker_id,
                            code="worker_exception", message=message, retryable=True,
                        )
                except RuntimeError:
                    # A stale lease must never overwrite a recovered worker's state.
                    pass
                outcomes["worker_exception"] = outcomes.get("worker_exception", 0) + 1
                print(json.dumps({"job_id": job_id, "outcome": "worker_exception", "error": message}, ensure_ascii=False))
            finally:
                queue.export_projection(db, queue.projection_path(root, batch_id))
    finally:
        counts = queue.status_counts(db)
        queue.export_projection(db, queue.projection_path(root, batch_id))
        db.close()
    return {"batch_id": batch_id, "processed": processed, "outcomes": outcomes, "status_counts": counts}


def validate_retry_policy(
    job: sqlite3.Row | dict[str, object], policy: str, *, upload_authorized: bool,
) -> None:
    """Require the recovery action that corresponds to the audited failure."""
    processor = str(job["processor"])
    error_code = str(job["error_code"] or "")
    required = {
        "archive_url_expired": "refresh-archive",
        "remote_parse_failed": "resubmit",
        "submit_outcome_unknown": "resubmit",
    }.get(error_code)
    if required and policy != required:
        raise ValueError(f"error {error_code!r} requires --retry-policy {required}")
    if policy == "resume":
        return
    if processor != "mineru":
        raise ValueError(f"--retry-policy {policy} is only valid for MinerU jobs")
    if policy == "refresh-archive" and error_code != "archive_url_expired":
        raise ValueError("--retry-policy refresh-archive requires error archive_url_expired")
    if policy == "resubmit" and error_code not in {
        "remote_parse_failed", "submit_outcome_unknown",
    }:
        raise ValueError(
            "--retry-policy resubmit requires remote_parse_failed or submit_outcome_unknown")
    if policy == "resubmit" and not upload_authorized:
        raise ValueError("--retry-policy resubmit requires --external-upload-authorized")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch_id")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--worker-id", default=f"{socket.gethostname()}-{os.getpid()}")
    parser.add_argument("--lease-seconds", type=int, default=900)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--once", action="store_true", help="claim at most one due job")
    group.add_argument("--max-jobs", type=int, default=0, help="maximum claims; 0 means until no job is due")
    parser.add_argument(
        "--external-upload-authorized", action="store_true",
        help="confirm that accepted PDFs may be uploaded to the configured MinerU service",
    )
    parser.add_argument("--model-version", choices=("vlm", "pipeline"), default="vlm")
    parser.add_argument("--disable-table", action="store_true")
    parser.add_argument("--disable-formula", action="store_true")
    parser.add_argument("--language", default="en")
    parser.add_argument("--retry-job", help="coordinator-only job ID to grant one audited retry")
    parser.add_argument("--retry-reason", help="non-empty diagnosis/correction recorded with --retry-job")
    parser.add_argument(
        "--retry-policy", choices=("resume", "refresh-archive", "resubmit"),
        default="resume",
        help="remote checkpoint policy for --retry-job; resubmit requires upload authorization",
    )
    parser.add_argument("--actor-role", choices=("coordinator", "worker"), default="worker")
    args = parser.parse_args()
    if args.lease_seconds < 30:
        parser.error("--lease-seconds must be at least 30")
    if args.max_jobs < 0:
        parser.error("--max-jobs cannot be negative")
    if args.retry_job:
        if args.actor_role != "coordinator":
            parser.error("--retry-job requires --actor-role coordinator")
        if not (args.retry_reason or "").strip():
            parser.error("--retry-job requires --retry-reason")
        db_path = queue.database_path(Path(args.root).expanduser().resolve(), args.batch_id)
        if not db_path.is_file():
            parser.error(f"document-processing queue is missing: {db_path}")
        db = queue.connect(db_path)
        try:
            queue.initialize(db)
            try:
                current = db.execute(
                    "SELECT * FROM jobs WHERE job_id=?", (args.retry_job,),
                ).fetchone()
                if current is None:
                    raise KeyError(args.retry_job)
                validate_retry_policy(
                    current, args.retry_policy,
                    upload_authorized=args.external_upload_authorized,
                )
                if args.retry_policy != "resume":
                    parsers.prepare_mineru_retry(
                        queue.resolve_stored_path(
                            Path(args.root).expanduser().resolve(), current["output_dir"]),
                        job_id=current["job_id"],
                        source_sha256=current["source_sha256"],
                        policy=args.retry_policy,
                        actor=args.worker_id,
                        reason=args.retry_reason,
                    )
                job = queue.retry_job(
                    db, args.retry_job, args.worker_id, args.retry_reason,
                    policy=args.retry_policy,
                )
            except (KeyError, ValueError, RuntimeError, sqlite3.Error) as exc:
                parser.error(str(exc))
            queue.export_projection(
                db, queue.projection_path(Path(args.root).expanduser().resolve(), args.batch_id),
            )
        finally:
            db.close()
        print(json.dumps({
            "batch_id": args.batch_id,
            "job_id": args.retry_job,
            "status": job["status"],
            "attempts": job["attempts"],
            "max_attempts": job["max_attempts"],
        }, ensure_ascii=False, indent=2))
        return 0
    try:
        report = run_worker(
            Path(args.root), args.batch_id, worker_id=args.worker_id,
            lease_seconds=args.lease_seconds,
            max_jobs=1 if args.once else args.max_jobs,
            external_upload_authorized=args.external_upload_authorized,
            model_version=args.model_version,
            enable_table=not args.disable_table,
            enable_formula=not args.disable_formula,
            language=args.language,
        )
    except (ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
