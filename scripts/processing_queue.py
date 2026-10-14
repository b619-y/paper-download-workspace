#!/usr/bin/env python3
"""SQLite primitives for the post-download document-processing queue.

The database is authoritative.  ``queue.csv`` is an atomically regenerated,
read-only projection for people and tools which should not write SQLite.
"""
from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import sqlite3
import socket
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator


SCHEMA_VERSION = "3"
STATUSES = (
    "queued", "claimed", "running", "waiting_remote", "retry_wait",
    "review", "accepted", "blocked", "failed",
)
ACTIVE_LEASE_STATUSES = ("claimed", "running")
CLAIMABLE_STATUSES = ("queued", "retry_wait", "waiting_remote")
PROCESSORS = {"pdf": "mineru", "xml": "xml_to_markdown"}
PROCESSOR_VERSIONS = {"mineru": "mineru-api-v4-queue-v1", "xml_to_markdown": "xml-local-v1"}

JOB_COLUMNS = [
    "job_id", "batch_id", "source_item_id", "paper_id", "document_id", "title", "doi",
    "source_kind", "source_path", "source_sha256", "processor", "processor_version",
    "output_dir", "parsed_text_path", "parsed_text_sha256", "sections_path",
    "sections_sha256", "sections_count", "state_path", "state_sha256",
    "result_path", "result_sha256", "status", "priority", "attempts", "max_attempts",
    "available_at", "claimed_by", "claimed_host", "lease_token", "lease_expires_at",
    "claim_origin", "heartbeat_at", "started_at", "finished_at", "reviewed_by", "reviewed_at",
    "accepted_at", "error_code", "error_message", "created_at", "updated_at",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def add_seconds(timestamp: str, seconds: int | float) -> str:
    moment = _parse_timestamp(timestamp)
    if moment is None:
        raise ValueError(f"invalid timestamp: {timestamp!r}")
    return (moment + timedelta(seconds=seconds)).isoformat(timespec="seconds")


def _parse_timestamp(value: str) -> datetime | None:
    """Parse a queue timestamp as an aware instant, accepting ISO-8601 offsets."""
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _lease_expired(expires_at: str, now: str) -> bool:
    expires = _parse_timestamp(expires_at)
    current = _parse_timestamp(now)
    return expires is None or current is None or expires <= current


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_doi(value: str) -> str:
    value = (value or "").strip().casefold()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if value.startswith(prefix):
            value = value[len(prefix):].strip()
            break
    return value


def make_document_id(doi: str, source_sha256: str) -> str:
    """Return a path-safe identity derived from DOI, otherwise source content."""
    normalized = canonical_doi(doi)
    if normalized:
        return "doi-" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]
    digest = source_sha256.strip().casefold()
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise ValueError("source_sha256 must be a 64-character hexadecimal digest")
    return "sha256-" + digest


def make_job_id(batch_id: str, source_item_id: str, processor: str) -> str:
    payload = json.dumps(
        [batch_id, source_item_id, processor], ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return "dp-" + hashlib.sha256(payload).hexdigest()[:32]


def queue_dir(root: Path, batch_id: str) -> Path:
    return root / "02_batches" / batch_id / "outputs" / "document_processing"


def database_path(root: Path, batch_id: str) -> Path:
    return queue_dir(root, batch_id) / "queue.sqlite3"


def projection_path(root: Path, batch_id: str) -> Path:
    return queue_dir(root, batch_id) / "queue.csv"


def portable_path(root: Path, path: Path) -> str:
    """Persist workspace paths relatively; preserve external paths explicitly."""
    root = root.resolve()
    path = path.expanduser().resolve()
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)


def resolve_stored_path(root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def artifact_output_dir(
    root: Path, source_kind: str, source_sha256: str, job_id: str = "",
) -> Path:
    if source_kind == "pdf":
        base = root / "06_mineru" / "by-sha256"
    elif source_kind == "xml":
        base = root / "07_xml_markdown" / "by-sha256"
    else:
        raise ValueError(f"unsupported source_kind: {source_kind}")
    path = base / source_sha256[:2] / source_sha256
    # The SHA directory is the content-address boundary.  A final deterministic
    # job segment prevents two accepted rows for the same bytes from racing on
    # job-bound state.json/result.json files.
    return path / job_id if job_id else path


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    return db


def initialize(db: sqlite3.Connection) -> None:
    statuses = ",".join(repr(status) for status in STATUSES)
    db.executescript(f"""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            batch_id TEXT NOT NULL,
            source_item_id TEXT NOT NULL,
            paper_id TEXT NOT NULL,
            document_id TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            doi TEXT NOT NULL DEFAULT '',
            source_kind TEXT NOT NULL CHECK(source_kind IN ('pdf','xml')),
            source_path TEXT NOT NULL,
            source_sha256 TEXT NOT NULL,
            processor TEXT NOT NULL CHECK(processor IN ('mineru','xml_to_markdown')),
            processor_version TEXT NOT NULL,
            output_dir TEXT NOT NULL,
            parsed_text_path TEXT NOT NULL DEFAULT '',
            parsed_text_sha256 TEXT NOT NULL DEFAULT '',
            sections_path TEXT NOT NULL DEFAULT '',
            sections_sha256 TEXT NOT NULL DEFAULT '',
            sections_count INTEGER NOT NULL DEFAULT 0 CHECK(sections_count >= 0),
            state_path TEXT NOT NULL DEFAULT '',
            state_sha256 TEXT NOT NULL DEFAULT '',
            result_path TEXT NOT NULL DEFAULT '',
            result_sha256 TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL CHECK(status IN ({statuses})),
            priority INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
            max_attempts INTEGER NOT NULL DEFAULT 3 CHECK(max_attempts > 0),
            available_at TEXT NOT NULL,
            claimed_by TEXT NOT NULL DEFAULT '',
            claimed_host TEXT NOT NULL DEFAULT '',
            lease_token TEXT NOT NULL DEFAULT '',
            lease_expires_at TEXT NOT NULL DEFAULT '',
            claim_origin TEXT NOT NULL DEFAULT '',
            heartbeat_at TEXT NOT NULL DEFAULT '',
            started_at TEXT NOT NULL DEFAULT '',
            finished_at TEXT NOT NULL DEFAULT '',
            reviewed_by TEXT NOT NULL DEFAULT '',
            reviewed_at TEXT NOT NULL DEFAULT '',
            accepted_at TEXT NOT NULL DEFAULT '',
            error_code TEXT NOT NULL DEFAULT '',
            error_message TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(batch_id, source_item_id, processor)
        );
        CREATE INDEX IF NOT EXISTS jobs_claimable
            ON jobs(status, available_at, priority DESC, created_at);
        CREATE INDEX IF NOT EXISTS jobs_batch_status ON jobs(batch_id, status);
        CREATE TABLE IF NOT EXISTS events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            event_type TEXT NOT NULL,
            from_status TEXT NOT NULL DEFAULT '',
            to_status TEXT NOT NULL DEFAULT '',
            actor TEXT NOT NULL DEFAULT '',
            details_json TEXT NOT NULL DEFAULT '{{}}',
            FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        );
        CREATE INDEX IF NOT EXISTS events_job ON events(job_id, event_id);
    """)
    current = db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
    current_version = str(current[0]) if current else ""
    if current_version == "1":
        # v2 adds deterministic, source-bound section chunks.  The migration is
        # additive for claimable jobs.  Review/accepted v1 rows cannot be
        # silently upgraded because they have no independently reviewed chunk
        # artifact; require an explicit coordinator backfill instead.
        derived = db.execute(
            "SELECT COUNT(*) FROM jobs WHERE status IN ('review','accepted')"
        ).fetchone()[0]
        if derived:
            raise RuntimeError(
                "document-processing schema v1 contains review/accepted jobs without "
                "section_chunks.jsonl; coordinator backfill is required before migration")
        columns = {str(row[1]) for row in db.execute("PRAGMA table_info(jobs)")}
        for statement, name in (
            ("ALTER TABLE jobs ADD COLUMN sections_path TEXT NOT NULL DEFAULT ''", "sections_path"),
            ("ALTER TABLE jobs ADD COLUMN sections_sha256 TEXT NOT NULL DEFAULT ''", "sections_sha256"),
            ("ALTER TABLE jobs ADD COLUMN sections_count INTEGER NOT NULL DEFAULT 0", "sections_count"),
        ):
            if name not in columns:
                db.execute(statement)
        db.execute("UPDATE metadata SET value='2' WHERE key='schema_version'")
        current_version = "2"
    if current_version == "2":
        columns = {str(row[1]) for row in db.execute("PRAGMA table_info(jobs)")}
        if "claim_origin" not in columns:
            db.execute("ALTER TABLE jobs ADD COLUMN claim_origin TEXT NOT NULL DEFAULT ''")
        db.execute("UPDATE metadata SET value=? WHERE key='schema_version'", (SCHEMA_VERSION,))
        current_version = SCHEMA_VERSION
    if current_version and current_version != SCHEMA_VERSION:
        raise RuntimeError(
            f"unsupported document-processing schema {current_version!r}; expected {SCHEMA_VERSION!r}")
    db.execute(
        "INSERT OR IGNORE INTO metadata(key,value) VALUES('schema_version',?)",
        (SCHEMA_VERSION,),
    )


@contextmanager
def immediate(db: sqlite3.Connection) -> Iterator[None]:
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        db.rollback()
        raise
    else:
        db.commit()


def _event(
    db: sqlite3.Connection, job_id: str, event_type: str, from_status: str,
    to_status: str, actor: str, details: dict[str, Any] | None = None,
    *, at: str | None = None,
) -> None:
    db.execute(
        """INSERT INTO events(job_id,occurred_at,event_type,from_status,to_status,actor,details_json)
           VALUES(?,?,?,?,?,?,?)""",
        (job_id, at or utc_now(), event_type, from_status, to_status, actor,
         json.dumps(details or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))),
    )


def insert_job(db: sqlite3.Connection, job: dict[str, Any], *, actor: str = "queue-builder") -> bool:
    """Insert an immutable source binding, or prove an existing job is identical."""
    required = {
        "job_id", "batch_id", "source_item_id", "paper_id", "document_id", "source_kind",
        "source_path", "source_sha256", "processor", "processor_version", "output_dir",
    }
    missing = sorted(required - job.keys())
    if missing:
        raise ValueError(f"job is missing required fields: {missing}")
    now = utc_now()
    payload = {column: "" for column in JOB_COLUMNS}
    payload.update({
        "status": "queued", "priority": 0, "attempts": 0, "max_attempts": 3,
        "available_at": now, "created_at": now, "updated_at": now,
    })
    payload.update(job)
    immutable = (
        "batch_id", "source_item_id", "paper_id", "document_id", "source_kind",
        "source_path", "source_sha256", "processor", "processor_version", "output_dir",
        "max_attempts",
    )
    with immediate(db):
        existing = db.execute("SELECT * FROM jobs WHERE job_id=?", (payload["job_id"],)).fetchone()
        if existing:
            changed = [name for name in immutable if str(existing[name]) != str(payload[name])]
            if changed:
                raise RuntimeError(
                    f"job {payload['job_id']} already exists with a different immutable binding: {changed}")
            return False
        columns = list(payload)
        db.execute(
            f"INSERT INTO jobs({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
            tuple(payload[column] for column in columns),
        )
        _event(db, payload["job_id"], "enqueued", "", "queued", actor,
               {"source_sha256": payload["source_sha256"]}, at=now)
    return True


def _recover_expired(db: sqlite3.Connection, *, now: str, actor: str) -> int:
    candidates = db.execute(
        """SELECT job_id,status,attempts,max_attempts,lease_expires_at,claim_origin
             FROM jobs
            WHERE status IN ('claimed','running')""",
    ).fetchall()
    rows = [row for row in candidates if _lease_expired(row["lease_expires_at"], now)]
    for row in rows:
        if row["claim_origin"] == "waiting_remote":
            target = "waiting_remote"
        else:
            target = "failed" if row["attempts"] >= row["max_attempts"] else "retry_wait"
        db.execute(
            """UPDATE jobs SET status=?, available_at=?, claimed_by='', claimed_host='',
                      lease_token='', lease_expires_at='', claim_origin='', heartbeat_at='',
                      error_code='lease_expired', error_message='worker lease expired', updated_at=?
                 WHERE job_id=?""",
            (target, now, now, row["job_id"]),
        )
        _event(db, row["job_id"], "lease_expired", row["status"], target, actor,
               {"expired_at": row["lease_expires_at"]}, at=now)
    return len(rows)


def claim_one(
    db: sqlite3.Connection, worker_id: str, *, host: str | None = None,
    lease_seconds: int = 900, now: str | None = None,
) -> dict[str, Any] | None:
    """Atomically recover expired leases and claim one due job.

    A remote poll does not consume a new parsing attempt; fresh and retry claims do.
    """
    if lease_seconds < 30:
        raise ValueError("lease_seconds must be at least 30")
    now = now or utc_now()
    current = _parse_timestamp(now)
    if current is None:
        raise ValueError(f"invalid claim timestamp: {now!r}")
    host = host or socket.gethostname()
    token = uuid.uuid4().hex
    with immediate(db):
        _recover_expired(db, now=now, actor=worker_id)
        candidates = db.execute(
            """SELECT * FROM jobs
                WHERE ((status IN ('queued','retry_wait') AND attempts < max_attempts)
                       OR status='waiting_remote')""",
        ).fetchall()
        due: list[tuple[int, datetime, datetime, str, sqlite3.Row]] = []
        for candidate in candidates:
            available = _parse_timestamp(candidate["available_at"])
            if available is None:
                raise RuntimeError(
                    f"job {candidate['job_id']} has an invalid available_at timestamp")
            if available > current:
                continue
            created = _parse_timestamp(candidate["created_at"]) or datetime.max.replace(
                tzinfo=timezone.utc)
            due.append((
                -int(candidate["priority"]), available, created,
                str(candidate["job_id"]), candidate,
            ))
        row = min(due, key=lambda item: item[:4])[4] if due else None
        if row is None:
            return None
        previous = row["status"]
        attempts = row["attempts"] + (1 if previous in ("queued", "retry_wait") else 0)
        expires = add_seconds(now, lease_seconds)
        changed = db.execute(
            """UPDATE jobs SET status='claimed', attempts=?, claimed_by=?, claimed_host=?,
                      lease_token=?, lease_expires_at=?, claim_origin=?, heartbeat_at=?,
                      started_at=CASE WHEN started_at='' THEN ? ELSE started_at END,
                      error_code='', error_message='', updated_at=?
                 WHERE job_id=? AND status=?""",
            (attempts, worker_id, host, token, expires, previous, now, now, now, row["job_id"], previous),
        )
        if changed.rowcount != 1:  # defensive; BEGIN IMMEDIATE should already serialize this
            raise RuntimeError("claim lost despite write transaction")
        _event(db, row["job_id"], "claimed", previous, "claimed", worker_id,
               {"lease_expires_at": expires, "attempts": attempts}, at=now)
        claimed = db.execute("SELECT * FROM jobs WHERE job_id=?", (row["job_id"],)).fetchone()
        return dict(claimed)


def _owned_transition(
    db: sqlite3.Connection, job_id: str, lease_token: str, target: str, actor: str,
    *, allowed: tuple[str, ...], fields: dict[str, Any] | None = None,
    event_type: str = "transition", details: dict[str, Any] | None = None,
) -> None:
    if target not in STATUSES:
        raise ValueError(f"invalid status: {target}")
    now = utc_now()
    with immediate(db):
        row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        if (row["lease_token"] != lease_token or row["status"] not in allowed
                or row["claimed_by"] != actor or _lease_expired(row["lease_expires_at"], now)):
            raise RuntimeError(f"stale or invalid lease for {job_id}")
        updates = dict(fields or {})
        updates.update({"status": target, "updated_at": now})
        if target not in ACTIVE_LEASE_STATUSES:
            updates.update({
                "claimed_by": "", "claimed_host": "", "lease_token": "",
                "lease_expires_at": "", "claim_origin": "", "heartbeat_at": "",
            })
        assignments = ",".join(f"{key}=?" for key in updates)
        db.execute(
            f"UPDATE jobs SET {assignments} WHERE job_id=?",
            (*updates.values(), job_id),
        )
        _event(db, job_id, event_type, row["status"], target, actor, details, at=now)


def mark_running(db: sqlite3.Connection, job_id: str, lease_token: str, actor: str) -> None:
    _owned_transition(
        db, job_id, lease_token, "running", actor, allowed=("claimed",),
        event_type="started",
    )


def heartbeat(
    db: sqlite3.Connection, job_id: str, lease_token: str, actor: str,
    *, lease_seconds: int = 900,
) -> None:
    if lease_seconds < 30:
        raise ValueError("lease_seconds must be at least 30")
    now = utc_now()
    with immediate(db):
        row = db.execute(
            "SELECT status,claimed_by FROM jobs WHERE job_id=? AND lease_token=?",
            (job_id, lease_token),
        ).fetchone()
        if (row is None or row["status"] not in ACTIVE_LEASE_STATUSES
                or row["claimed_by"] != actor):
            raise RuntimeError(f"stale or invalid lease for {job_id}")
        lease = db.execute(
            "SELECT lease_expires_at FROM jobs WHERE job_id=?", (job_id,),
        ).fetchone()
        if lease is None or _lease_expired(lease["lease_expires_at"], now):
            raise RuntimeError(f"stale or invalid lease for {job_id}")
        changed = db.execute(
            """UPDATE jobs SET heartbeat_at=?, lease_expires_at=?, updated_at=?
                 WHERE job_id=? AND lease_token=? AND status IN ('claimed','running')""",
            (now, add_seconds(now, lease_seconds), now, job_id, lease_token),
        )
        if changed.rowcount != 1:
            raise RuntimeError(f"stale or invalid lease for {job_id}")
        _event(db, job_id, "heartbeat", row["status"], row["status"], actor, {}, at=now)


def assert_lease(
    db: sqlite3.Connection, job_id: str, lease_token: str, actor: str,
    *, now: str | None = None,
) -> None:
    """Fence artifact promotion on the currently owned, unexpired lease."""
    now = now or utc_now()
    row = db.execute(
        """SELECT status,claimed_by,lease_expires_at FROM jobs
             WHERE job_id=? AND lease_token=?""",
        (job_id, lease_token),
    ).fetchone()
    if (row is None or row["status"] not in ACTIVE_LEASE_STATUSES
            or row["claimed_by"] != actor
            or _lease_expired(row["lease_expires_at"], now)):
        raise RuntimeError(f"stale or invalid lease for {job_id}")


def mark_waiting_remote(
    db: sqlite3.Connection, job_id: str, lease_token: str, actor: str,
    *, poll_after_seconds: int = 30, code: str = "", message: str = "",
) -> None:
    now = utc_now()
    _owned_transition(
        db, job_id, lease_token, "waiting_remote", actor,
        allowed=("claimed", "running"),
        fields={
            "available_at": add_seconds(now, max(1, poll_after_seconds)),
            "error_code": code, "error_message": message,
        },
        event_type="remote_pending",
        details={"poll_after_seconds": poll_after_seconds, "code": code},
    )


def mark_review(
    db: sqlite3.Connection, job_id: str, lease_token: str, actor: str, *,
    parsed_text_path: str, parsed_text_sha256: str, sections_path: str,
    sections_sha256: str, sections_count: int, state_path: str, state_sha256: str,
    result_path: str, result_sha256: str,
) -> None:
    _owned_transition(
        db, job_id, lease_token, "review", actor, allowed=("claimed", "running"),
        fields={
            "parsed_text_path": parsed_text_path,
            "parsed_text_sha256": parsed_text_sha256,
            "sections_path": sections_path,
            "sections_sha256": sections_sha256,
            "sections_count": sections_count,
            "state_path": state_path,
            "state_sha256": state_sha256,
            "result_path": result_path,
            "result_sha256": result_sha256,
            "finished_at": utc_now(),
        },
        event_type="delivered",
        details={"parsed_text_sha256": parsed_text_sha256},
    )


def mark_blocked(
    db: sqlite3.Connection, job_id: str, lease_token: str, actor: str,
    *, code: str, message: str,
) -> None:
    _owned_transition(
        db, job_id, lease_token, "blocked", actor, allowed=("claimed", "running"),
        fields={"error_code": code, "error_message": message, "finished_at": utc_now()},
        event_type="blocked", details={"code": code},
    )


def mark_failure(
    db: sqlite3.Connection, job_id: str, lease_token: str, actor: str, *,
    code: str, message: str, retryable: bool, base_backoff_seconds: int = 60,
) -> str:
    row = db.execute("SELECT attempts,max_attempts FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    if row is None:
        raise KeyError(job_id)
    retry = retryable and row["attempts"] < row["max_attempts"]
    target = "retry_wait" if retry else "failed"
    backoff = min(24 * 3600, max(1, base_backoff_seconds) * (2 ** max(0, row["attempts"] - 1)))
    fields: dict[str, Any] = {
        "error_code": code, "error_message": message,
        "finished_at": "" if retry else utc_now(),
    }
    if retry:
        fields["available_at"] = add_seconds(utc_now(), backoff)
    _owned_transition(
        db, job_id, lease_token, target, actor, allowed=("claimed", "running"),
        fields=fields, event_type="attempt_failed",
        details={"code": code, "retryable": retryable, "backoff_seconds": backoff if retry else 0},
    )
    return target


def rows(db: sqlite3.Connection) -> list[dict[str, Any]]:
    return [dict(row) for row in db.execute(
        "SELECT * FROM jobs ORDER BY priority DESC, created_at, job_id")]


def export_projection(db: sqlite3.Connection, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    # Multiple workers may finish at nearly the same time.  Serialize the
    # read-and-replace operation so an older snapshot cannot win merely because
    # it wrote a larger CSV more slowly.
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        tmp = path.with_name(path.name + f".tmp-{uuid.uuid4().hex}")
        try:
            current_rows = rows(db)
            with tmp.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=JOB_COLUMNS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(current_rows)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def status_counts(db: sqlite3.Connection) -> dict[str, int]:
    counts = {status: 0 for status in STATUSES}
    for row in db.execute("SELECT status,COUNT(*) AS n FROM jobs GROUP BY status"):
        counts[row["status"]] = row["n"]
    return counts


def requeue_blocked_by_code(
    db: sqlite3.Connection, codes: set[str], actor: str,
) -> int:
    """Explicitly release resolvable blockers; never touches source-integrity blockers."""
    if not codes:
        return 0
    now = utc_now()
    placeholders = ",".join("?" for _ in codes)
    with immediate(db):
        blocked = db.execute(
            f"SELECT job_id,status,error_code FROM jobs WHERE status='blocked' "
            f"AND error_code IN ({placeholders})",
            tuple(sorted(codes)),
        ).fetchall()
        for row in blocked:
            db.execute(
                """UPDATE jobs SET status='queued', available_at=?, error_code='', error_message='',
                          finished_at='', attempts=CASE WHEN attempts>0 THEN attempts-1 ELSE 0 END,
                          updated_at=? WHERE job_id=?""",
                (now, now, row["job_id"]),
            )
            _event(db, row["job_id"], "blocker_resolved", "blocked", "queued", actor,
                   {"previous_error_code": row["error_code"]}, at=now)
    return len(blocked)


def retry_job(
    db: sqlite3.Connection, job_id: str, actor: str, reason: str, *, policy: str = "resume",
) -> dict[str, Any]:
    """Explicitly grant one audited retry after a coordinator diagnoses a job."""
    reason = " ".join(str(reason or "").split())
    if not reason:
        raise ValueError("retry reason must be non-empty")
    if policy not in {"resume", "refresh-archive", "resubmit"}:
        raise ValueError(f"unsupported retry policy: {policy}")
    now = utc_now()
    with immediate(db):
        row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        if row["status"] not in {"blocked", "failed", "retry_wait"}:
            raise ValueError(
                f"job {job_id} status {row['status']!r} is not explicitly retryable")
        max_attempts = max(int(row["max_attempts"]), int(row["attempts"]) + 1)
        changed = db.execute(
            """UPDATE jobs SET status='queued', available_at=?, max_attempts=?,
                      claimed_by='', claimed_host='', lease_token='', lease_expires_at='',
                      heartbeat_at='', finished_at='', error_code='', error_message='', updated_at=?
                 WHERE job_id=? AND status=?""",
            (now, max_attempts, now, job_id, row["status"]),
        )
        if changed.rowcount != 1:
            raise RuntimeError(f"concurrent status change for {job_id}")
        _event(
            db, job_id, "explicit_retry", row["status"], "queued", actor,
            {
                "reason": reason,
                "policy": policy,
                "previous_error_code": row["error_code"],
                "previous_attempts": row["attempts"],
                "max_attempts": max_attempts,
            },
            at=now,
        )
        return dict(db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone())
