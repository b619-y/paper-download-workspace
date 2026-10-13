#!/usr/bin/env python3
"""Update one batch status and the shared index under a lock."""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import os
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_ROOT = Path("/Users/b/Desktop/论文下载")
WORKER_TRANSITIONS = {
    "CLAIMED": {"RUNNING", "BLOCKED", "FAILED"},
    "RUNNING": {"REVIEW", "BLOCKED", "FAILED"},
    "BLOCKED": {"RUNNING", "FAILED"},
}
COORDINATOR_TRANSITIONS = {
    "REVIEW": {"ACCEPTED", "RUNNING", "FAILED"},
    "BLOCKED": {"READY", "FAILED"},
    "FAILED": {"READY"},
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_id")
    parser.add_argument("status", choices=["READY", "RUNNING", "REVIEW", "ACCEPTED", "BLOCKED", "FAILED"])
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--actor-role", choices=["worker", "coordinator"], required=True)
    parser.add_argument("--worker", default="")
    parser.add_argument("--note", default="")
    args = parser.parse_args()
    root = Path(args.root).expanduser().resolve()
    batch = root / "02_batches" / args.batch_id
    status_path = batch / "STATUS.json"
    if not status_path.is_file():
        parser.error(f"unknown batch: {args.batch_id}")

    lock_path = root / "00_control/.queue.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        status = json.loads(status_path.read_text(encoding="utf-8"))
        current = status.get("status")
        allowed = WORKER_TRANSITIONS if args.actor_role == "worker" else COORDINATOR_TRANSITIONS
        if args.status not in allowed.get(current, set()):
            parser.error(f"invalid transition for {args.actor_role}: {current} -> {args.status}")
        if args.actor_role == "worker":
            if not args.worker or status.get("claimed_by") != args.worker:
                parser.error("worker identity does not match the batch claim")

        now = utc_now()
        status["status"] = args.status
        status["updated_at"] = now
        if args.note:
            status["last_note"] = args.note
        temp_status = status_path.with_suffix(".json.tmp")
        temp_status.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(temp_status, status_path)

        index_path = root / "00_control/BATCH_INDEX.csv"
        with index_path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames or []
            rows = list(reader)
        matched = False
        for row in rows:
            if row["batch_id"] == args.batch_id:
                row["status"] = args.status
                row["updated_at"] = now
                if args.status == "ACCEPTED":
                    row["accepted_at"] = now
                matched = True
        if not matched:
            parser.error(f"batch missing from index: {args.batch_id}")
        temp_index = index_path.with_suffix(".csv.tmp")
        with temp_index.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temp_index, index_path)
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    print(f"{args.batch_id} {current} -> {args.status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

