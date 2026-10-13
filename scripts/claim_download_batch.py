#!/usr/bin/env python3
"""Atomically claim one READY paper-download batch."""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
import os
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_ROOT = Path("/Users/b/Desktop/论文下载")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def update_index(root: Path, batch_id: str, worker: str, now: str) -> None:
    path = root / "00_control/BATCH_INDEX.csv"
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames or []
        rows = list(reader)
    matched = False
    for row in rows:
        if row["batch_id"] == batch_id:
            row["status"] = "CLAIMED"
            row["claimed_by"] = worker
            row["updated_at"] = now
            matched = True
    if not matched:
        raise SystemExit(f"batch missing from index: {batch_id}")
    temp = path.with_suffix(".csv.tmp")
    with temp.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_id")
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--worker", required=True)
    parser.add_argument("--host", default="local")
    parser.add_argument("--harness", default="DSH")
    args = parser.parse_args()
    root = Path(args.root).expanduser().resolve()
    batch = root / "02_batches" / args.batch_id
    status_path = batch / "STATUS.json"
    if not status_path.is_file():
        parser.error(f"unknown batch: {args.batch_id}")
    status = json.loads(status_path.read_text(encoding="utf-8"))
    if status.get("status") != "READY":
        parser.error(f"batch is not READY: {status.get('status')}")

    now = utc_now()
    claim_path = batch / "CLAIM.json"
    claim = {
        "batch_id": args.batch_id,
        "worker": args.worker,
        "host": args.host,
        "harness": args.harness,
        "claimed_at": now,
    }
    try:
        fd = os.open(claim_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        parser.error(f"batch already claimed: {claim_path.read_text(encoding='utf-8', errors='replace')}")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(claim, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    lock_path = root / "00_control/.queue.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        status["status"] = "CLAIMED"
        status["claimed_by"] = args.worker
        status["updated_at"] = now
        status["attempt"] = int(status.get("attempt", 0)) + 1
        temp = status_path.with_suffix(".json.tmp")
        temp.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(temp, status_path)
        update_index(root, args.batch_id, args.worker, now)
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    print(claim_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

