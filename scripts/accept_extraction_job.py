#!/usr/bin/env python3
"""Coordinator-only human-review acceptance gate for one extraction job."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from extraction_common import (
    acceptance_path, atomic_write_json, exclusive_lock, get_job, load_queue,
    parse_time, resolve_path, result_path, sha256_file, sha256_json, task_paths, utc_now,
    validation_path, write_queue,
)
from validate_extraction_result import validate as validate_result, write_validation


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--root", default="."); parser.add_argument("--task-id", required=True); parser.add_argument("--job-id", required=True); parser.add_argument("--actor-role", required=True, choices=["coordinator"]); args = parser.parse_args()
    paths = task_paths(Path(args.root), args.task_id)
    try:
        # Stable lock order prevents review regeneration and queue transitions
        # from interleaving with an acceptance snapshot.
        with exclusive_lock(paths["review_lock"]), exclusive_lock(paths["queue_lock"]):
            queue = load_queue(paths["queue"], expected_task_id=args.task_id)
            job = get_job(queue, args.job_id)
            queue_snapshot_sha = sha256_file(paths["queue"])
            if job["status"] not in {"review", "accepted"}:
                raise ValueError(f"job is not reviewable: {job['status']}")
            result_file = result_path(paths, args.job_id)
            validation_file = validation_path(paths, args.job_id)
            marker_file = acceptance_path(paths, args.job_id)
            if job["status"] == "review":
                valid, errors = write_validation(paths, args.job_id, require_source=True)
                if not valid:
                    raise ValueError("invalid extraction result: " + "; ".join(errors))
            else:
                errors, _, _ = validate_result(
                    paths, args.job_id, require_source=True,
                )
                if errors:
                    raise ValueError("accepted extraction snapshot is stale: " + "; ".join(errors))
            result = json.loads(result_file.read_text(encoding="utf-8"))
            validation = json.loads(validation_file.read_text(encoding="utf-8"))
            review_file_sha = sha256_file(paths["review_queue"])
            with paths["review_queue"].open(encoding="utf-8-sig", newline="") as handle:
                rows = [row for row in csv.DictReader(handle) if row.get("job_id") == args.job_id]
            expected_ids = {record["record_id"] for record in result["records"]} or {"NO_RECORDS"}
            if {row.get("record_id") for row in rows} != expected_ids or len(rows) != len(expected_ids):
                raise ValueError("review queue does not exactly cover current result")
            result_hash = sha256_file(result_file)
            validation_hash = sha256_file(validation_file)
            accepted: list[str] = []
            rejected: list[str] = []
            for row in rows:
                candidate = next((record for record in result["records"] if record["record_id"] == row["record_id"]), None)
                expected_candidate = candidate if candidate else {"no_records_found": True, "absence_claim": False, "note": "Human acknowledgement does not mean target data are absent."}
                expected_json = json.dumps(expected_candidate, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                expected_evidence = " | ".join(
                    f"{field['field_id']}: {citation['exact_quote']}"
                    for field in (candidate or {}).get("fields", [])
                    for citation in field["evidence"]
                )
                if (row.get("candidate_sha256") != sha256_json(expected_candidate)
                        or row.get("candidate_json") != expected_json
                        or row.get("evidence_summary", "") != expected_evidence):
                    raise ValueError(f"tampered/stale review row {row.get('review_id')}")
                bindings = {
                    "paper_id": job["paper_id"], "source_path": job["source_path"],
                    "source_sha256": job["source_sha256"],
                    "parsed_text_path": job["parsed_text_path"],
                    "parsed_text_sha256": job["parsed_text_sha256"],
                    "sections_sha256": job.get("sections_sha256", ""),
                    "sections_count": str(job.get("sections_count", 0)),
                    "sections_schema_version": job.get("sections_schema_version", ""),
                    "spec_sha256": job["spec_sha256"],
                    "attempt_id": job.get("attempt_id", ""), "run_id": job.get("run_id", ""),
                    "result_sha256": result_hash, "validation_sha256": validation_hash,
                    "receipt_sha256": job.get("receipt_sha256", ""),
                }
                if any(row.get(key, "") != expected for key, expected in bindings.items()):
                    raise ValueError(f"stale review bindings in {row.get('review_id')}")
                decision = (row.get("review_decision") or "").strip().casefold()
                if decision not in {"accept", "reject"}:
                    raise ValueError(f"review row {row.get('review_id')} needs accept/reject")
                if row.get("record_id") == "NO_RECORDS" and decision != "accept":
                    raise ValueError("NO_RECORDS must be affirmatively acknowledged")
                if (not (row.get("reviewer") or "").strip()
                        or parse_time(row.get("reviewed_at", "")) is None):
                    raise ValueError(f"review row {row.get('review_id')} lacks reviewer/time")
                if decision == "accept" and (row.get("source_verified") or "").strip().casefold() != "true":
                    raise ValueError(f"accepted row {row.get('review_id')} was not source-verified")
                (accepted if decision == "accept" else rejected).append(row["record_id"])
            review_rows_hash = sha256_json(rows)
            expected_validation = {
                "validation_version": "2.0", "task_id": args.task_id,
                "job_id": args.job_id, "attempt_id": job.get("attempt_id"),
                "run_id": job.get("run_id"), "source_sha256": job["source_sha256"],
                "parsed_text_sha256": job["parsed_text_sha256"],
                "sections_sha256": job.get("sections_sha256", ""),
                "sections_count": job.get("sections_count", 0),
                "sections_schema_version": job.get("sections_schema_version", ""),
                "spec_sha256": job["spec_sha256"], "result_sha256": result_hash,
                "valid": True,
            }
            if any(validation.get(key) != value for key, value in expected_validation.items()):
                raise ValueError("validation.json does not bind the reviewed snapshot")
            source = resolve_path(paths["root"], job["source_path"])
            parsed = resolve_path(paths["root"], job["parsed_text_path"])
            if not source.is_file() or sha256_file(source) != job["source_sha256"]:
                raise ValueError("original PDF/XML is missing or changed at acceptance")
            if not parsed.is_file() or sha256_file(parsed) != job["parsed_text_sha256"]:
                raise ValueError("parsed Markdown is missing or changed at acceptance")
            if job.get("sections_path"):
                sections = resolve_path(paths["root"], job["sections_path"])
                if not sections.is_file() or sha256_file(sections) != job["sections_sha256"]:
                    raise ValueError("section chunk sidecar is missing or changed at acceptance")
            receipt = resolve_path(paths["root"], str(job.get("receipt_path") or ""))
            if (not receipt.is_file()
                    or sha256_file(receipt) != job.get("receipt_sha256")):
                raise ValueError("current review receipt is missing or changed at acceptance")
            # Advisory locks do not stop a spreadsheet/editor or arbitrary
            # process from replacing files. Rehash the complete snapshot at the
            # last possible point before publishing the acceptance marker.
            if sha256_file(paths["queue"]) != queue_snapshot_sha:
                raise ValueError("queue changed during human acceptance")
            if sha256_file(paths["review_queue"]) != review_file_sha:
                raise ValueError("review_queue.csv changed during human acceptance")
            if (sha256_file(result_file) != result_hash
                    or sha256_file(validation_file) != validation_hash
                    or sha256_file(receipt) != job.get("receipt_sha256")
                    or sha256_file(source) != job["source_sha256"]
                    or sha256_file(parsed) != job["parsed_text_sha256"]
                    or (job.get("sections_path") and sha256_file(sections) != job["sections_sha256"])):
                raise ValueError("an accepted input changed during human acceptance")
            marker = {
                "acceptance_version": "2.0", "status": "accepted",
                "task_id": args.task_id, "job_id": args.job_id,
                "attempt_id": job.get("attempt_id", ""), "run_id": job.get("run_id", ""),
                "paper_id": job["paper_id"], "source_sha256": job["source_sha256"],
                "parsed_text_sha256": job["parsed_text_sha256"],
                "sections_sha256": job.get("sections_sha256", ""),
                "sections_count": job.get("sections_count", 0),
                "sections_schema_version": job.get("sections_schema_version", ""),
                "spec_sha256": job["spec_sha256"], "result_sha256": result_hash,
                "validation_sha256": validation_hash, "review_rows_sha256": review_rows_hash,
                "receipt_sha256": job.get("receipt_sha256", ""),
                "accepted_record_ids": sorted(accepted), "rejected_record_ids": sorted(rejected),
                "no_records_acknowledged": set(accepted) == {"NO_RECORDS"},
                "accepted_by": args.actor_role, "accepted_at": utc_now(),
            }
            if marker_file.is_file():
                existing = json.loads(marker_file.read_text(encoding="utf-8"))
                if ({key: value for key, value in existing.items() if key != "accepted_at"}
                        != {key: value for key, value in marker.items() if key != "accepted_at"}):
                    raise ValueError("existing acceptance marker conflicts with current snapshot")
                marker = existing
            else:
                atomic_write_json(marker_file, marker)
            job["status"] = "accepted"; job["accepted_at"] = marker["accepted_at"]
            job["updated_at"] = marker["accepted_at"]
            job["acceptance_path"] = str(marker_file.relative_to(paths["root"]))
            write_queue(paths, queue)
        print(acceptance_path(paths, args.job_id)); return 0
    except (OSError, ValueError, json.JSONDecodeError, csv.Error) as exc: parser.error(str(exc))


if __name__ == "__main__": raise SystemExit(main())
