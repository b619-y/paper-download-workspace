#!/usr/bin/env python3
"""Build the human review queue from validated candidate extraction results."""

from __future__ import annotations

import argparse
import csv
import json
from io import StringIO
from pathlib import Path

from extraction_common import acceptance_path, atomic_write_text, exclusive_lock, load_queue, parse_time, result_path, sha256_file, sha256_json, task_paths, validation_path
from validate_extraction_result import validate, write_validation

FIELDS = [
    "review_id", "job_id", "row_kind", "record_id", "paper_id", "title", "doi",
    "candidate_json", "candidate_sha256", "evidence_summary", "source_path",
    "source_sha256", "parsed_text_path", "parsed_text_sha256", "spec_sha256",
    "sections_sha256", "sections_count", "sections_schema_version",
    "attempt_id", "run_id", "result_sha256", "validation_sha256", "receipt_sha256",
    "review_decision", "source_verified", "reviewer", "reviewed_at", "notes",
]

VALIDATION_FIELDS = {
    "validation_version", "task_id", "job_id", "attempt_id", "run_id",
    "source_sha256", "parsed_text_sha256", "sections_sha256", "sections_count",
    "sections_schema_version", "spec_sha256", "result_sha256", "valid", "errors",
    "validated_at",
}

DECISION_BINDING_FIELDS = {
    "row_kind", "record_id", "paper_id", "candidate_sha256", "source_path",
    "source_sha256", "parsed_text_path", "parsed_text_sha256", "spec_sha256",
    "sections_sha256", "sections_count", "sections_schema_version", "attempt_id",
    "run_id", "result_sha256", "validation_sha256", "receipt_sha256",
}


def validation_snapshot_matches(
    validation: object, job: dict[str, object], result_sha256: str,
) -> bool:
    if not isinstance(validation, dict) or set(validation) != VALIDATION_FIELDS:
        return False
    expected = {
        "validation_version": "2.0", "task_id": job["task_id"],
        "job_id": job["job_id"], "attempt_id": job.get("attempt_id", ""),
        "run_id": job.get("run_id", ""), "source_sha256": job["source_sha256"],
        "parsed_text_sha256": job["parsed_text_sha256"],
        "sections_sha256": job.get("sections_sha256", ""),
        "sections_count": job.get("sections_count", 0),
        "sections_schema_version": job.get("sections_schema_version", ""),
        "spec_sha256": job["spec_sha256"], "result_sha256": result_sha256,
        "valid": True, "errors": [],
    }
    return (
        all(validation.get(key) == value for key, value in expected.items())
        and parse_time(str(validation.get("validated_at") or "")) is not None
    )


def read_existing(path: Path) -> dict[str, dict[str, str]]:
    if not path.is_file(): return {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    identifiers = [row.get("review_id", "") for row in rows if row.get("review_id")]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("review_queue.csv contains duplicate review_id values")
    return {row.get("review_id", ""): row for row in rows if row.get("review_id")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--root", default="."); parser.add_argument("--task-id", required=True); parser.add_argument("--overwrite-decisions", action="store_true"); args = parser.parse_args()
    paths = task_paths(Path(args.root), args.task_id)
    try:
        # Global lock order is review -> queue for review generation,
        # acceptance and delivery snapshotting.
        with exclusive_lock(paths["review_lock"]), exclusive_lock(paths["queue_lock"]):
            queue = load_queue(paths["queue"], expected_task_id=args.task_id)
            before_hash = sha256_file(paths["review_queue"]) if paths["review_queue"].is_file() else ""
            existing = read_existing(paths["review_queue"])
            rows = []
            for job in queue["jobs"]:
                if job["status"] not in {"review", "accepted"}: continue
                if job["status"] == "accepted":
                    errors, _, _ = validate(paths, job["job_id"], require_source=True)
                    valid = not errors
                    if valid:
                        try:
                            validation = json.loads(validation_path(paths, job["job_id"]).read_text(encoding="utf-8"))
                            acceptance = json.loads(acceptance_path(paths, job["job_id"]).read_text(encoding="utf-8"))
                            validation_sha = sha256_file(validation_path(paths, job["job_id"]))
                            valid = validation_snapshot_matches(
                                validation, job,
                                sha256_file(result_path(paths, job["job_id"])),
                            ) and (
                                acceptance.get("status") == "accepted"
                                and acceptance.get("task_id") == job["task_id"]
                                and acceptance.get("job_id") == job["job_id"]
                                and acceptance.get("validation_sha256") == validation_sha
                            )
                            if not valid: errors = ["accepted job validation snapshot is stale"]
                        except (OSError, json.JSONDecodeError) as exc:
                            valid, errors = False, [f"accepted job validation is unreadable: {exc}"]
                else:
                    valid, errors = write_validation(paths, job["job_id"])
                if not valid: raise ValueError(f"invalid extraction result for {job['job_id']}: {'; '.join(errors)}")
                result_file = result_path(paths, job["job_id"])
                result = json.loads(result_file.read_text(encoding="utf-8")); records = result["records"] or [None]
                result_sha = sha256_file(result_file)
                validation_sha = sha256_file(validation_path(paths, job["job_id"]))
                for record in records:
                    row_kind = "record" if record is not None else "no_records"
                    record_id = record["record_id"] if record is not None else "NO_RECORDS"
                    candidate = record if record is not None else {"no_records_found": True, "absence_claim": False, "note": "Human acknowledgement does not mean target data are absent."}
                    candidate_json = json.dumps(candidate, ensure_ascii=False, sort_keys=True, separators=(",", ":")); candidate_sha = sha256_json(candidate)
                    review_id = f"{job['job_id']}::{record_id}"; old = existing.get(review_id, {})
                    evidence = []
                    if record:
                        for field in record["fields"]:
                            for citation in field["evidence"]: evidence.append(f"{field['field_id']}: {citation['exact_quote']}")
                    current = {
                        "review_id": review_id, "job_id": job["job_id"], "row_kind": row_kind,
                        "record_id": record_id, "paper_id": job["paper_id"], "title": job.get("title", ""), "doi": job.get("doi", ""),
                        "candidate_json": candidate_json, "candidate_sha256": candidate_sha,
                        "evidence_summary": " | ".join(evidence), "source_path": job["source_path"],
                        "source_sha256": job["source_sha256"], "parsed_text_path": job["parsed_text_path"],
                        "parsed_text_sha256": job["parsed_text_sha256"], "spec_sha256": job["spec_sha256"],
                        "sections_sha256": job.get("sections_sha256", ""),
                        "sections_count": str(job.get("sections_count", 0)),
                        "sections_schema_version": job.get("sections_schema_version", ""),
                        "attempt_id": job.get("attempt_id", ""), "run_id": job.get("run_id", ""),
                        "result_sha256": result_sha, "validation_sha256": validation_sha,
                        "receipt_sha256": job.get("receipt_sha256", ""),
                    }
                    preserve = (
                        not args.overwrite_decisions
                        and all(old.get(key, "") == str(current.get(key, ""))
                                for key in DECISION_BINDING_FIELDS)
                    )
                    current.update({
                        "review_decision": old.get("review_decision", "") if preserve else "",
                        "source_verified": old.get("source_verified", "") if preserve else "",
                        "reviewer": old.get("reviewer", "") if preserve else "",
                        "reviewed_at": old.get("reviewed_at", "") if preserve else "",
                        "notes": old.get("notes", "") if preserve else "",
                    })
                    rows.append(current)
            current_hash = sha256_file(paths["review_queue"]) if paths["review_queue"].is_file() else ""
            if current_hash != before_hash:
                raise ValueError("review_queue.csv changed while it was being regenerated")
            stream = StringIO(newline=""); writer = csv.DictWriter(stream, fieldnames=FIELDS); writer.writeheader(); writer.writerows(rows); atomic_write_text(paths["review_queue"], stream.getvalue())
    except (OSError, ValueError, csv.Error) as exc:
        parser.error(str(exc))
    print(f"{paths['review_queue']} rows={len(rows)}"); return 0


if __name__ == "__main__": raise SystemExit(main())
