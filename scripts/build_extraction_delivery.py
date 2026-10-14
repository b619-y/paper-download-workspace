#!/usr/bin/env python3
"""Build accepted-only CSV/JSON/SQLite/Frictionless extraction delivery."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import tempfile
from io import StringIO
from pathlib import Path
from typing import Any

from extraction_common import acceptance_path, atomic_write_json, atomic_write_text, exclusive_lock, field_map, load_queue, load_spec, resolve_path, result_path, sha256_file, sha256_json, task_paths, utc_now, validation_path
from validate_extraction_result import validate


def review_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle: return list(csv.DictReader(handle))


def write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    stream = StringIO(newline=""); writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore"); writer.writeheader(); writer.writerows(rows); atomic_write_text(path, stream.getvalue())


def build_sqlite(path: Path, records: list[dict[str, Any]], evidence_rows: list[dict[str, Any]]) -> None:
    fd, temporary = tempfile.mkstemp(prefix=".extraction.", suffix=".sqlite3", dir=path.parent); os.close(fd)
    try:
        connection = sqlite3.connect(temporary)
        connection.executescript("""
        CREATE TABLE records(record_id TEXT PRIMARY KEY, paper_id TEXT NOT NULL, job_id TEXT NOT NULL, record_json TEXT NOT NULL);
        CREATE TABLE field_values(record_id TEXT NOT NULL, field_id TEXT NOT NULL, value_json TEXT NOT NULL, unit TEXT, value_semantics TEXT NOT NULL, PRIMARY KEY(record_id,field_id));
        CREATE TABLE field_evidence(record_id TEXT NOT NULL, field_id TEXT NOT NULL, evidence_index INTEGER NOT NULL, exact_quote TEXT NOT NULL, locator TEXT NOT NULL, source_sha256 TEXT NOT NULL, parsed_text_sha256 TEXT NOT NULL, source_verified INTEGER NOT NULL, PRIMARY KEY(record_id,field_id,evidence_index));
        """)
        for record in records:
            connection.execute("INSERT INTO records VALUES(?,?,?,?)", (record["record_id"], record["paper_id"], record["job_id"], json.dumps(record, ensure_ascii=False, sort_keys=True)))
            for field in record["fields"]:
                connection.execute("INSERT INTO field_values VALUES(?,?,?,?,?)", (record["record_id"], field["field_id"], json.dumps(field["value"], ensure_ascii=False), field["unit"], field["value_semantics"]))
        for row in evidence_rows:
            connection.execute("INSERT INTO field_evidence VALUES(?,?,?,?,?,?,?,?)", (row["record_id"], row["field_id"], row["evidence_index"], row["exact_quote"], row["locator"], row["source_sha256"], row["parsed_text_sha256"], 1))
        connection.commit(); connection.close(); os.replace(temporary, path)
    except BaseException:
        try: os.unlink(temporary)
        except FileNotFoundError: pass
        raise


ACCEPTANCE_KEYS = {
    "acceptance_version", "status", "task_id", "job_id", "attempt_id", "run_id",
    "paper_id", "source_sha256", "parsed_text_sha256", "sections_sha256",
    "sections_count", "sections_schema_version", "spec_sha256", "result_sha256",
    "validation_sha256", "review_rows_sha256", "receipt_sha256",
    "accepted_record_ids", "rejected_record_ids", "no_records_acknowledged",
    "accepted_by", "accepted_at",
}


def _build_delivery(paths: dict[str, Path], args: argparse.Namespace) -> int:
    try:
        queue = load_queue(paths["queue"], expected_task_id=args.task_id); spec, spec_hash = load_spec(paths["spec"], args.task_id)
        queue_snapshot_sha = sha256_file(paths["queue"])
        review_snapshot_sha = sha256_file(paths["review_queue"])
        input_snapshots: dict[Path, str] = {}
        if queue["spec_sha256"] != spec_hash: raise ValueError("queue binds a stale extraction spec")
        unfinished = [job["job_id"] for job in queue["jobs"] if job["status"] not in {"accepted", "skipped"}]
        if unfinished: raise ValueError(f"jobs are not accepted/skipped: {unfinished}")
        all_review = review_rows(paths["review_queue"]); output_records, evidence_rows, accepted_jobs, job_bindings = [], [], [], []
        for job in queue["jobs"]:
            if job["status"] == "skipped": continue
            errors, result, _ = validate(paths, job["job_id"], require_source=True)
            if errors: raise ValueError(f"stale result {job['job_id']}: {'; '.join(errors)}")
            marker = json.loads(acceptance_path(paths, job["job_id"]).read_text(encoding="utf-8"))
            validation = json.loads(validation_path(paths, job["job_id"]).read_text(encoding="utf-8"))
            if set(marker) != ACCEPTANCE_KEYS or marker.get("acceptance_version") != "2.0":
                raise ValueError(f"acceptance marker schema mismatch: {job['job_id']}")
            current_result_sha = sha256_file(result_path(paths, job["job_id"]))
            if validation.get("valid") is not True or validation.get("result_sha256") != current_result_sha:
                raise ValueError(f"validation does not bind current result: {job['job_id']}")
            rows = [row for row in all_review if row.get("job_id") == job["job_id"]]
            checks = {
                "status": "accepted", "task_id": args.task_id, "job_id": job["job_id"],
                "attempt_id": job.get("attempt_id", ""), "run_id": job.get("run_id", ""),
                "source_sha256": job["source_sha256"],
                "parsed_text_sha256": job["parsed_text_sha256"],
                "sections_sha256": job.get("sections_sha256", ""),
                "sections_count": job.get("sections_count", 0),
                "sections_schema_version": job.get("sections_schema_version", ""),
                "spec_sha256": job["spec_sha256"],
                "result_sha256": sha256_file(result_path(paths, job["job_id"])),
                "validation_sha256": sha256_file(validation_path(paths, job["job_id"])),
                "review_rows_sha256": sha256_json(rows),
                "receipt_sha256": job.get("receipt_sha256", ""),
            }
            for key, value in checks.items():
                if marker.get(key) != value: raise ValueError(f"stale acceptance {job['job_id']}: {key}")
            expected_records = {record["record_id"]: record for record in result["records"]}
            expected_review_ids = set(expected_records) or {"NO_RECORDS"}
            if {row.get("record_id") for row in rows} != expected_review_ids or len(rows) != len(expected_review_ids):
                raise ValueError(f"review rows do not cover current result: {job['job_id']}")
            for row in rows:
                record = expected_records.get(row["record_id"])
                candidate = record if record is not None else {"no_records_found": True, "absence_claim": False, "note": "Human acknowledgement does not mean target data are absent."}
                candidate_json = json.dumps(candidate, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                evidence_summary = " | ".join(
                    f"{field['field_id']}: {citation['exact_quote']}"
                    for field in (record or {}).get("fields", [])
                    for citation in field["evidence"]
                )
                if row.get("candidate_sha256") != sha256_json(candidate) or row.get("candidate_json") != candidate_json or row.get("evidence_summary", "") != evidence_summary:
                    raise ValueError(f"review row no longer binds candidate: {row.get('review_id')}")
            accepted_ids = set(marker.get("accepted_record_ids", [])); accepted_jobs.append(job["job_id"])
            review_by_id = {row["record_id"]: row for row in rows}
            expected_accepted = {row["record_id"] for row in rows if row.get("review_decision", "").strip().casefold() == "accept"}
            expected_rejected = {row["record_id"] for row in rows if row.get("review_decision", "").strip().casefold() == "reject"}
            if accepted_ids != expected_accepted or set(marker.get("rejected_record_ids", [])) != expected_rejected:
                raise ValueError(f"acceptance marker does not partition current human decisions: {job['job_id']}")
            binding = {
                "job_id": job["job_id"], "attempt_id": job.get("attempt_id", ""),
                "run_id": job.get("run_id", ""), "source_sha256": job["source_sha256"],
                "parsed_text_sha256": job["parsed_text_sha256"],
                "sections_sha256": job.get("sections_sha256", ""),
                "sections_count": job.get("sections_count", 0),
                "sections_schema_version": job.get("sections_schema_version", ""),
                "spec_sha256": job["spec_sha256"],
                "result_sha256": checks["result_sha256"],
                "validation_sha256": checks["validation_sha256"],
                "review_rows_sha256": checks["review_rows_sha256"],
                "acceptance_sha256": sha256_file(acceptance_path(paths, job["job_id"])),
                "receipt_sha256": job.get("receipt_sha256", ""),
            }
            job_bindings.append(binding)
            bound_files = [
                result_path(paths, job["job_id"]), validation_path(paths, job["job_id"]),
                acceptance_path(paths, job["job_id"]),
                resolve_path(paths["root"], job["receipt_path"]),
                resolve_path(paths["root"], job["source_path"]),
                resolve_path(paths["root"], job["parsed_text_path"]),
            ]
            if job.get("sections_path"):
                bound_files.append(resolve_path(paths["root"], job["sections_path"]))
            for bound_file in bound_files:
                if not bound_file.is_file():
                    raise ValueError(f"delivery input is missing: {bound_file}")
                input_snapshots[bound_file] = sha256_file(bound_file)
            for record in result["records"]:
                if record["record_id"] not in accepted_ids: continue
                review = review_by_id.get(record["record_id"], {})
                if review.get("review_decision", "").strip().casefold() != "accept" or review.get("source_verified", "").strip().casefold() != "true": raise ValueError(f"record lost human source approval: {record['record_id']}")
                approved = {**record, "job_id": job["job_id"], "source_path": job["source_path"], "source_sha256": job["source_sha256"], "parsed_text_path": job["parsed_text_path"], "parsed_text_sha256": job["parsed_text_sha256"], "sections_path": job.get("sections_path", ""), "sections_sha256": job.get("sections_sha256", ""), "sections_count": job.get("sections_count", 0), "sections_schema_version": job.get("sections_schema_version", ""), "spec_sha256": job["spec_sha256"], "human_review_status": "accepted", "source_verified": True, "reviewer": review["reviewer"], "reviewed_at": review["reviewed_at"]}
                output_records.append(approved)
                for field in record["fields"]:
                    for index, evidence in enumerate(field["evidence"], 1): evidence_rows.append({"record_id": record["record_id"], "paper_id": job["paper_id"], "job_id": job["job_id"], "field_id": field["field_id"], "evidence_index": index, "exact_quote": evidence["exact_quote"], "locator": evidence["locator"], "source_path": job["source_path"], "source_sha256": job["source_sha256"], "parsed_text_path": job["parsed_text_path"], "parsed_text_sha256": job["parsed_text_sha256"], "quote_verified": "true", "source_verified": "true", "reviewer": review["reviewer"], "reviewed_at": review["reviewed_at"]})
        delivery = paths["delivery"]; delivery.mkdir(parents=True, exist_ok=True)
        atomic_write_json(delivery / "ACCEPTANCE.json", {
            "acceptance_version": "2.0", "status": "building",
            "task_id": args.task_id, "spec_sha256": spec_hash, "started_at": utc_now(),
        })
        job_bindings.sort(key=lambda row: row["job_id"])
        job_bindings_sha = sha256_json(job_bindings)
        records_doc = {"delivery_version": "2.0", "task_id": args.task_id, "spec_version": spec["spec_version"], "spec_sha256": spec_hash, "accepted_job_ids": sorted(accepted_jobs), "job_bindings": job_bindings, "job_bindings_sha256": job_bindings_sha, "record_count": len(output_records), "records": output_records, "created_at": utc_now()}; atomic_write_json(delivery / "records.json", records_doc)
        fields = field_map(spec); csv_fields = ["record_id", "paper_id", "job_id", "source_sha256", "reviewer", "reviewed_at"] + [name for field_id in fields for name in (field_id, field_id + "__unit", field_id + "__value_semantics")]
        flat = []
        for record in output_records:
            row = {key: record.get(key, "") for key in csv_fields}; row.update({"source_sha256": record["source_sha256"]})
            for field in record["fields"]: row[field["field_id"]] = json.dumps(field["value"], ensure_ascii=False) if not isinstance(field["value"], str) else field["value"]; row[field["field_id"] + "__unit"] = field["unit"] or ""; row[field["field_id"] + "__value_semantics"] = field["value_semantics"]
            flat.append(row)
        write_csv(delivery / "records.csv", csv_fields, flat)
        evidence_fields = ["record_id", "paper_id", "job_id", "field_id", "evidence_index", "exact_quote", "locator", "source_path", "source_sha256", "parsed_text_path", "parsed_text_sha256", "quote_verified", "source_verified", "reviewer", "reviewed_at"]; write_csv(delivery / "evidence.csv", evidence_fields, evidence_rows)
        build_sqlite(delivery / "extraction.sqlite3", output_records, evidence_rows)
        datapackage = {"profile": "tabular-data-package", "name": f"{args.task_id}-accepted-extraction", "resources": [{"name": "records", "path": "records.csv", "format": "csv", "mediatype": "text/csv"}, {"name": "evidence", "path": "evidence.csv", "format": "csv", "mediatype": "text/csv"}]}; atomic_write_json(delivery / "datapackage.json", datapackage)
        payload_files = ["records.json", "records.csv", "evidence.csv", "extraction.sqlite3", "datapackage.json"]
        manifest = {"manifest_version": "2.0", "task_id": args.task_id, "spec_version": spec["spec_version"], "spec_sha256": spec_hash, "accepted_job_ids": sorted(accepted_jobs), "job_bindings": job_bindings, "job_bindings_sha256": job_bindings_sha, "files": [{"path": str((delivery / name).relative_to(paths["root"])), "sha256": sha256_file(delivery / name), "bytes": (delivery / name).stat().st_size} for name in payload_files], "created_at": utc_now()}; atomic_write_json(delivery / "manifest.json", manifest)
        if (sha256_file(paths["queue"]) != queue_snapshot_sha
                or sha256_file(paths["review_queue"]) != review_snapshot_sha):
            raise ValueError("queue or human review changed while delivery was being built")
        changed = [str(path) for path, digest in input_snapshots.items()
                   if not path.is_file() or sha256_file(path) != digest]
        if changed:
            raise ValueError(f"accepted inputs changed while delivery was being built: {changed}")
        acceptance = {"acceptance_version": "2.0", "status": "accepted", "task_id": args.task_id, "spec_version": spec["spec_version"], "spec_sha256": spec_hash, "manifest_sha256": sha256_file(delivery / "manifest.json"), "job_bindings_sha256": job_bindings_sha, "accepted_job_ids": sorted(accepted_jobs), "record_count": len(output_records), "accepted_by": "coordinator", "accepted_at": utc_now()}; atomic_write_json(delivery / "ACCEPTANCE.json", acceptance)
        print(f"{delivery} records={len(output_records)} jobs={len(accepted_jobs)}"); return 0
    except (OSError, ValueError, json.JSONDecodeError, sqlite3.Error, csv.Error) as exc:
        raise ValueError(str(exc)) from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--root", default="."); parser.add_argument("--task-id", required=True); args = parser.parse_args(); paths = task_paths(Path(args.root), args.task_id)
    try:
        # Snapshot all human-review and queue inputs under the shared lock order.
        with exclusive_lock(paths["review_lock"]), exclusive_lock(paths["queue_lock"]):
            return _build_delivery(paths, args)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__": raise SystemExit(main())
