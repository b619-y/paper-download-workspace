from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import accept_extraction_job
import build_extraction_delivery
import extraction_common
import luna_extract_data
import make_data_extraction_queue
import make_extraction_review_queue
import merge_extraction_receipts
import paper_section_chunking
import workflow_status
from extraction_common import (
    acceptance_path, attempt_id_for, completion_receipt_path, load_queue,
    task_paths, utc_now, validation_path, write_queue,
)
from validate_extraction_result import validate, write_validation


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def setup_task(root: Path) -> tuple[str, Path]:
    task_id = "test-task"
    extraction = root / "runtime/tasks" / task_id / "extraction"
    extraction.mkdir(parents=True)
    spec = {
        "spec_version": "1.0", "task_id": task_id, "title": "Test", "purpose": "Extract target values",
        "record_identity_fields": ["sample_id", "outcome"],
        "review_policy": {"human_review_required": True},
        "model": {"name": "gpt-5.6-luna", "chunk_chars": 2000, "chunk_overlap": 100},
        "fields": [
            {"field_id": "sample_id", "label": "Sample", "description": "Sample identifier", "data_type": "string", "required": True, "multiple": False, "value_semantics": "identifier", "unit": {"required": False, "allowed": []}},
            {"field_id": "outcome", "label": "Outcome", "description": "Reported outcome", "data_type": "number", "required": True, "multiple": False, "value_semantics": "reported", "unit": {"required": True, "allowed": ["mg/L"]}},
        ],
    }
    (extraction / "extraction_spec.json").write_text(json.dumps(spec), encoding="utf-8")
    source = root / "runtime/inputs/paper.pdf"; source.parent.mkdir(parents=True); source.write_bytes(b"%PDF-1.4\noriginal")
    markdown = root / "runtime/inputs/full.md"; markdown.write_text("# Results\nSample A contained 5 mg/L target.\n", encoding="utf-8")
    sections = root / "runtime/inputs/section_chunks.jsonl"
    sections_sha, sections_count = paper_section_chunking.write_sections_jsonl(
        markdown, sections, paper_id="P1", source_file=source.name,
        source_format="pdf", source_sha256=digest(source), overlap=0,
    )
    documents = root / "runtime/inputs/documents.json"
    documents.write_text(json.dumps({"documents": [{
        "status": "accepted", "batch_id": "B1", "item_id": "D001", "paper_id": "P1", "title": "Paper", "doi": "10.test/1",
        "source_kind": "pdf", "source_path": str(source), "source_sha256": digest(source),
        "parsed_text_path": str(markdown), "parsed_text_sha256": digest(markdown),
        "sections_path": str(sections), "sections_sha256": sections_sha,
        "sections_count": sections_count,
        "sections_schema_version": paper_section_chunking.SECTION_SCHEMA_VERSION,
        "parser": "mineru", "parser_version": "test-v1", "fulltext_decision": "include",
    }]}), encoding="utf-8")
    with patch.object(sys, "argv", ["make", "--root", str(root), "--task-id", task_id, "--documents", str(documents)]):
        assert make_data_extraction_queue.main() == 0
    return task_id, documents


def paperqa_marker(root: Path, task_id: str) -> str:
    paths = task_paths(root, task_id); queue = load_queue(paths["queue"]); job = queue["jobs"][0]
    paths["paperqa_results"].mkdir(parents=True)
    marker = {
        "schema_version": "1.0", "mode": "judged", "completed": True,
        "job_id": job["job_id"], "question": job["paperqa_question"],
        "question_sha256": job["paperqa_question_sha256"],
        "spec_version": job["spec_version"], "spec_sha256": job["spec_sha256"],
        "parser": [job["parser"]], "retriever": "local-bm25",
        "embedding_base_url": None, "embedding_error": None,
        "model": "gpt-5.6-luna",
        "source_bindings": [{
            "paper_id": job["paper_id"],
            "source_path": str((root / job["source_path"]).resolve()),
            "source_sha256": job["source_sha256"],
            "source_revalidation": job.get("source_revalidation", "verified_local"),
            "source_format": job["source_kind"],
            "source_file": Path(job["source_path"]).name,
            "parsed_text_path": str((root / job["parsed_text_path"]).resolve()),
            "parsed_text_sha256": job["parsed_text_sha256"],
            "parser": job["parser"], "parser_version": job["parser_version"],
            "sections_path": str((root / job["sections_path"]).resolve()),
            "sections_sha256": job["sections_sha256"],
            "sections_count": job["sections_count"],
            "sections_schema_version": job["sections_schema_version"],
            "sections_origin": "accepted_sidecar",
        }],
        "candidate_count": 0, "candidates": [],
        "result": {"answer": "", "decision": "uncertain", "uncertainty": "no evidence", "evidence": [], "rejected_evidence": [], "evidence_status": "no_evidence"},
        "limitations": ["no_evidence does not mean data absent"],
    }
    (paths["paperqa_results"] / f"{job['job_id']}.json").write_text(json.dumps(marker), encoding="utf-8")
    return job["job_id"]


def model_response(_prompt: str, _codex: str, _model: str) -> dict:
    return {"records": [{"record_key_hint": "A", "fields": [
        {"field_id": "sample_id", "value": "A", "unit": None, "value_semantics": "identifier", "evidence": [{"exact_quote": "Sample A", "locator": "Results"}]},
        {"field_id": "outcome", "value": 5.0, "unit": "mg/L", "value_semantics": "reported", "evidence": [{"exact_quote": "5 mg/L", "locator": "Results"}]},
    ]}], "no_relevant_data": False, "notes": ""}


def test_projection_export_loads_authority_inside_queue_lock(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path)
    paths = task_paths(tmp_path, task_id)
    paths["queue_csv"].unlink()
    events: list[str] = []
    real_lock = extraction_common.PinnedDirectory.locked_file
    real_read = extraction_common.PinnedDirectory.read_json

    @contextmanager
    def observed_lock(directory, name: str):
        with real_lock(directory, name):
            events.append("locked")
            yield
            events.append("unlocked")

    def observed_read(directory, name: str, **kwargs):
        assert events == ["locked"]
        events.append(f"read:{name}")
        value = real_read(directory, name, **kwargs)
        events.pop()
        return value

    with patch.object(
        extraction_common.PinnedDirectory, "locked_file", observed_lock,
    ), patch.object(
        extraction_common.PinnedDirectory, "read_json", observed_read,
    ):
        extraction_common.export_queue_projection(paths)

    assert events == ["locked", "unlocked"]
    assert paths["queue_csv"].is_file()


def test_projection_export_rejects_concurrent_ancestor_symlink_swap(
    tmp_path: Path,
) -> None:
    task_id, _ = setup_task(tmp_path)
    paths = task_paths(tmp_path, task_id)
    extraction = paths["extraction"]
    moved = extraction.with_name("extraction.old")
    outside = tmp_path.parent / f"{tmp_path.name}-outside-extraction"
    outside.mkdir()
    outside_csv = outside / "data_extraction_queue.csv"
    outside_csv.write_text("outside-sentinel\n", encoding="utf-8")
    real_read = extraction_common.PinnedDirectory.read_json
    swapped = False

    def swap_after_queue_read(directory, name: str, **kwargs):
        nonlocal swapped
        value = real_read(directory, name, **kwargs)
        if name == "data_extraction_queue.json" and not swapped:
            extraction.rename(moved)
            extraction.symlink_to(outside, target_is_directory=True)
            swapped = True
        return value

    with patch.object(
        extraction_common.PinnedDirectory, "read_json", swap_after_queue_read,
    ), pytest.raises((OSError, ValueError)):
        extraction_common.export_queue_projection(paths)

    assert outside_csv.read_text(encoding="utf-8") == "outside-sentinel\n"


def test_projection_export_rejects_dangling_lock_symlink(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path)
    paths = task_paths(tmp_path, task_id)
    paths["queue_lock"].unlink(missing_ok=True)
    outside = tmp_path.parent / f"{tmp_path.name}-outside-lock"
    paths["queue_lock"].symlink_to(outside)

    with pytest.raises(OSError):
        extraction_common.export_queue_projection(paths)

    assert not outside.exists()


def test_end_to_end_candidate_review_accept_and_delivery(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path); job_id = paperqa_marker(tmp_path, task_id)
    with patch.object(luna_extract_data, "call_codex", model_response), patch.object(sys, "argv", ["extract", "--root", str(tmp_path), "--task-id", task_id, "--job-id", job_id, "--worker", "test", "--external-model-authorized"]):
        assert luna_extract_data.main() == 0
    paths = task_paths(tmp_path, task_id); assert load_queue(paths["queue"])["jobs"][0]["status"] == "review"
    errors, result, _ = validate(paths, job_id); assert errors == []; assert len(result["records"]) == 1
    with patch.object(sys, "argv", ["review", "--root", str(tmp_path), "--task-id", task_id]): assert make_extraction_review_queue.main() == 0
    with paths["review_queue"].open(encoding="utf-8", newline="") as handle: reader = csv.DictReader(handle); fields, rows = reader.fieldnames, list(reader)
    rows[0].update({"review_decision": "accept", "source_verified": "true", "reviewer": "human", "reviewed_at": "2026-10-14T12:00:00+00:00"})
    with paths["review_queue"].open("w", encoding="utf-8", newline="") as handle: writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader(); writer.writerows(rows)
    with patch.object(sys, "argv", ["accept", "--root", str(tmp_path), "--task-id", task_id, "--job-id", job_id, "--actor-role", "coordinator"]): assert accept_extraction_job.main() == 0
    validation_digest = digest(validation_path(paths, job_id))
    # Duplicate acceptance is idempotent and must not mutate validation bytes.
    with patch.object(sys, "argv", ["accept", "--root", str(tmp_path), "--task-id", task_id, "--job-id", job_id, "--actor-role", "coordinator"]): assert accept_extraction_job.main() == 0
    assert digest(validation_path(paths, job_id)) == validation_digest
    validation_file = validation_path(paths, job_id)
    original_validation = validation_file.read_bytes()
    stale_validation = json.loads(original_validation)
    stale_validation["source_sha256"] = "0" * 64
    validation_file.write_text(json.dumps(stale_validation), encoding="utf-8")
    with patch.object(sys, "argv", ["review", "--root", str(tmp_path), "--task-id", task_id]), pytest.raises(SystemExit):
        make_extraction_review_queue.main()
    validation_file.write_bytes(original_validation)
    current = load_queue(paths["queue"])["jobs"][0]
    receipt_file = tmp_path / current["receipt_path"]
    original_receipt = receipt_file.read_bytes()
    changed_receipt = json.loads(original_receipt)
    changed_receipt["completed_at"] = "2026-10-15T00:00:00+00:00"
    receipt_file.write_text(json.dumps(changed_receipt), encoding="utf-8")
    with patch.object(sys, "argv", ["accept", "--root", str(tmp_path), "--task-id", task_id, "--job-id", job_id, "--actor-role", "coordinator"]), pytest.raises(SystemExit):
        accept_extraction_job.main()
    receipt_file.write_bytes(original_receipt)
    with patch.object(sys, "argv", ["deliver", "--root", str(tmp_path), "--task-id", task_id]): assert build_extraction_delivery.main() == 0
    assert load_queue(paths["queue"])["jobs"][0]["status"] == "accepted"
    acceptance = json.loads((paths["delivery"] / "ACCEPTANCE.json").read_text()); assert acceptance["status"] == "accepted" and acceptance["record_count"] == 1
    manifest = json.loads((paths["delivery"] / "manifest.json").read_text()); assert {Path(item["path"]).name for item in manifest["files"]} == {"records.json", "records.csv", "evidence.csv", "extraction.sqlite3", "datapackage.json"}
    connection = sqlite3.connect(paths["delivery"] / "extraction.sqlite3"); assert connection.execute("SELECT COUNT(*) FROM records").fetchone()[0] == 1; connection.close()
    spec_gate = workflow_status.extraction_spec_gate(tmp_path, paths["task"], task_id)
    queue_gate = {"_jobs": load_queue(paths["queue"])["jobs"]}
    assert workflow_status.extraction_review_gate(tmp_path, paths["task"], task_id, spec_gate, queue_gate)["valid"]
    assert workflow_status.extraction_delivery_gate(tmp_path, paths["task"], task_id, spec_gate, queue_gate)["valid"]
    # Delivery acceptance is a snapshot of every per-job acceptance digest.
    marker = json.loads(acceptance_path(paths, job_id).read_text())
    marker["accepted_at"] = "2026-10-15T00:00:00+00:00"
    acceptance_path(paths, job_id).write_text(json.dumps(marker), encoding="utf-8")
    workflow_status.extraction_review_gate(
        tmp_path, paths["task"], task_id, spec_gate, queue_gate
    )
    assert not workflow_status.extraction_delivery_gate(
        tmp_path, paths["task"], task_id, spec_gate, queue_gate
    )["valid"]


def test_validation_rejects_unknown_field_and_stale_hash(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path); job_id = paperqa_marker(tmp_path, task_id)
    with patch.object(luna_extract_data, "call_codex", model_response), patch.object(sys, "argv", ["extract", "--root", str(tmp_path), "--task-id", task_id, "--job-id", job_id, "--worker", "test", "--external-model-authorized"]): luna_extract_data.main()
    paths = task_paths(tmp_path, task_id); result_path = paths["extraction_results"] / job_id / "result.json"; result = json.loads(result_path.read_text()); result["records"][0]["fields"].append({"field_id": "unknown", "value": "x", "unit": None, "value_semantics": "free_text", "evidence": []}); result_path.write_text(json.dumps(result), encoding="utf-8")
    errors, _, _ = validate(paths, job_id); assert any("unknown fields" in error for error in errors)
    parsed = tmp_path / "runtime/inputs/full.md"; parsed.write_text(parsed.read_text() + "changed", encoding="utf-8")
    errors, _, _ = validate(paths, job_id); assert any("parsed text" in error and "stale" in error for error in errors)


def test_queue_rejects_stale_document_hash(tmp_path: Path) -> None:
    task_id, documents = setup_task(tmp_path)
    paths = task_paths(tmp_path, task_id); paths["queue"].unlink(); paths["queue_csv"].unlink()
    value = json.loads(documents.read_text()); value["documents"][0]["source_sha256"] = "0" * 64; documents.write_text(json.dumps(value), encoding="utf-8")
    with patch.object(sys, "argv", ["make", "--root", str(tmp_path), "--task-id", task_id, "--documents", str(documents)]), pytest.raises(SystemExit):
        make_data_extraction_queue.main()


def test_queue_read_and_write_reject_incomplete_job_schema(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path)
    paths = task_paths(tmp_path, task_id)
    queue = load_queue(paths["queue"])
    queue["jobs"][0].pop("parser")
    with pytest.raises(ValueError, match=r"missing=.*parser"):
        write_queue(paths, queue)

    persisted = json.loads(paths["queue"].read_text(encoding="utf-8"))
    persisted["jobs"][0].pop("source_path")
    paths["queue"].write_text(json.dumps(persisted), encoding="utf-8")
    with pytest.raises(ValueError, match=r"missing=.*source_path"):
        load_queue(paths["queue"])


def test_queue_accepts_hash_only_staged_source_only_with_explicit_flag(tmp_path: Path) -> None:
    parsed_dir = tmp_path / "runtime/inputs/T/documents/D/parsed"
    parsed_dir.mkdir(parents=True)
    provisional = parsed_dir / "provisional.md"
    provisional.write_text("evidence", encoding="utf-8")
    parsed = parsed_dir / f"{digest(provisional)}.full.md"
    provisional.rename(parsed)
    parsed_ref = str(parsed.relative_to(tmp_path))
    row = {
        "schema_version": 3, "task_id": "T", "document_id": "D",
        "batch_id": "B", "source_item_id": "D", "item_id": "D",
        "paper_id": "D", "title": "Paper", "doi": "", "parser": "staged_parsed_text",
        "parser_version": "test-v1", "status": "accepted",
        "source_sha256": "a" * 64, "parsed_text_sha256": digest(parsed),
        "source_kind": "pdf", "source_ref": "02_batches/B/outputs/missing.pdf",
        "source_staged_ref": None, "source_path": "02_batches/B/outputs/missing.pdf",
        "parsed_text_source_ref": parsed_ref, "parsed_text_ref": parsed_ref,
        "parsed_text_path": parsed_ref, "section_chunks_source_ref": None,
        "section_chunks_ref": None, "section_chunks_path": None,
        "section_chunks_sha256": None, "section_chunks_count": 0,
        "section_chunks_schema_version": None, "fulltext_copied": False,
        "source_bytes": 123, "parsed_text_bytes": parsed.stat().st_size,
        "section_chunks_bytes": 0,
    }
    with pytest.raises(ValueError, match="source is missing"):
        make_data_extraction_queue.canonical_document(tmp_path, row, "manifest")
    document = make_data_extraction_queue.canonical_document(
        tmp_path, row, "manifest", allow_hash_only_source=True
    )
    assert document["source_revalidation"] == "deferred_to_mac"
    assert document["parser"] == "staged_parsed_text"


def test_identity_fields_deduplicate_overlapping_chunk_candidates(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path); paths = task_paths(tmp_path, task_id)
    job = load_queue(paths["queue"])["jobs"][0]
    job["attempt_count"] = 1; job["run_id"] = "1" * 32
    job["attempt_id"] = attempt_id_for(job["job_id"], 1, job["run_id"])
    spec = json.loads(paths["spec"].read_text())
    chunks = [
        {"chunk_id": "c1", "char_start": 0, "char_end": 30, "text": "Sample A had 5 mg/L in table.", "sha256": hashlib.sha256(b"Sample A had 5 mg/L in table.").hexdigest(), "canonical_section": "results", "original_heading": "Results", "heading_path": ["Results"], "table_or_figure": "table", "section_index": 1, "part_index": 1},
        {"chunk_id": "c2", "char_start": 20, "char_end": 60, "text": "Again Sample A had 5 mg/L.", "sha256": hashlib.sha256(b"Again Sample A had 5 mg/L.").hexdigest(), "canonical_section": "results", "original_heading": "Results", "heading_path": ["Results"], "table_or_figure": "", "section_index": 1, "part_index": 2},
    ]
    def response(sample_quote: str, outcome_quote: str) -> dict:
        return {"records": [{"record_key_hint": "A", "fields": [
            {"field_id": "sample_id", "value": "A", "unit": None, "value_semantics": "identifier", "evidence": [{"exact_quote": sample_quote, "locator": "Results"}]},
            {"field_id": "outcome", "value": 5.0, "unit": "mg/L", "value_semantics": "reported", "evidence": [{"exact_quote": outcome_quote, "locator": "Results"}]},
        ]}], "no_relevant_data": False, "notes": ""}
    wrappers = [
        {"wrapper_version": "2.0", "task_id": job["task_id"], "job_id": job["job_id"], "attempt_id": job["attempt_id"], "run_id": job["run_id"], "chunk_id": chunk["chunk_id"], "chunk_sha256": chunk["sha256"], "source_sha256": job["source_sha256"], "parsed_text_sha256": job["parsed_text_sha256"], "sections_sha256": job["sections_sha256"], "spec_sha256": job["spec_sha256"], "model": "mock", "response": response("Sample A", "5 mg/L"), "created_at": utc_now()}
        for chunk in chunks
    ]
    result = luna_extract_data.merge(job, spec, chunks, wrappers, "mock")
    assert len(result["records"]) == 1
    outcome = next(field for field in result["records"][0]["fields"] if field["field_id"] == "outcome")
    assert len(outcome["evidence"]) == 2


def test_acceptance_rejects_tampered_human_review_payload(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path); job_id = paperqa_marker(tmp_path, task_id)
    with patch.object(luna_extract_data, "call_codex", model_response), patch.object(sys, "argv", ["extract", "--root", str(tmp_path), "--task-id", task_id, "--job-id", job_id, "--worker", "test", "--external-model-authorized"]): luna_extract_data.main()
    paths = task_paths(tmp_path, task_id)
    with patch.object(sys, "argv", ["review", "--root", str(tmp_path), "--task-id", task_id]): make_extraction_review_queue.main()
    with paths["review_queue"].open(encoding="utf-8", newline="") as handle: reader = csv.DictReader(handle); fields, rows = reader.fieldnames, list(reader)
    rows[0].update({"candidate_json": "{}", "review_decision": "accept", "source_verified": "true", "reviewer": "human", "reviewed_at": "2026-10-14T12:00:00+00:00"})
    with paths["review_queue"].open("w", encoding="utf-8", newline="") as handle: writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader(); writer.writerows(rows)
    with patch.object(sys, "argv", ["accept", "--root", str(tmp_path), "--task-id", task_id, "--job-id", job_id, "--actor-role", "coordinator"]), pytest.raises(SystemExit):
        accept_extraction_job.main()


def test_no_records_decision_is_not_inherited_across_changed_bindings(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path)
    job_id = paperqa_marker(tmp_path, task_id)

    def no_records(_prompt: str, _codex: str, _model: str) -> dict:
        return {"records": [], "no_relevant_data": True, "notes": "not found in this chunk"}

    with patch.object(luna_extract_data, "call_codex", no_records), patch.object(
        sys, "argv", ["extract", "--root", str(tmp_path), "--task-id", task_id,
                      "--job-id", job_id, "--worker", "test", "--external-model-authorized"],
    ):
        assert luna_extract_data.main() == 0
    paths = task_paths(tmp_path, task_id)
    review_argv = ["review", "--root", str(tmp_path), "--task-id", task_id]
    with patch.object(sys, "argv", review_argv):
        assert make_extraction_review_queue.main() == 0

    binding_fields = [
        "source_sha256", "parsed_text_sha256", "sections_sha256", "spec_sha256",
        "attempt_id", "run_id", "result_sha256", "validation_sha256", "receipt_sha256",
    ]
    for field in binding_fields:
        with paths["review_queue"].open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle); names, rows = reader.fieldnames, list(reader)
        assert rows[0]["record_id"] == "NO_RECORDS"
        rows[0].update({
            "review_decision": "accept", "source_verified": "true",
            "reviewer": "human", "reviewed_at": "2026-10-14T12:00:00+00:00",
            field: "stale-binding",
        })
        with paths["review_queue"].open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=names); writer.writeheader(); writer.writerows(rows)
        with patch.object(sys, "argv", review_argv):
            assert make_extraction_review_queue.main() == 0
        with paths["review_queue"].open(encoding="utf-8", newline="") as handle:
            row = next(csv.DictReader(handle))
        assert row["review_decision"] == ""
        assert row["reviewer"] == ""


def test_deferred_source_must_be_rehashed_on_mac_before_acceptance(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path); paths = task_paths(tmp_path, task_id)
    queue = load_queue(paths["queue"]); queue["jobs"][0]["source_revalidation"] = "deferred_to_mac"; write_queue(paths, queue)
    job_id = paperqa_marker(tmp_path, task_id)
    source = tmp_path / "runtime/inputs/paper.pdf"; original = source.read_bytes(); source.unlink()
    with patch.object(luna_extract_data, "call_codex", model_response), patch.object(sys, "argv", ["extract", "--root", str(tmp_path), "--task-id", task_id, "--job-id", job_id, "--worker", "capg", "--external-model-authorized"]): luna_extract_data.main()
    with patch.object(sys, "argv", ["review", "--root", str(tmp_path), "--task-id", task_id]): make_extraction_review_queue.main()
    with paths["review_queue"].open(encoding="utf-8", newline="") as handle: reader = csv.DictReader(handle); fields, rows = reader.fieldnames, list(reader)
    rows[0].update({"review_decision": "accept", "source_verified": "true", "reviewer": "human", "reviewed_at": "2026-10-14T12:00:00+00:00"})
    with paths["review_queue"].open("w", encoding="utf-8", newline="") as handle: writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader(); writer.writerows(rows)
    argv = ["accept", "--root", str(tmp_path), "--task-id", task_id, "--job-id", job_id, "--actor-role", "coordinator"]
    with patch.object(sys, "argv", argv), pytest.raises(SystemExit): accept_extraction_job.main()
    assert load_queue(paths["queue"])["jobs"][0]["status"] == "review"
    source.write_bytes(original)
    with patch.object(sys, "argv", argv): assert accept_extraction_job.main() == 0
    assert load_queue(paths["queue"])["jobs"][0]["status"] == "accepted"
    source.unlink()
    with patch.object(sys, "argv", ["review", "--root", str(tmp_path), "--task-id", task_id]), pytest.raises(SystemExit):
        make_extraction_review_queue.main()


def test_expired_attempt_reconciles_fsynced_receipt_before_reclaim(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path); paths = task_paths(tmp_path, task_id)
    job_id = load_queue(paths["queue"])["jobs"][0]["job_id"]
    first, _ = luna_extract_data.claim(paths, "worker-1", 30, job_id)
    receipt = luna_extract_data.receipt_for(
        first, "retry_wait", error_code="interrupted",
        error_message="receipt persisted before queue transition", available_at=utc_now(),
    )
    receipt_file = completion_receipt_path(paths, job_id, first["attempt_id"])
    luna_extract_data.write_immutable_receipt(receipt_file, receipt)
    queue = load_queue(paths["queue"])
    queue["jobs"][0]["lease_expires_at"] = "2000-01-01T00:00:00+00:00"
    queue["jobs"][0]["heartbeat_at"] = "2000-01-01T00:00:00+00:00"
    write_queue(paths, queue)
    second, _ = luna_extract_data.claim(paths, "worker-2", 30, job_id)
    assert second["attempt_count"] == 2
    assert second["receipt_history"] == [{
        "attempt_id": first["attempt_id"], "run_id": first["run_id"],
        "attempt_count": 1, "status": "retry_wait",
        "receipt_sha256": digest(receipt_file),
    }]


def test_expired_final_attempt_is_persisted_as_failed(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path)
    paths = task_paths(tmp_path, task_id)
    queue = load_queue(paths["queue"])
    queue["jobs"][0]["max_attempts"] = 1
    write_queue(paths, queue)
    job_id = queue["jobs"][0]["job_id"]
    luna_extract_data.claim(paths, "worker-1", 30, job_id)
    queue = load_queue(paths["queue"])
    active = queue["jobs"][0]
    torn = completion_receipt_path(paths, job_id, active["attempt_id"])
    torn.parent.mkdir(parents=True, exist_ok=True)
    torn.write_bytes(b"")
    queue["jobs"][0]["lease_expires_at"] = "2000-01-01T00:00:00+00:00"
    queue["jobs"][0]["heartbeat_at"] = "2000-01-01T00:00:00+00:00"
    write_queue(paths, queue)
    with pytest.raises(ValueError, match="no claimable"):
        luna_extract_data.claim(paths, "worker-2", 30, job_id)
    failed = load_queue(paths["queue"])["jobs"][0]
    assert failed["status"] == "failed"
    assert failed["available_at"] == ""
    assert failed["error_code"] == "torn_receipt_recovered"
    assert failed["receipt_history"][0]["status"] == "failed"
    assert any((torn.parent / ".torn").glob(f"{torn.name}.*.torn"))


def test_receipt_merge_is_idempotent_and_rejects_changed_replay(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path); paths = task_paths(tmp_path, task_id)
    local = load_queue(paths["queue"])["jobs"][0]
    remote = {**local, "attempt_count": 1, "run_id": "2" * 32, "claimed_by": "capg"}
    remote["attempt_id"] = attempt_id_for(remote["job_id"], 1, remote["run_id"])
    receipt = luna_extract_data.receipt_for(
        remote, "retry_wait", error_code="model_timeout", error_message="timeout",
        available_at=utc_now(),
    )
    receipt_file = completion_receipt_path(paths, remote["job_id"], remote["attempt_id"])
    luna_extract_data.write_immutable_receipt(receipt_file, receipt)
    assert merge_extraction_receipts.merge_one(paths, receipt_file) == "retry_wait"
    assert merge_extraction_receipts.merge_one(paths, receipt_file) == "unchanged"
    altered = json.loads(receipt_file.read_text())
    altered["error_message"] = "changed replay"
    receipt_file.write_text(json.dumps(altered), encoding="utf-8")
    with pytest.raises(ValueError, match="conflicts with applied history"):
        merge_extraction_receipts.merge_one(paths, receipt_file)


def test_response_contract_rejects_blank_evidence_and_nonfinite_values() -> None:
    valid = model_response("", "", "")
    blank = json.loads(json.dumps(valid)); blank["records"][0]["fields"][0]["evidence"][0]["exact_quote"] = " "
    with pytest.raises(ValueError, match="evidence"):
        luna_extract_data.validate_response(blank)
    zero_width = json.loads(json.dumps(valid)); zero_width["records"][0]["fields"][0]["evidence"][0]["exact_quote"] = "\u200b"
    with pytest.raises(ValueError, match="evidence"):
        luna_extract_data.validate_response(zero_width)
    nonfinite = json.loads(json.dumps(valid)); nonfinite["records"][0]["fields"][1]["value"] = float("nan")
    with pytest.raises(ValueError, match="invalid value"):
        luna_extract_data.validate_response(nonfinite)
    extra = {**valid, "unexpected": True}
    with pytest.raises(ValueError, match="keys"):
        luna_extract_data.validate_response(extra)
