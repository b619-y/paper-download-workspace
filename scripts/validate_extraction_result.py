#!/usr/bin/env python3
"""Validate an extraction candidate against its queue, spec, source and Markdown."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from extraction_common import (
    RESULT_VERSION, atomic_write_json, attempt_id_for, chunk_document,
    completion_receipt_path, field_map,
    get_job, has_visible_text, load_queue, load_spec, parse_time, record_id_for,
    resolve_path, result_path, sha256_file, task_paths, utc_now,
    validate_value_type, validation_path,
)


RESULT_KEYS = {
    "result_version", "task_id", "job_id", "attempt_id", "run_id",
    "attempt_count", "paper_id", "batch_id", "item_id", "source_path",
    "source_sha256", "source_revalidation", "parsed_text_path",
    "parsed_text_sha256", "parser", "parser_version", "sections_path",
    "sections_sha256", "sections_count", "sections_schema_version", "chunking_source", "spec_version",
    "spec_sha256", "model", "candidate_only", "review_status",
    "full_markdown_scanned", "chunk_count", "chunks_completed", "records",
    "rejected_candidates", "unresolved_rejected_candidates", "no_records_found",
    "absence_claim", "created_at",
}
RECORD_KEYS = {"record_id", "paper_id", "record_key_hint", "fields", "review_status"}
FIELD_KEYS = {"field_id", "value", "unit", "value_semantics", "evidence"}
EVIDENCE_KEYS = {
    "chunk_id", "exact_quote", "locator", "model_locator", "char_start", "char_end",
    "canonical_section", "original_heading", "heading_path", "table_or_figure",
    "section_index", "part_index", "source_sha256", "source_revalidation",
    "parsed_text_sha256", "quote_verified", "locator_status", "evidence_authority",
}


def validate(
    paths: dict[str, Path], job_id: str, *, require_source: bool = False,
    attempt_binding: dict[str, Any] | None = None,
    result_file: Path | None = None,
) -> tuple[list[str], dict[str, Any], dict[str, Any]]:
    queue = load_queue(paths["queue"], expected_task_id=paths["task"].name); job = get_job(queue, job_id)
    if attempt_binding is not None:
        allowed = {"attempt_id", "run_id", "attempt_count"}
        if set(attempt_binding) != allowed:
            raise ValueError("attempt_binding keys are invalid")
        job = {**job, **attempt_binding}
    spec, spec_hash = load_spec(paths["spec"], queue["task_id"]); contracts = field_map(spec)
    path = result_file.resolve() if result_file is not None else result_path(paths, job_id)
    expected_parent = (paths["extraction_results"] / job_id).resolve()
    try:
        path.relative_to(expected_parent)
    except ValueError as exc:
        raise ValueError("result_file escapes the selected extraction job") from exc
    if not path.is_file(): return ["result.json is missing"], {}, job
    try: result = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc: return [f"result.json unreadable: {exc}"], {}, job
    errors = []
    if queue["task_id"] != paths["task"].name:
        errors.append("queue task_id does not match selected task directory")
    if set(result) != RESULT_KEYS:
        errors.append(
            f"result keys mismatch (missing={sorted(RESULT_KEYS - set(result))}, "
            f"unknown={sorted(set(result) - RESULT_KEYS)})"
        )
    expected = {
        "result_version": RESULT_VERSION,
        "task_id": job["task_id"], "job_id": job_id,
        "attempt_id": job.get("attempt_id", ""), "run_id": job.get("run_id", ""),
        "attempt_count": job.get("attempt_count"), "paper_id": job["paper_id"],
        "batch_id": str(job.get("batch_id") or ""),
        "item_id": str(job.get("item_id") or ""),
        "source_sha256": job["source_sha256"],
        "source_revalidation": job.get("source_revalidation", "verified_local"),
        "parsed_text_sha256": job["parsed_text_sha256"], "parser": job["parser"],
        "parser_version": job["parser_version"],
        "sections_sha256": str(job.get("sections_sha256") or ""),
        "sections_count": int(job.get("sections_count") or 0),
        "sections_schema_version": str(job.get("sections_schema_version") or ""),
        "chunking_source": (
            "accepted_section_sidecar" if job.get("sections_path")
            else "deterministic_parsed_text_fallback"
        ),
        "spec_version": job["spec_version"],
        "spec_sha256": job["spec_sha256"],
    }
    for key, value in expected.items():
        if key == "source_revalidation" and require_source and value == "verified_local":
            # A compute host may truthfully record that it only had the source
            # hash. Coordinator validation below rehashes the local authority.
            if result.get(key) not in {"verified_local", "deferred_to_mac"}:
                errors.append(f"stale or mismatched {key}")
        elif result.get(key) != value:
            errors.append(f"stale or mismatched {key}")
    try:
        if result.get("attempt_id") != attempt_id_for(
            job_id, result.get("attempt_count"), str(result.get("run_id") or "")
        ):
            errors.append("attempt_id does not bind job_id/attempt_count/run_id")
    except ValueError as exc:
        errors.append(f"invalid extraction attempt binding: {exc}")
    for key in ("source_path", "parsed_text_path"):
        if not isinstance(result.get(key), str) or not result[key].strip():
            errors.append(f"result {key} must be a non-empty worker-local reference")
    if job.get("sections_path"):
        if not isinstance(result.get("sections_path"), str) or not result["sections_path"].strip():
            errors.append("result sections_path must identify the worker-local accepted sidecar")
    elif result.get("sections_path") != "":
        errors.append("result unexpectedly claims a section sidecar")
    if spec_hash != job["spec_sha256"]: errors.append("spec hash changed after queue creation")
    configured_model = str(spec.get("model", {}).get("name") or "")
    if (not isinstance(result.get("model"), str) or not result.get("model", "").strip()
            or (configured_model and result.get("model") != configured_model)):
        errors.append("result model is missing or does not match the extraction spec")
    if job.get("status") in {"review", "accepted"}:
        try:
            receipt_file = completion_receipt_path(
                paths, job_id, str(job.get("attempt_id") or "")
            )
            if (not receipt_file.is_file()
                    or job.get("receipt_path") != str(receipt_file.relative_to(paths["root"]))
                    or job.get("receipt_sha256") != sha256_file(receipt_file)):
                raise ValueError("current review receipt is missing or its queue projection is stale")
            from merge_extraction_receipts import read_receipt
            receipt = read_receipt(receipt_file)
            expected_receipt = {
                "task_id": job["task_id"], "job_id": job_id,
                "attempt_id": job.get("attempt_id"), "run_id": job.get("run_id"),
                "attempt_count": job.get("attempt_count"), "status": "review",
                "source_sha256": job["source_sha256"],
                "parsed_text_sha256": job["parsed_text_sha256"],
                "sections_sha256": str(job.get("sections_sha256") or ""),
                "sections_count": int(job.get("sections_count") or 0),
                "sections_schema_version": str(job.get("sections_schema_version") or ""),
                "spec_sha256": job["spec_sha256"],
                "result_sha256": sha256_file(path), "model": result.get("model"),
            }
            mismatched = [
                key for key, expected_value in expected_receipt.items()
                if receipt.get(key) != expected_value
            ]
            if mismatched:
                raise ValueError(f"current review receipt binding mismatch: {mismatched}")
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(str(exc))
    if parse_time(str(result.get("created_at") or "")) is None:
        errors.append("result created_at is not an ISO timestamp")
    if result.get("candidate_only") is not True or result.get("review_status") != "candidate": errors.append("model output must remain candidate-only")
    if result.get("full_markdown_scanned") is not True or result.get("chunks_completed") != result.get("chunk_count"): errors.append("full Markdown was not completely scanned")
    try:
        source = resolve_path(paths["root"], job["source_path"]); parsed = resolve_path(paths["root"], job["parsed_text_path"])
        if not source.is_file():
            if require_source or job.get("source_revalidation") != "deferred_to_mac": errors.append("source file missing or SHA-256 is stale")
        elif sha256_file(source) != job["source_sha256"]:
            errors.append("source file missing or SHA-256 is stale")
        if not parsed.is_file() or sha256_file(parsed) != job["parsed_text_sha256"]: errors.append("parsed text missing or SHA-256 is stale")
        text = parsed.read_bytes().decode("utf-8") if parsed.is_file() else ""
    except (OSError, UnicodeDecodeError) as exc:
        errors.append(f"cannot read source artifacts: {exc}"); text = ""
    expected_chunks: list[dict[str, Any]] = []
    if text and job.get("sections_path"):
        try:
            from paper_section_chunking import validate_sections_jsonl
            sections = resolve_path(paths["root"], str(job["sections_path"]))
            if (not sections.is_file()
                    or sha256_file(sections) != job.get("sections_sha256")):
                raise ValueError("section chunk sidecar is missing or stale")
            expected_chunks = validate_sections_jsonl(
                sections, parsed, source_sha256=job["source_sha256"],
                parsed_text_sha256=job["parsed_text_sha256"],
                expected_count=job["sections_count"], paper_id=job["paper_id"],
                source_format=job.get("source_kind") or None,
            )
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            errors.append(f"cannot validate section chunk sidecar: {exc}")
    elif text:
        model_cfg = spec.get("model", {})
        expected_chunks = chunk_document(
            text, int(model_cfg.get("chunk_chars", 12000)),
            int(model_cfg.get("chunk_overlap", 800)),
        )
    if not expected_chunks:
        errors.append("parsed Markdown produced no extraction chunks")
    if result.get("chunk_count") != len(expected_chunks) or result.get("chunks_completed") != len(expected_chunks):
        errors.append("chunk counts do not cover the complete parsed Markdown")
    attempt_id = str(result.get("attempt_id") or "")
    chunk_dir = result_path(paths, job_id).parent / "attempts" / attempt_id / "chunks"
    expected_names = {f"{chunk['chunk_id']}.json" for chunk in expected_chunks}
    actual_names = {path.name for path in chunk_dir.glob("*.json")} if chunk_dir.is_dir() else set()
    if actual_names != expected_names:
        errors.append("chunk artifacts do not exactly cover the complete parsed Markdown")
    wrappers: list[dict[str, Any]] = []
    wrapper_set_complete = actual_names == expected_names
    for chunk in expected_chunks:
        wrapper_path = chunk_dir / f"{chunk['chunk_id']}.json"
        if not wrapper_path.is_file():
            continue
        try:
            wrapper = json.loads(wrapper_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            errors.append(f"chunk artifact unreadable: {chunk['chunk_id']}")
            wrapper_set_complete = False
            continue
        try:
            from luna_extract_data import validate_wrapper
            validate_wrapper(wrapper, {**job, "attempt_id": result.get("attempt_id"),
                                       "run_id": result.get("run_id")},
                             chunk, str(result.get("model") or ""))
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(f"chunk artifact is invalid {chunk['chunk_id']}: {exc}")
            wrapper_set_complete = False
        if wrapper_set_complete or isinstance(wrapper, dict):
            wrappers.append(wrapper)
    if wrapper_set_complete and len(wrappers) == len(expected_chunks):
        try:
            # The public candidate must be exactly reproducible from every
            # hash-bound full-text chunk response; a worker cannot silently
            # delete or insert records after model generation.
            from luna_extract_data import merge
            rebuild_job = {
                **job,
                "attempt_id": result.get("attempt_id"),
                "run_id": result.get("run_id"),
                "attempt_count": result.get("attempt_count"),
                "source_revalidation": result.get(
                    "source_revalidation", job.get("source_revalidation", "verified_local")
                ),
            }
            rebuilt = merge(rebuild_job, spec, expected_chunks, wrappers, str(result.get("model") or ""))
            for key in (
                "records", "rejected_candidates", "unresolved_rejected_candidates", "no_records_found",
                "absence_claim", "chunk_count", "chunks_completed",
            ):
                if result.get(key) != rebuilt.get(key):
                    errors.append(f"result is not the deterministic merge of chunk artifacts: {key}")
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(f"cannot deterministically rebuild chunk outputs: {exc}")
    records = result.get("records")
    if not isinstance(records, list): errors.append("records must be an array"); records = []
    rejected = result.get("rejected_candidates")
    if not isinstance(rejected, list):
        errors.append("rejected_candidates must be an array")
        rejected = []
    unresolved = bool(rejected)
    if result.get("unresolved_rejected_candidates") is not unresolved:
        errors.append("unresolved_rejected_candidates does not match rejected_candidates")
    if unresolved:
        errors.append("result contains unresolved rejected candidates and cannot enter review")
    seen: set[str] = set()
    required_fields = {key for key, value in contracts.items() if value.get("required")}
    for index, record in enumerate(records, 1):
        if not isinstance(record, dict): errors.append(f"record {index} is not an object"); continue
        if set(record) != RECORD_KEYS:
            errors.append(f"record {index} keys do not match result schema")
        if record.get("paper_id") != job["paper_id"]: errors.append(f"record {index} crosses paper_id boundary")
        if record.get("review_status") != "candidate": errors.append(f"record {index} is not candidate-only")
        if not isinstance(record.get("record_key_hint"), str): errors.append(f"record {index} record_key_hint must be string")
        fields = record.get("fields")
        if not isinstance(fields, list) or not fields: errors.append(f"record {index} has no fields"); continue
        field_ids = [str(item.get("field_id", "")) for item in fields if isinstance(item, dict)]
        unknown = sorted(set(field_ids) - set(contracts))
        if unknown: errors.append(f"record {index} has unknown fields: {unknown}")
        missing = sorted(required_fields - set(field_ids))
        if missing: errors.append(f"record {index} misses required fields: {missing}")
        if len(field_ids) != len(set(field_ids)): errors.append(f"record {index} repeats a field")
        for item in fields:
            if not isinstance(item, dict): errors.append(f"record {index} has malformed field"); continue
            if set(item) != FIELD_KEYS: errors.append(f"record {index} field keys do not match result schema")
            field_id = str(item.get("field_id", "")); contract = contracts.get(field_id)
            if not contract: continue
            if item.get("value_semantics") != contract["value_semantics"]: errors.append(f"record {index}/{field_id} value_semantics mismatch")
            raw_value = item.get("value")
            values = raw_value if contract.get("multiple") and isinstance(raw_value, list) else [raw_value]
            if contract.get("multiple"):
                if not isinstance(raw_value, list) or not raw_value or not all(validate_value_type(value, contract["data_type"]) for value in values): errors.append(f"record {index}/{field_id} multi-value type mismatch")
            elif not validate_value_type(raw_value, contract["data_type"]): errors.append(f"record {index}/{field_id} value type mismatch")
            if contract.get("allowed_values") and any(value not in contract["allowed_values"] for value in values): errors.append(f"record {index}/{field_id} value outside allowed_values")
            unit, unit_contract = item.get("unit"), contract.get("unit", {})
            if unit_contract.get("required") and (not isinstance(unit, str) or not unit.strip()): errors.append(f"record {index}/{field_id} missing unit")
            if unit is not None and not isinstance(unit, str): errors.append(f"record {index}/{field_id} unit must be string or null")
            if unit_contract.get("allowed") and unit not in unit_contract["allowed"]: errors.append(f"record {index}/{field_id} unit is outside contract")
            evidence = item.get("evidence")
            if not isinstance(evidence, list) or not evidence: errors.append(f"record {index}/{field_id} has no evidence"); continue
            for citation in evidence:
                if not isinstance(citation, dict): errors.append(f"record {index}/{field_id} malformed evidence"); continue
                if set(citation) != EVIDENCE_KEYS: errors.append(f"record {index}/{field_id} evidence keys do not match result schema")
                quote = citation.get("exact_quote")
                if not has_visible_text(quote) or quote not in text: errors.append(f"record {index}/{field_id} quote is unverified")
                for key in ("source_sha256", "parsed_text_sha256"):
                    if citation.get(key) != job[key]: errors.append(f"record {index}/{field_id} stale evidence {key}")
                if citation.get("source_revalidation") != result.get("source_revalidation"):
                    errors.append(f"record {index}/{field_id} stale evidence source_revalidation")
                if (citation.get("quote_verified") is not True
                        or citation.get("locator_status") != "derived_from_parsed_text"
                        or citation.get("evidence_authority") != "original_pdf_or_xml"):
                    errors.append(f"record {index}/{field_id} evidence was not locator-verified")
                if not has_visible_text(citation.get("locator")): errors.append(f"record {index}/{field_id} evidence locator is missing")
                if not has_visible_text(citation.get("model_locator")): errors.append(f"record {index}/{field_id} model locator is missing")
                if not isinstance(citation.get("heading_path"), list): errors.append(f"record {index}/{field_id} heading_path is invalid")
                chunk_id = str(citation.get("chunk_id") or "")
                chunk = next((value for value in expected_chunks if value["chunk_id"] == chunk_id), None)
                if chunk is None:
                    errors.append(f"record {index}/{field_id} evidence references an unknown chunk")
                else:
                    start, end = citation.get("char_start"), citation.get("char_end")
                    if (isinstance(start, bool) or isinstance(end, bool)
                            or not isinstance(start, int) or not isinstance(end, int)
                            or not isinstance(quote, str) or text[start:end] != quote
                            or start < chunk["char_start"] or end > chunk["char_end"]):
                        errors.append(f"record {index}/{field_id} evidence offsets are invalid")
                    structure = {
                        "canonical_section": chunk.get("canonical_section", "other"),
                        "original_heading": chunk.get("original_heading", ""),
                        "heading_path": chunk.get("heading_path", []),
                        "table_or_figure": chunk.get("table_or_figure", ""),
                        "section_index": chunk.get("section_index"),
                        "part_index": chunk.get("part_index"),
                    }
                    if any(citation.get(key) != value for key, value in structure.items()):
                        errors.append(f"record {index}/{field_id} evidence section metadata is stale")
        expected_id = record_id_for(
            job["paper_id"], fields,
            [str(value) for value in spec.get("record_identity_fields", [])],
        )
        if record.get("record_id") != expected_id: errors.append(f"record {index} has unstable/cross-paper record_id")
        if expected_id in seen: errors.append(f"duplicate record_id {expected_id}")
        seen.add(expected_id)
    expected_no_records = len(records) == 0 and not rejected
    if result.get("no_records_found") is not expected_no_records: errors.append("no_records_found does not match records/rejected candidates")
    if result.get("absence_claim") is not False: errors.append("no-record result must not claim data absence")
    return sorted(set(errors)), result, job


def write_validation(
    paths: dict[str, Path], job_id: str, *, require_source: bool = False,
    attempt_binding: dict[str, Any] | None = None,
    result_file: Path | None = None,
) -> tuple[bool, list[str]]:
    errors, result, job = validate(
        paths, job_id, require_source=require_source, attempt_binding=attempt_binding,
        result_file=result_file,
    )
    payload = {
        "validation_version": "2.0", "task_id": job.get("task_id", ""),
        "job_id": job_id, "attempt_id": result.get("attempt_id", "") if result else "",
        "run_id": result.get("run_id", "") if result else "",
        "source_sha256": job.get("source_sha256", ""),
        "parsed_text_sha256": job.get("parsed_text_sha256", ""),
        "sections_sha256": job.get("sections_sha256", ""),
        "sections_count": job.get("sections_count", 0),
        "sections_schema_version": job.get("sections_schema_version", ""),
        "spec_sha256": job.get("spec_sha256", ""),
        "result_sha256": sha256_file(
            result_file.resolve() if result_file is not None else result_path(paths, job_id)
        ) if result else "",
        "valid": not errors, "errors": errors, "validated_at": utc_now(),
    }
    target = validation_path(paths, job_id)
    # A validation snapshot is a content binding, not a clock.  Re-running the
    # validator over identical inputs must not invalidate an acceptance marker
    # or an already-built delivery merely by changing ``validated_at``.
    if target.is_file():
        try:
            existing = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing = None
        if (isinstance(existing, dict)
                and parse_time(str(existing.get("validated_at") or "")) is not None
                and {key: value for key, value in existing.items() if key != "validated_at"}
                == {key: value for key, value in payload.items() if key != "validated_at"}):
            return not errors, errors
    atomic_write_json(target, payload)
    return not errors, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--root", default="."); parser.add_argument("--task-id", required=True); parser.add_argument("--job-id", required=True); args = parser.parse_args()
    paths = task_paths(Path(args.root), args.task_id)
    try: valid, errors = write_validation(paths, args.job_id)
    except (OSError, ValueError) as exc: parser.error(str(exc))
    print(validation_path(paths, args.job_id))
    if not valid:
        for error in errors: print(f"ERROR: {error}")
        return 1
    return 0


if __name__ == "__main__": raise SystemExit(main())
