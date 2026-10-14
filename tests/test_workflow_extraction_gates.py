from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from scripts import extraction_common, workflow_status

from test_data_extraction import paperqa_marker, setup_task


def gate_context(root: Path) -> tuple[str, dict, list[dict], dict]:
    task_id, _ = setup_task(root)
    paths = extraction_common.task_paths(root, task_id)
    job = extraction_common.load_queue(paths["queue"])["jobs"][0]
    accepted_processing_job = {
        "source_item_id": job["item_id"],
        "paper_id": job["paper_id"],
        "source_kind": job["source_kind"],
        "source_path": job["source_path"],
        "source_sha256": job["source_sha256"],
        "parsed_text_path": job["parsed_text_path"],
        "parsed_text_sha256": job["parsed_text_sha256"],
        "sections_path": job["sections_path"],
        "sections_sha256": job["sections_sha256"],
        "sections_count": job["sections_count"],
        "processor": job["parser"],
        "processor_version": job["parser_version"],
    }
    batches = [{
        "batch_id": job["batch_id"],
        "_processing": {"_accepted_jobs": [accepted_processing_job]},
        "queue": {"_overall": {job["item_id"]: job["fulltext_decision"]}},
    }]
    spec_gate = workflow_status.extraction_spec_gate(
        root, paths["task"], task_id,
    )
    assert spec_gate["valid"]
    return task_id, paths, batches, job


def run_queue_gate(root: Path, task_id: str, paths: dict,
                   batches: list[dict]) -> dict:
    spec_gate = workflow_status.extraction_spec_gate(root, paths["task"], task_id)
    return workflow_status.extraction_queue_gate(
        root, paths["task"], task_id, spec_gate, batches,
    )


def test_workflow_gate_uses_full_paperqa_contract(tmp_path: Path) -> None:
    task_id, paths, batches, job = gate_context(tmp_path)
    marker_id = paperqa_marker(tmp_path, task_id)
    assert marker_id == job["job_id"]
    valid = run_queue_gate(tmp_path, task_id, paths, batches)
    assert valid["valid"]
    assert valid["paperqa_invalid"] == []

    marker_path = paths["paperqa_results"] / f"{marker_id}.json"
    marker_path.write_text(json.dumps({
        "job_id": marker_id,
        "spec_version": job["spec_version"],
        "spec_sha256": job["spec_sha256"],
        "source_bindings": [],
        "result": {"evidence_status": "no_evidence", "evidence": []},
    }), encoding="utf-8")
    rejected = run_queue_gate(tmp_path, task_id, paths, batches)
    assert rejected["valid"]
    assert rejected["paperqa_invalid"] == [marker_id]
    assert "marker missing keys" in " ".join(
        rejected["paperqa_issue_details"]
    ).casefold()


def test_workflow_gate_rejects_symlink_and_oversize_paperqa_markers(
    tmp_path: Path,
) -> None:
    task_id, paths, batches, job = gate_context(tmp_path)
    marker_id = paperqa_marker(tmp_path, task_id)
    marker_path = paths["paperqa_results"] / f"{marker_id}.json"
    target = paths["paperqa_results"] / "real-marker.json"
    marker_path.rename(target)
    marker_path.symlink_to(target.name)
    symlinked = run_queue_gate(tmp_path, task_id, paths, batches)
    assert symlinked["paperqa_invalid"] == [job["job_id"]]

    marker_path.unlink()
    target.rename(marker_path)
    with patch.object(workflow_status, "MAX_PAPERQA_MARKER_BYTES", 16):
        oversized = run_queue_gate(tmp_path, task_id, paths, batches)
    assert oversized["paperqa_invalid"] == [job["job_id"]]
    assert "exceeds 16 bytes" in " ".join(oversized["paperqa_issue_details"])


def test_workflow_gate_uses_authoritative_queue_contract(tmp_path: Path) -> None:
    task_id, paths, batches, job = gate_context(tmp_path)
    queue = json.loads(paths["queue"].read_text(encoding="utf-8"))
    queue["unexpected"] = True
    paths["queue"].write_text(json.dumps(queue), encoding="utf-8")
    invalid = run_queue_gate(tmp_path, task_id, paths, batches)
    assert not invalid["valid"]
    assert "queue keys mismatch" in " ".join(invalid["issues"])

    queue.pop("unexpected")
    queue["jobs"][0]["job_id"] = "unsafe/rebound"
    paths["queue"].write_text(json.dumps(queue), encoding="utf-8")
    rebound = run_queue_gate(tmp_path, task_id, paths, batches)
    assert not rebound["valid"]
    assert "job_id" in " ".join(rebound["issues"])


def test_workflow_gate_binds_section_sidecar_to_processing_acceptance(
    tmp_path: Path,
) -> None:
    task_id, paths, batches, job = gate_context(tmp_path)
    baseline = run_queue_gate(tmp_path, task_id, paths, batches)
    assert baseline["valid"]
    assert baseline["paperqa_missing"] == [job["job_id"]]

    processing_job = batches[0]["_processing"]["_accepted_jobs"][0]
    processing_job["sections_count"] += 1
    drifted = run_queue_gate(tmp_path, task_id, paths, batches)
    assert not drifted["valid"]
    assert "sections_count binding mismatch" in " ".join(drifted["issues"])

    processing_job["sections_count"] -= 1
    sidecar = tmp_path / job["sections_path"]
    sidecar.write_bytes(sidecar.read_bytes() + b"{}\n")
    tampered = run_queue_gate(tmp_path, task_id, paths, batches)
    assert not tampered["valid"]
    assert "sections_path content hash mismatch" in " ".join(tampered["issues"])
