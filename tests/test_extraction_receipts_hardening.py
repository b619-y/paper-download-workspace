from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import luna_extract_data
import merge_extraction_receipts
from extraction_common import (
    completion_receipt_path, load_queue, task_paths, utc_now, write_queue,
)
from test_data_extraction import digest, model_response, paperqa_marker, setup_task


def _expire_first_attempt(paths: dict[str, Path], job_id: str) -> dict:
    first, _ = luna_extract_data.claim(paths, "worker-1", 30, job_id)
    queue = load_queue(paths["queue"])
    queue["jobs"][0]["lease_expires_at"] = "2000-01-01T00:00:00+00:00"
    queue["jobs"][0]["heartbeat_at"] = "2000-01-01T00:00:00+00:00"
    write_queue(paths, queue)
    return first


def test_immutable_receipt_uses_atomic_no_clobber_publish(tmp_path: Path) -> None:
    target = tmp_path / "receipts" / "attempt.json"
    receipt = {"attempt": 1, "status": "retry_wait"}

    def fail_before_publish(source: os.PathLike[str], destination: os.PathLike[str]) -> None:
        assert Path(source).is_file()
        assert Path(destination) == target
        assert not target.exists()
        raise OSError("injected publication failure")

    with patch.object(luna_extract_data.os, "link", fail_before_publish), pytest.raises(
        OSError, match="injected"
    ):
        luna_extract_data.write_immutable_receipt(target, receipt)
    assert not target.exists()
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []

    luna_extract_data.write_immutable_receipt(target, receipt)
    original = target.read_bytes()
    luna_extract_data.write_immutable_receipt(target, dict(receipt))
    with pytest.raises(ValueError, match="refusing to replace"):
        luna_extract_data.write_immutable_receipt(target, {**receipt, "status": "failed"})
    assert target.read_bytes() == original


def test_expired_torn_receipt_is_quarantined_and_recovered(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path)
    paths = task_paths(tmp_path, task_id)
    job_id = load_queue(paths["queue"])["jobs"][0]["job_id"]
    first = _expire_first_attempt(paths, job_id)
    receipt_file = completion_receipt_path(paths, job_id, first["attempt_id"])
    receipt_file.parent.mkdir(parents=True, exist_ok=True)
    receipt_file.write_bytes(b'{"receipt_version":')

    second, _ = luna_extract_data.claim(paths, "worker-2", 30, job_id)
    assert second["attempt_count"] == 2
    assert second["receipt_history"][0]["status"] == "retry_wait"
    recovered = merge_extraction_receipts.read_receipt(receipt_file)
    assert recovered["error_code"] == "torn_receipt_recovered"
    assert recovered["attempt_id"] == first["attempt_id"]
    quarantined = list((receipt_file.parent / ".torn").glob("*.torn"))
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == b'{"receipt_version":'
    assert second["receipt_history"][0]["receipt_sha256"] == digest(receipt_file)


def test_expired_valid_wrong_binding_receipt_fails_closed(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path)
    paths = task_paths(tmp_path, task_id)
    job_id = load_queue(paths["queue"])["jobs"][0]["job_id"]
    first = _expire_first_attempt(paths, job_id)
    receipt = luna_extract_data.receipt_for(
        first, "retry_wait", error_code="interrupted", error_message="interrupted",
        available_at=utc_now(),
    )
    receipt["paper_id"] = "wrong-paper"
    receipt_file = completion_receipt_path(paths, job_id, first["attempt_id"])
    luna_extract_data.write_immutable_receipt(receipt_file, receipt)
    queue_before = paths["queue"].read_bytes()
    receipt_before = receipt_file.read_bytes()

    with pytest.raises(ValueError, match="binding mismatch: paper_id"):
        luna_extract_data.claim(paths, "worker-2", 30, job_id)
    assert paths["queue"].read_bytes() == queue_before
    assert receipt_file.read_bytes() == receipt_before
    assert not (receipt_file.parent / ".torn").exists()


@pytest.mark.parametrize("tamper", ["receipt", "immutable_result"])
def test_merge_rechecks_receipt_and_result_at_final_commit(
    tmp_path: Path, tamper: str,
) -> None:
    task_id, _ = setup_task(tmp_path)
    job_id = paperqa_marker(tmp_path, task_id)
    argv = [
        "extract", "--root", str(tmp_path), "--task-id", task_id,
        "--job-id", job_id, "--worker", "remote", "--external-model-authorized",
    ]
    with patch.object(luna_extract_data, "call_codex", model_response), patch.object(
        sys, "argv", argv,
    ):
        assert luna_extract_data.main() == 0

    paths = task_paths(tmp_path, task_id)
    completed = load_queue(paths["queue"])["jobs"][0]
    receipt_file = tmp_path / completed["receipt_path"]
    immutable = (
        paths["extraction_results"] / job_id / "attempts"
        / completed["attempt_id"] / "result.json"
    )
    queue = load_queue(paths["queue"])
    queued = queue["jobs"][0]
    queued.update({
        "status": "queued", "attempt_count": 0, "run_id": "", "attempt_id": "",
        "claimed_by": "", "lease_token": "", "lease_expires_at": "",
        "heartbeat_at": "", "available_at": utc_now(), "error_code": "",
        "error_message": "", "receipt_history": [],
    })
    queued.pop("receipt_path", None)
    queued.pop("receipt_sha256", None)
    write_queue(paths, queue)
    queue_before = paths["queue"].read_bytes()
    real_write_validation = merge_extraction_receipts.write_validation

    def validate_then_tamper(*args, **kwargs):
        outcome = real_write_validation(*args, **kwargs)
        target = receipt_file if tamper == "receipt" else immutable
        target.write_bytes(target.read_bytes() + b" \n")
        return outcome

    with patch.object(
        merge_extraction_receipts, "write_validation", validate_then_tamper,
    ), pytest.raises(ValueError, match="changed before queue commit"):
        merge_extraction_receipts.merge_one(paths, receipt_file)
    assert paths["queue"].read_bytes() == queue_before


def test_zero_width_locator_is_not_visible_evidence() -> None:
    response = model_response("", "", "")
    response["records"][0]["fields"][0]["evidence"][0]["locator"] = "\u200b"
    with pytest.raises(ValueError, match="evidence"):
        luna_extract_data.validate_response(response)


def test_receipt_merge_rejects_a_symlinked_task_results_root(tmp_path: Path) -> None:
    task_id, _ = setup_task(tmp_path)
    paths = task_paths(tmp_path, task_id)
    outside = tmp_path.parent / f"{tmp_path.name}-external-results"
    receipt = outside / "data_extraction/job/receipts/attempt.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text("{}", encoding="utf-8")
    paths["results"].parent.mkdir(parents=True, exist_ok=True)
    paths["results"].symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="must not contain symlinks"):
        merge_extraction_receipts.merge_one(paths, receipt)
