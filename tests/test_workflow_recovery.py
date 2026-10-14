from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import processing_queue as queue
import workflow_recovery as recovery


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


class WorkflowRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        write_json(self.root / "00_control/WORKSPACE.json", {"workspace_id": "test"})
        self.task_id = "T1"
        self.batch_id = "DL-1"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def status(
        self, *, issues: list[str] | None = None,
        stage: str | None = None,
    ) -> dict:
        issue_values = list(issues or [])
        gate = {
            "exists": True,
            "valid": not issue_values,
            "complete": False,
            "issues": issue_values,
            "status_counts": {},
        }
        return {
            "workspace": str(self.root),
            "stage": stage or (
                "STAGE_3_DOCUMENT_PROCESSING_INVALID"
                if issues else "STAGE_3_DOCUMENT_PROCESSING"
            ),
            "reason": "test",
            "next_action": "test",
            "evidence": {
                "task_id": self.task_id,
                "selected_batch_id": self.batch_id,
                "all_related_batches": [{"batch_id": self.batch_id}],
                "batches": [{
                    "batch_id": self.batch_id,
                    "status": "ACCEPTED",
                    "document_processing": gate,
                }],
            },
        }

    def make_database(self):
        path = queue.database_path(self.root, self.batch_id)
        db = queue.connect(path)
        queue.initialize(db)
        queue.insert_job(db, {
            "job_id": "dp-one",
            "batch_id": self.batch_id,
            "source_item_id": "I-1",
            "paper_id": "P-1",
            "document_id": "sha256-" + "a" * 64,
            "title": "One",
            "doi": "",
            "source_kind": "xml",
            "source_path": "source.xml",
            "source_sha256": "a" * 64,
            "processor": "xml_to_markdown",
            "processor_version": "xml-local-v1",
            "output_dir": "out",
            "max_attempts": 3,
        })
        return path, db

    def test_planner_only_rebuilds_a_pure_projection_failure(self) -> None:
        _, db = self.make_database()
        db.close()
        safe = recovery.plan_safe_actions(
            self.root,
            self.status(issues=["queue.csv missing"]),
            task_id=self.task_id,
            batch_id=self.batch_id,
        )
        self.assertEqual(
            [action["kind"] for action in safe],
            ["rebuild_document_processing_projection"],
        )

        unsafe = recovery.plan_safe_actions(
            self.root,
            self.status(issues=[
                "queue.csv missing",
                "processing job dp-one source_sha256 differs from the current PDF/XML",
            ]),
            task_id=self.task_id,
            batch_id=self.batch_id,
        )
        self.assertEqual(unsafe, [])

    def test_projection_repair_uses_sqlite_authority(self) -> None:
        _, db = self.make_database()
        db.close()
        status = self.status(issues=["queue.csv missing"])
        action = recovery.plan_safe_actions(
            self.root, status, task_id=self.task_id, batch_id=self.batch_id
        )[0]
        with mock.patch.object(
            recovery.workflow_status, "inspect", return_value=status,
        ):
            outcome = recovery._execute_action(self.root, action)
        self.assertTrue(outcome["projection_rebuilt"])
        projection = queue.projection_path(self.root, self.batch_id)
        self.assertTrue(projection.is_file())
        self.assertIn("dp-one", projection.read_text(encoding="utf-8"))

    def test_projection_repair_is_not_offered_for_an_invalid_database(self) -> None:
        path = queue.database_path(self.root, self.batch_id)
        path.parent.mkdir(parents=True)
        path.write_bytes(b"not sqlite")
        actions = recovery.plan_safe_actions(
            self.root,
            self.status(issues=["queue.csv missing"]),
            task_id=self.task_id,
            batch_id=self.batch_id,
        )
        self.assertEqual(actions, [])

    def test_status_is_read_only(self) -> None:
        status = self.status()
        with mock.patch.object(recovery.workflow_status, "inspect", return_value=status):
            report = recovery.inspect_recovery(
                self.root, task_id=self.task_id, batch_id=self.batch_id
            )
        self.assertEqual(report["classification"], "workflow_action_required")
        self.assertFalse((self.root / "runtime/recovery").exists())

    def test_same_defect_is_not_replayed_after_failed_postcondition(self) -> None:
        status = self.status(issues=["queue.csv missing"])
        action = recovery._action(
            "rebuild_document_processing_projection",
            status=status,
            task_id=self.task_id,
            batch_id=self.batch_id,
            target="02_batches/DL-1/outputs/document_processing/queue.csv",
        )
        with (
            mock.patch.object(recovery.workflow_status, "inspect", return_value=status),
            mock.patch.object(recovery, "plan_safe_actions", return_value=[action]),
            mock.patch.object(recovery, "_execute_action", return_value={"projection_rebuilt": True}),
        ):
            first = recovery.run_recovery(
                self.root,
                task_id=self.task_id,
                batch_id=self.batch_id,
                max_actions=3,
            )
            second = recovery.run_recovery(
                self.root,
                task_id=self.task_id,
                batch_id=self.batch_id,
                max_actions=3,
            )
        self.assertEqual(len(first["attempts"]), 1)
        self.assertEqual(first["attempts"][0]["result"], "unresolved_or_worsened")
        self.assertEqual(first["stop_reason"], "repair_unresolved_or_worsened")
        self.assertEqual(second["attempts"], [])
        self.assertEqual(second["stop_reason"], "automatic_repair_budget_exhausted")
        ledger = self.root / first["ledger"]
        events = [json.loads(line) for line in ledger.read_text().splitlines()]
        self.assertEqual([event["phase"] for event in events], ["started", "finished"])

    def test_hard_action_limit_is_enforced(self) -> None:
        with self.assertRaisesRegex(recovery.RecoveryError, "between 1 and 3"):
            recovery.run_recovery(
                self.root,
                task_id=self.task_id,
                batch_id=self.batch_id,
                max_actions=4,
            )

    def test_unsafe_task_and_batch_ids_fail_closed(self) -> None:
        status = self.status()
        status["evidence"]["task_id"] = "../escape"
        with self.assertRaises(recovery.RecoveryError):
            recovery._selected_task(status, None)
        status = self.status()
        status["evidence"]["selected_batch_id"] = "../escape"
        with self.assertRaises(recovery.RecoveryError):
            recovery._selected_batch(status, None)
        status = self.status()
        with self.assertRaisesRegex(recovery.RecoveryError, "differs"):
            recovery._selected_task(status, "T2")
        with self.assertRaisesRegex(recovery.RecoveryError, "differs"):
            recovery._selected_batch(status, "DL-2")

    def test_action_id_binds_the_exact_repair_targets(self) -> None:
        status = self.status()
        first = recovery._action(
            "rebuild_document_processing_projection",
            status=status,
            task_id=self.task_id,
            batch_id=self.batch_id,
            target="02_batches/DL-1/outputs/document_processing/queue.csv",
        )
        second = recovery._action(
            "rebuild_document_processing_projection",
            status=status,
            task_id=self.task_id,
            batch_id=self.batch_id,
            target="02_batches/DL-2/outputs/document_processing/queue.csv",
        )
        self.assertNotEqual(first["action_id"], second["action_id"])

    def test_expired_lease_is_not_recovered_when_source_binding_is_invalid(self) -> None:
        _, db = self.make_database()
        try:
            start = datetime.now(timezone.utc).replace(microsecond=0)
            queue.claim_one(db, "worker-a", lease_seconds=30, now=start.isoformat())
            db.execute(
                "UPDATE jobs SET lease_expires_at='2000-01-01T00:00:00+00:00' "
                "WHERE job_id='dp-one'"
            )
            db.commit()
        finally:
            db.close()
        status = self.status(issues=[
            "processing job dp-one source_sha256 differs from the current PDF/XML",
        ])
        actions = recovery.plan_safe_actions(
            self.root, status, task_id=self.task_id, batch_id=self.batch_id,
        )
        self.assertEqual(actions, [])

    def test_stale_processing_database_is_ignored_during_download_stage(self) -> None:
        _, db = self.make_database()
        try:
            start = datetime.now(timezone.utc).replace(microsecond=0)
            queue.claim_one(db, "worker-a", lease_seconds=30, now=start.isoformat())
            db.execute(
                "UPDATE jobs SET lease_expires_at='2000-01-01T00:00:00+00:00' "
                "WHERE job_id='dp-one'"
            )
            db.commit()
        finally:
            db.close()
        status = self.status(stage="STAGE_2_DOWNLOAD")
        actions = recovery.plan_safe_actions(
            self.root, status, task_id=self.task_id, batch_id=self.batch_id,
        )
        self.assertEqual(actions, [])

    def test_projection_only_invalid_gate_repairs_projection_but_not_lease(self) -> None:
        _, db = self.make_database()
        try:
            start = datetime.now(timezone.utc).replace(microsecond=0)
            queue.claim_one(db, "worker-a", lease_seconds=30, now=start.isoformat())
            db.execute(
                "UPDATE jobs SET lease_expires_at='2000-01-01T00:00:00+00:00' "
                "WHERE job_id='dp-one'"
            )
            db.commit()
        finally:
            db.close()
        actions = recovery.plan_safe_actions(
            self.root,
            self.status(issues=["queue.csv missing"]),
            task_id=self.task_id,
            batch_id=self.batch_id,
        )
        self.assertEqual(
            [action["kind"] for action in actions],
            ["rebuild_document_processing_projection"],
        )
        database = queue.database_path(self.root, self.batch_id)
        check = queue.connect(database)
        try:
            self.assertEqual(
                check.execute(
                    "SELECT status FROM jobs WHERE job_id='dp-one'"
                ).fetchone()[0],
                "claimed",
            )
        finally:
            check.close()

    def test_receipt_merge_is_never_an_automatic_recovery_action(self) -> None:
        status = self.status(stage="STAGE_5_MERGE_EXTRACTION_RECEIPTS")
        status["evidence"]["data_extraction_queue"] = {
            "issues": [],
            "pending_receipts": [
                f"runtime/results/{self.task_id}/data_extraction/job/receipts/attempt.json"
            ],
        }
        actions = recovery.plan_safe_actions(
            self.root, status, task_id=self.task_id, batch_id=self.batch_id,
        )
        self.assertEqual(actions, [])

    def test_stable_budget_blocks_recurrence_even_when_evidence_changes(self) -> None:
        first_status = self.status()
        second_status = self.status()
        second_status["reason"] = "an unrelated clock/status field changed"
        first = recovery._action(
            "rebuild_document_processing_projection",
            status=first_status,
            task_id=self.task_id,
            batch_id=self.batch_id,
            target="02_batches/DL-1/outputs/document_processing/queue.csv",
        )
        second = recovery._action(
            "rebuild_document_processing_projection",
            status=second_status,
            task_id=self.task_id,
            batch_id=self.batch_id,
            target="02_batches/DL-1/outputs/document_processing/queue.csv",
        )
        self.assertNotEqual(first["action_id"], second["action_id"])
        self.assertEqual(first["budget_keys"], second["budget_keys"])
        eligible, exhausted = recovery._eligible_actions(
            [second], attempted_action_ids={first["action_id"]},
            spent_budget_keys=set(first["budget_keys"]),
        )
        self.assertEqual(eligible, [])
        self.assertEqual(exhausted, [second])

    def test_different_projection_target_remains_eligible(self) -> None:
        status = self.status()
        first = recovery._action(
            "rebuild_document_processing_projection",
            status=status,
            task_id=self.task_id,
            batch_id=self.batch_id,
            target="02_batches/DL-1/outputs/document_processing/queue.csv",
        )
        second = recovery._action(
            "rebuild_document_processing_projection",
            status=status,
            task_id=self.task_id,
            batch_id=self.batch_id,
            target="runtime/tasks/T1/extraction/data_extraction_queue.csv",
        )
        eligible, exhausted = recovery._eligible_actions(
            [second], attempted_action_ids={first["action_id"]},
            spent_budget_keys=set(first["budget_keys"]),
        )
        self.assertEqual(eligible, [second])
        self.assertEqual(exhausted, [])

    def test_unrelated_status_change_does_not_mask_an_unresolved_defect(self) -> None:
        before = self.status(issues=["queue.csv missing"])
        after = self.status(issues=["queue.csv missing"])
        after["reason"] = "unrelated concurrent update"
        action = recovery._action(
            "rebuild_document_processing_projection",
            status=before,
            task_id=self.task_id,
            batch_id=self.batch_id,
            target="02_batches/DL-1/outputs/document_processing/queue.csv",
        )
        with (
            mock.patch.object(
                recovery.workflow_status, "inspect",
                side_effect=[before, before, after, after],
            ),
            mock.patch.object(recovery, "plan_safe_actions", return_value=[action]),
            mock.patch.object(
                recovery, "_execute_action", return_value={"projection_rebuilt": True},
            ),
        ):
            report = recovery.run_recovery(
                self.root,
                task_id=self.task_id,
                batch_id=self.batch_id,
                max_actions=3,
            )
        self.assertEqual(report["attempts"][0]["result"], "unresolved_or_worsened")
        self.assertEqual(report["stop_reason"], "repair_unresolved_or_worsened")

    def test_dangling_recovery_lock_never_creates_its_external_target(self) -> None:
        recovery_dir = self.root / "runtime/recovery"
        recovery_dir.mkdir(parents=True)
        outside = self.root.parent / f"{self.root.name}-outside-recovery-lock"
        (recovery_dir / ".recovery.lock").symlink_to(outside)
        status = self.status(stage="STAGE_MINUS_1_QUERY_DESIGN")
        with mock.patch.object(
            recovery.workflow_status, "inspect", return_value=status,
        ), self.assertRaises(OSError):
            recovery.run_recovery(
                self.root,
                task_id=self.task_id,
                batch_id=self.batch_id,
                max_actions=1,
            )
        self.assertFalse(outside.exists())

    def test_dangling_ledger_is_rejected_without_resetting_budget(self) -> None:
        ledger_dir = (
            self.root / "runtime/recovery" / recovery._task_key(self.task_id)
        )
        ledger_dir.mkdir(parents=True)
        outside = self.root.parent / f"{self.root.name}-outside-ledger"
        (ledger_dir / "events.jsonl").symlink_to(outside)
        status = self.status(stage="STAGE_MINUS_1_QUERY_DESIGN")
        with mock.patch.object(
            recovery.workflow_status, "inspect", return_value=status,
        ), self.assertRaises(OSError):
            recovery.inspect_recovery(
                self.root, task_id=self.task_id, batch_id=self.batch_id,
            )
        self.assertFalse(outside.exists())

    def test_hardlinked_ledger_is_rejected_without_external_write(self) -> None:
        ledger_dir = (
            self.root / "runtime/recovery" / recovery._task_key(self.task_id)
        )
        ledger_dir.mkdir(parents=True)
        outside = self.root.parent / f"{self.root.name}-outside-hardlink-ledger"
        outside.write_text("outside-sentinel\n", encoding="utf-8")
        (ledger_dir / "events.jsonl").hardlink_to(outside)
        status = self.status(stage="STAGE_MINUS_1_QUERY_DESIGN")
        try:
            with mock.patch.object(
                recovery.workflow_status, "inspect", return_value=status,
            ), self.assertRaisesRegex(ValueError, "single-link regular file"):
                recovery.inspect_recovery(
                    self.root, task_id=self.task_id, batch_id=self.batch_id,
                )
            self.assertEqual(
                outside.read_text(encoding="utf-8"), "outside-sentinel\n",
            )
        finally:
            outside.unlink(missing_ok=True)

    def test_document_projection_rejects_dangling_lock_symlink(self) -> None:
        _, db = self.make_database()
        db.close()
        status = self.status(issues=["queue.csv missing"])
        action = recovery.plan_safe_actions(
            self.root, status, task_id=self.task_id, batch_id=self.batch_id,
        )[0]
        lock = queue.projection_path(self.root, self.batch_id).with_name(
            "queue.csv.lock"
        )
        outside = self.root.parent / f"{self.root.name}-outside-doc-lock"
        lock.symlink_to(outside)
        with mock.patch.object(
            recovery.workflow_status, "inspect", return_value=status,
        ), self.assertRaises(OSError):
            recovery._execute_action(self.root, action)
        self.assertFalse(outside.exists())

    def test_document_projection_rejects_concurrent_ancestor_symlink_swap(self) -> None:
        _, db = self.make_database()
        db.close()
        status = self.status(issues=["queue.csv missing"])
        action = recovery.plan_safe_actions(
            self.root, status, task_id=self.task_id, batch_id=self.batch_id,
        )[0]
        processing = queue.database_path(self.root, self.batch_id).parent
        moved = processing.with_name("document_processing.old")
        outside = self.root.parent / f"{self.root.name}-outside-doc-processing"
        outside.mkdir()
        outside_projection = outside / "queue.csv"
        outside_projection.write_text("outside-sentinel\n", encoding="utf-8")
        real_write = recovery.extraction_common.PinnedDirectory.atomic_write_text
        swapped = False

        def swap_before_write(directory, name: str, value: str) -> None:
            nonlocal swapped
            if name == "queue.csv" and not swapped:
                processing.rename(moved)
                processing.symlink_to(outside, target_is_directory=True)
                swapped = True
            real_write(directory, name, value)

        try:
            with (
                mock.patch.object(
                    recovery.workflow_status, "inspect", return_value=status,
                ),
                mock.patch.object(
                    recovery.extraction_common.PinnedDirectory,
                    "atomic_write_text",
                    swap_before_write,
                ),
                self.assertRaises((OSError, ValueError)),
            ):
                recovery._execute_action(self.root, action)
            self.assertEqual(
                outside_projection.read_text(encoding="utf-8"),
                "outside-sentinel\n",
            )
        finally:
            if processing.is_symlink():
                processing.unlink()
            if moved.exists():
                moved.rename(processing)
            outside_projection.unlink(missing_ok=True)
            outside.rmdir()


if __name__ == "__main__":
    unittest.main()
