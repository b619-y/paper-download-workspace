from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts import langgraph_coordinator as coordinator


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


class CoordinatorPureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        write_json(self.root / "00_control/WORKSPACE.json", {"workspace_id": "test"})

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_checkpoint_must_remain_below_runtime(self) -> None:
        expected = self.root / "runtime/orchestration/langgraph.sqlite3"
        self.assertEqual(
            coordinator.resolve_checkpoint_path(self.root), expected.resolve()
        )
        with self.assertRaisesRegex(coordinator.CoordinatorError, "inside.*runtime"):
            coordinator.resolve_checkpoint_path(self.root, "langgraph.sqlite3")
        with self.assertRaisesRegex(coordinator.CoordinatorError, "inside.*runtime"):
            coordinator.resolve_checkpoint_path(self.root, self.root.parent / "outside.sqlite3")

    def test_thread_ids_are_stable_and_short(self) -> None:
        self.assertEqual(coordinator.default_thread_id("task-1"), "task-1")
        long_task = "任" * 300
        mapped = coordinator.default_thread_id(long_task)
        self.assertEqual(mapped, coordinator.default_thread_id(long_task))
        self.assertLess(len(mapped.encode("utf-8")), 255)
        with self.assertRaises(coordinator.CoordinatorError):
            coordinator.validate_thread_id("bad\nthread")

    def test_decisions_are_strictly_whitelisted_per_interrupt(self) -> None:
        self.assertEqual(
            coordinator.normalize_resume_decision({"action": "APPROVE"}, {"approve", "stop"}),
            "approve",
        )
        with self.assertRaisesRegex(coordinator.CoordinatorError, "approve, stop"):
            coordinator.normalize_resume_decision("retry", {"approve", "stop"})
        with self.assertRaises(coordinator.CoordinatorError):
            coordinator.normalize_resume_decision({"action": "run-shell"}, {"approve"})

    def test_partition_reference_contains_only_allowlisted_references(self) -> None:
        partition = self.root / "runtime/orchestration/parts/openalex-1"
        partition.mkdir(parents=True)
        raw = {
            "source": "openalex",
            "status": "complete",
            "complete": True,
            "partition_dir": str(partition),
            "config_sha256": "a" * 64,
            "manifest_path": "run_manifest.json",
            "manifest_sha256": "b" * 64,
            "records_path": "openalex/records.csv",
            "records_sha256": "c" * 64,
            "source_tree_sha256": "d" * 64,
            "query_id": "Q1",
            "fetched_count": 7,
            "pages": 1,
            "max_records": 0,
            "binding": {
                "version": coordinator.PARTITION_BINDING_VERSION,
                "source": "openalex",
                "config_sha256": "a" * 64,
                "manifest_sha256": "b" * 64,
                "records_sha256": "c" * 64,
                "source_tree_sha256": "d" * 64,
                "native_query_sha256": "e" * 64,
                "max_records": 0,
            },
            "original_query": "must not enter checkpoint",
            "native_query": "must not enter checkpoint",
            "secret": "must not enter checkpoint",
        }
        reference = coordinator.partition_result_reference(
            self.root, raw, orchestration_id="lg-1", search_attempt=1
        )
        self.assertNotIn("original_query", reference)
        self.assertNotIn("native_query", reference)
        self.assertNotIn("secret", reference)
        self.assertEqual(
            reference["partition_dir"],
            "runtime/orchestration/parts/openalex-1",
        )
        self.assertEqual(reference["binding"]["version"], coordinator.PARTITION_BINDING_VERSION)
        invalid = dict(raw)
        invalid["max_records"] = True
        with self.assertRaisesRegex(coordinator.CoordinatorError, "max_records"):
            coordinator.partition_result_reference(
                self.root, invalid, orchestration_id="lg-1", search_attempt=1
            )

    def test_partition_reducer_is_idempotent_and_rejects_conflicts(self) -> None:
        item = {
            "orchestration_id": "lg-1",
            "search_attempt": 1,
            "source": "openalex",
            "partition_dir": "runtime/p",
        }
        self.assertEqual(coordinator.merge_partition_results([item], [dict(item)]), [item])
        conflict = {**item, "partition_dir": "runtime/other"}
        with self.assertRaisesRegex(coordinator.CoordinatorError, "conflicting"):
            coordinator.merge_partition_results([item], [conflict])

    def test_stage_routing_and_interrupt_contract(self) -> None:
        self.assertEqual(
            coordinator.route_for_stage("STAGE_0_SEARCH_INCOMPLETE"),
            "search_incomplete",
        )
        self.assertEqual(coordinator.route_for_stage("STAGE_2_DOWNLOAD"), "end")
        view = coordinator.expected_interrupt({"stage": "STAGE_1_HANDOFF_TO_DOWNLOAD"})
        self.assertTrue(view["required"])
        self.assertEqual(view["allowed_actions"], ["approve", "stop"])

    def test_status_cli_does_not_import_langgraph(self) -> None:
        blocker = self.root / "blocker"
        blocker.mkdir()
        (blocker / "langgraph.py").write_text(
            "raise RuntimeError('status imported langgraph')\n", encoding="utf-8"
        )
        script = Path(coordinator.__file__).resolve()
        environment = dict(os.environ)
        environment["PYTHONPATH"] = str(blocker)
        completed = subprocess.run(
            [sys.executable, str(script), "status", "--root", str(self.root)],
            text=True,
            capture_output=True,
            env=environment,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        report = json.loads(completed.stdout)
        self.assertEqual(report["stage"], "NOT_STARTED")
        self.assertIn("interrupt", report)
        self.assertIn("next_action", report)

    def test_checkpoint_schema_and_control_plane_versions_fail_closed(self) -> None:
        state = dict(coordinator.initial_state("T1", "T1"))
        coordinator.validate_checkpoint_state(
            self.root, state, task_id="T1", thread_id="T1"
        )
        state["schema_version"] = 99
        with self.assertRaisesRegex(coordinator.CoordinatorError, "unsupported"):
            coordinator.validate_checkpoint_state(
                self.root, state, task_id="T1", thread_id="T1"
            )


try:
    from langgraph.checkpoint.sqlite import SqliteSaver
    from langgraph.types import Command
except ImportError:  # pragma: no cover - exercised in the minimal dependency job
    SqliteSaver = None
    Command = None


@unittest.skipIf(SqliteSaver is None, coordinator.DEPENDENCY_HINT)
class CoordinatorLangGraphIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        write_json(self.root / "00_control/WORKSPACE.json", {"workspace_id": "test"})
        self.task_root = self.root / "runtime/tasks/T1"
        write_json(self.task_root / "task_brief.json", {
            "task_id": "T1",
            "title": "Test",
            "research_question": "Which papers?",
            "inclusion_criteria": ["relevant"],
            "exclusion_criteria": ["irrelevant"],
            "target_evidence": ["data"],
            "requires_fulltext_screening": True,
        })
        self.config_path = self.task_root / "query/configs/recall.json"
        write_json(self.config_path, {
            "query_id": "Q1",
            "original_query": "q",
            "source_order": ["openalex", "crossref"],
            "sources": {
                "openalex": {"query": "q"},
                "crossref": {"query": "q"},
            },
        })

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _status(self, stage: str) -> dict:
        return {
            "workspace": str(self.root),
            "stage": stage,
            "reason": f"reason:{stage}",
            "next_action": f"next:{stage}",
            "evidence": {
                "task_id": "T1",
                "task_root": "runtime/tasks/T1",
                "task_brief": "runtime/tasks/T1/task_brief.json",
                "query": {
                    "valid": True,
                    "config": "runtime/tasks/T1/query/configs/recall.json",
                    "planned_sources": ["openalex", "crossref"],
                },
                "search": {
                    "manifest": None,
                    "paths": {},
                },
            },
        }

    def test_start_interrupts_for_repair_without_search_side_effects(self) -> None:
        calls: list[str] = []

        class Search:
            def run_source_partition(self, *args, **kwargs):
                calls.append("run")
                raise AssertionError("search must not run before approval")

            def publish_search_run(self, *args, **kwargs):
                calls.append("publish")
                raise AssertionError("publish must not run before approval")

        def inspect(_root: Path, task_id: str | None = None) -> dict:
            return self._status("STAGE_MINUS_1_QUERY_DESIGN")

        checkpoint = self.root / "runtime/orchestration/test.sqlite3"
        checkpoint.parent.mkdir(parents=True)
        with SqliteSaver.from_conn_string(str(checkpoint)) as saver:
            graph = coordinator.build_graph(
                self.root, saver, inspect_fn=inspect, search_api=Search()
            )
            config = {"configurable": {"thread_id": "T1"}}
            graph.invoke(coordinator.initial_state("T1", "T1"), config)
            snapshot = graph.get_state(config)
        self.assertEqual(calls, [])
        self.assertEqual(snapshot.next, ("repair",))
        self.assertEqual(snapshot.interrupts[0].value["kind"], "repair_required")
        self.assertFalse(snapshot.interrupts[0].value["side_effects_before_interrupt"])

    def test_cli_runner_redisplays_pending_interrupt_and_rejects_stale_id(self) -> None:
        report = coordinator.run_graph_command(
            "start",
            root=self.root,
            task_id="T1",
            thread_id=None,
            checkpoint_db=coordinator.DEFAULT_CHECKPOINT,
        )
        current_id = report["interrupt"]["items"][0]["id"]

        redisplayed = coordinator.run_graph_command(
            "resume",
            root=self.root,
            task_id="T1",
            thread_id=None,
            checkpoint_db=coordinator.DEFAULT_CHECKPOINT,
        )
        self.assertTrue(redisplayed["interrupt"]["pending"])
        self.assertEqual(redisplayed["interrupt"]["items"][0]["id"], current_id)

        with self.assertRaisesRegex(coordinator.CoordinatorError, "stale"):
            coordinator.run_graph_command(
                "resume",
                root=self.root,
                task_id="T1",
                thread_id=None,
                checkpoint_db=coordinator.DEFAULT_CHECKPOINT,
                decision="stop",
                interrupt_id="stale-id",
            )

        stopped = coordinator.run_graph_command(
            "resume",
            root=self.root,
            task_id="T1",
            thread_id=None,
            checkpoint_db=coordinator.DEFAULT_CHECKPOINT,
            decision="stop",
            interrupt_id=current_id,
        )
        self.assertFalse(stopped["interrupt"]["pending"])
        self.assertEqual(stopped["checkpoint"]["next_nodes"], [])

    def test_incomplete_retry_creates_a_new_attempt_and_republishes(self) -> None:
        class Search:
            def __init__(self, root: Path) -> None:
                self.root = root
                self.publications = 0
                self.partition_calls: list[tuple[int, str]] = []

            def run_source_partition(
                self, config_path: Path, source: str, work_root: Path,
                max_records: int = 0, python_executable: str = sys.executable,
            ) -> dict:
                attempt = int(work_root.parent.name.split("-")[-1])
                self.partition_calls.append((attempt, source))
                partition = work_root / f"{source}-partition"
                partition.mkdir(parents=True, exist_ok=True)
                config_sha = hashlib.sha256(config_path.read_bytes()).hexdigest()
                return {
                    "source": source,
                    "status": "complete" if attempt == 2 else "failed",
                    "complete": attempt == 2,
                    "partition_dir": str(partition),
                    "config_sha256": config_sha,
                    "manifest_path": "run_manifest.json",
                    "manifest_sha256": "a" * 64,
                    "records_path": f"{source}/records.csv",
                    "records_sha256": "b" * 64,
                    "source_tree_sha256": "c" * 64,
                    "query_id": "Q1",
                    "fetched_count": 0,
                    "pages": 1,
                    "max_records": max_records,
                    "binding": {
                        "version": coordinator.PARTITION_BINDING_VERSION,
                        "source": source,
                        "config_sha256": config_sha,
                        "manifest_sha256": "a" * 64,
                        "records_sha256": "b" * 64,
                        "source_tree_sha256": "c" * 64,
                        "native_query_sha256": "d" * 64,
                        "max_records": max_records,
                    },
                }

            def publish_search_run(
                self, config_path: Path, source_results: list[dict], output_dir: Path,
                orchestration_id: str, search_attempt: int,
            ) -> dict:
                self.publications += 1
                output_dir.mkdir(parents=True, exist_ok=True)
                return {
                    "output_dir": str(output_dir),
                    "manifest_path": "run_manifest.json",
                    "manifest_sha256": str(search_attempt) * 64,
                    "reused": False,
                    "complete": search_attempt == 2,
                    "raw_record_count": 0,
                    "deduplicated_record_count": 0,
                    "abstract_present_count": 0,
                    "source_status": {
                        item["source"]: item["status"] for item in source_results
                    },
                    "config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
                    "orchestration_id": orchestration_id,
                    "search_attempt": search_attempt,
                }

        search = Search(self.root)

        def inspect(_root: Path, task_id: str | None = None) -> dict:
            stage = (
                "STAGE_0_API_SEARCH" if search.publications == 0
                else "STAGE_0_SEARCH_INCOMPLETE" if search.publications == 1
                else "STAGE_1_TITLE_ABSTRACT_SCREENING"
            )
            return self._status(stage)

        checkpoint = self.root / "runtime/orchestration/test.sqlite3"
        checkpoint.parent.mkdir(parents=True)
        with SqliteSaver.from_conn_string(str(checkpoint)) as saver:
            graph = coordinator.build_graph(
                self.root, saver, inspect_fn=inspect, search_api=search
            )
            config = {"configurable": {"thread_id": "T1"}}
            graph.invoke(coordinator.initial_state("T1", "T1"), config)
            first = graph.get_state(config)
            self.assertEqual(first.interrupts[0].value["kind"], "search_approval")

            graph.invoke(Command(resume={"action": "approve"}), config)
            incomplete = graph.get_state(config)
            self.assertEqual(incomplete.values["search_attempt"], 1)
            self.assertEqual(incomplete.interrupts[0].value["kind"], "search_incomplete")

            graph.invoke(Command(resume={"action": "retry"}), config)
            screening = graph.get_state(config)

        self.assertEqual(screening.values["search_attempt"], 2)
        self.assertEqual(screening.interrupts[0].value["kind"], "screening_required")
        self.assertEqual(search.publications, 2)
        self.assertCountEqual(
            search.partition_calls,
            [(1, "openalex"), (1, "crossref"), (2, "openalex"), (2, "crossref")],
        )


if __name__ == "__main__":
    unittest.main()
