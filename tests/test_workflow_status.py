from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.workflow_status import FULLTEXT_FIELDS, inspect


INDEX_FIELDS = ["batch_id", "status", "claimed_by"]
RESULT_FIELDS = ["item_id", "paper_id", "status", "title", "doi", "pdf_path", "identity_verified"]
PAPER_INDEX_FIELDS = [
    "paper_id", "title", "doi", "pdf_path", "supplement_paths", "sha256",
    "source_batch_id", "identity_verified", "accepted_at", "notes",
    "mineru_md_path", "xml_md_path",
]
SCREEN_FIELDS = [
    "record_id", "search_query", "source_database", "source_record", "title",
    "abstract", "authors", "year", "journal", "doi", "landing_url", "decision",
    "matched_criteria", "reason", "needs_fulltext_reason", "exclusion_reason",
]


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class WorkflowStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        write_json(self.root / "00_control/WORKSPACE.json", {"workspace_id": "test"})
        write_csv(self.root / "00_control/BATCH_INDEX.csv", [], INDEX_FIELDS)
        write_csv(self.root / "04_library_index/PAPER_INDEX.csv", [], PAPER_INDEX_FIELDS)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def task(self, task_id: str = "T1", *, fulltext: bool = True,
             active: bool = True, batch_ids: list[str] | None = None) -> Path:
        task = self.root / "runtime/tasks" / task_id
        write_json(task / "task_brief.json", {
            "task_id": task_id,
            "title": "Test task",
            "research_question": "Which papers match?",
            "inclusion_criteria": ["relevant"],
            "exclusion_criteria": ["irrelevant"],
            "target_evidence": ["target data"],
            "requires_fulltext_screening": fulltext,
        })
        if active:
            write_json(self.root / "runtime/ACTIVE_TASK.json", {
                "task_id": task_id,
                "task_root": f"runtime/tasks/{task_id}",
                "batch_ids": batch_ids or [],
            })
        return task

    def valid_query(self, task: Path, *, validated: bool = True) -> Path:
        query = task / "query"
        write_json(query / "query_brief.json", {"question": "q"})
        plan = {
            "query_id": f"{task.name}-query",
            "final_variant": "recall",
            "validation": {
                "live_counts_recorded": validated,
                "known_record_check": "passed",
                "noise_sample_reviewed": True,
                "selection_rationale": "best seed recall",
            },
        }
        write_json(query / "query_plan.json", plan)
        write_csv(query / "query_matrix.csv", [{"variant": "recall", "source": "openalex"}], ["variant", "source"])
        (query / "search_strategy.md").write_text("validated strategy", encoding="utf-8")
        write_json(query / "configs/recall.json", {
            "query_id": f"{task.name}-query-recall",
            "source_order": ["openalex"],
            "sources": {"openalex": {"query": "q"}},
        })
        return query

    def search(self, task: Path, rows: list[dict[str, str]], *, complete: bool = True,
               source_status: str = "complete", raw_extra: int = 0,
               run_id: str = "R1") -> Path:
        run = task / "search" / run_id
        raw_rows = [dict(row) for row in rows] + [
            {"record_id": f"RAW{i}", "title": f"raw {i}", "year": "", "doi": ""}
            for i in range(raw_extra)
        ]
        write_json(run / "run_manifest.json", {
            "query_id": f"{task.name}-query-recall",
            "run_id": run_id,
            "complete": complete,
            "requested_sources": ["openalex"],
            "sources": {"openalex": {
                "status": source_status, "native_query": "q",
                "fetched_count": len(raw_rows), "pages": 1,
            }},
            "raw_record_count": len(raw_rows),
            "deduplicated_record_count": len(rows),
        })
        write_csv(run / "all_records.csv", raw_rows, SCREEN_FIELDS)
        write_csv(run / "deduplicated_records.csv", rows, SCREEN_FIELDS)
        write_csv(run / "pre_screening.csv", rows, SCREEN_FIELDS)
        write_csv(run / "openalex/records.csv", raw_rows, SCREEN_FIELDS)
        write_json(run / "openalex/raw/page-000001.json", {"results": []})
        return run

    def batch(self, task_id: str, batch_id: str, status: str, *,
              index_status: str | None = None, claimed_by: str = "") -> Path:
        batch = self.root / "02_batches" / batch_id
        write_json(batch / "STATUS.json", {
            "batch_id": batch_id, "status": status, "claimed_by": claimed_by or None,
        })
        write_csv(
            batch / "MANIFEST.csv",
            [{"item_id": "D001", "paper_id": "P1", "title": "Paper", "doi": "10/test"}],
            ["item_id", "paper_id", "title", "doi"],
        )
        info_path = self.root / "00_control/BATCH_INDEX.csv"
        with info_path.open(encoding="utf-8", newline="") as handle:
            existing = list(csv.DictReader(handle))
        existing.append({
            "batch_id": batch_id, "status": index_status or status, "claimed_by": claimed_by,
        })
        write_csv(info_path, existing, INDEX_FIELDS)
        if status == "ACCEPTED":
            fulltext = batch / "outputs/files/P1.pdf"
            fulltext.parent.mkdir(parents=True, exist_ok=True)
            fulltext.write_bytes(b"%PDF-test")
            write_csv(batch / "outputs/download_results.csv", [{
                "item_id": "D001", "paper_id": "P1", "status": "downloaded",
                "title": "Paper", "doi": "10/test", "pdf_path": str(fulltext),
                "identity_verified": "true",
            }], RESULT_FIELDS)
            acceptance = self.root / "03_review" / f"{batch_id}-acceptance.md"
            acceptance.parent.mkdir(parents=True, exist_ok=True)
            acceptance.write_text(f"# {batch_id} accepted by coordinator", encoding="utf-8")
            paper_index = self.root / "04_library_index/PAPER_INDEX.csv"
            with paper_index.open(encoding="utf-8", newline="") as handle:
                papers = list(csv.DictReader(handle))
            papers.append({
                "paper_id": "P1", "title": "Paper", "doi": "10/test",
                "pdf_path": str(fulltext), "source_batch_id": batch_id,
                "identity_verified": "true",
            })
            write_csv(paper_index, papers, PAPER_INDEX_FIELDS)
        return batch

    def queue_row(self, **overrides: str) -> dict[str, str]:
        row = {field: "" for field in FULLTEXT_FIELDS}
        row.update({
            "item_id": "D001", "paper_id": "P1", "title": "Paper", "doi": "10/test",
            "fulltext_path": str(self.root / "02_batches/DL-1/outputs/files/P1.pdf"),
            "fulltext_status": "ready", "criterion_id": "OVERALL",
        })
        row.update(overrides)
        return row

    def test_empty_workspace_starts_with_task_brief(self) -> None:
        self.assertEqual(inspect(self.root)["stage"], "NOT_STARTED")

    def test_task_without_query_routes_to_query_design(self) -> None:
        self.task()
        self.assertEqual(inspect(self.root)["stage"], "STAGE_MINUS_1_QUERY_DESIGN")

    def test_multiple_tasks_are_ambiguous_and_cli_isolates_one(self) -> None:
        self.task("T1", active=False)
        self.task("T2", active=False)
        self.assertEqual(inspect(self.root)["stage"], "AMBIGUOUS")
        selected = inspect(self.root, task_id="T2")
        self.assertEqual(selected["stage"], "STAGE_MINUS_1_QUERY_DESIGN")
        self.assertEqual(selected["evidence"]["task_id"], "T2")

    def test_selected_task_never_borrows_another_tasks_search_files(self) -> None:
        first = self.task("T1", active=False)
        second = self.task("T2", active=False)
        self.valid_query(first)
        self.valid_query(second)
        rows = [{"record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x", "decision": ""}]
        self.search(second, rows)
        self.assertEqual(inspect(self.root, task_id="T1")["stage"], "STAGE_0_API_SEARCH")

    def test_unvalidated_query_does_not_advance_to_search(self) -> None:
        task = self.task()
        self.valid_query(task, validated=False)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_MINUS_1_QUERY_DESIGN")
        self.assertIn("live_counts_recorded", " ".join(status["evidence"]["query"]["issues"]))

    def test_validated_query_without_run_routes_to_api_search(self) -> None:
        task = self.task()
        self.valid_query(task)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_0_API_SEARCH")

    def test_direct_ready_pointer_is_not_hidden_by_a_valid_query(self) -> None:
        task = self.task(batch_ids=["DL-1"])
        self.valid_query(task)
        self.batch("T1", "DL-1", "READY")
        self.assertEqual(inspect(self.root)["stage"], "STAGE_2_DOWNLOAD")

    def test_direct_ready_pointer_is_not_hidden_by_a_completed_search(self) -> None:
        task = self.task(batch_ids=["DL-1"])
        self.valid_query(task)
        rows = [{
            "record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x",
            "decision": "uncertain", "needs_fulltext_reason": "need full text",
        }]
        self.search(task, rows)
        self.batch("T1", "DL-1", "READY")
        self.assertEqual(inspect(self.root)["stage"], "STAGE_2_DOWNLOAD")

    def test_incomplete_source_blocks_screening_even_when_top_complete(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{"record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x", "decision": ""}]
        self.search(task, rows, complete=True, source_status="failed")
        self.assertEqual(inspect(self.root)["stage"], "STAGE_0_SEARCH_INCOMPLETE")

    def test_run_sources_and_native_queries_must_match_selected_config(self) -> None:
        task = self.task()
        query = self.valid_query(task)
        write_json(query / "configs/recall.json", {
            "query_id": "T1-query-recall",
            "source_order": ["openalex", "crossref"],
            "sources": {
                "openalex": {"query": "q"},
                "crossref": {"query": "q-crossref"},
            },
        })
        rows = [{"record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x", "decision": ""}]
        self.search(task, rows)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_0_SEARCH_INCOMPLETE")
        self.assertIn("planned sources", " ".join(status["evidence"]["search"]["issues"]))

        write_json(query / "configs/recall.json", {
            "query_id": "T1-query-recall", "source_order": ["openalex"],
            "sources": {"openalex": {"query": "different query"}},
        })
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_0_SEARCH_INCOMPLETE")
        self.assertIn("native_query", " ".join(status["evidence"]["search"]["issues"]))

    def test_run_manifest_cannot_contain_unrequested_extra_sources(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{"record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x", "decision": ""}]
        run = self.search(task, rows)
        manifest_path = run / "run_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["sources"]["foreign"] = {
            "status": "complete", "native_query": "foreign", "fetched_count": 0, "pages": 1,
        }
        write_json(manifest_path, manifest)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_0_SEARCH_INCOMPLETE")
        self.assertIn("sources keys", " ".join(status["evidence"]["search"]["issues"]))

    def test_missing_raw_source_page_blocks_screening(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{"record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x", "decision": ""}]
        run = self.search(task, rows)
        (run / "openalex/raw/page-000001.json").unlink()
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_0_SEARCH_INCOMPLETE")
        self.assertIn("raw pages", " ".join(status["evidence"]["search"]["issues"]))

    def test_csv_count_or_coverage_mismatch_blocks_screening(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{"record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x", "decision": ""}]
        run = self.search(task, rows)
        write_csv(run / "pre_screening.csv", [{**rows[0], "record_id": "OTHER"}], SCREEN_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_0_SEARCH_INCOMPLETE")

    def test_pre_screening_identity_must_match_deduplicated_record(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{"record_id": "R1", "title": "Original Paper", "year": "2025", "doi": "10/x", "decision": ""}]
        run = self.search(task, rows)
        write_csv(run / "pre_screening.csv", [{
            **rows[0], "title": "Different Paper", "year": "2024", "doi": "10/y",
        }], SCREEN_FIELDS)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_0_SEARCH_INCOMPLETE")
        issues = " ".join(status["evidence"]["search"]["issues"])
        self.assertIn("record_id=R1 title", issues)
        self.assertIn("record_id=R1 doi", issues)
        self.assertIn("record_id=R1 year", issues)

    def test_pre_screening_cannot_replace_the_abstract(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{
            "record_id": "R1", "title": "Paper", "abstract": "Original evidence",
            "year": "2025", "doi": "10/x", "decision": "",
        }]
        run = self.search(task, rows)
        write_csv(run / "pre_screening.csv", [{
            **rows[0], "abstract": "Different evidence",
        }], SCREEN_FIELDS)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_0_SEARCH_INCOMPLETE")
        self.assertIn("abstract differs", " ".join(status["evidence"]["search"]["issues"]))

    def test_pre_screening_accepts_normalized_equivalent_identity_fields(self) -> None:
        task = self.task()
        self.valid_query(task)
        source = [{
            "record_id": "R1", "title": "Paper—One", "year": "２０２５",
            "doi": "https://doi.org/10.1000/ABC.", "abstract": "Same evidence",
        }]
        run = self.search(task, source)
        screened = [{
            **source[0], "title": "paper one", "year": "2025", "doi": "10.1000/abc",
            "decision": "uncertain", "needs_fulltext_reason": "need full text",
        }]
        write_csv(run / "pre_screening.csv", screened, SCREEN_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_1_HANDOFF_TO_DOWNLOAD")

    def test_complete_zero_hit_search_has_distinct_terminal(self) -> None:
        task = self.task()
        self.valid_query(task)
        self.search(task, [])
        self.assertEqual(inspect(self.root)["stage"], "NO_RESULTS")

    def test_screening_replays_handoff_reason_and_identity_checks(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [
            {"record_id": "R1", "title": "", "year": "", "doi": "", "decision": "exclude"},
            {"record_id": "R2", "title": "Paper", "year": "2025", "doi": "10/x", "decision": "download"},
        ]
        self.search(task, rows)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_1_TITLE_ABSTRACT_SCREENING")
        self.assertGreaterEqual(len(status["evidence"]["screening"]["issues"]), 2)

    def test_conflicting_duplicate_screening_is_rejected(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [
            {"record_id": "R1", "title": "One", "year": "2025", "doi": "10/x", "decision": "download", "matched_criteria": "yes"},
            {"record_id": "R2", "title": "One", "year": "2025", "doi": "10/x", "decision": "exclude", "exclusion_reason": "no"},
        ]
        self.search(task, rows)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_1_TITLE_ABSTRACT_SCREENING")

    def test_all_exclude_is_terminal_and_does_not_suggest_handoff(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{"record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x", "decision": "exclude", "exclusion_reason": "wrong topic"}]
        self.search(task, rows)
        self.assertEqual(inspect(self.root)["stage"], "COMPLETE_NO_DOWNLOAD_TARGETS")

    def test_exclude_requires_an_exclusion_reason(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{"record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x", "decision": "exclude"}]
        self.search(task, rows)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_1_TITLE_ABSTRACT_SCREENING")

    def test_uncertain_requires_needs_fulltext_reason_not_matched_criteria(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{
            "record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x",
            "decision": "uncertain", "matched_criteria": "topic matches",
        }]
        self.search(task, rows)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_1_TITLE_ABSTRACT_SCREENING")

    def test_download_requires_matched_criteria_or_reason_not_fulltext_reason(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{
            "record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x",
            "decision": "download", "needs_fulltext_reason": "need a table",
        }]
        self.search(task, rows)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_1_TITLE_ABSTRACT_SCREENING")

    def test_valid_selected_screening_routes_to_handoff(self) -> None:
        task = self.task()
        self.valid_query(task)
        rows = [{"record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x", "decision": "uncertain", "needs_fulltext_reason": "need table"}]
        self.search(task, rows)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_1_HANDOFF_TO_DOWNLOAD")

    def test_multiple_actionable_pointer_batches_require_cli_selection(self) -> None:
        self.task(batch_ids=["DL-1", "DL-2"])
        self.batch("T1", "DL-1", "READY")
        self.batch("T1", "DL-2", "RUNNING", claimed_by="worker")
        self.assertEqual(inspect(self.root)["stage"], "AMBIGUOUS_BATCH")
        self.assertEqual(inspect(self.root, batch_id="DL-1")["stage"], "STAGE_2_DOWNLOAD")
        self.assertEqual(inspect(self.root, task_id="T1", batch_id="DL-2")["stage"], "STAGE_2_DOWNLOAD")

    def test_batch_can_be_related_by_copied_handoff_query_id(self) -> None:
        task = self.task(batch_ids=[])
        self.valid_query(task)
        rows = [{
            "record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x",
            "decision": "uncertain", "needs_fulltext_reason": "need full text",
        }]
        run = self.search(task, rows)
        batch = self.batch("T1", "DL-1", "READY")
        write_json(
            batch / "inputs/search_screening/run_manifest.json",
            json.loads((run / "run_manifest.json").read_text(encoding="utf-8")),
        )
        self.assertEqual(inspect(self.root)["stage"], "STAGE_2_DOWNLOAD")

    def test_colliding_query_id_does_not_auto_attach_unpointed_batch(self) -> None:
        first = self.task("T1", batch_ids=[])
        second = self.task("T2", active=False)
        self.valid_query(first)
        second_query = self.valid_query(second)
        write_json(second_query / "configs/recall.json", {
            "query_id": "T1-query-recall", "source_order": ["openalex"],
            "sources": {"openalex": {"query": "q"}},
        })
        rows = [{
            "record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x",
            "decision": "uncertain", "needs_fulltext_reason": "need full text",
        }]
        run = self.search(first, rows)
        batch = self.batch("T1", "DL-1", "READY")
        write_json(
            batch / "inputs/search_screening/run_manifest.json",
            json.loads((run / "run_manifest.json").read_text(encoding="utf-8")),
        )
        self.assertEqual(inspect(self.root)["stage"], "STAGE_1_HANDOFF_TO_DOWNLOAD")

    def test_explicit_pointer_remains_authoritative_when_query_id_collides(self) -> None:
        first = self.task("T1", batch_ids=["DL-1"])
        second = self.task("T2", active=False)
        self.valid_query(first)
        second_query = self.valid_query(second)
        write_json(second_query / "configs/recall.json", {
            "query_id": "T1-query-recall", "source_order": ["openalex"],
            "sources": {"openalex": {"query": "q"}},
        })
        rows = [{
            "record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x",
            "decision": "uncertain", "needs_fulltext_reason": "need full text",
        }]
        run = self.search(first, rows)
        batch = self.batch("T1", "DL-1", "READY")
        write_json(
            batch / "inputs/search_screening/run_manifest.json",
            json.loads((run / "run_manifest.json").read_text(encoding="utf-8")),
        )
        self.assertEqual(inspect(self.root)["stage"], "STAGE_2_DOWNLOAD")

    def test_pointer_copied_manifest_requires_a_matching_task_run(self) -> None:
        task = self.task(batch_ids=["DL-1"])
        self.valid_query(task)
        batch = self.batch("T1", "DL-1", "READY")
        write_json(batch / "inputs/search_screening/run_manifest.json", {
            "query_id": "T1-query-recall", "run_id": "foreign-run",
        })
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_pointer_copied_manifest_cannot_be_corrupt(self) -> None:
        self.task(batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "READY")
        copied = batch / "inputs/search_screening/run_manifest.json"
        copied.parent.mkdir(parents=True, exist_ok=True)
        copied.write_text("{", encoding="utf-8")
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_explicit_batch_cannot_bypass_incomplete_copied_run(self) -> None:
        task = self.task(batch_ids=["DL-1"])
        self.valid_query(task)
        rows = [{
            "record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x",
            "decision": "uncertain", "needs_fulltext_reason": "need full text",
        }]
        run = self.search(task, rows, complete=False)
        batch = self.batch("T1", "DL-1", "READY")
        write_json(
            batch / "inputs/search_screening/run_manifest.json",
            json.loads((run / "run_manifest.json").read_text(encoding="utf-8")),
        )
        self.assertEqual(inspect(self.root, batch_id="DL-1")["stage"], "BATCH_STATE_ERROR")

    def test_pointer_copied_manifest_must_match_current_config_queries(self) -> None:
        task = self.task(batch_ids=["DL-1"])
        query = self.valid_query(task)
        rows = [{
            "record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x",
            "decision": "uncertain", "needs_fulltext_reason": "need full text",
        }]
        run = self.search(task, rows)
        batch = self.batch("T1", "DL-1", "READY")
        write_json(
            batch / "inputs/search_screening/run_manifest.json",
            json.loads((run / "run_manifest.json").read_text(encoding="utf-8")),
        )
        write_json(query / "configs/recall.json", {
            "query_id": "T1-query-recall", "source_order": ["openalex"],
            "sources": {"openalex": {"query": "changed after handoff"}},
        })
        self.assertEqual(inspect(self.root, batch_id="DL-1")["stage"], "BATCH_STATE_ERROR")

    def test_pointer_handoff_metadata_cannot_claim_a_different_task(self) -> None:
        task = self.task(batch_ids=["DL-1"])
        self.valid_query(task)
        rows = [{
            "record_id": "R1", "title": "Paper", "year": "2025", "doi": "10/x",
            "decision": "uncertain", "needs_fulltext_reason": "need full text",
        }]
        run = self.search(task, rows)
        batch = self.batch("T1", "DL-1", "READY")
        write_json(
            batch / "inputs/search_screening/run_manifest.json",
            json.loads((run / "run_manifest.json").read_text(encoding="utf-8")),
        )
        write_json(batch / "REQUEST.json", {
            "batch_id": "DL-1", "task_id": "T2",
        })
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_direct_pointer_metadata_cannot_claim_a_different_task(self) -> None:
        self.task(batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "READY")
        write_json(batch / "REQUEST.json", {"batch_id": "DL-1", "task_id": "T2"})
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_pointer_copied_manifest_without_query_artifacts_is_rejected(self) -> None:
        self.task(batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "READY")
        write_json(batch / "inputs/search_screening/run_manifest.json", {
            "query_id": "foreign-query", "run_id": "foreign-run",
        })
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_pointer_batch_with_foreign_query_is_rejected(self) -> None:
        task = self.task(batch_ids=["DL-1"])
        self.valid_query(task)
        batch = self.batch("T1", "DL-1", "READY")
        write_json(batch / "inputs/search_screening/run_manifest.json", {"query_id": "FOREIGN-query"})
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_new_search_run_is_not_hidden_by_old_accepted_batch(self) -> None:
        task = self.task(fulltext=False, batch_ids=["DL-old"])
        self.valid_query(task)
        old_rows = [{
            "record_id": "R1", "title": "Old Paper", "year": "2024", "doi": "10/old",
            "decision": "uncertain", "needs_fulltext_reason": "need old full text",
        }]
        old_run = self.search(task, old_rows, run_id="R1")
        old_batch = self.batch("T1", "DL-old", "ACCEPTED")
        write_json(
            old_batch / "inputs/search_screening/run_manifest.json",
            json.loads((old_run / "run_manifest.json").read_text(encoding="utf-8")),
        )
        new_rows = [{
            "record_id": "R2", "title": "New Paper", "year": "2026", "doi": "10/new",
            "decision": "uncertain", "needs_fulltext_reason": "need new full text",
        }]
        self.search(task, new_rows, run_id="R2")
        self.assertEqual(inspect(self.root)["stage"], "STAGE_1_HANDOFF_TO_DOWNLOAD")

    def test_new_incomplete_run_is_not_hidden_by_old_accepted_batch(self) -> None:
        task = self.task(fulltext=False, batch_ids=["DL-old"])
        self.valid_query(task)
        old_rows = [{
            "record_id": "R1", "title": "Old Paper", "year": "2024", "doi": "10/old",
            "decision": "uncertain", "needs_fulltext_reason": "need old full text",
        }]
        old_run = self.search(task, old_rows, run_id="R1")
        old_manifest_path = old_run / "run_manifest.json"
        old_manifest = json.loads(old_manifest_path.read_text(encoding="utf-8"))
        old_manifest["finished_at"] = "2026-01-01T04:00:00+00:00"
        write_json(old_manifest_path, old_manifest)
        old_batch = self.batch("T1", "DL-old", "ACCEPTED")
        write_json(old_batch / "inputs/search_screening/run_manifest.json", old_manifest)

        new_rows = [{
            "record_id": "R2", "title": "New Paper", "year": "2026", "doi": "10/new",
            "decision": "uncertain", "needs_fulltext_reason": "need new full text",
        }]
        new_run = self.search(task, new_rows, complete=False, run_id="R2")
        new_manifest_path = new_run / "run_manifest.json"
        new_manifest = json.loads(new_manifest_path.read_text(encoding="utf-8"))
        new_manifest["finished_at"] = "2026-01-01T05:00:00+00:00"
        write_json(new_manifest_path, new_manifest)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_0_SEARCH_INCOMPLETE")

    def test_newest_run_compares_iso_timestamps_by_instant(self) -> None:
        task = self.task()
        self.valid_query(task)
        old_rows = [{
            "record_id": "R1", "title": "Old Paper", "year": "2024", "doi": "10/old",
            "decision": "exclude", "exclusion_reason": "irrelevant",
        }]
        old_run = self.search(task, old_rows, run_id="R1")
        old_manifest_path = old_run / "run_manifest.json"
        old_manifest = json.loads(old_manifest_path.read_text(encoding="utf-8"))
        old_manifest["finished_at"] = "2026-01-01T12:00:00+08:00"
        write_json(old_manifest_path, old_manifest)
        new_rows = [{
            "record_id": "R2", "title": "New Paper", "year": "2026", "doi": "10/new",
            "decision": "uncertain", "needs_fulltext_reason": "need full text",
        }]
        new_run = self.search(task, new_rows, complete=False, run_id="R2")
        new_manifest_path = new_run / "run_manifest.json"
        new_manifest = json.loads(new_manifest_path.read_text(encoding="utf-8"))
        new_manifest["finished_at"] = "2026-01-01T05:00:00+00:00"
        write_json(new_manifest_path, new_manifest)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_0_SEARCH_INCOMPLETE")

    def test_blocked_and_failed_are_recovery_states(self) -> None:
        self.task(batch_ids=["DL-1"])
        self.batch("T1", "DL-1", "BLOCKED")
        self.assertEqual(inspect(self.root)["stage"], "STAGE_2_DOWNLOAD_BLOCKED")
        write_json(self.root / "02_batches/DL-1/STATUS.json", {"batch_id": "DL-1", "status": "FAILED", "claimed_by": None})
        write_csv(self.root / "00_control/BATCH_INDEX.csv", [{"batch_id": "DL-1", "status": "FAILED", "claimed_by": ""}], INDEX_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_2_DOWNLOAD_FAILED")

    def test_current_run_blocked_batch_is_not_hidden_by_mutated_screening(self) -> None:
        task = self.task(batch_ids=["DL-1"])
        self.valid_query(task)
        rows = [{
            "record_id": "R1", "title": "Paper", "abstract": "Original abstract",
            "year": "2025", "doi": "10/x", "decision": "uncertain",
            "needs_fulltext_reason": "need full text",
        }]
        run = self.search(task, rows)
        batch = self.batch("T1", "DL-1", "BLOCKED")
        write_json(
            batch / "inputs/search_screening/run_manifest.json",
            json.loads((run / "run_manifest.json").read_text(encoding="utf-8")),
        )
        write_csv(run / "pre_screening.csv", [{**rows[0], "abstract": "Tampered"}], SCREEN_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_2_DOWNLOAD_BLOCKED")

    def test_batch_index_status_mismatch_is_an_error(self) -> None:
        self.task(batch_ids=["DL-1"])
        self.batch("T1", "DL-1", "RUNNING", index_status="READY", claimed_by="worker")
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_unknown_batch_status_is_an_error(self) -> None:
        self.task(batch_ids=["DL-1"])
        self.batch("T1", "DL-1", "UNKNOWN")
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_running_batch_queue_cannot_advance_early(self) -> None:
        self.task(batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "RUNNING", claimed_by="worker")
        write_csv(batch / "outputs/fulltext_screening_queue.csv", [self.queue_row()], FULLTEXT_FIELDS)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_2_DOWNLOAD")
        self.assertTrue(status["evidence"]["batches"][0]["premature_fulltext_queue"])

    def test_accepted_without_fulltext_requirement_is_complete(self) -> None:
        self.task(fulltext=False, batch_ids=["DL-1"])
        self.batch("T1", "DL-1", "ACCEPTED")
        self.assertEqual(inspect(self.root)["stage"], "COMPLETE_DOWNLOAD")

    def test_accepted_fulltext_task_creates_queue_only_after_acceptance(self) -> None:
        self.task(fulltext=True, batch_ids=["DL-1"])
        self.batch("T1", "DL-1", "ACCEPTED")
        self.assertEqual(inspect(self.root)["stage"], "STAGE_3_CREATE_FULLTEXT_QUEUE")

    def test_accepted_batch_requires_results_coverage_and_acceptance_record(self) -> None:
        self.task(fulltext=False, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        (self.root / "03_review/DL-1-acceptance.md").unlink()
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")
        (self.root / "03_review/DL-1-acceptance.md").write_text("DL-1 accepted", encoding="utf-8")
        write_csv(batch / "outputs/download_results.csv", [], RESULT_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_acceptance_record_must_name_the_same_batch(self) -> None:
        self.task(fulltext=False, batch_ids=["DL-1"])
        self.batch("T1", "DL-1", "ACCEPTED")
        (self.root / "03_review/DL-1-acceptance.md").write_text(
            "# DL-other accepted", encoding="utf-8",
        )
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_successful_acceptance_must_be_registered_in_paper_index(self) -> None:
        self.task(fulltext=False, batch_ids=["DL-1"])
        self.batch("T1", "DL-1", "ACCEPTED")
        write_csv(self.root / "04_library_index/PAPER_INDEX.csv", [], PAPER_INDEX_FIELDS)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "BATCH_STATE_ERROR")
        self.assertIn(
            "not registered in PAPER_INDEX",
            " ".join(status["evidence"]["batches"][0]["acceptance"]["issues"]),
        )

    def test_paper_index_identity_must_match_the_manifest(self) -> None:
        self.task(fulltext=False, batch_ids=["DL-1"])
        self.batch("T1", "DL-1", "ACCEPTED")
        index_path = self.root / "04_library_index/PAPER_INDEX.csv"
        with index_path.open(encoding="utf-8-sig", newline="") as handle:
            row = next(csv.DictReader(handle))
        row["title"] = "Different title"
        write_csv(index_path, [row], PAPER_INDEX_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_accepted_success_requires_verified_identity_and_existing_fulltext(self) -> None:
        self.task(fulltext=False, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        result_row = {
            "item_id": "D001", "paper_id": "P1", "status": "downloaded",
            "title": "Paper", "doi": "10/test", "pdf_path": str(batch / "missing.pdf"),
            "identity_verified": "false",
        }
        write_csv(batch / "outputs/download_results.csv", [result_row], RESULT_FIELDS)
        write_csv(self.root / "04_library_index/PAPER_INDEX.csv", [], PAPER_INDEX_FIELDS)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "BATCH_STATE_ERROR")
        issues = " ".join(status["evidence"]["batches"][0]["acceptance"]["issues"])
        self.assertIn("identity_verified", issues)
        self.assertIn("PAPER_INDEX", issues)

    def test_accepted_fulltext_must_remain_nonempty(self) -> None:
        self.task(fulltext=False, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        (batch / "outputs/files/P1.pdf").write_bytes(b"")
        status = inspect(self.root)
        self.assertEqual(status["stage"], "BATCH_STATE_ERROR")
        self.assertIn(
            "is empty", " ".join(status["evidence"]["batches"][0]["acceptance"]["issues"]),
        )

    def test_accepted_batch_allows_explicit_failure_record(self) -> None:
        self.task(fulltext=True, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        write_csv(batch / "outputs/download_results.csv", [{
            "item_id": "D001", "paper_id": "P1", "status": "paywalled",
            "title": "Paper", "doi": "10/test", "pdf_path": "", "identity_verified": "false",
        }], RESULT_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_3_CREATE_FULLTEXT_QUEUE")

    def test_unknown_download_result_status_is_not_accepted_evidence(self) -> None:
        self.task(fulltext=False, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        write_csv(batch / "outputs/download_results.csv", [{
            "item_id": "D001", "paper_id": "P1", "status": "typo_status",
            "title": "Paper", "doi": "10/test", "pdf_path": "", "identity_verified": "false",
        }], RESULT_FIELDS)
        write_csv(self.root / "04_library_index/PAPER_INDEX.csv", [], PAPER_INDEX_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "BATCH_STATE_ERROR")

    def test_xml_only_accepted_fulltext_can_be_ready(self) -> None:
        self.task(fulltext=True, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        xml_path = batch / "outputs/tdm-xml/P1.xml"
        xml_path.parent.mkdir(parents=True, exist_ok=True)
        xml_path.write_text("<article><body>full text</body></article>", encoding="utf-8")
        write_csv(batch / "outputs/download_results.csv", [{
            "item_id": "D001", "paper_id": "P1", "status": "downloaded",
            "title": "Paper", "doi": "10/test", "pdf_path": str(xml_path),
            "identity_verified": "true",
        }], RESULT_FIELDS)
        relative_xml = xml_path.relative_to(self.root)
        write_csv(self.root / "04_library_index/PAPER_INDEX.csv", [{
            "paper_id": "P1", "title": "Paper", "doi": "10/test",
            "pdf_path": str(relative_xml), "source_batch_id": "DL-1",
            "identity_verified": "true",
        }], PAPER_INDEX_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_3_CREATE_FULLTEXT_QUEUE")
        write_csv(batch / "outputs/fulltext_screening_queue.csv", [self.queue_row(
            fulltext_path=str(relative_xml),
        )], FULLTEXT_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_3_FULLTEXT_SCREENING")

    def test_workspace_relative_pdf_paths_are_valid_after_acceptance(self) -> None:
        self.task(fulltext=True, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        relative_pdf = Path("02_batches/DL-1/outputs/files/P1.pdf")
        write_csv(batch / "outputs/download_results.csv", [{
            "item_id": "D001", "paper_id": "P1", "status": "downloaded",
            "title": "Paper", "doi": "10/test", "pdf_path": str(relative_pdf),
            "identity_verified": "true",
        }], RESULT_FIELDS)
        write_csv(self.root / "04_library_index/PAPER_INDEX.csv", [{
            "paper_id": "P1", "title": "Paper", "doi": "10/test",
            "pdf_path": str(relative_pdf), "source_batch_id": "DL-1",
            "identity_verified": "true",
        }], PAPER_INDEX_FIELDS)
        write_csv(batch / "outputs/fulltext_screening_queue.csv", [self.queue_row(
            fulltext_path=str(relative_pdf),
        )], FULLTEXT_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_3_FULLTEXT_SCREENING")

    def test_failure_result_has_a_valid_unavailable_queue_row(self) -> None:
        self.task(fulltext=True, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        write_csv(batch / "outputs/download_results.csv", [{
            "item_id": "D001", "paper_id": "P1", "status": "paywalled",
            "title": "Paper", "doi": "10/test", "pdf_path": "", "identity_verified": "false",
        }], RESULT_FIELDS)
        write_csv(self.root / "04_library_index/PAPER_INDEX.csv", [], PAPER_INDEX_FIELDS)
        write_csv(batch / "outputs/fulltext_screening_queue.csv", [self.queue_row(
            fulltext_path="", fulltext_status="fulltext_unavailable", decision="not_assessed",
        )], FULLTEXT_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "COMPLETE_OR_HUMAN_REVIEW")

    def test_queue_identity_and_path_must_match_manifest_and_accepted_result(self) -> None:
        self.task(fulltext=True, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        write_csv(batch / "outputs/fulltext_screening_queue.csv", [self.queue_row(
            title="Wrong title", doi="10/wrong", fulltext_path=str(self.root / "wrong.pdf"),
        )], FULLTEXT_FIELDS)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_3_FULLTEXT_SCREENING_INVALID")
        issues = " ".join(status["evidence"]["batches"][0]["queue"]["issues"])
        self.assertIn("differs from MANIFEST", issues)
        self.assertIn("accepted PDF/XML path", issues)

    def test_invalid_queue_decision_or_missing_evidence_is_not_complete(self) -> None:
        self.task(fulltext=True, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        row = self.queue_row(decision="include", reviewer="reviewer", reviewed_at="2026-10-13")
        write_csv(batch / "outputs/fulltext_screening_queue.csv", [row], FULLTEXT_FIELDS)
        status = inspect(self.root)
        self.assertEqual(status["stage"], "STAGE_3_FULLTEXT_SCREENING_INVALID")
        self.assertIn("evidence_quote", " ".join(status["evidence"]["batches"][0]["queue"]["issues"]))

    def test_uncertain_fulltext_decision_requires_reviewer_attribution(self) -> None:
        self.task(fulltext=True, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        write_csv(
            batch / "outputs/fulltext_screening_queue.csv",
            [self.queue_row(decision="uncertain")], FULLTEXT_FIELDS,
        )
        self.assertEqual(inspect(self.root)["stage"], "STAGE_3_FULLTEXT_SCREENING_INVALID")

    def test_valid_pending_and_complete_fulltext_queues(self) -> None:
        self.task(fulltext=True, batch_ids=["DL-1"])
        batch = self.batch("T1", "DL-1", "ACCEPTED")
        queue = batch / "outputs/fulltext_screening_queue.csv"
        write_csv(queue, [self.queue_row()], FULLTEXT_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "STAGE_3_FULLTEXT_SCREENING")
        write_csv(queue, [self.queue_row(
            decision="include", evidence_quote="exact quote", evidence_page="4",
            reviewer="human", reviewed_at="2026-10-13T12:00:00+08:00",
        )], FULLTEXT_FIELDS)
        self.assertEqual(inspect(self.root)["stage"], "COMPLETE_OR_HUMAN_REVIEW")


if __name__ == "__main__":
    unittest.main()
