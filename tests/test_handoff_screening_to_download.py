from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.handoff_screening_to_download import (
    build_download_rows,
    validate_search_manifest,
)


class SearchManifestValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "run_manifest.json"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_manifest(self, **overrides: object) -> Path:
        manifest: dict[str, object] = {
            "complete": True,
            "requested_sources": ["openalex"],
            "sources": {"openalex": {"status": "complete"}},
        }
        manifest.update(overrides)
        self.path.write_text(json.dumps(manifest), encoding="utf-8")
        return self.path

    def test_rejects_empty_requested_sources(self) -> None:
        path = self.write_manifest(requested_sources=[])
        with self.assertRaisesRegex(ValueError, "requested_sources must be a non-empty list"):
            validate_search_manifest(path, allow_incomplete=False)

    def test_rejects_requested_source_missing_from_sources(self) -> None:
        path = self.write_manifest(
            requested_sources=["openalex", "crossref"],
            sources={"openalex": {"status": "complete"}},
        )
        with self.assertRaisesRegex(ValueError, "crossref.*missing"):
            validate_search_manifest(path, allow_incomplete=False)

    def test_rejects_incomplete_requested_source(self) -> None:
        path = self.write_manifest(sources={"openalex": {"status": "failed"}})
        with self.assertRaisesRegex(ValueError, "openalex.*failed"):
            validate_search_manifest(path, allow_incomplete=False)

    def test_rejects_malformed_sources_object(self) -> None:
        path = self.write_manifest(sources=[])
        with self.assertRaisesRegex(ValueError, "sources must be an object"):
            validate_search_manifest(path, allow_incomplete=False)

    def test_allow_incomplete_does_not_waive_malformed_source_entry(self) -> None:
        path = self.write_manifest(sources={"openalex": "complete"})
        with self.assertRaisesRegex(ValueError, "source openalex must be an object"):
            validate_search_manifest(path, allow_incomplete=True)

    def test_unrequested_source_does_not_define_completeness(self) -> None:
        path = self.write_manifest(sources={
            "openalex": {"status": "complete"},
            "crossref": {"status": "failed"},
        })
        loaded = validate_search_manifest(path, allow_incomplete=False)
        self.assertTrue(loaded["complete"])

    def test_rejects_non_boolean_true_complete(self) -> None:
        path = self.write_manifest(complete="true")
        with self.assertRaisesRegex(ValueError, "complete must be a boolean"):
            validate_search_manifest(path, allow_incomplete=False)

    def test_allow_incomplete_does_not_waive_non_boolean_complete(self) -> None:
        path = self.write_manifest(complete=1)
        with self.assertRaisesRegex(ValueError, "complete must be a boolean"):
            validate_search_manifest(path, allow_incomplete=True)

    def test_explicit_allow_incomplete_accepts_disclosed_gaps(self) -> None:
        path = self.write_manifest(
            complete=False,
            requested_sources=["openalex", "crossref"],
            sources={"openalex": {"status": "failed"}},
        )
        loaded = validate_search_manifest(path, allow_incomplete=True)
        self.assertFalse(loaded["complete"])

    def test_allow_incomplete_does_not_waive_missing_source_plan(self) -> None:
        path = self.write_manifest(requested_sources=[])
        with self.assertRaisesRegex(ValueError, "requested_sources"):
            validate_search_manifest(path, allow_incomplete=True)


class ScreeningDecisionValidationTests(unittest.TestCase):
    def row(self, **overrides: str) -> dict[str, str]:
        row = {
            "record_id": "R1",
            "title": "Example paper",
            "year": "2026",
            "doi": "10.1000/example",
            "decision": "download",
            "matched_criteria": "Matches population and outcome",
        }
        row.update(overrides)
        return row

    def build(self, rows: list[dict[str, str]]) -> list[dict[str, str]]:
        selected, _ = build_download_rows(
            rows,
            include_uncertain=True,
            priority="normal",
            requested_files="article_pdf",
        )
        return selected

    def test_exclude_requires_nonempty_exclusion_reason(self) -> None:
        row = self.row(decision="exclude", matched_criteria="", exclusion_reason="   ")
        with self.assertRaisesRegex(ValueError, "decision=exclude requires exclusion_reason"):
            self.build([row])

    def test_download_accepts_explicit_generic_reason(self) -> None:
        row = self.row(matched_criteria="", reason="Clearly matches the protocol")
        selected = self.build([row])
        self.assertEqual(selected[0]["download_reason"], "Clearly matches the protocol")

    def test_uncertain_requires_needs_fulltext_reason_even_with_match(self) -> None:
        row = self.row(
            decision="uncertain",
            matched_criteria="Population matches",
            needs_fulltext_reason="",
        )
        with self.assertRaisesRegex(ValueError, "decision=uncertain requires needs_fulltext_reason"):
            self.build([row])

    def test_duplicate_row_cannot_hide_missing_reason(self) -> None:
        rows = [self.row(), self.row(record_id="R2", matched_criteria="")]
        with self.assertRaisesRegex(ValueError, "row 3.*matched_criteria or reason"):
            self.build(rows)

    def test_download_only_still_validates_unselected_rows(self) -> None:
        rows = [
            self.row(),
            self.row(
                record_id="R2",
                doi="10.1000/uncertain",
                decision="uncertain",
                matched_criteria="Population matches",
                needs_fulltext_reason="",
            ),
        ]
        with self.assertRaisesRegex(ValueError, "needs_fulltext_reason"):
            build_download_rows(
                rows,
                include_uncertain=False,
                priority="normal",
                requested_files="article_pdf",
            )

    def test_valid_decision_reasons_preserve_selection_behavior(self) -> None:
        rows = [
            self.row(),
            self.row(
                record_id="R2",
                doi="10.1000/uncertain",
                decision="uncertain",
                matched_criteria="Population matches",
                needs_fulltext_reason="Need the methods table",
            ),
            self.row(
                record_id="R3",
                doi="10.1000/exclude",
                decision="exclude",
                matched_criteria="",
                exclusion_reason="Wrong intervention",
            ),
        ]
        selected = self.build(rows)
        self.assertEqual([row["screening_decision"] for row in selected], ["download", "uncertain"])

    def test_conflicting_duplicate_decisions_remain_rejected(self) -> None:
        rows = [
            self.row(),
            self.row(
                record_id="R2",
                decision="exclude",
                matched_criteria="",
                exclusion_reason="Wrong intervention",
            ),
        ]
        with self.assertRaisesRegex(ValueError, "conflicting decisions"):
            self.build(rows)

    def test_record_identity_is_still_required(self) -> None:
        row = self.row(title="", doi="")
        with self.assertRaisesRegex(ValueError, "requires DOI or title"):
            self.build([row])


class HandoffCliCompatibilityTests(unittest.TestCase):
    def test_existing_dry_run_cli_arguments_still_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "run_manifest.json"
            manifest.write_text(json.dumps({
                "complete": True,
                "requested_sources": ["openalex"],
                "sources": {"openalex": {"status": "complete"}},
            }), encoding="utf-8")
            screening = root / "pre_screening.csv"
            with screening.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=[
                    "record_id", "title", "year", "doi", "decision", "matched_criteria",
                ])
                writer.writeheader()
                writer.writerow({
                    "record_id": "R1",
                    "title": "Example paper",
                    "year": "2026",
                    "doi": "10.1000/example",
                    "decision": "download",
                    "matched_criteria": "Matches the protocol",
                })
            script = Path(__file__).resolve().parents[1] / "scripts/handoff_screening_to_download.py"
            completed = subprocess.run([
                sys.executable,
                str(script),
                "--screening", str(screening),
                "--search-manifest", str(manifest),
                "--purpose", "regression test",
                "--title", "CLI compatibility",
                "--dry-run",
            ], check=True, capture_output=True, text=True)
            result = json.loads(completed.stdout)
            self.assertEqual(result["status"], "dry_run")
            self.assertEqual(result["selected_count"], 1)


if __name__ == "__main__":
    unittest.main()
