from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import langgraph_search as search_graph
from search_literature import (
    CSV_FIELDS,
    PRE_SCREENING_FIELDS,
    merge_records,
    pre_screening_rows,
    write_csv,
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class LangGraphSearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.config = self.root / "query.json"
        self.config_doc = {
            "query_id": "Q-20261014",
            "original_query": "soil chromium",
            "source_order": ["crossref", "openalex"],
            "sources": {
                "crossref": {"query": "soil chromium"},
                "openalex": {"query": "soil AND chromium"},
            },
        }
        self.config.write_text(
            json.dumps(self.config_doc, ensure_ascii=False), encoding="utf-8"
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def row(self, source: str, source_record: str, *, title: str, doi: str = "",
            abstract: str = "", year: str = "2025") -> dict[str, str]:
        native = self.config_doc["sources"][source]["query"]
        value = {field: "" for field in CSV_FIELDS}
        value.update({
            "record_id": f"{source}:{source_record}",
            "source_database": source,
            "source_record": source_record,
            "search_query": self.config_doc["original_query"],
            "native_query": native,
            "title": title,
            "abstract": abstract,
            "authors": "A. Author",
            "year": year,
            "publication_date": f"{year}-01-01",
            "journal": "Journal",
            "doi": doi,
            "landing_url": f"https://example.test/{source_record}",
            "document_type": "article",
            "updated_at": "2026-10-14",
            "abstract_status": "present" if abstract else "missing",
        })
        return value

    def make_partition(self, directory: Path, source: str,
                       rows: list[dict[str, str]], *, status: str = "complete") -> None:
        source_dir = directory / source
        raw_dir = source_dir / "raw"
        raw_dir.mkdir(parents=True)
        adjusted: list[dict[str, str]] = []
        if status in {"complete", "incomplete"}:
            raw = raw_dir / "page-000001.json"
            raw.write_text(json.dumps({"source": source}), encoding="utf-8")
            for row in rows:
                item = dict(row)
                item["raw_page"] = str(raw.resolve())
                adjusted.append(item)
            write_csv(source_dir / "records.csv", CSV_FIELDS, adjusted)
        else:
            adjusted = []

        deduplicated = merge_records(adjusted)
        write_csv(directory / "all_records.csv", CSV_FIELDS, adjusted)
        write_csv(directory / "deduplicated_records.csv", CSV_FIELDS, deduplicated)
        write_csv(
            directory / "pre_screening.csv",
            PRE_SCREENING_FIELDS,
            pre_screening_rows(deduplicated),
        )
        fetched = len(adjusted)
        info: dict[str, object] = {
            "status": status,
            "native_query": self.config_doc["sources"][source]["query"],
            "started_at": "2026-10-14T00:00:00+00:00",
            "finished_at": "2026-10-14T00:01:00+00:00",
        }
        if status in {"complete", "incomplete"}:
            info.update({
                "reported_total": fetched if status == "complete" else fetched + 10,
                "fetched_count": fetched,
                "complete": status == "complete",
                "pages": 1,
                "note": "" if status == "complete" else "partial_by_max_records",
            })
        else:
            info["error"] = "deliberately omitted from canonical state"
        manifest = {
            "query_id": self.config_doc["query_id"],
            "original_query": self.config_doc["original_query"],
            "started_at": "2026-10-14T00:00:00+00:00",
            "requested_sources": [source],
            "max_records_per_source": 0,
            "sources": {source: info},
            "finished_at": "2026-10-14T00:01:00+00:00",
            "raw_record_count": fetched,
            "deduplicated_record_count": len(deduplicated),
            "abstract_present_count": sum(bool(row["abstract"]) for row in deduplicated),
            "complete": status == "complete",
        }
        (directory / "run_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

    def partition_result(self, source: str, rows: list[dict[str, str]], *,
                         status: str = "complete", name: str | None = None) -> dict:
        directory = self.root / (name or f"partition-{source}")
        directory.mkdir()
        self.make_partition(directory, source, rows, status=status)
        return search_graph._partition_result(
            directory,
            self.config_doc,
            sha256(self.config),
            source,
        )

    def test_run_source_partition_isolates_each_source_and_passes_allow_incomplete(self) -> None:
        work_root = self.root / "partitions"
        rows = {
            "crossref": [self.row("crossref", "C1", title="One")],
            "openalex": [self.row("openalex", "O1", title="Two")],
        }
        commands: list[list[str]] = []

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            commands.append(command)
            source = command[command.index("--source") + 1]
            output = Path(command[command.index("--output-dir") + 1])
            self.make_partition(output, source, rows[source])
            return subprocess.CompletedProcess(command, 0, stdout="ignored", stderr="ignored")

        with mock.patch.object(search_graph.subprocess, "run", side_effect=fake_run):
            first = search_graph.run_source_partition(
                self.config, "crossref", work_root, python_executable=sys.executable
            )
            second = search_graph.run_source_partition(
                self.config, "openalex", work_root, python_executable=sys.executable
            )
            replayed = search_graph.run_source_partition(
                self.config, "crossref", work_root, python_executable=sys.executable
            )

        self.assertNotEqual(first["partition_dir"], second["partition_dir"])
        self.assertEqual(first, replayed)
        self.assertEqual(len(commands), 2)
        self.assertEqual(first["manifest_path"], "run_manifest.json")
        self.assertEqual(first["records_path"], "crossref/records.csv")
        self.assertEqual(first["max_records"], 0)
        self.assertEqual(first["binding"]["config_sha256"], sha256(self.config))
        self.assertNotIn("original_query", first)
        self.assertNotIn("native_query", first)
        json.dumps(first)
        self.assertTrue(all("--allow-incomplete" in command for command in commands))
        self.assertTrue(all(command.count("--source") == 1 for command in commands))

    def test_publish_two_sources_is_ordered_deduplicated_and_rewrites_raw_pages(self) -> None:
        crossref = self.partition_result("crossref", [
            self.row(
                "crossref", "C1", title="Shared Paper", doi="10.1/shared",
                abstract="short",
            )
        ])
        openalex = self.partition_result("openalex", [
            self.row(
                "openalex", "O1", title="Shared Paper", doi="10.1/shared",
                abstract="a much longer abstract",
            ),
            self.row("openalex", "O2", title="Unique Paper", doi="10.1/unique"),
        ])
        output = self.root / "published" / "run-1"
        # Deliberately pass completion order rather than configured source order.
        result = search_graph.publish_search_run(
            self.config, [openalex, crossref], output, "orch-1", 1
        )

        self.assertFalse(result["reused"])
        self.assertTrue(result["complete"])
        manifest = json.loads((output / "run_manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["requested_sources"], ["crossref", "openalex"])
        self.assertEqual(list(manifest["sources"]), ["crossref", "openalex"])
        self.assertEqual(manifest["raw_record_count"], 3)
        self.assertEqual(manifest["deduplicated_record_count"], 2)
        self.assertNotIn("error", json.dumps(manifest))

        with (output / "all_records.csv").open(
            "r", encoding="utf-8-sig", newline=""
        ) as handle:
            all_rows = list(csv.DictReader(handle))
        self.assertEqual(
            [row["source_database"] for row in all_rows],
            ["crossref", "openalex", "openalex"],
        )
        for row in all_rows:
            self.assertFalse(Path(row["raw_page"]).is_absolute())
            self.assertTrue((output / row["raw_page"]).is_file())

        with (output / "deduplicated_records.csv").open(
            "r", encoding="utf-8-sig", newline=""
        ) as handle:
            deduplicated = list(csv.DictReader(handle))
        self.assertEqual(deduplicated[0]["record_id"], "crossref:C1;openalex:O1")
        self.assertEqual(deduplicated[0]["source_database"], "crossref;openalex")
        self.assertEqual(deduplicated[0]["abstract"], "a much longer abstract")

    def test_incomplete_partition_publishes_audit_run_but_never_complete(self) -> None:
        crossref = self.partition_result("crossref", [
            self.row("crossref", "C1", title="Complete")
        ])
        openalex = self.partition_result(
            "openalex",
            [self.row("openalex", "O1", title="Capped")],
            status="incomplete",
        )
        output = self.root / "incomplete-run"
        result = search_graph.publish_search_run(
            self.config, [crossref, openalex], output, "orch-incomplete", 1
        )
        manifest = json.loads((output / "run_manifest.json").read_text(encoding="utf-8"))

        self.assertFalse(result["complete"])
        self.assertFalse(manifest["complete"])
        self.assertEqual(manifest["sources"]["openalex"]["status"], "incomplete")

    def test_failed_partition_gets_empty_canonical_records_without_leaking_error(self) -> None:
        crossref = self.partition_result("crossref", [
            self.row("crossref", "C1", title="Complete")
        ])
        openalex = self.partition_result("openalex", [], status="failed")
        output = self.root / "failed-run"
        search_graph.publish_search_run(
            self.config, [crossref, openalex], output, "orch-failed", 1
        )

        with (output / "openalex/records.csv").open(
            "r", encoding="utf-8-sig", newline=""
        ) as handle:
            reader = csv.DictReader(handle)
            self.assertEqual(reader.fieldnames, CSV_FIELDS)
            self.assertEqual(list(reader), [])
        manifest_text = (output / "run_manifest.json").read_text(encoding="utf-8")
        self.assertNotIn("deliberately omitted", manifest_text)
        self.assertIn('"error_code": "failed"', manifest_text)

    def test_identical_repeat_reuses_and_conflicting_binding_is_rejected(self) -> None:
        crossref = self.partition_result("crossref", [
            self.row("crossref", "C1", title="One")
        ])
        openalex = self.partition_result("openalex", [
            self.row("openalex", "O1", title="Two")
        ])
        output = self.root / "run"
        first = search_graph.publish_search_run(
            self.config, [crossref, openalex], output, "orch-repeat", 1
        )
        before = (output / "run_manifest.json").read_bytes()
        second = search_graph.publish_search_run(
            self.config, [openalex, crossref], output, "orch-repeat", 1
        )
        self.assertFalse(first["reused"])
        self.assertTrue(second["reused"])
        self.assertEqual(before, (output / "run_manifest.json").read_bytes())

        with self.assertRaisesRegex(
            search_graph.SearchOrchestrationError, "different orchestration binding"
        ):
            search_graph.publish_search_run(
                self.config, [crossref, openalex], output, "other-orchestration", 1
            )
        with self.assertRaisesRegex(
            search_graph.SearchOrchestrationError, "different orchestration binding"
        ):
            search_graph.publish_search_run(
                self.config, [crossref, openalex], output, "orch-repeat", 2
            )

        replacement_openalex = self.partition_result(
            "openalex",
            [self.row("openalex", "O-NEW", title="Different API result")],
            name="partition-openalex-replacement",
        )
        with self.assertRaisesRegex(
            search_graph.SearchOrchestrationError, "different partition bindings"
        ):
            search_graph.publish_search_run(
                self.config,
                [crossref, replacement_openalex],
                output,
                "orch-repeat",
                1,
            )

    def test_tampered_partition_binding_is_rejected(self) -> None:
        crossref = self.partition_result("crossref", [
            self.row("crossref", "C1", title="One")
        ])
        openalex = self.partition_result("openalex", [
            self.row("openalex", "O1", title="Two")
        ])
        records = Path(openalex["partition_dir"]) / "openalex/records.csv"
        records.write_text(records.read_text(encoding="utf-8-sig") + "\n", encoding="utf-8")
        with self.assertRaisesRegex(
            search_graph.SearchOrchestrationError, "binding summary is stale"
        ):
            search_graph.publish_search_run(
                self.config,
                [crossref, openalex],
                self.root / "tampered",
                "orch-tampered",
                1,
            )


if __name__ == "__main__":
    unittest.main()
