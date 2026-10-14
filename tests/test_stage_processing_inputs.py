from __future__ import annotations

import hashlib
import importlib.util
import json
import csv
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "stage_processing_inputs", ROOT / "scripts/stage_processing_inputs.py"
)
stage = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(stage)

PAPERQA_SPEC = importlib.util.spec_from_file_location(
    "paperqa_luna_for_stage_test", ROOT / "scripts/paperqa_luna.py"
)
paperqa = importlib.util.module_from_spec(PAPERQA_SPEC)
assert PAPERQA_SPEC.loader is not None
PAPERQA_SPEC.loader.exec_module(paperqa)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class StageProcessingInputsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "accepted/P-1.pdf"
        self.parsed = self.root / "parsed/P-1/full.md"
        self.source.parent.mkdir(parents=True)
        self.parsed.parent.mkdir(parents=True)
        self.source.write_bytes(b"%PDF-1.7\nverified full text\n")
        self.parsed.write_text("# Parsed paper\n\nEvidence.\n", encoding="utf-8")
        self.sections = self.parsed.parent / "section_chunks.jsonl"
        self.sections_sha, self.sections_count = stage._section_chunking.write_sections_jsonl(
            self.parsed,
            self.sections,
            paper_id="P-1",
            source_file="P-1.pdf",
            source_format="pdf",
            source_sha256=digest(self.source),
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def plan(self, **overrides: object) -> dict:
        values: dict[str, object] = {
            "root": self.root,
            "task_id": "task-20261014",
            "document_id": "P-1",
            "source_ref": "accepted/P-1.pdf",
            "source_sha256": digest(self.source),
            "parsed_text_ref": "parsed/P-1/full.md",
            "parsed_text_sha256": digest(self.parsed),
            "allow_copy_fulltext": False,
        }
        values.update(overrides)
        return stage.prepare_stage(**values)

    def write_authoritative_acceptance(
        self,
        *,
        status: str = "accepted",
        accepted_at: str = "2026-10-14T00:00:00Z",
        index_overrides: dict[str, str] | None = None,
    ) -> None:
        lock = self.root / "00_control/.queue.lock"
        lock.parent.mkdir(parents=True, exist_ok=True)
        lock.touch()
        queue_path = (
            self.root
            / "02_batches/DL-TEST/outputs/document_processing/queue.sqlite3"
        )
        queue_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(queue_path)
        try:
            connection.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            connection.execute(
                "INSERT INTO metadata VALUES('schema_version',?)",
                (stage.CURRENT_QUEUE_SCHEMA_VERSION,),
            )
            connection.execute(
                """CREATE TABLE jobs(
                    batch_id TEXT, source_item_id TEXT, paper_id TEXT,
                    document_id TEXT, title TEXT, doi TEXT, source_kind TEXT,
                    source_path TEXT, source_sha256 TEXT, processor TEXT,
                    processor_version TEXT, parsed_text_path TEXT,
                    parsed_text_sha256 TEXT, sections_path TEXT,
                    sections_sha256 TEXT, sections_count INTEGER,
                    status TEXT, accepted_at TEXT
                )"""
            )
            connection.execute(
                "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "DL-TEST", "D001", "P-1", "P-1", "Example paper", "",
                    "pdf", "accepted/P-1.pdf", digest(self.source), "mineru",
                    "pipeline-2.1", "parsed/P-1/full.md", digest(self.parsed),
                    "parsed/P-1/section_chunks.jsonl", self.sections_sha,
                    self.sections_count, status, accepted_at,
                ),
            )
            connection.commit()
        finally:
            connection.close()

        fields = [
            "paper_id", "title", "doi", "pdf_path", "supplement_paths",
            "sha256", "source_batch_id", "identity_verified", "accepted_at",
            "notes", "mineru_md_path", "xml_md_path", "source_item_id",
            "document_id", "source_kind", "processed_text_sha256",
            "processing_method", "processing_version", "processing_accepted_at",
            "section_chunks_path", "section_chunks_sha256",
            "section_chunks_count", "section_chunks_schema_version",
        ]
        row = {
            "paper_id": "P-1",
            "title": "Example paper",
            "doi": "",
            "pdf_path": "accepted/P-1.pdf",
            "supplement_paths": "",
            "sha256": digest(self.source),
            "source_batch_id": "DL-TEST",
            "identity_verified": "true",
            "accepted_at": "2026-10-13T00:00:00Z",
            "notes": "",
            "mineru_md_path": "parsed/P-1/full.md",
            "xml_md_path": "",
            "source_item_id": "D001",
            "document_id": "P-1",
            "source_kind": "pdf",
            "processed_text_sha256": digest(self.parsed),
            "processing_method": "mineru",
            "processing_version": "pipeline-2.1",
            "processing_accepted_at": accepted_at,
            "section_chunks_path": "parsed/P-1/section_chunks.jsonl",
            "section_chunks_sha256": self.sections_sha,
            "section_chunks_count": str(self.sections_count),
            "section_chunks_schema_version": stage._section_chunking.SECTION_SCHEMA_VERSION,
        }
        row.update(index_overrides or {})
        index_path = self.root / "04_library_index/PAPER_INDEX.csv"
        index_path.parent.mkdir(parents=True, exist_ok=True)
        with index_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerow(row)

    def test_stages_content_addressed_parsed_text_and_relative_manifest(self) -> None:
        plan = self.plan()
        manifest_path = stage.stage_document(plan)
        destination = self.root.resolve() / "runtime/inputs/task-20261014/documents/P-1"

        self.assertEqual(manifest_path, destination / "manifest.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        parsed_sha = digest(self.parsed)
        self.assertEqual(manifest["document_id"], "P-1")
        self.assertEqual(manifest["paper_id"], "P-1")
        self.assertEqual(manifest["parser"], "mineru")
        self.assertEqual(manifest["parser_version"], "unspecified")
        self.assertEqual(manifest["status"], "accepted")
        self.assertEqual(manifest["source_sha256"], digest(self.source))
        self.assertEqual(manifest["parsed_text_sha256"], parsed_sha)
        self.assertEqual(manifest["source_ref"], "accepted/P-1.pdf")
        self.assertEqual(manifest["source_path"], "accepted/P-1.pdf")
        self.assertEqual(manifest["parsed_text_source_ref"], "parsed/P-1/full.md")
        self.assertEqual(
            manifest["parsed_text_ref"],
            f"runtime/inputs/task-20261014/documents/P-1/parsed/{parsed_sha}.md",
        )
        self.assertEqual(manifest["parsed_text_path"], manifest["parsed_text_ref"])
        self.assertIsNone(manifest["source_staged_ref"])
        self.assertFalse(manifest["fulltext_copied"])
        self.assertEqual(
            (self.root / manifest["parsed_text_ref"]).read_bytes(), self.parsed.read_bytes()
        )
        self.assertFalse((destination / "source").exists())
        for key, value in manifest.items():
            if key.endswith("_ref") and value is not None:
                self.assertFalse(Path(value).is_absolute(), key)
                self.assertNotIn("..", Path(value).parts, key)

    def test_fulltext_is_copied_only_with_explicit_allow_flag(self) -> None:
        plan = self.plan(allow_copy_fulltext=True)
        manifest_path = stage.stage_document(plan)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        expected_ref = (
            "runtime/inputs/task-20261014/documents/P-1/source/"
            f"{digest(self.source)}.pdf"
        )
        self.assertTrue(manifest["fulltext_copied"])
        self.assertEqual(manifest["source_staged_ref"], expected_ref)
        self.assertEqual(manifest["source_path"], expected_ref)
        self.assertEqual((self.root / expected_ref).read_bytes(), self.source.read_bytes())

    def test_stages_authoritative_section_chunks_with_parsed_text(self) -> None:
        plan = self.plan(
            sections_ref="parsed/P-1/section_chunks.jsonl",
            sections_sha256=self.sections_sha,
            sections_count=self.sections_count,
            sections_schema_version=stage._section_chunking.SECTION_SCHEMA_VERSION,
        )
        manifest_path = stage.stage_document(plan)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(manifest["section_chunks_sha256"], self.sections_sha)
        self.assertEqual(manifest["section_chunks_count"], self.sections_count)
        self.assertEqual(
            (self.root / manifest["section_chunks_ref"]).read_bytes(),
            self.sections.read_bytes(),
        )

    def test_rejects_tampered_section_chunks(self) -> None:
        with self.assertRaisesRegex(stage.StageError, "stale sections_sha256"):
            self.plan(
                sections_ref="parsed/P-1/section_chunks.jsonl",
                sections_sha256="0" * 64,
                sections_count=self.sections_count,
                sections_schema_version=stage._section_chunking.SECTION_SCHEMA_VERSION,
            )

    def test_records_explicit_paper_and_parser_metadata(self) -> None:
        plan = self.plan(
            batch_id="DL-TEST",
            source_item_id="D001",
            paper_id="doi:10.1000/example",
            title="Example paper",
            doi="10.1000/example",
            parser="mineru",
            parser_version="pipeline-2.1",
        )
        manifest_path = stage.stage_document(plan)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(manifest["paper_id"], "doi:10.1000/example")
        self.assertEqual(manifest["batch_id"], "DL-TEST")
        self.assertEqual(manifest["source_item_id"], "D001")
        self.assertEqual(manifest["item_id"], "D001")
        self.assertEqual(manifest["title"], "Example paper")
        self.assertEqual(manifest["doi"], "10.1000/example")
        self.assertEqual(manifest["parser"], "mineru")
        self.assertEqual(manifest["parser_version"], "pipeline-2.1")

    def test_batch_and_source_item_must_be_supplied_together(self) -> None:
        with self.assertRaisesRegex(stage.StageError, "must be supplied together"):
            self.plan(batch_id="DL-TEST")
        with self.assertRaisesRegex(stage.StageError, "must be supplied together"):
            self.plan(source_item_id="D001")

    def test_fulltext_stage_is_directly_consumable_by_paperqa(self) -> None:
        manifest_path = stage.stage_document(
            self.plan(
                paper_id="P-1",
                parser="mineru",
                parser_version="vlm-1",
                allow_copy_fulltext=True,
                sections_ref="parsed/P-1/section_chunks.jsonl",
                sections_sha256=self.sections_sha,
                sections_count=self.sections_count,
                sections_schema_version=stage._section_chunking.SECTION_SCHEMA_VERSION,
            )
        )
        rows = paperqa._manifest_rows(manifest_path)
        chunks, binding = paperqa._markdown(rows[0], manifest_path, self.root.resolve())

        self.assertTrue(chunks)
        self.assertEqual(binding["paper_id"], "P-1")
        self.assertEqual(binding["parser"], "mineru")
        self.assertEqual(binding["parser_version"], "vlm-1")

    def test_xml_source_is_supported(self) -> None:
        xml = self.root / "accepted/P-1.xml"
        xml.write_text("<article><body>full text</body></article>", encoding="utf-8")
        plan = self.plan(
            source_ref="accepted/P-1.xml",
            source_sha256=digest(xml),
            allow_copy_fulltext=True,
        )
        manifest_path = stage.stage_document(plan)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(manifest["source_kind"], "xml")
        self.assertTrue(manifest["source_staged_ref"].endswith(".xml"))

    def test_rejects_stale_source_and_parsed_hashes(self) -> None:
        with self.assertRaisesRegex(stage.StageError, "stale source_sha256"):
            self.plan(source_sha256="0" * 64)
        with self.assertRaisesRegex(stage.StageError, "stale parsed_text_sha256"):
            self.plan(parsed_text_sha256="0" * 64)

    def test_rechecks_reference_only_source_before_atomic_publish(self) -> None:
        plan = self.plan()
        self.source.write_bytes(b"%PDF-1.7\nchanged after preparation\n")

        with self.assertRaisesRegex(stage.StageError, "source changed after validation"):
            stage.stage_document(plan)
        self.assertFalse(plan["destination"].exists())

    def test_rechecks_parsed_source_before_atomic_publish(self) -> None:
        plan = self.plan()
        original_copy = stage.copy_verified

        def mutate_after_copy(source: Path, destination: Path, expected_sha256: str) -> None:
            original_copy(source, destination, expected_sha256)
            if source.resolve() == self.parsed.resolve():
                self.parsed.write_text("changed after verified copy\n", encoding="utf-8")

        with mock.patch.object(stage, "copy_verified", side_effect=mutate_after_copy):
            with self.assertRaisesRegex(stage.StageError, "parsed text changed after validation"):
                stage.stage_document(plan)
        self.assertFalse(plan["destination"].exists())

    def test_rejects_absolute_parent_and_windows_refs(self) -> None:
        cases = [
            str(self.source),
            "../accepted/P-1.pdf",
            "accepted/../accepted/P-1.pdf",
            r"C:\\papers\\P-1.pdf",
        ]
        for source_ref in cases:
            with self.subTest(source_ref=source_ref):
                with self.assertRaises(stage.StageError):
                    self.plan(source_ref=source_ref)

    def test_rejects_noncanonical_lexical_refs(self) -> None:
        for source_ref in (
            "./accepted/P-1.pdf",
            "accepted//P-1.pdf",
            "accepted/./P-1.pdf",
            "accepted/P-1.pdf/",
        ):
            with self.subTest(source_ref=source_ref):
                with self.assertRaises(stage.StageError):
                    self.plan(source_ref=source_ref)

    def test_rejects_input_symlink_escape(self) -> None:
        outside_dir = Path(tempfile.mkdtemp())
        try:
            outside = outside_dir / "outside.pdf"
            outside.write_bytes(b"%PDF-1.7\noutside")
            link = self.root / "accepted/escape.pdf"
            link.symlink_to(outside)
            with self.assertRaisesRegex(stage.StageError, "escapes workspace"):
                self.plan(source_ref="accepted/escape.pdf", source_sha256=digest(outside))
        finally:
            outside.unlink(missing_ok=True)
            outside_dir.rmdir()

    def test_rejects_destination_parent_symlink_escape(self) -> None:
        outside_dir = Path(tempfile.mkdtemp())
        try:
            (self.root / "runtime").mkdir()
            (self.root / "runtime/inputs").symlink_to(outside_dir, target_is_directory=True)
            with self.assertRaisesRegex(stage.StageError, "destination escapes workspace"):
                self.plan()
        finally:
            (self.root / "runtime/inputs").unlink(missing_ok=True)
            outside_dir.rmdir()

    def test_rejects_unsafe_task_and_document_ids(self) -> None:
        for task_id in ("", ".", "..", "bad/id", "任务", "a" * 129):
            with self.subTest(task_id=task_id):
                with self.assertRaises(stage.StageError):
                    self.plan(task_id=task_id)
        for document_id in ("", ".", "..", "bad/id", "paper id", "a" * 129):
            with self.subTest(document_id=document_id):
                with self.assertRaises(stage.StageError):
                    self.plan(document_id=document_id)

    def test_identical_existing_document_is_idempotent(self) -> None:
        first_plan = self.plan()
        manifest_path = stage.stage_document(first_plan)
        original = manifest_path.read_bytes()

        second_plan = self.plan()
        self.assertTrue(second_plan["already_staged"])
        self.assertEqual(stage.stage_document(second_plan), manifest_path)
        self.assertEqual(manifest_path.read_bytes(), original)

    def test_existing_document_with_different_manifest_is_never_overwritten(self) -> None:
        first_plan = self.plan()
        manifest_path = stage.stage_document(first_plan)
        original = manifest_path.read_bytes()
        alternate = self.root / "parsed/P-1/alternate.md"
        alternate.write_text("# Deliberately changed parse\n", encoding="utf-8")

        with self.assertRaisesRegex(stage.StageError, "different manifest"):
            self.plan(
                parsed_text_ref="parsed/P-1/alternate.md",
                parsed_text_sha256=digest(alternate),
            )
        self.assertEqual(manifest_path.read_bytes(), original)

    def test_racing_empty_destination_is_not_replaced(self) -> None:
        plan = self.plan()

        def race(destination: Path) -> None:
            destination.mkdir()
            raise FileExistsError(destination)

        with mock.patch.object(stage, "claim_destination", side_effect=race):
            with self.assertRaisesRegex(stage.StageError, "incomplete"):
                stage.stage_document(plan)
        self.assertTrue(plan["destination"].is_dir())
        self.assertEqual(list(plan["destination"].iterdir()), [])

    def test_existing_stage_with_tampered_content_is_rejected(self) -> None:
        first_plan = self.plan()
        manifest_path = stage.stage_document(first_plan)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        (self.root.resolve() / manifest["parsed_text_ref"]).write_text(
            "tampered", encoding="utf-8"
        )

        with self.assertRaisesRegex(stage.StageError, "hash mismatch"):
            self.plan()

    def test_dry_run_validates_but_writes_nothing(self) -> None:
        self.write_authoritative_acceptance()
        output = StringIO()
        argv = [
            "--root", str(self.root),
            "--task-id", "task-20261014",
            "--document-id", "P-1",
            "--batch-id", "DL-TEST",
            "--source-item-id", "D001",
            "--paper-id", "P-1",
            "--title", "Example paper",
            "--parser", "mineru",
            "--parser-version", "pipeline-2.1",
            "--source-ref", "accepted/P-1.pdf",
            "--source-sha256", digest(self.source),
            "--parsed-text-ref", "parsed/P-1/full.md",
            "--parsed-text-sha256", digest(self.parsed),
            "--dry-run",
        ]
        with redirect_stdout(output):
            result = stage.main(argv)

        self.assertEqual(result, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "validated")
        self.assertTrue(payload["dry_run"])
        self.assertFalse((self.root / "runtime").exists())

    def test_cli_rejects_self_asserted_acceptance_without_authoritative_queue(self) -> None:
        argv = [
            "--root", str(self.root),
            "--task-id", "task-20261014",
            "--document-id", "P-1",
            "--batch-id", "DL-TEST",
            "--source-item-id", "D001",
            "--source-ref", "accepted/P-1.pdf",
            "--source-sha256", digest(self.source),
            "--parsed-text-ref", "parsed/P-1/full.md",
            "--parsed-text-sha256", digest(self.parsed),
            "--dry-run",
        ]
        with self.assertRaises(SystemExit):
            stage.main(argv)
        self.assertFalse((self.root / "runtime").exists())

    def test_cli_rejects_nonaccepted_processing_job(self) -> None:
        self.write_authoritative_acceptance(status="review", accepted_at="")
        argv = [
            "--root", str(self.root),
            "--task-id", "task-20261014",
            "--document-id", "P-1",
            "--batch-id", "DL-TEST",
            "--source-item-id", "D001",
            "--source-ref", "accepted/P-1.pdf",
            "--source-sha256", digest(self.source),
            "--parsed-text-ref", "parsed/P-1/full.md",
            "--parsed-text-sha256", digest(self.parsed),
            "--dry-run",
        ]
        with self.assertRaises(SystemExit):
            stage.main(argv)

    def test_cli_rejects_stale_processing_queue_schema(self) -> None:
        self.write_authoritative_acceptance()
        queue_path = self.root / "02_batches/DL-TEST/outputs/document_processing/queue.sqlite3"
        connection = sqlite3.connect(queue_path)
        try:
            connection.execute(
                "UPDATE metadata SET value='1' WHERE key='schema_version'"
            )
            connection.commit()
        finally:
            connection.close()
        argv = [
            "--root", str(self.root),
            "--task-id", "task-20261014",
            "--document-id", "P-1",
            "--batch-id", "DL-TEST",
            "--source-item-id", "D001",
            "--source-ref", "accepted/P-1.pdf",
            "--source-sha256", digest(self.source),
            "--parsed-text-ref", "parsed/P-1/full.md",
            "--parsed-text-sha256", digest(self.parsed),
            "--dry-run",
        ]
        with self.assertRaises(SystemExit):
            stage.main(argv)

    def test_cli_rejects_paper_index_hash_mismatch(self) -> None:
        self.write_authoritative_acceptance(
            index_overrides={"processed_text_sha256": "0" * 64}
        )
        argv = [
            "--root", str(self.root),
            "--task-id", "task-20261014",
            "--document-id", "P-1",
            "--batch-id", "DL-TEST",
            "--source-item-id", "D001",
            "--source-ref", "accepted/P-1.pdf",
            "--source-sha256", digest(self.source),
            "--parsed-text-ref", "parsed/P-1/full.md",
            "--parsed-text-sha256", digest(self.parsed),
            "--dry-run",
        ]
        with self.assertRaises(SystemExit):
            stage.main(argv)

    def test_rejects_wrong_file_kinds(self) -> None:
        wrong_source = self.root / "accepted/P-1.docx"
        wrong_source.write_bytes(b"not a supported source")
        with self.assertRaisesRegex(stage.StageError, "PDF or XML"):
            self.plan(source_ref="accepted/P-1.docx", source_sha256=digest(wrong_source))

        wrong_parsed = self.root / "parsed/P-1/full.json"
        wrong_parsed.write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(stage.StageError, "Markdown or plain text"):
            self.plan(
                parsed_text_ref="parsed/P-1/full.json",
                parsed_text_sha256=digest(wrong_parsed),
            )


if __name__ == "__main__":
    unittest.main()
