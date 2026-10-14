from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("accept_batch", ROOT / "scripts/accept_batch.py")
accept_batch = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(accept_batch)
QUEUE_SPEC = importlib.util.spec_from_file_location(
    "make_fulltext_screening_queue", ROOT / "scripts/make_fulltext_screening_queue.py")
fulltext_queue = importlib.util.module_from_spec(QUEUE_SPEC)
assert QUEUE_SPEC.loader is not None
QUEUE_SPEC.loader.exec_module(fulltext_queue)


class FakeBuilder:
    SUCCESS = ("downloaded", "reused_verified")

    def __init__(self, *, xml: Path | None = None, pdf_gate: dict | None = None,
                 xml_gate: dict | None = None,
                 tokens: dict[str, tuple[str, ...]] | None = None):
        self.xml = xml
        self.pdf_gate = pdf_gate or {
            "ok": True, "identity_verified": True, "reason": "PDF_FULL", "how": "doi"
        }
        self.xml_gate = xml_gate or {
            "ok": True, "identity_verified": True, "reason": "XML_FULL", "how": "ce:doi"
        }
        self.pdf_calls: list[Path] = []
        self.xml_calls: list[Path] = []
        self.tokens = tokens or {}

    def load_tokens(self, batch: Path) -> dict:
        return self.tokens

    def find_xml(self, xml_dir: Path, paper_id: str, target: str, doi: str) -> Path:
        return self.xml if self.xml is not None else xml_dir / f"{paper_id}.xml"

    def gate_pdf(self, path: Path, doi: str, title: str, tokens: tuple[str, ...]) -> dict:
        self.pdf_calls.append(path)
        return self.pdf_gate

    def gate_xml(self, path: Path, doi: str, title: str) -> dict:
        self.xml_calls.append(path)
        return self.xml_gate


class AcceptBatchValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.batch = self.root / "02_batches/DL-TEST"
        (self.batch / "outputs/files").mkdir(parents=True)
        manifest_row = {field: "" for field in accept_batch.MANIFEST_FIELDS}
        manifest_row.update({
            "item_id": "I-1", "paper_id": "P-1", "title": "Target paper",
            "doi": "10.1234/example", "suggested_filename": "P-1.pdf",
        })
        self._write_csv(
            self.batch / "MANIFEST.csv", accept_batch.MANIFEST_FIELDS, [manifest_row])

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    def _result(self, *, pdf_path: str = "", status: str = "downloaded") -> None:
        row = {field: "" for field in accept_batch.DOWNLOAD_RESULT_FIELDS}
        row.update({
            "item_id": "I-1", "paper_id": "P-1", "status": status,
            "title": "Target paper", "doi": "10.1234/example",
            "pdf_path": pdf_path, "identity_verified": "true", "notes": "",
        })
        self._write_csv(
            self.batch / "outputs/download_results.csv",
            accept_batch.DOWNLOAD_RESULT_FIELDS, [row],
        )

    def _prepare_batch_state(self, status: str = "REVIEW") -> Path:
        index_path = self.root / "00_control/BATCH_INDEX.csv"
        self._write_csv(
            index_path, ["batch_id", "status", "updated_at", "accepted_at"],
            [{
                "batch_id": "DL-TEST", "status": status,
                "updated_at": "2026-10-13T00:00:00+00:00", "accepted_at": "",
            }],
        )
        (self.batch / "STATUS.json").write_text(
            json.dumps({"batch_id": "DL-TEST", "status": status}) + "\n",
            encoding="utf-8",
        )
        return index_path

    def test_success_with_missing_path_and_no_xml_is_rejected(self) -> None:
        self._result()

        errors, success, gaps = accept_batch.validate(self.root, self.batch, FakeBuilder())

        self.assertEqual(success, [])
        self.assertEqual(gaps, [])
        self.assertTrue(any("未填写 pdf_path" in error for error in errors))

    def test_success_with_nonexistent_pdf_is_rejected(self) -> None:
        missing = self.batch / "outputs/files/missing.pdf"
        self._result(pdf_path=str(missing))

        errors, success, _ = accept_batch.validate(self.root, self.batch, FakeBuilder())

        self.assertEqual(success, [])
        self.assertTrue(any("交付路径不存在" in error for error in errors))

    def test_success_with_empty_pdf_is_rejected_before_gate(self) -> None:
        empty = self.batch / "outputs/files/empty.pdf"
        empty.touch()
        self._result(pdf_path=str(empty))
        builder = FakeBuilder()

        errors, success, _ = accept_batch.validate(self.root, self.batch, builder)

        self.assertEqual(success, [])
        self.assertEqual(builder.pdf_calls, [])
        self.assertTrue(any("空文件" in error for error in errors))

    def test_success_with_failed_pdf_gate_is_rejected(self) -> None:
        pdf = self.batch / "outputs/files/bad.pdf"
        pdf.write_bytes(b"not really a PDF")
        self._result(pdf_path=str(pdf))
        builder = FakeBuilder(pdf_gate={
            "ok": False, "identity_verified": False,
            "reason": "文件头不是 %PDF", "how": "",
        })

        errors, success, _ = accept_batch.validate(self.root, self.batch, builder)

        self.assertEqual(success, [])
        self.assertTrue(any("文件头不是 %PDF" in error for error in errors))

    def test_existing_pdf_that_passes_gate_is_accepted(self) -> None:
        pdf = self.batch / "outputs/files/good.pdf"
        pdf.write_bytes(b"nonempty")
        self._result(pdf_path=str(pdf), status="reused_verified")

        errors, success, gaps = accept_batch.validate(self.root, self.batch, FakeBuilder())

        self.assertEqual(errors, [])
        self.assertEqual(gaps, [])
        self.assertEqual(len(success), 1)
        self.assertEqual(success[0]["pdf_path"], str(pdf))

    def test_blank_pdf_path_with_valid_fulltext_xml_is_accepted(self) -> None:
        xml = self.batch / "outputs/tdm-xml/P-1.xml"
        xml.parent.mkdir(parents=True)
        xml.write_text("<article>full text</article>", encoding="utf-8")
        self._result()

        errors, success, _ = accept_batch.validate(
            self.root, self.batch, FakeBuilder(xml=xml))

        self.assertEqual(errors, [])
        self.assertEqual(len(success), 1)
        self.assertEqual(success[0]["pdf_path"], str(xml))
        self.assertIn("[XML_FULL]", success[0]["notes"])

    def test_explicit_xml_path_must_pass_xml_gate(self) -> None:
        xml = self.batch / "outputs/tdm-xml/P-1.xml"
        xml.parent.mkdir(parents=True)
        xml.write_text("<article>wrong paper</article>", encoding="utf-8")
        self._result(pdf_path=str(xml))
        builder = FakeBuilder(xml_gate={
            "ok": True, "identity_verified": False,
            "reason": "XML_FULL but identity unverified", "how": "unverified",
        })

        errors, success, _ = accept_batch.validate(self.root, self.batch, builder)

        self.assertEqual(success, [])
        self.assertEqual(builder.pdf_calls, [])
        self.assertEqual(builder.xml_calls, [xml])
        self.assertTrue(any("identity unverified" in error for error in errors))

    def test_result_identity_must_match_manifest_for_same_item_id(self) -> None:
        pdf = self.batch / "outputs/files/good.pdf"
        pdf.write_bytes(b"nonempty")
        cases = {
            "paper_id": "P-WRONG",
            "title": "A different paper",
            "doi": "10.1234/wrong",
        }
        for field, wrong_value in cases.items():
            with self.subTest(field=field):
                row = {field_name: "" for field_name in accept_batch.DOWNLOAD_RESULT_FIELDS}
                row.update({
                    "item_id": "I-1", "paper_id": "P-1", "status": "downloaded",
                    "title": "Target paper", "doi": "10.1234/example",
                    "pdf_path": str(pdf), "identity_verified": "true", "notes": "",
                })
                row[field] = wrong_value
                self._write_csv(
                    self.batch / "outputs/download_results.csv",
                    accept_batch.DOWNLOAD_RESULT_FIELDS, [row])

                errors, success, _ = accept_batch.validate(
                    self.root, self.batch, FakeBuilder())

                self.assertEqual(success, [])
                self.assertTrue(any(f"身份不一致：{field}" in error for error in errors))

    def test_missing_pdf_path_column_cannot_silently_drop_xml_delivery(self) -> None:
        xml = self.batch / "outputs/tdm-xml/P-1.xml"
        xml.parent.mkdir(parents=True)
        xml.write_text("<article>full text</article>", encoding="utf-8")
        fields = [field for field in accept_batch.DOWNLOAD_RESULT_FIELDS if field != "pdf_path"]
        row = {field: "" for field in fields}
        row.update({
            "item_id": "I-1", "paper_id": "P-1", "status": "downloaded",
            "title": "Target paper", "doi": "10.1234/example",
            "identity_verified": "true",
        })
        self._write_csv(self.batch / "outputs/download_results.csv", fields, [row])

        errors, success, gaps = accept_batch.validate(
            self.root, self.batch, FakeBuilder(xml=xml))

        self.assertEqual(success, [])
        self.assertEqual(gaps, [])
        self.assertTrue(any("缺少必需列" in error and "pdf_path" in error for error in errors))

    def test_manifest_primary_keys_must_not_be_blank(self) -> None:
        manifest_row = {field: "" for field in accept_batch.MANIFEST_FIELDS}
        manifest_row.update({"title": "Target paper", "doi": "10.1234/example"})
        self._write_csv(
            self.batch / "MANIFEST.csv", accept_batch.MANIFEST_FIELDS, [manifest_row])
        self._result()

        errors, success, gaps = accept_batch.validate(
            self.root, self.batch, FakeBuilder())

        self.assertEqual(success, [])
        self.assertEqual(gaps, [])
        self.assertTrue(any("item_id/paper_id 不得为空" in error for error in errors))

    def test_empty_or_unknown_result_status_is_rejected(self) -> None:
        for status in ("", "mystery_status"):
            with self.subTest(status=status):
                self._result(status=status)
                errors, success, gaps = accept_batch.validate(
                    self.root, self.batch, FakeBuilder())
                self.assertEqual(success, [])
                self.assertEqual(gaps, [])
                self.assertTrue(any("status" in error and "允许集合" in error
                                    for error in errors))

    def test_weak_identity_tokens_are_rejected(self) -> None:
        pdf = self.batch / "outputs/files/good.pdf"
        pdf.write_bytes(b"nonempty")
        self._result(pdf_path=str(pdf))
        builder = FakeBuilder(tokens={"P-1": ("", "study", "study method")})

        errors, _, _ = accept_batch.validate(self.root, self.batch, builder)

        token_errors = [error for error in errors if "identity_tokens.json" in error]
        self.assertEqual(len(token_errors), 3)
        self.assertTrue(any("为空" in error for error in token_errors))
        self.assertTrue(any("过短" in error for error in token_errors))
        self.assertTrue(any("过常见" in error for error in token_errors))

    def test_empty_identity_token_array_is_rejected(self) -> None:
        pdf = self.batch / "outputs/files/good.pdf"
        pdf.write_bytes(b"nonempty")
        self._result(pdf_path=str(pdf))
        token_path = self.batch / "outputs/identity_tokens.json"
        token_path.write_text('{"P-1": []}\n', encoding="utf-8")

        errors, _, _ = accept_batch.validate(self.root, self.batch, FakeBuilder())

        self.assertTrue(any("必须是非空 token 数组" in error for error in errors))

    def test_validated_xml_path_and_type_are_persisted_for_downstream_queue(self) -> None:
        xml = self.batch / "outputs/tdm-xml/P-1.xml"
        xml.parent.mkdir(parents=True)
        xml.write_text("<article>full text</article>", encoding="utf-8")
        self._result()
        results_path = self.batch / "outputs/download_results.csv"

        errors, success, _ = accept_batch.validate(
            self.root, self.batch, FakeBuilder(xml=xml))
        changed = accept_batch.persist_validated_results(results_path, success)

        self.assertEqual(errors, [])
        self.assertTrue(changed)
        persisted = accept_batch.read_csv(results_path)[0]
        self.assertEqual(persisted["pdf_path"], str(xml))
        self.assertEqual(persisted["identity_verified"], "true")
        self.assertIn("[XML_FULL]", persisted["notes"])
        self.assertTrue(Path(persisted["pdf_path"]).is_file())
        state, downstream_path = fulltext_queue.fulltext_status(persisted)
        self.assertEqual(state, "ready")
        self.assertEqual(downstream_path, str(xml.resolve()))

        second_errors, second_success, _ = accept_batch.validate(
            self.root, self.batch, FakeBuilder(xml=xml))
        changed_again = accept_batch.persist_validated_results(results_path, second_success)
        persisted_again = accept_batch.read_csv(results_path)[0]
        self.assertEqual(second_errors, [])
        self.assertFalse(changed_again)
        self.assertEqual(persisted_again["notes"].count("[XML_FULL]"), 1)

    def test_persist_rejects_result_row_changed_after_validation(self) -> None:
        pdf = self.batch / "outputs/files/good.pdf"
        pdf.write_bytes(b"original payload")
        self._result(pdf_path=str(pdf))
        results_path = self.batch / "outputs/download_results.csv"
        errors, success, gaps = accept_batch.validate(
            self.root, self.batch, FakeBuilder())
        changed_row = accept_batch.read_csv(results_path)[0]
        changed_row["doi"] = "10.1234/changed-after-validation"
        self._write_csv(results_path, accept_batch.DOWNLOAD_RESULT_FIELDS, [changed_row])

        self.assertEqual(errors, [])
        with self.assertRaisesRegex(RuntimeError, "被修改"):
            accept_batch.persist_validated_results(results_path, success, success + gaps)

    def test_persist_rejects_payload_changed_after_validation(self) -> None:
        pdf = self.batch / "outputs/files/good.pdf"
        pdf.write_bytes(b"original payload")
        self._result(pdf_path=str(pdf))
        results_path = self.batch / "outputs/download_results.csv"
        errors, success, gaps = accept_batch.validate(
            self.root, self.batch, FakeBuilder())
        pdf.write_bytes(b"replacement payload")

        self.assertEqual(errors, [])
        with self.assertRaisesRegex(RuntimeError, "内容发生变化"):
            accept_batch.persist_validated_results(results_path, success, success + gaps)

    def test_main_aborts_without_writes_when_success_payload_is_missing(self) -> None:
        self._result()
        index_path = self._prepare_batch_state()
        argv = [
            "accept_batch.py", "DL-TEST", "--actor-role", "coordinator",
            "--root", str(self.root),
        ]

        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(accept_batch, "load_builder", return_value=FakeBuilder()), \
                mock.patch.object(accept_batch.subprocess, "run") as subprocess_run:
            code = accept_batch.main()

        self.assertEqual(code, 1)
        subprocess_run.assert_not_called()
        self.assertFalse((self.batch / "outputs/manual_backlog.csv").exists())
        self.assertFalse((self.root / "04_library_index/PAPER_INDEX.csv").exists())
        self.assertFalse((self.root / "03_review/DL-TEST-acceptance.md").exists())
        self.assertEqual(accept_batch.read_csv(index_path)[0]["status"], "REVIEW")

    def test_manual_backlog_generation_failure_aborts_acceptance(self) -> None:
        pdf = self.batch / "outputs/files/good.pdf"
        pdf.write_bytes(b"nonempty")
        self._result(pdf_path=str(pdf))
        index_path = self._prepare_batch_state()
        argv = [
            "accept_batch.py", "DL-TEST", "--actor-role", "coordinator",
            "--root", str(self.root),
        ]

        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(accept_batch, "load_builder", return_value=FakeBuilder()), \
                mock.patch.object(
                    accept_batch.subprocess, "run",
                    side_effect=accept_batch.subprocess.CalledProcessError(2, "backlog")):
            code = accept_batch.main()

        self.assertEqual(code, 1)
        self.assertEqual(accept_batch.read_csv(index_path)[0]["status"], "REVIEW")
        self.assertFalse((self.root / "04_library_index/PAPER_INDEX.csv").exists())

    def test_stale_manual_backlog_coverage_aborts_acceptance(self) -> None:
        self._result(status="failed")
        index_path = self._prepare_batch_state()
        stale = {field: "" for field in accept_batch.BACKLOG_FIELDS}
        stale.update({
            "batch_id": "DL-TEST", "item_id": "I-OTHER", "paper_id": "P-OTHER",
            "status": "failed", "title": "Wrong paper",
        })
        self._write_csv(
            self.batch / "outputs/manual_backlog.csv", accept_batch.BACKLOG_FIELDS, [stale])
        argv = [
            "accept_batch.py", "DL-TEST", "--actor-role", "coordinator",
            "--root", str(self.root),
        ]

        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(accept_batch, "load_builder", return_value=FakeBuilder()), \
                mock.patch.object(
                    accept_batch.subprocess, "run",
                    return_value=accept_batch.subprocess.CompletedProcess("backlog", 0)):
            code = accept_batch.main()

        self.assertEqual(code, 1)
        self.assertEqual(accept_batch.read_csv(index_path)[0]["status"], "REVIEW")
        self.assertFalse((self.root / "04_library_index/PAPER_INDEX.csv").exists())

    def test_successful_main_updates_indices_and_both_status_files_in_one_run(self) -> None:
        pdf = self.batch / "outputs/files/good.pdf"
        pdf.write_bytes(b"nonempty")
        self._result(pdf_path=str(pdf))
        index_path = self._prepare_batch_state()
        argv = [
            "accept_batch.py", "DL-TEST", "--actor-role", "coordinator",
            "--root", str(self.root),
        ]

        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(accept_batch, "load_builder", return_value=FakeBuilder()):
            code = accept_batch.main()

        self.assertEqual(code, 0)
        self.assertEqual(accept_batch.read_csv(index_path)[0]["status"], "ACCEPTED")
        status_doc = json.loads((self.batch / "STATUS.json").read_text(encoding="utf-8"))
        self.assertEqual(status_doc["status"], "ACCEPTED")
        paper_rows = accept_batch.read_csv(self.root / accept_batch.PAPER_INDEX)
        self.assertEqual(len(paper_rows), 1)
        self.assertEqual(paper_rows[0]["pdf_path"], str(pdf))
        self.assertTrue(paper_rows[0]["sha256"])
        self.assertEqual(paper_rows[0]["source_item_id"], "I-1")
        self.assertEqual(paper_rows[0]["source_kind"], "pdf")
        self.assertEqual(paper_rows[0]["mineru_md_path"], "")
        self.assertEqual(paper_rows[0]["processed_text_sha256"], "")
        self.assertEqual(paper_rows[0]["section_chunks_path"], "")
        self.assertEqual(paper_rows[0]["section_chunks_sha256"], "")
        self.assertEqual(paper_rows[0]["section_chunks_count"], "")
        self.assertEqual(paper_rows[0]["section_chunks_schema_version"], "")
        self.assertTrue((self.root / "03_review/DL-TEST-acceptance.md").is_file())
        self.assertFalse(
            accept_batch.acceptance_transaction_dir(self.root, "DL-TEST").exists())

    def test_partial_acceptance_commit_rolls_forward_on_rerun(self) -> None:
        pdf = self.batch / "outputs/files/good.pdf"
        pdf.write_bytes(b"nonempty")
        self._result(pdf_path=str(pdf))
        index_path = self._prepare_batch_state()
        argv = [
            "accept_batch.py", "DL-TEST", "--actor-role", "coordinator",
            "--root", str(self.root),
        ]
        original_install = accept_batch.install_transaction_target
        installed: list[str] = []

        def fail_after_two_targets(root: Path, batch_id: str,
                                   entry: dict[str, object], payload: bytes) -> None:
            if len(installed) == 2:
                raise OSError("injected crash between target replacements")
            original_install(root, batch_id, entry, payload)
            installed.append(str(entry["relative_path"]))

        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(accept_batch, "load_builder", return_value=FakeBuilder()), \
                mock.patch.object(
                    accept_batch, "install_transaction_target",
                    side_effect=fail_after_two_targets):
            first_code = accept_batch.main()

        transaction_dir = accept_batch.acceptance_transaction_dir(self.root, "DL-TEST")
        self.assertEqual(first_code, 1)
        self.assertEqual(installed, [
            accept_batch.PAPER_INDEX.as_posix(),
            accept_batch.MANUAL_BACKLOG.as_posix(),
        ])
        self.assertTrue((transaction_dir / "prepared.json").is_file())
        prepared = json.loads(
            (transaction_dir / "prepared.json").read_text(encoding="utf-8"))
        self.assertEqual(
            [entry["relative_path"] for entry in prepared["targets"]],
            accept_batch.transaction_target_paths("DL-TEST"),
        )
        for entry in prepared["targets"]:
            payload = (transaction_dir / entry["payload"]).read_bytes()
            self.assertEqual(len(payload), entry["size"])
            self.assertEqual(hashlib.sha256(payload).hexdigest(), entry["sha256"])
        self.assertTrue((self.root / accept_batch.PAPER_INDEX).is_file())
        self.assertTrue((self.root / accept_batch.MANUAL_BACKLOG).is_file())
        self.assertFalse((self.root / "03_review/DL-TEST-acceptance.md").exists())
        self.assertEqual(accept_batch.read_csv(index_path)[0]["status"], "REVIEW")
        self.assertEqual(
            json.loads((self.batch / "STATUS.json").read_text(encoding="utf-8"))["status"],
            "REVIEW",
        )

        # Recovery happens before the REVIEW/ACCEPTED consistency checks and
        # must not rerun the expensive validation path.
        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(accept_batch, "load_builder") as load_builder:
            second_code = accept_batch.main()

        self.assertEqual(second_code, 0)
        load_builder.assert_not_called()
        self.assertFalse(transaction_dir.exists())
        self.assertEqual(accept_batch.read_csv(index_path)[0]["status"], "ACCEPTED")
        self.assertEqual(
            json.loads((self.batch / "STATUS.json").read_text(encoding="utf-8"))["status"],
            "ACCEPTED",
        )
        paper_rows = accept_batch.read_csv(self.root / accept_batch.PAPER_INDEX)
        self.assertEqual([(row["source_batch_id"], row["source_item_id"])
                          for row in paper_rows], [("DL-TEST", "I-1")])
        self.assertTrue((self.root / "03_review/DL-TEST-acceptance.md").is_file())

    def test_other_batch_recovers_global_pending_before_building_its_snapshot(self) -> None:
        first_pdf = self.batch / "outputs/files/good.pdf"
        first_pdf.write_bytes(b"first fulltext")
        self._result(pdf_path=str(first_pdf))

        second_batch = self.root / "02_batches/DL-SECOND"
        (second_batch / "outputs/files").mkdir(parents=True)
        second_manifest = {field: "" for field in accept_batch.MANIFEST_FIELDS}
        second_manifest.update({
            "item_id": "I-2", "paper_id": "P-2", "title": "Second paper",
            "doi": "10.1234/second", "suggested_filename": "P-2.pdf",
        })
        self._write_csv(
            second_batch / "MANIFEST.csv", accept_batch.MANIFEST_FIELDS,
            [second_manifest],
        )
        second_pdf = second_batch / "outputs/files/good.pdf"
        second_pdf.write_bytes(b"second fulltext")
        second_result = {field: "" for field in accept_batch.DOWNLOAD_RESULT_FIELDS}
        second_result.update({
            "item_id": "I-2", "paper_id": "P-2", "status": "downloaded",
            "title": "Second paper", "doi": "10.1234/second",
            "pdf_path": str(second_pdf), "identity_verified": "true",
        })
        self._write_csv(
            second_batch / "outputs/download_results.csv",
            accept_batch.DOWNLOAD_RESULT_FIELDS, [second_result],
        )
        index_path = self.root / "00_control/BATCH_INDEX.csv"
        index_fields = ["batch_id", "status", "updated_at", "accepted_at"]
        self._write_csv(index_path, index_fields, [
            {"batch_id": "DL-TEST", "status": "REVIEW",
             "updated_at": "2026-10-13T00:00:00+00:00", "accepted_at": ""},
            {"batch_id": "DL-SECOND", "status": "REVIEW",
             "updated_at": "2026-10-13T00:00:00+00:00", "accepted_at": ""},
        ])
        for batch_id, batch_path in (("DL-TEST", self.batch),
                                     ("DL-SECOND", second_batch)):
            (batch_path / "STATUS.json").write_text(
                json.dumps({"batch_id": batch_id, "status": "REVIEW"}) + "\n",
                encoding="utf-8",
            )

        first_argv = [
            "accept_batch.py", "DL-TEST", "--actor-role", "coordinator",
            "--root", str(self.root),
        ]
        original_install = accept_batch.install_transaction_target
        install_count = 0

        def crash_first_batch(root: Path, batch_id: str,
                              entry: dict[str, object], payload: bytes) -> None:
            nonlocal install_count
            if install_count == 2:
                raise OSError("injected first-batch crash")
            original_install(root, batch_id, entry, payload)
            install_count += 1

        with mock.patch.object(sys, "argv", first_argv), \
                mock.patch.object(accept_batch, "load_builder", return_value=FakeBuilder()), \
                mock.patch.object(
                    accept_batch, "install_transaction_target",
                    side_effect=crash_first_batch):
            self.assertEqual(accept_batch.main(), 1)

        self.assertEqual(
            accept_batch.pending_transaction_batch_ids(self.root), ["DL-TEST"])

        second_argv = [
            "accept_batch.py", "DL-SECOND", "--actor-role", "coordinator",
            "--root", str(self.root),
        ]
        with mock.patch.object(sys, "argv", second_argv), \
                mock.patch.object(accept_batch, "load_builder", return_value=FakeBuilder()):
            self.assertEqual(accept_batch.main(), 0)

        self.assertEqual(accept_batch.pending_transaction_batch_ids(self.root), [])
        paper_rows = accept_batch.read_csv(self.root / accept_batch.PAPER_INDEX)
        self.assertEqual(
            {(row["source_batch_id"], row["source_item_id"]) for row in paper_rows},
            {("DL-TEST", "I-1"), ("DL-SECOND", "I-2")},
        )
        statuses = {
            row["batch_id"]: row["status"] for row in accept_batch.read_csv(index_path)
        }
        self.assertEqual(statuses, {"DL-TEST": "ACCEPTED", "DL-SECOND": "ACCEPTED"})

    def test_recovery_fails_closed_if_validated_fulltext_changes(self) -> None:
        pdf = self.batch / "outputs/files/good.pdf"
        pdf.write_bytes(b"original fulltext")
        self._result(pdf_path=str(pdf))
        index_path = self._prepare_batch_state()
        argv = [
            "accept_batch.py", "DL-TEST", "--actor-role", "coordinator",
            "--root", str(self.root),
        ]
        original_install = accept_batch.install_transaction_target
        install_count = 0

        def crash_after_two(root: Path, batch_id: str,
                            entry: dict[str, object], payload: bytes) -> None:
            nonlocal install_count
            if install_count == 2:
                raise OSError("injected crash")
            original_install(root, batch_id, entry, payload)
            install_count += 1

        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(accept_batch, "load_builder", return_value=FakeBuilder()), \
                mock.patch.object(
                    accept_batch, "install_transaction_target",
                    side_effect=crash_after_two):
            self.assertEqual(accept_batch.main(), 1)

        pdf.write_bytes(b"mutated fulltext")
        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(accept_batch, "load_builder") as load_builder:
            self.assertEqual(accept_batch.main(), 1)

        load_builder.assert_not_called()
        self.assertEqual(accept_batch.read_csv(index_path)[0]["status"], "REVIEW")
        self.assertEqual(
            json.loads((self.batch / "STATUS.json").read_text(encoding="utf-8"))["status"],
            "REVIEW",
        )
        self.assertEqual(
            accept_batch.pending_transaction_batch_ids(self.root), ["DL-TEST"])

    def test_path_traversal_batch_id_is_rejected_before_filesystem_access(self) -> None:
        for unsafe_id in ("../DL-TEST", "DL..TEST", "DL-TEST.committed"):
            with self.subTest(unsafe_id=unsafe_id):
                argv = [
                    "accept_batch.py", unsafe_id, "--actor-role", "coordinator",
                    "--root", str(self.root),
                ]
                with mock.patch.object(sys, "argv", argv), self.assertRaisesRegex(
                        SystemExit, "batch_id"):
                    accept_batch.main()

    def test_successful_gap_batch_is_fully_registered_in_manual_backlog(self) -> None:
        self._result(status="failed")
        index_path = self._prepare_batch_state()
        argv = [
            "accept_batch.py", "DL-TEST", "--actor-role", "coordinator",
            "--root", str(self.root),
        ]

        with mock.patch.object(sys, "argv", argv), \
                mock.patch.object(accept_batch, "load_builder", return_value=FakeBuilder()):
            code = accept_batch.main()

        self.assertEqual(code, 0)
        self.assertEqual(accept_batch.read_csv(index_path)[0]["status"], "ACCEPTED")
        backlog = accept_batch.read_csv(self.root / accept_batch.MANUAL_BACKLOG)
        self.assertEqual([(row["item_id"], row["status"]) for row in backlog],
                         [("I-1", "failed")])
        self.assertEqual(accept_batch.read_csv(self.root / accept_batch.PAPER_INDEX), [])


if __name__ == "__main__":
    unittest.main()
