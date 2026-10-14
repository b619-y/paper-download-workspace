from __future__ import annotations

import csv
import io
import json
import os
import sys
import tempfile
import time
import unittest
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import accept_document_processing as acceptance
import document_processing_worker as worker
import fulltext_parsers as parsers
import make_document_processing_queue as maker
import processing_queue as queue


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def zip_bytes(markdown: str, *, unsafe: bool = False) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("../escape.md" if unsafe else "result/full.md", markdown)
        archive.writestr("result/images/figure.png", b"png")
    return stream.getvalue()


class FakeTransport:
    def __init__(
        self, archive: bytes, *, remote_state: str = "done",
        poll_results: list[object] | None = None, poll_status: int = 200,
    ) -> None:
        self.archive = archive
        self.remote_state = remote_state
        self.posts: list[dict] = []
        self.uploads = 0
        self.polls = 0
        self.poll_results = poll_results
        self.poll_status = poll_status

    def post_json(self, url, headers, payload, timeout):
        self.posts.append(payload)
        return 200, {"code": 0, "data": {"batch_id": "remote-1", "file_urls": ["https://upload.invalid/one"]}}, ""

    def get_json(self, url, headers, timeout):
        self.polls += 1
        if self.poll_status != 200:
            return self.poll_status, {}, "temporary failure"
        if self.poll_results is not None:
            return 200, {"code": 0, "data": {"extract_result": self.poll_results}}, ""
        result = {"data_id": "dp-test", "state": self.remote_state}
        if self.remote_state == "done":
            result["full_zip_url"] = "https://download.invalid/result.zip"
        return 200, {"code": 0, "data": {"extract_result": [result]}}, ""

    def upload_file(self, url, path, timeout):
        self.uploads += 1
        return 200

    def download_file(self, url, path, timeout):
        path.write_bytes(self.archive)
        return 200


class QueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / "queue.sqlite3"
        self.db = queue.connect(self.db_path)
        queue.initialize(self.db)
        self.job = {
            "job_id": "dp-one", "batch_id": "DL-1", "source_item_id": "I-1",
            "paper_id": "unsafe/paper", "document_id": "sha256-" + "a" * 64,
            "title": "One", "doi": "", "source_kind": "xml", "source_path": "source.xml",
            "source_sha256": "a" * 64, "processor": "xml_to_markdown",
            "processor_version": "xml-local-v1", "output_dir": "out", "max_attempts": 3,
        }
        queue.insert_job(self.db, self.job)

    def tearDown(self) -> None:
        self.db.close()
        self.temp.cleanup()

    def test_ids_are_deterministic_and_path_safe(self) -> None:
        doc = queue.make_document_id("https://doi.org/10.1000/A/B", "a" * 64)
        self.assertRegex(doc, r"^doi-[0-9a-f]{24}$")
        self.assertEqual(doc, queue.make_document_id("doi:10.1000/a/b", "b" * 64))
        first = queue.make_job_id("DL/1", "../../item", "mineru")
        self.assertEqual(first, queue.make_job_id("DL/1", "../../item", "mineru"))
        self.assertRegex(first, r"^dp-[0-9a-f]{32}$")

    def test_begin_immediate_claim_is_exclusive(self) -> None:
        other = queue.connect(self.db_path)
        try:
            claimed = queue.claim_one(self.db, "worker-a", lease_seconds=30)
            self.assertIsNotNone(claimed)
            self.assertIsNone(queue.claim_one(other, "worker-b", lease_seconds=30))
            self.assertEqual(claimed["attempts"], 1)
            events = self.db.execute("SELECT event_type FROM events ORDER BY event_id").fetchall()
            self.assertEqual([row[0] for row in events], ["enqueued", "claimed"])
        finally:
            other.close()

    def test_expired_lease_is_recovered_and_stale_worker_rejected(self) -> None:
        start = datetime.now(timezone.utc).replace(microsecond=0)
        first = queue.claim_one(self.db, "worker-a", lease_seconds=30, now=start.isoformat())
        later = (start + timedelta(seconds=31)).isoformat()
        second = queue.claim_one(self.db, "worker-b", lease_seconds=30, now=later)
        self.assertIsNotNone(second)
        self.assertEqual(second["attempts"], 2)
        self.assertNotEqual(first["lease_token"], second["lease_token"])
        with self.assertRaisesRegex(RuntimeError, "stale"):
            queue.mark_running(self.db, first["job_id"], first["lease_token"], "worker-a")
        event_types = [row[0] for row in self.db.execute("SELECT event_type FROM events")]
        self.assertIn("lease_expired", event_types)

    def test_lease_recovery_compares_offset_timestamps_as_instants(self) -> None:
        first = queue.claim_one(
            self.db, "worker-a", lease_seconds=30,
            now="2030-01-01T08:00:00+08:00",
        )
        self.assertEqual(first["lease_expires_at"], "2030-01-01T00:00:30+00:00")

        second = queue.claim_one(
            self.db, "worker-b", lease_seconds=30,
            now="2030-01-01T00:00:30Z",
        )

        self.assertIsNotNone(second)
        self.assertEqual(second["attempts"], 2)
        self.assertNotEqual(first["lease_token"], second["lease_token"])

    def test_available_at_compares_offset_timestamps_as_instants(self) -> None:
        self.db.execute(
            "UPDATE jobs SET available_at=? WHERE job_id=?",
            ("2026-01-01T08:00:30+08:00", self.job["job_id"]),
        )

        self.assertIsNone(queue.claim_one(
            self.db, "worker", lease_seconds=30,
            now="2026-01-01T00:00:29+00:00",
        ))
        claimed = queue.claim_one(
            self.db, "worker", lease_seconds=30,
            now="2026-01-01T00:00:30Z",
        )

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["attempts"], 1)

    def test_remote_poll_does_not_consume_a_new_attempt(self) -> None:
        claimed = queue.claim_one(self.db, "worker", lease_seconds=30)
        queue.mark_running(self.db, claimed["job_id"], claimed["lease_token"], "worker")
        queue.mark_waiting_remote(
            self.db, claimed["job_id"], claimed["lease_token"], "worker", poll_after_seconds=1)
        far_future = (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(timespec="seconds")
        again = queue.claim_one(self.db, "worker", lease_seconds=30, now=far_future)
        self.assertEqual(again["attempts"], 1)

    def test_expired_lease_cannot_heartbeat_or_transition(self) -> None:
        claimed = queue.claim_one(self.db, "worker", lease_seconds=30)
        self.db.execute(
            "UPDATE jobs SET lease_expires_at=? WHERE job_id=?",
            ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(), claimed["job_id"]),
        )
        events_before = self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        with self.assertRaisesRegex(RuntimeError, "stale"):
            queue.heartbeat(
                self.db, claimed["job_id"], claimed["lease_token"], "worker",
                lease_seconds=30,
            )
        with self.assertRaisesRegex(RuntimeError, "stale"):
            queue.mark_running(self.db, claimed["job_id"], claimed["lease_token"], "worker")
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0], events_before)

    def test_expired_remote_poll_lease_preserves_attempt_budget(self) -> None:
        start = datetime.now(timezone.utc).replace(microsecond=0)
        claimed = queue.claim_one(
            self.db, "worker-a", lease_seconds=30, now=start.isoformat())
        queue.mark_running(self.db, claimed["job_id"], claimed["lease_token"], "worker-a")
        queue.mark_waiting_remote(
            self.db, claimed["job_id"], claimed["lease_token"], "worker-a",
            poll_after_seconds=1,
        )
        polling = queue.claim_one(
            self.db, "worker-a", lease_seconds=30,
            now=(start + timedelta(seconds=2)).isoformat(),
        )
        self.assertEqual(polling["claim_origin"], "waiting_remote")
        recovered = queue.claim_one(
            self.db, "worker-b", lease_seconds=30,
            now=(start + timedelta(seconds=33)).isoformat(),
        )
        self.assertEqual(recovered["attempts"], 1)

    def test_explicit_retry_is_audited_and_grants_one_attempt(self) -> None:
        claimed = queue.claim_one(self.db, "worker", lease_seconds=30)
        queue.mark_running(self.db, claimed["job_id"], claimed["lease_token"], "worker")
        self.db.execute(
            "UPDATE jobs SET attempts=max_attempts WHERE job_id=?", (claimed["job_id"],))
        queue.mark_failure(
            self.db, claimed["job_id"], claimed["lease_token"], "worker",
            code="broken", message="diagnosed", retryable=False,
        )
        retried = queue.retry_job(
            self.db, claimed["job_id"], "coordinator", "fixed credentials",
            policy="resume",
        )
        self.assertEqual(retried["status"], "queued")
        self.assertGreater(retried["max_attempts"], retried["attempts"])
        event = self.db.execute(
            "SELECT event_type,details_json FROM events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(event["event_type"], "explicit_retry")
        self.assertIn("fixed credentials", event["details_json"])

    def test_fenced_publish_rejects_replaced_lease_before_rename(self) -> None:
        claimed = queue.claim_one(self.db, "worker-a", lease_seconds=30)
        heartbeat = worker.LeaseHeartbeat(
            self.db_path, claimed["job_id"], claimed["lease_token"], "worker-a", 30,
        )
        temporary = self.root / "artifact.tmp"
        destination = self.root / "artifact.json"
        temporary.write_text("stale", encoding="utf-8")
        destination.write_text("current", encoding="utf-8")
        self.db.execute(
            "UPDATE jobs SET lease_token=?,claimed_by=? WHERE job_id=?",
            ("replacement-token", "worker-b", claimed["job_id"]),
        )
        with self.assertRaisesRegex(RuntimeError, "stale"):
            heartbeat.publish(temporary, destination)
        self.assertEqual(destination.read_text(encoding="utf-8"), "current")
        self.assertEqual(temporary.read_text(encoding="utf-8"), "stale")


class WorkerRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.batch_id = "DL-remote"
        self.source = self.root / "paper.pdf"
        self.source.write_bytes(b"%PDF-1.7\nremote poll checkpoint\n%%EOF")
        self.db_path = queue.database_path(self.root, self.batch_id)
        database = queue.connect(self.db_path)
        try:
            queue.initialize(database)
            queue.insert_job(database, {
                "job_id": "dp-remote", "batch_id": self.batch_id,
                "source_item_id": "I-remote", "paper_id": "P-remote",
                "document_id": "sha256-" + queue.file_sha256(self.source),
                "title": "Remote", "doi": "", "source_kind": "pdf",
                "source_path": queue.portable_path(self.root, self.source),
                "source_sha256": queue.file_sha256(self.source),
                "processor": "mineru", "processor_version": "mineru-test-v1",
                "output_dir": queue.portable_path(self.root, self.root / "output"),
                "max_attempts": 3,
            })
            claimed = queue.claim_one(database, "submitter", lease_seconds=30)
            queue.mark_running(
                database, claimed["job_id"], claimed["lease_token"], "submitter")
            queue.mark_waiting_remote(
                database, claimed["job_id"], claimed["lease_token"], "submitter",
                poll_after_seconds=1,
            )
            database.execute(
                "UPDATE jobs SET available_at=? WHERE job_id=?",
                ("2000-01-01T00:00:00Z", claimed["job_id"]),
            )
        finally:
            database.close()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_poll_worker_exception_preserves_waiting_state_and_attempt_budget(self) -> None:
        with mock.patch.object(
            worker.parsers, "mineru_step", side_effect=RuntimeError("temporary poll crash"),
        ):
            report = worker.run_worker(
                self.root, self.batch_id, worker_id="poll-worker",
                lease_seconds=30, max_jobs=1,
            )

        database = queue.connect(self.db_path)
        try:
            row = queue.rows(database)[0]
            self.assertEqual(row["status"], "waiting_remote")
            self.assertEqual(row["attempts"], 1)
            self.assertEqual(row["error_code"], "worker_exception")
            next_claim = queue.claim_one(
                database, "poll-worker-2", lease_seconds=30,
                now="2100-01-01T00:00:00+00:00",
            )
            self.assertEqual(next_claim["attempts"], 1)
            self.assertEqual(next_claim["claim_origin"], "waiting_remote")
        finally:
            database.close()
        self.assertEqual(report["outcomes"], {"worker_exception": 1})

    def test_retry_policy_must_match_remote_failure(self) -> None:
        base = {"processor": "mineru", "error_code": "archive_url_expired"}
        with self.assertRaisesRegex(ValueError, "requires --retry-policy refresh-archive"):
            worker.validate_retry_policy(base, "resume", upload_authorized=False)
        worker.validate_retry_policy(base, "refresh-archive", upload_authorized=False)
        unknown = {"processor": "mineru", "error_code": "submit_outcome_unknown"}
        with self.assertRaisesRegex(ValueError, "external-upload-authorized"):
            worker.validate_retry_policy(unknown, "resubmit", upload_authorized=False)
        worker.validate_retry_policy(unknown, "resubmit", upload_authorized=True)


class ParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.pdf = self.root / "paper.pdf"
        self.pdf.write_bytes(b"%PDF-1.7\n" + b"x" * 2000 + b"\n%%EOF")
        self.digest = queue.file_sha256(self.pdf)
        self.output = self.root / "out"
        self.markdown = "# Parsed paper\n\n" + ("Substantive content from the accepted source. " * 20)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_mineru_checkpoint_resumes_without_duplicate_submission(self) -> None:
        transport = FakeTransport(zip_bytes(self.markdown))
        with mock.patch.dict(os.environ, {"MINERU_API_TOKEN": "top-secret-value"}, clear=False), \
                mock.patch.object(parsers, "best_effort_pdf_pages", return_value=10):
            first = parsers.mineru_step(
                source=self.pdf, expected_sha256=self.digest, output_dir=self.output,
                job_id="dp-test", document_id="doi-abc", processor_version="mineru-v1",
                external_upload_authorized=True, transport=transport,
                model_version="vlm", enable_table=True, enable_formula=False,
            )
            checkpoint = json.loads((self.output / "state.json").read_text())
            self.assertEqual(first.status, "waiting_remote")
            self.assertEqual(checkpoint["remote_batch_id"], "remote-1")
            self.assertNotIn("top-secret-value", (self.output / "state.json").read_text())
            second = parsers.mineru_step(
                source=self.pdf, expected_sha256=self.digest, output_dir=self.output,
                job_id="dp-test", document_id="doi-abc", processor_version="mineru-v1",
                external_upload_authorized=False, transport=transport,
                model_version="vlm", enable_table=True, enable_formula=False,
            )
        self.assertEqual(second.status, "review")
        self.assertEqual(len(transport.posts), 1)
        self.assertEqual(transport.uploads, 1)
        self.assertEqual(transport.posts[0]["model_version"], "vlm")
        self.assertIs(transport.posts[0]["enable_table"], True)
        self.assertIs(transport.posts[0]["enable_formula"], False)
        self.assertTrue((self.output / "mineru_result.zip").is_file())
        self.assertFalse((self.output / "mineru_result.zip.part").exists())
        result = json.loads((self.output / "result.json").read_text())
        self.assertEqual(result["job_id"], "dp-test")
        self.assertEqual(result["source_sha256"], self.digest)
        self.assertEqual(result["parsed_text_sha256"], queue.file_sha256(self.output / "full.md"))
        self.assertEqual(result["sections_path"], "section_chunks.jsonl")
        self.assertTrue((self.output / "section_chunks.jsonl").is_file())
        self.assertEqual(result["remote_data_id"], "dp-test")

    def test_mineru_corrupt_checkpoint_fails_closed_without_post(self) -> None:
        self.output.mkdir()
        (self.output / "state.json").write_text("{not-json", encoding="utf-8")
        transport = FakeTransport(zip_bytes(self.markdown))
        with mock.patch.object(parsers, "best_effort_pdf_pages", return_value=10):
            outcome = parsers.mineru_step(
                source=self.pdf, expected_sha256=self.digest, output_dir=self.output,
                job_id="dp-test", document_id="doi-abc", processor_version="mineru-v1",
                external_upload_authorized=True, transport=transport,
            )
        self.assertEqual((outcome.status, outcome.code), ("blocked", "checkpoint_invalid"))
        self.assertEqual(transport.posts, [])

    def test_mineru_remote_batch_without_data_id_fails_closed(self) -> None:
        self.output.mkdir()
        (self.output / "state.json").write_text(json.dumps({
            "status": "submitted", "source_sha256": self.digest,
            "job_id": "dp-test", "document_id": "doi-abc",
            "remote_batch_id": "remote-1",
        }), encoding="utf-8")
        transport = FakeTransport(zip_bytes(self.markdown))
        with mock.patch.object(parsers, "best_effort_pdf_pages", return_value=10):
            outcome = parsers.mineru_step(
                source=self.pdf, expected_sha256=self.digest, output_dir=self.output,
                job_id="dp-test", document_id="doi-abc", processor_version="mineru-v1",
                external_upload_authorized=True, transport=transport,
            )
        self.assertEqual(
            (outcome.status, outcome.code), ("blocked", "remote_result_identity_mismatch"))
        self.assertEqual(transport.posts, [])
        self.assertEqual(transport.polls, 0)

    def test_unknown_submit_outcome_never_automatically_posts_again(self) -> None:
        class TimeoutAfterAccept(FakeTransport):
            def post_json(self, url, headers, payload, timeout):
                self.posts.append(payload)
                raise TimeoutError("response lost after server accepted request")

        first_transport = TimeoutAfterAccept(zip_bytes(self.markdown))
        with mock.patch.dict(os.environ, {"MINERU_API_TOKEN": "secret"}, clear=False), \
                mock.patch.object(parsers, "best_effort_pdf_pages", return_value=10):
            first = parsers.mineru_step(
                source=self.pdf, expected_sha256=self.digest, output_dir=self.output,
                job_id="dp-test", document_id="doi-abc", processor_version="mineru-v1",
                external_upload_authorized=True, transport=first_transport,
            )
            second_transport = FakeTransport(zip_bytes(self.markdown))
            second = parsers.mineru_step(
                source=self.pdf, expected_sha256=self.digest, output_dir=self.output,
                job_id="dp-test", document_id="doi-abc", processor_version="mineru-v1",
                external_upload_authorized=True, transport=second_transport,
            )
        self.assertEqual((first.status, first.code), ("blocked", "submit_outcome_unknown"))
        self.assertEqual((second.status, second.code), ("blocked", "submit_outcome_unknown"))
        self.assertEqual(len(first_transport.posts), 1)
        self.assertEqual(second_transport.posts, [])
        state = json.loads((self.output / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "submit_outcome_unknown")

    def test_mineru_poll_fails_closed_on_ambiguous_or_mismatched_results(self) -> None:
        cases = (
            [{"data_id": "other", "state": "done", "full_zip_url": "https://bad"}],
            [{"data_id": "dp-test", "state": "done", "full_zip_url": "https://one"},
             {"data_id": "dp-test", "state": "done", "full_zip_url": "https://two"}],
            [{"data_id": "dp-test", "state": "done", "full_zip_url": "https://one"},
             {"data_id": "other", "state": "done", "full_zip_url": "https://two"}],
            ["not-an-object"],
        )
        for results in cases:
            with self.subTest(results=results):
                output = self.root / ("case-" + str(cases.index(results)))
                output.mkdir()
                (output / "state.json").write_text(json.dumps({
                    "status": "submitted", "source_sha256": self.digest,
                    "job_id": "dp-test", "document_id": "doi-abc",
                    "remote_batch_id": "remote-1", "remote_data_id": "dp-test",
                }), encoding="utf-8")
                transport = FakeTransport(zip_bytes(self.markdown), poll_results=results)
                with mock.patch.dict(os.environ, {"MINERU_API_TOKEN": "secret"}, clear=False), \
                        mock.patch.object(parsers, "best_effort_pdf_pages", return_value=10):
                    outcome = parsers.mineru_step(
                        source=self.pdf, expected_sha256=self.digest, output_dir=output,
                        job_id="dp-test", document_id="doi-abc", processor_version="mineru-v1",
                        external_upload_authorized=False, transport=transport,
                    )
                self.assertEqual(outcome.status, "blocked")
                self.assertEqual(outcome.code, "remote_result_identity_mismatch")
                self.assertFalse((output / "full.md").exists())

    def test_transient_poll_failure_stays_remote_without_consuming_attempt(self) -> None:
        self.output.mkdir()
        (self.output / "state.json").write_text(json.dumps({
            "status": "submitted", "source_sha256": self.digest,
            "job_id": "dp-test", "document_id": "doi-abc",
            "remote_batch_id": "remote-1", "remote_data_id": "dp-test",
        }), encoding="utf-8")
        transport = FakeTransport(zip_bytes(self.markdown), poll_status=503)
        with mock.patch.dict(os.environ, {"MINERU_API_TOKEN": "secret"}, clear=False), \
                mock.patch.object(parsers, "best_effort_pdf_pages", return_value=10):
            outcome = parsers.mineru_step(
                source=self.pdf, expected_sha256=self.digest, output_dir=self.output,
                job_id="dp-test", document_id="doi-abc", processor_version="mineru-v1",
                external_upload_authorized=False, transport=transport,
            )
        self.assertEqual(outcome.status, "waiting_remote")
        self.assertEqual(outcome.code, "poll_failed")

    def test_external_upload_requires_explicit_authorization(self) -> None:
        transport = FakeTransport(zip_bytes(self.markdown))
        with mock.patch.dict(os.environ, {"MINERU_API_TOKEN": "secret"}, clear=False), \
                mock.patch.object(parsers, "best_effort_pdf_pages", return_value=10):
            result = parsers.mineru_step(
                source=self.pdf, expected_sha256=self.digest, output_dir=self.output,
                job_id="dp-test", document_id="sha256-abc", processor_version="mineru-v1",
                external_upload_authorized=False, transport=transport,
            )
        self.assertEqual(result.status, "blocked")
        self.assertEqual(result.code, "external_upload_not_authorized")
        self.assertEqual(transport.posts, [])

    def test_best_effort_page_gate_blocks_before_http(self) -> None:
        transport = FakeTransport(zip_bytes(self.markdown))
        with mock.patch.object(parsers, "best_effort_pdf_pages", return_value=201):
            result = parsers.mineru_step(
                source=self.pdf, expected_sha256=self.digest, output_dir=self.output,
                job_id="dp-test", document_id="sha256-abc", processor_version="mineru-v1",
                external_upload_authorized=True, transport=transport,
            )
        self.assertEqual(result.status, "blocked")
        self.assertIn("200 pages", result.message)
        self.assertEqual(transport.posts, [])

    def test_zip_traversal_is_rejected(self) -> None:
        archive = self.root / "bad.zip"
        archive.write_bytes(zip_bytes(self.markdown, unsafe=True))
        with self.assertRaisesRegex(RuntimeError, "unsafe path"):
            parsers.safe_extract_zip(archive, self.root / "extract")
        self.assertFalse((self.root / "escape.md").exists())

    def test_zip_member_count_size_and_ratio_limits_are_enforced(self) -> None:
        archive = self.root / "bounded.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zipped:
            zipped.writestr("a.txt", b"A" * 4096)
            zipped.writestr("b.txt", b"B")
        with mock.patch.object(parsers, "MAX_ARCHIVE_MEMBERS", 1):
            with self.assertRaisesRegex(RuntimeError, "members"):
                parsers.safe_extract_zip(archive, self.root / "members")
        with mock.patch.object(parsers, "MAX_ARCHIVE_UNCOMPRESSED_BYTES", 100):
            with self.assertRaisesRegex(RuntimeError, "uncompressed size"):
                parsers.safe_extract_zip(archive, self.root / "total")
        with mock.patch.object(parsers, "MAX_ARCHIVE_COMPRESSION_RATIO", 2):
            with self.assertRaisesRegex(RuntimeError, "compression ratio"):
                parsers.safe_extract_zip(archive, self.root / "ratio")

    def test_downloaded_archive_has_a_compressed_size_cap_for_custom_transports(self) -> None:
        self.output.mkdir()
        (self.output / "state.json").write_text(json.dumps({
            "status": "remote_done", "source_sha256": self.digest,
            "job_id": "dp-test", "document_id": "doi-abc",
            "remote_batch_id": "remote-1", "remote_data_id": "dp-test",
            "remote_zip_url": "https://download.invalid/result.zip",
        }), encoding="utf-8")
        transport = FakeTransport(b"x" * 100)
        with mock.patch.dict(os.environ, {"MINERU_API_TOKEN": "secret"}, clear=False), \
                mock.patch.object(parsers, "best_effort_pdf_pages", return_value=10), \
                mock.patch.object(parsers, "MAX_ARCHIVE_COMPRESSED_BYTES", 10):
            outcome = parsers.mineru_step(
                source=self.pdf, expected_sha256=self.digest, output_dir=self.output,
                job_id="dp-test", document_id="doi-abc", processor_version="mineru-v1",
                external_upload_authorized=False, transport=transport,
            )
        self.assertEqual(outcome.status, "failed")
        self.assertEqual(outcome.code, "archive_too_large")
        self.assertFalse(any(self.output.glob("*.part-*")))

    def test_xml_converter_labels_parser_and_preserves_hash(self) -> None:
        source = self.root / "paper.xml"
        source.write_text(
            "<article><front><article-title>Measured study</article-title></front>"
            "<body><sec><title>Methods</title><p>" + ("Observed source text. " * 30)
            + "</p><fig><label>Figure 1</label><caption>Observed relation.</caption></fig>"
              "</sec></body></article>", encoding="utf-8")
        digest = queue.file_sha256(source)
        result = parsers.xml_to_markdown_step(
            source=source, expected_sha256=digest, output_dir=self.output,
            job_id="dp-xml", document_id="doi-xml", processor_version="xml-v1")
        self.assertEqual(result.status, "review")
        markdown = (self.output / "full.md").read_text()
        self.assertIn("parser: xml_to_markdown", markdown)
        self.assertIn(digest, markdown)
        state = json.loads((self.output / "state.json").read_text())
        self.assertEqual(state["source_sha256"], digest)
        self.assertEqual(state["job_id"], "dp-xml")
        self.assertEqual(state["sections_path"], "section_chunks.jsonl")
        self.assertTrue((self.output / "section_chunks.jsonl").is_file())


class EndToEndTests(unittest.TestCase):
    MANIFEST_FIELDS = [
        "item_id", "paper_id", "title", "authors", "year", "journal", "doi",
        "landing_url", "priority", "download_reason", "requested_files",
        "target_filename", "suggested_filename", "source_record", "local_existing_path",
        "queue_action", "existing_batch_id", "notes",
    ]
    RESULT_FIELDS = [
        "item_id", "paper_id", "status", "title", "doi", "source_url", "pdf_path",
        "supplement_paths", "sha256", "file_bytes", "page_count", "identity_verified",
        "downloaded_at", "notes",
    ]
    INDEX_FIELDS = [
        "paper_id", "title", "doi", "pdf_path", "supplement_paths", "sha256",
        "source_batch_id", "identity_verified", "accepted_at", "notes",
        "mineru_md_path", "xml_md_path", "source_item_id", "document_id", "source_kind",
        "processed_text_sha256", "processing_method", "processing_version",
        "processing_accepted_at", "section_chunks_path", "section_chunks_sha256",
        "section_chunks_count", "section_chunks_schema_version",
    ]

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.batch_id = "DL-TEST"
        self.batch = self.root / "02_batches" / self.batch_id
        (self.batch / "outputs/files").mkdir(parents=True)
        self.source = self.batch / "outputs/files/paper.xml"
        self.source.write_text(
            "<article><front><article-title>Target paper</article-title></front><body><sec>"
            "<title>Results</title><p>" + ("Measured result in source XML. " * 30)
            + "</p></sec></body></article>", encoding="utf-8")
        self.digest = queue.file_sha256(self.source)
        (self.batch / "STATUS.json").write_text(
            json.dumps({"batch_id": self.batch_id, "status": "ACCEPTED"}), encoding="utf-8")
        manifest = {field: "" for field in self.MANIFEST_FIELDS}
        manifest.update({
            "item_id": "I-1", "paper_id": "unsafe/../../paper", "title": "Target paper",
            "doi": "10.1000/test", "priority": "2",
        })
        write_csv(self.batch / "MANIFEST.csv", self.MANIFEST_FIELDS, [manifest])
        result = {field: "" for field in self.RESULT_FIELDS}
        result.update({
            "item_id": "I-1", "paper_id": "unsafe/../../paper", "status": "downloaded",
            "title": "Target paper", "doi": "10.1000/test", "pdf_path": str(self.source),
            "sha256": self.digest, "identity_verified": "true", "notes": "[XML_FULL]",
        })
        write_csv(self.batch / "outputs/download_results.csv", self.RESULT_FIELDS, [result])
        self._index()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _index(self, *, include_source_item: bool = True) -> None:
        fields = list(self.INDEX_FIELDS)
        if not include_source_item:
            fields.remove("source_item_id")
        row = {field: "" for field in fields}
        row.update({
            "paper_id": "unsafe/../../paper", "title": "Target paper", "doi": "10.1000/test",
            "pdf_path": str(self.source), "sha256": self.digest,
            "source_batch_id": self.batch_id, "identity_verified": "true",
        })
        if include_source_item:
            row["source_item_id"] = "I-1"
        write_csv(self.root / "04_library_index/PAPER_INDEX.csv", fields, [row])

    def _install_review_artifacts(self, markdown: bytes) -> dict:
        maker.build_queue(self.root, self.batch_id)
        db_path = queue.database_path(self.root, self.batch_id)
        db = queue.connect(db_path)
        try:
            job = queue.rows(db)[0]
            output = queue.resolve_stored_path(self.root, job["output_dir"])
            output.mkdir(parents=True, exist_ok=True)
            parsed = output / "full.md"
            parsed.write_bytes(markdown)
            parsed_sha = queue.file_sha256(parsed)
            sections = output / "section_chunks.jsonl"
            sections_sha, sections_count = parsers.section_chunking.write_sections_jsonl(
                parsed,
                sections,
                paper_id=job["paper_id"],
                source_file=self.source.name,
                source_format="xml",
                source_sha256=self.digest,
            )
            state_path = output / "state.json"
            state = {
                "status": "done", "job_id": job["job_id"],
                "document_id": job["document_id"], "paper_id": job["paper_id"],
                "source_file": self.source.name, "source_format": "xml",
                "source_sha256": self.digest, "processor": job["processor"],
                "processor_version": job["processor_version"],
                "parsed_text_path": "full.md", "parsed_text_sha256": parsed_sha,
                "sections_path": "section_chunks.jsonl",
                "sections_sha256": sections_sha, "sections_count": sections_count,
            }
            parsers.atomic_json(state_path, state)
            state_sha = queue.file_sha256(state_path)
            result_path = output / "result.json"
            parsers.atomic_json(result_path, {
                **state, "status": "succeeded", "state_path": "state.json",
                "state_sha256": state_sha,
            })
            result_sha = queue.file_sha256(result_path)
            db.execute(
                """UPDATE jobs SET status='review', parsed_text_path=?, parsed_text_sha256=?,
                          sections_path=?, sections_sha256=?, sections_count=?,
                          state_path=?, state_sha256=?, result_path=?, result_sha256=?
                     WHERE job_id=?""",
                (
                    queue.portable_path(self.root, parsed), parsed_sha,
                    queue.portable_path(self.root, sections), sections_sha, sections_count,
                    queue.portable_path(self.root, state_path), state_sha,
                    queue.portable_path(self.root, result_path), result_sha, job["job_id"],
                ),
            )
            queue.export_projection(db, queue.projection_path(self.root, self.batch_id))
            return {**job, "parsed": parsed, "sections": sections,
                    "state": state_path, "result": result_path}
        finally:
            db.close()

    def test_queue_worker_and_acceptance_backfill_exact_source_item(self) -> None:
        report = maker.build_queue(self.root, self.batch_id)
        self.assertEqual(report["jobs"], 1)
        db = queue.connect(queue.database_path(self.root, self.batch_id))
        job = queue.rows(db)[0]
        db.close()
        self.assertEqual(job["processor"], "xml_to_markdown")
        self.assertNotIn("unsafe", job["output_dir"])
        worker_report = worker.run_worker(
            self.root, self.batch_id, worker_id="test-worker", max_jobs=1)
        self.assertEqual(worker_report["status_counts"]["review"], 1)
        accepted = acceptance.accept(self.root, self.batch_id, actor="test-coordinator")
        self.assertEqual(accepted["accepted"], 1)
        with (self.root / "04_library_index/PAPER_INDEX.csv").open(
                encoding="utf-8-sig", newline="") as handle:
            row = next(csv.DictReader(handle))
        self.assertEqual(row["source_item_id"], "I-1")
        self.assertEqual(row["document_id"], job["document_id"])
        self.assertEqual(row["source_kind"], "xml")
        self.assertEqual(row["processing_method"], "xml_to_markdown")
        self.assertTrue(row["xml_md_path"].endswith("/full.md"))
        self.assertEqual(row["processed_text_sha256"], queue.file_sha256(
            queue.resolve_stored_path(self.root, row["xml_md_path"])))
        self.assertTrue(row["section_chunks_path"].endswith("/section_chunks.jsonl"))
        self.assertEqual(
            row["section_chunks_sha256"],
            queue.file_sha256(queue.resolve_stored_path(self.root, row["section_chunks_path"])),
        )
        self.assertGreater(int(row["section_chunks_count"]), 0)
        self.assertEqual(
            row["section_chunks_schema_version"],
            parsers.section_chunking.SECTION_SCHEMA_VERSION,
        )

    def test_acceptance_preserves_crlf_offsets_in_section_chunks(self) -> None:
        self._install_review_artifacts(
            b"# Results\r\n\r\n" + (b"Measured 5 mg/L in source. " * 20) + b"\r\n")
        report = acceptance.accept(self.root, self.batch_id, actor="coordinator")
        self.assertEqual(report["accepted"], 1)

    def test_acceptance_rejects_noncanonical_section_routing_even_with_rehashed_files(self) -> None:
        artifacts = self._install_review_artifacts(
            b"# Results\n\n" + (b"Measured 5 mg/L in source. " * 20) + b"\n")
        records = parsers.section_chunking.read_sections_jsonl(artifacts["sections"])
        records[0]["canonical_section"] = "discussion"
        artifacts["sections"].write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in records),
            encoding="utf-8",
        )
        sections_sha = queue.file_sha256(artifacts["sections"])
        state = json.loads(artifacts["state"].read_text())
        state["sections_sha256"] = sections_sha
        parsers.atomic_json(artifacts["state"], state)
        state_sha = queue.file_sha256(artifacts["state"])
        result = json.loads(artifacts["result"].read_text())
        result["sections_sha256"] = sections_sha
        result["state_sha256"] = state_sha
        parsers.atomic_json(artifacts["result"], result)
        db = queue.connect(queue.database_path(self.root, self.batch_id))
        try:
            db.execute(
                """UPDATE jobs SET sections_sha256=?,state_sha256=?,result_sha256=?
                     WHERE job_id=?""",
                (
                    sections_sha, state_sha, queue.file_sha256(artifacts["result"]),
                    artifacts["job_id"],
                ),
            )
        finally:
            db.close()
        with self.assertRaisesRegex(
            ValueError,
            "canonical deterministic projection|differs from Markdown structure",
        ):
            acceptance.accept(self.root, self.batch_id, actor="coordinator")

    def test_acceptance_refuses_index_without_source_item_id(self) -> None:
        maker.build_queue(self.root, self.batch_id)
        worker.run_worker(self.root, self.batch_id, worker_id="test-worker", max_jobs=1)
        self._index(include_source_item=False)
        with self.assertRaisesRegex(ValueError, "no source_item_id"):
            acceptance.accept(self.root, self.batch_id, actor="coordinator")

    def test_queue_creation_rejects_changed_source_hash(self) -> None:
        self.source.write_text("changed", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            maker.build_queue(self.root, self.batch_id)

    def test_queue_creation_rejects_unaccepted_batch(self) -> None:
        (self.batch / "STATUS.json").write_text(
            json.dumps({"batch_id": self.batch_id, "status": "REVIEW"}), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "not ACCEPTED"):
            maker.build_queue(self.root, self.batch_id)

    def test_existing_queue_rejects_changed_max_attempts_policy(self) -> None:
        maker.build_queue(self.root, self.batch_id, max_attempts=3)
        with self.assertRaisesRegex(RuntimeError, "max_attempts"):
            maker.build_queue(self.root, self.batch_id, max_attempts=4)

    def test_worker_heartbeats_during_slow_parser_io(self) -> None:
        maker.build_queue(self.root, self.batch_id)
        original = worker.LeaseHeartbeat

        class FastHeartbeat(original):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs, interval_seconds=0.01)

        def slow_parser(**kwargs):
            time.sleep(0.05)
            kwargs["lease_guard"]()
            return parsers.ParserOutcome("blocked", "test_stop", "finished slow I/O")

        with mock.patch.object(worker, "LeaseHeartbeat", FastHeartbeat), \
                mock.patch.object(parsers, "xml_to_markdown_step", side_effect=slow_parser):
            report = worker.run_worker(
                self.root, self.batch_id, worker_id="slow-worker",
                lease_seconds=30, max_jobs=1,
            )
        self.assertEqual(report["status_counts"]["blocked"], 1)
        db = queue.connect(queue.database_path(self.root, self.batch_id))
        try:
            heartbeat_events = db.execute(
                "SELECT COUNT(*) FROM events WHERE event_type='heartbeat'"
            ).fetchone()[0]
        finally:
            db.close()
        self.assertGreaterEqual(heartbeat_events, 2)


if __name__ == "__main__":
    unittest.main()
