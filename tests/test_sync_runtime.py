from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/sync_runtime.sh"


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def snapshot_entry(value: bytes) -> dict[str, object]:
    return {"sha256": sha256(value), "size": len(value)}


class SyncRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.runtime = self.base / "runtime"
        self.runtime.mkdir()
        self.bin = self.base / "bin"
        self.bin.mkdir()
        self.ssh_log = self.base / "ssh.log"
        self.rsync_log = self.base / "rsync.json"
        self._write_fake_commands()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_fake_commands(self) -> None:
        ssh = self.bin / "ssh"
        ssh.write_text(
            """#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
log = Path(os.environ['FAKE_SSH_LOG'])
with log.open('a', encoding='utf-8') as handle:
    handle.write(json.dumps(sys.argv[1:]) + '\\n')
if os.environ.get('FAKE_SSH_EXEC') == '1':
    os.execvp(sys.argv[2], sys.argv[2:])
if 'python3' in sys.argv:
    sys.stdin.read()
    fallback = ({'exists': True, 'files': {}, 'workflow_state': None}
                if 'push_workflow' in sys.argv else {'exists': True, 'files': {}})
    print(os.environ.get('FAKE_REMOTE_SNAPSHOT', json.dumps(fallback)))
""",
            encoding="utf-8",
        )
        ssh.chmod(0o755)

        rsync = self.bin / "rsync"
        rsync.write_text(
            """#!/usr/bin/env python3
import json, os, shutil, sys
from pathlib import Path
args = sys.argv[1:]
list_args = [item for item in args if item.startswith('--files-from=')]
paths = (Path(list_args[0].split('=', 1)[1]).read_text(encoding='utf-8').splitlines()
         if list_args else [])
Path(os.environ['FAKE_RSYNC_LOG']).write_text(json.dumps({'args': args, 'paths': paths}), encoding='utf-8')
if '--dry-run' not in args and ':' in args[-2]:
    destination = Path(args[-1])
    content = json.loads(os.environ.get('FAKE_REMOTE_CONTENT_JSON', '{}'))
    for relative in paths:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if os.environ.get('FAKE_RSYNC_SYMLINK') == '1':
            target.symlink_to('/tmp/fake-unsafe-target')
        else:
            target.write_bytes(content[relative].encode('utf-8'))
elif '--dry-run' not in args and ':' in args[-1]:
    source = Path(args[-2])
    destination = Path(args[-1].split(':', 1)[1])
    if paths:
        for relative in paths:
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source / relative, target)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
""",
            encoding="utf-8",
        )
        rsync.chmod(0o755)

    def env(self, **updates: str) -> dict[str, str]:
        value = dict(os.environ)
        value.update(
            {
                "PATH": f"{self.bin}{os.pathsep}{value.get('PATH', '')}",
                "PAPER_SYNC_MAC_ROOT": str(self.runtime),
                "PAPER_SYNC_REMOTE_HOST": "worker-test",
                "PAPER_SYNC_REMOTE_ROOT": "/srv/paper-runtime",
                "FAKE_SSH_LOG": str(self.ssh_log),
                "FAKE_RSYNC_LOG": str(self.rsync_log),
            }
        )
        value.update(updates)
        return value

    def run_script(
        self, *arguments: str, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(SCRIPT), *arguments],
            cwd=ROOT,
            env=env or self.env(),
            text=True,
            capture_output=True,
        )

    def make_staged_document(self, task_id: str = "T", document_id: str = "D-1") -> Path:
        document = self.runtime / f"inputs/{task_id}/documents/{document_id}"
        parsed_bytes = b"# Parsed\n\nEvidence.\n"
        sections_bytes = b'{"schema_version":"1","text":"Evidence."}\n'
        parsed_sha = sha256(parsed_bytes)
        sections_sha = sha256(sections_bytes)
        parsed_relative = f"parsed/{parsed_sha}.md"
        sections_relative = f"parsed/{sections_sha}.section_chunks.jsonl"
        (document / "parsed").mkdir(parents=True)
        (document / parsed_relative).write_bytes(parsed_bytes)
        (document / sections_relative).write_bytes(sections_bytes)
        prefix = f"runtime/inputs/{task_id}/documents/{document_id}"
        manifest = {
            "schema_version": 3,
            "status": "accepted",
            "task_id": task_id,
            "document_id": document_id,
            "source_sha256": "a" * 64,
            "source_bytes": 100,
            "source_staged_ref": None,
            "source_path": "02_batches/example/source.pdf",
            "fulltext_copied": False,
            "parsed_text_ref": f"{prefix}/{parsed_relative}",
            "parsed_text_path": f"{prefix}/{parsed_relative}",
            "parsed_text_sha256": parsed_sha,
            "parsed_text_bytes": len(parsed_bytes),
            "section_chunks_ref": f"{prefix}/{sections_relative}",
            "section_chunks_path": f"{prefix}/{sections_relative}",
            "section_chunks_sha256": sections_sha,
            "section_chunks_count": 1,
            "section_chunks_schema_version": "1",
            "section_chunks_bytes": len(sections_bytes),
        }
        (document / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return document

    def test_legacy_modes_are_disabled_before_any_remote_access(self) -> None:
        env = self.env()
        env.pop("PAPER_SYNC_REMOTE_HOST")
        env.pop("PAPER_SYNC_REMOTE_ROOT")
        result = self.run_script("push-inputs", "--apply", env=env)
        self.assertEqual(result.returncode, 6)
        self.assertIn("已禁用", result.stderr)
        self.assertFalse(self.ssh_log.exists())

    def test_remote_coordinates_are_mandatory(self) -> None:
        env = self.env()
        env.pop("PAPER_SYNC_REMOTE_HOST")
        result = self.run_script("push-workflow", env=env)
        self.assertEqual(result.returncode, 2)
        self.assertIn("PAPER_SYNC_REMOTE_HOST", result.stderr)

    def test_workflow_uses_allowlist_and_excludes_runtime_data(self) -> None:
        (self.runtime / "tasks/T/query").mkdir(parents=True)
        (self.runtime / "tasks/T/extraction").mkdir(parents=True)
        (self.runtime / "inputs/private").mkdir(parents=True)
        (self.runtime / "ACTIVE_TASK.json").write_text('{"task_id":"T"}\n')
        (self.runtime / "tasks/T/query/plan.json").write_text("{}\n")
        (self.runtime / "tasks/T/query/secret.pdf").write_bytes(b"pdf")
        (self.runtime / "tasks/T/query/queue.Sqlite3").write_bytes(b"db")
        (self.runtime / "tasks/T/query/active.lock").write_text("lock")
        (self.runtime / "tasks/T/extraction/data_extraction_queue.json").write_text("{}\n")
        (self.runtime / "tasks/T/extraction/data_extraction_queue.csv").write_text("job_id\n")
        (self.runtime / "inputs/private/not-for-workflow.txt").write_text("private")

        result = self.run_script("push-workflow")
        self.assertEqual(result.returncode, 0, result.stderr)
        invocation = json.loads(self.rsync_log.read_text())
        self.assertEqual(invocation["paths"], ["ACTIVE_TASK.json", "tasks/T/query/plan.json"])
        self.assertIn("--ignore-existing", invocation["args"])
        self.assertIn("--no-links", invocation["args"])
        self.assertNotIn("--backup", invocation["args"])

    def test_workflow_refuses_unmanaged_remote_hash_conflict(self) -> None:
        payload = b'{"task_id":"T"}\n'
        (self.runtime / "tasks/T").mkdir(parents=True)
        (self.runtime / "ACTIVE_TASK.json").write_bytes(payload)
        (self.runtime / "tasks/T/task_brief.json").write_text("{}\n")
        remote = {
            "exists": True,
            "files": {"ACTIVE_TASK.json": {"sha256": "0" * 64, "size": len(payload)}},
            "workflow_state": None,
        }
        result = self.run_script(
            "push-workflow", env=self.env(FAKE_REMOTE_SNAPSHOT=json.dumps(remote))
        )
        self.assertEqual(result.returncode, 3)
        self.assertIn("content conflict", result.stderr)
        self.assertFalse(self.rsync_log.exists())

    def test_workflow_allows_update_when_remote_matches_cas_state(self) -> None:
        old = b'{"task_id":"OLD"}\n'
        new = b'{"task_id":"T"}\n'
        (self.runtime / "tasks/T").mkdir(parents=True)
        (self.runtime / "tasks/T/task_brief.json").write_text("{}\n")
        (self.runtime / "ACTIVE_TASK.json").write_bytes(new)
        remote = {
            "exists": True,
            "files": {"ACTIVE_TASK.json": snapshot_entry(old)},
            "workflow_state": {
                "schema_version": "paper-workflow-sync-state/v1",
                "files": {"ACTIVE_TASK.json": snapshot_entry(old)},
            },
        }
        result = self.run_script(
            "push-workflow", env=self.env(FAKE_REMOTE_SNAPSHOT=json.dumps(remote))
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_workflow_apply_is_repeatable_and_publishes_active_pointer_last(self) -> None:
        remote_root = self.base / "remote-runtime"
        (self.runtime / "tasks/T/query").mkdir(parents=True)
        (self.runtime / "tasks/T/extraction").mkdir(parents=True)
        active = self.runtime / "ACTIVE_TASK.json"
        plan = self.runtime / "tasks/T/query/plan.json"
        queue = self.runtime / "tasks/T/extraction/data_extraction_queue.json"
        active.write_text('{"task_id":"T","generation":1}\n')
        plan.write_text('{"generation":1}\n')
        queue.write_text('{"status":"queued"}\n')
        env = self.env(
            FAKE_SSH_EXEC="1",
            PAPER_SYNC_REMOTE_ROOT=str(remote_root),
        )

        first = self.run_script("push-workflow", "--apply", env=env)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual((remote_root / "ACTIVE_TASK.json").read_text(), active.read_text())
        self.assertFalse((remote_root / "tasks/T/extraction/data_extraction_queue.json").exists())

        active.write_text('{"task_id":"T","generation":2}\n')
        plan.write_text('{"generation":2}\n')
        second = self.run_script("push-workflow", "--apply", env=env)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual((remote_root / "ACTIVE_TASK.json").read_text(), active.read_text())
        self.assertEqual((remote_root / "tasks/T/query/plan.json").read_text(), plan.read_text())
        state = json.loads((remote_root / ".paper-sync/workflow-state.json").read_text())
        self.assertEqual(state["schema_version"], "paper-workflow-sync-state/v1")

    def test_workflow_rejects_disguised_sqlite_content(self) -> None:
        (self.runtime / "tasks/T/query").mkdir(parents=True)
        (self.runtime / "ACTIVE_TASK.json").write_text('{"task_id":"T"}\n')
        (self.runtime / "tasks/T/query/result.bin").write_bytes(
            b"SQLite format 3\x00" + b"\x00" * 32
        )
        result = self.run_script("push-workflow")
        self.assertEqual(result.returncode, 3)
        self.assertIn("SQLite content", result.stderr)

    def test_processing_pdf_requires_separate_fulltext_policy(self) -> None:
        document = self.runtime / "inputs/T/documents/D-1"
        document.mkdir(parents=True)
        (document / "manifest.json").write_text("{}\n")
        (document / "source.pdf").write_bytes(b"%PDF-1.7\n")
        result = self.run_script(
            "push-processing-inputs", "T", "--apply",
            env=self.env(PAPER_PROCESSING_TRANSFER_POLICY="allow-remote-worker"),
        )
        self.assertEqual(result.returncode, 4)
        self.assertIn("PAPER_FULLTEXT_TRANSFER_POLICY", result.stderr)
        self.assertFalse(self.ssh_log.exists())

    def test_processing_manifest_rejects_undeclared_file(self) -> None:
        document = self.make_staged_document()
        (document / "credentials.txt").write_text("not declared\n")
        result = self.run_script("push-processing-inputs", "T")
        self.assertEqual(result.returncode, 3)
        self.assertIn("undeclared files", result.stderr)
        self.assertFalse(self.rsync_log.exists())

    def test_processing_manifest_is_the_exact_transfer_allowlist(self) -> None:
        document = self.make_staged_document()
        result = self.run_script("push-processing-inputs", "T")
        self.assertEqual(result.returncode, 0, result.stderr)
        invocation = json.loads(self.rsync_log.read_text())
        self.assertEqual(
            invocation["paths"],
            sorted([
                "D-1/manifest.json",
                "D-1/parsed/" + next(document.joinpath("parsed").glob("*.md")).name,
                "D-1/parsed/" + next(document.joinpath("parsed").glob("*.jsonl")).name,
            ]),
        )

    def test_pull_publishes_only_hash_verified_attempt_and_receipt(self) -> None:
        attempt = "job-1-attempt-0001-0123456789abcdef0123456789abcdef"
        receipt_relative = f"data_extraction/job-1/receipts/{attempt}.json"
        result_relative = f"data_extraction/job-1/attempts/{attempt}/result.json"
        receipt_content = '{"receipt_version":"2.0"}\n'
        result_content = '{"records":[]}\n'
        remote = {
            "exists": True,
            "files": {
                receipt_relative: snapshot_entry(receipt_content.encode()),
                result_relative: snapshot_entry(result_content.encode()),
            },
        }
        env = self.env(
            PAPER_PROCESSING_TRANSFER_POLICY="allow-remote-worker",
            FAKE_REMOTE_SNAPSHOT=json.dumps(remote),
            FAKE_REMOTE_CONTENT_JSON=json.dumps(
                {receipt_relative: receipt_content, result_relative: result_content}
            ),
        )
        result = self.run_script("pull-processing-results", "T", "--apply", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.runtime / "results/T" / receipt_relative).read_text(), receipt_content)
        self.assertEqual((self.runtime / "results/T" / result_relative).read_text(), result_content)
        receipts = list(
            (self.runtime / "sync_receipts/v1/pull-processing-results/T").glob("*.json")
        )
        self.assertEqual(len(receipts), 1)
        self.assertEqual(
            json.loads(receipts[0].read_text())["schema_version"],
            "paper-runtime-sync-receipt/v1",
        )

    def test_pull_rejects_symlink_after_transfer_without_publishing_it(self) -> None:
        relative = "paperqa/job-1.json"
        content = "{}\n"
        remote = {"exists": True, "files": {relative: snapshot_entry(content.encode())}}
        env = self.env(
            PAPER_PROCESSING_TRANSFER_POLICY="allow-remote-worker",
            FAKE_REMOTE_SNAPSHOT=json.dumps(remote),
            FAKE_REMOTE_CONTENT_JSON=json.dumps({relative: content}),
            FAKE_RSYNC_SYMLINK="1",
        )
        result = self.run_script("pull-processing-results", "T", "--apply", env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("link/special", result.stderr)
        self.assertFalse((self.runtime / "results/T" / relative).exists())

    def test_pull_refuses_mutable_legacy_completion_receipt(self) -> None:
        relative = "data_extraction/job-1/completion.json"
        remote = {"exists": True, "files": {relative: {"sha256": "0" * 64, "size": 1}}}
        result = self.run_script(
            "pull-processing-results", "T",
            env=self.env(FAKE_REMOTE_SNAPSHOT=json.dumps(remote)),
        )
        self.assertEqual(result.returncode, 3)
        self.assertIn("forbidden path", result.stderr)

    def test_pull_refuses_worker_canonical_extraction_result(self) -> None:
        relative = "data_extraction/job-1/result.json"
        remote = {"exists": True, "files": {relative: {"sha256": "0" * 64, "size": 1}}}
        result = self.run_script(
            "pull-processing-results", "T",
            env=self.env(FAKE_REMOTE_SNAPSHOT=json.dumps(remote)),
        )
        self.assertEqual(result.returncode, 3)
        self.assertIn("forbidden path", result.stderr)

    def test_pull_refuses_result_over_size_cap_before_rsync(self) -> None:
        relative = "paperqa/job-1.json"
        remote = {"exists": True, "files": {relative: {"sha256": "0" * 64, "size": 11}}}
        result = self.run_script(
            "pull-processing-results", "T",
            env=self.env(
                FAKE_REMOTE_SNAPSHOT=json.dumps(remote),
                PAPER_SYNC_MAX_RESULT_FILE_BYTES="10",
            ),
        )
        self.assertEqual(result.returncode, 3)
        self.assertIn("per-file byte limit", result.stderr)
        self.assertFalse(self.rsync_log.exists())

    def test_pull_rejects_disguised_sqlite_after_transfer(self) -> None:
        relative = "paperqa/job-1.json"
        content = "SQLite format 3\x00payload"
        remote = {"exists": True, "files": {relative: snapshot_entry(content.encode())}}
        result = self.run_script(
            "pull-processing-results", "T", "--apply",
            env=self.env(
                PAPER_PROCESSING_TRANSFER_POLICY="allow-remote-worker",
                FAKE_REMOTE_SNAPSHOT=json.dumps(remote),
                FAKE_REMOTE_CONTENT_JSON=json.dumps({relative: content}),
            ),
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("SQLite content", result.stderr)
        self.assertFalse((self.runtime / "results/T" / relative).exists())


if __name__ == "__main__":
    unittest.main()
