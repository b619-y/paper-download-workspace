from __future__ import annotations

import hashlib
import json
import math
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import paperqa_luna as paperqa
from extraction_common import job_id_for


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def manifest(tmp_path: Path) -> Path:
    source = tmp_path / "paper.pdf"
    source.write_bytes(b"%PDF-1.4\nvalidated test source")
    markdown = tmp_path / "full.md"
    markdown.write_text("# Results\nSample A contained 5 mg/L target.\n", encoding="utf-8")
    sections = tmp_path / "section_chunks.jsonl"
    sections_sha, sections_count = paperqa.write_sections_jsonl(
        markdown, sections, paper_id="P1", source_file=source.name,
        source_format="pdf", source_sha256=digest(source), overlap=0,
    )
    path = tmp_path / "documents.json"
    path.write_text(json.dumps({"documents": [{
        "status": "accepted", "paper_id": "P1", "source_path": str(source),
        "source_sha256": digest(source), "parsed_text_path": str(markdown),
        "parsed_text_sha256": digest(markdown), "parser": "mineru",
        "parser_version": "test-v1", "source_kind": "pdf",
        "sections_path": str(sections), "sections_sha256": sections_sha,
        "sections_count": sections_count,
        "sections_schema_version": paperqa.SECTION_SCHEMA_VERSION,
    }]}), encoding="utf-8")
    return path


def strict_queue_manifest(tmp_path: Path, *, with_sections: bool = True) -> tuple[Path, str, Path | None]:
    source = tmp_path / "02_batches/B/outputs/paper.pdf"
    parsed = tmp_path / "06_mineru/by-sha256/aa/full.md"
    source.parent.mkdir(parents=True)
    parsed.parent.mkdir(parents=True)
    source.write_bytes(b"%PDF test")
    parsed.write_text("# Results\ntarget evidence", encoding="utf-8")
    now = "2026-10-14T00:00:00+00:00"
    document: dict[str, object] = {
        "task_id": "T", "batch_id": "B", "item_id": "I",
        "paper_id": "P", "title": "Paper", "doi": "",
        "status": "queued", "skip_reason": "",
        "source_kind": "pdf",
        "source_path": str(source.relative_to(tmp_path)),
        "source_sha256": digest(source),
        "source_revalidation": "verified_local",
        "parsed_text_path": str(parsed.relative_to(tmp_path)),
        "parsed_text_sha256": digest(parsed), "parser": "mineru",
        "parser_version": "v1", "spec_version": "v1",
        "spec_sha256": "b" * 64, "attempt_count": 0,
        "max_attempts": 3, "paperqa_question": "target?",
        "paperqa_question_sha256": paperqa.text_sha("target?"),
        "fulltext_decision": "include", "available_at": now,
        "claimed_by": "", "run_id": "", "attempt_id": "",
        "lease_token": "", "lease_expires_at": "", "heartbeat_at": "",
        "created_at": now, "updated_at": now, "error_code": "",
        "error_message": "", "receipt_history": [],
    }
    sidecar: Path | None = None
    if with_sections:
        sidecar = parsed.with_name("section_chunks.jsonl")
        section_sha, section_count = paperqa.write_sections_jsonl(
            parsed, sidecar, paper_id="P", source_file=source.name,
            source_format="pdf", source_sha256=digest(source), overlap=0,
        )
        document.update({
            "sections_path": str(sidecar.relative_to(tmp_path)),
            "sections_sha256": section_sha,
            "sections_count": section_count,
            "sections_schema_version": paperqa.SECTION_SCHEMA_VERSION,
        })
    else:
        document.update({
            "sections_path": "", "sections_sha256": "",
            "sections_count": 0, "sections_schema_version": "",
        })
    job_id = job_id_for("T", document, "b" * 64)
    document["job_id"] = job_id
    manifest_path = tmp_path / "runtime/tasks/T/extraction/data_extraction_queue.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps({
        "queue_version": "1.0", "task_id": "T", "spec_version": "v1",
        "spec_sha256": "b" * 64,
        "created_at": now, "updated_at": now,
        "jobs": [document],
    }), encoding="utf-8")
    return manifest_path, job_id, sidecar


def strict_staged_manifest(tmp_path: Path) -> Path:
    parsed_dir = tmp_path / "runtime/inputs/T/documents/D/parsed"
    parsed_dir.mkdir(parents=True)
    parsed_temporary = parsed_dir / "temporary.md"
    parsed_temporary.write_text(
        "# Results\ncandidate target evidence", encoding="utf-8"
    )
    parsed = parsed_dir / f"{digest(parsed_temporary)}.md"
    parsed_temporary.rename(parsed)
    sidecar_temporary = parsed_dir / "temporary.section_chunks.jsonl"
    sections_sha, sections_count = paperqa.write_sections_jsonl(
        parsed, sidecar_temporary, paper_id="P", source_file="paper.pdf",
        source_format="pdf", source_sha256="a" * 64, overlap=0,
    )
    sidecar = parsed_dir / f"{sections_sha}.section_chunks.jsonl"
    sidecar_temporary.rename(sidecar)
    parsed_ref = str(parsed.relative_to(tmp_path))
    sections_ref = str(sidecar.relative_to(tmp_path))
    source_ref = "02_batches/B/outputs/paper.pdf"
    manifest_path = parsed.parents[1] / "manifest.json"
    manifest_path.write_text(json.dumps({
        "schema_version": 3, "task_id": "T", "document_id": "D",
        "batch_id": "B", "source_item_id": "I", "item_id": "I",
        "paper_id": "P", "title": "Paper", "doi": "", "parser": "mineru",
        "parser_version": "v1", "status": "accepted",
        "source_sha256": "a" * 64, "parsed_text_sha256": digest(parsed),
        "source_kind": "pdf", "source_ref": source_ref,
        "source_staged_ref": None, "source_path": source_ref,
        "parsed_text_source_ref": "06_mineru/by-sha256/aa/full.md",
        "parsed_text_ref": parsed_ref, "parsed_text_path": parsed_ref,
        "section_chunks_source_ref": "06_mineru/by-sha256/aa/section_chunks.jsonl",
        "section_chunks_ref": sections_ref, "section_chunks_path": sections_ref,
        "section_chunks_sha256": sections_sha,
        "section_chunks_count": sections_count,
        "section_chunks_schema_version": paperqa.SECTION_SCHEMA_VERSION,
        "fulltext_copied": False, "source_bytes": 123,
        "parsed_text_bytes": parsed.stat().st_size,
        "section_chunks_bytes": sidecar.stat().st_size,
    }), encoding="utf-8")
    return manifest_path


def test_markdown_manifest_does_not_require_pdf_dependency(tmp_path: Path) -> None:
    records, bindings = paperqa.load_corpus(manifest(tmp_path))
    assert records and bindings[0]["paper_id"] == "P1"
    assert records[0]["parser"] == "mineru"
    assert "5 mg/L" in records[0]["text"]
    assert records[0]["canonical_section"] == "results"
    assert records[0]["original_heading"] == "Results"
    assert records[0]["heading_path"] == ["Results"]
    assert records[0]["section_chunk_id"] == "chunk-00001"
    assert bindings[0]["sections_origin"] == "accepted_sidecar"
    assert records[0]["text_sha256"] == hashlib.sha256(
        records[0]["text"].encode()
    ).hexdigest()


def test_markdown_manifest_rejects_stale_hash(tmp_path: Path) -> None:
    path = manifest(tmp_path)
    doc = json.loads(path.read_text())
    doc["documents"][0]["parsed_text_sha256"] = "0" * 64
    path.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(ValueError, match="stale"):
        paperqa.load_corpus(path)


def test_quote_verification_labels_candidate_locator(tmp_path: Path) -> None:
    candidates, _ = paperqa.load_corpus(manifest(tmp_path))
    valid = paperqa.verify({"answer": "yes", "decision": "include", "uncertainty": "", "evidence": [{"chunk_id": candidates[0]["chunk_id"], "source": "paper.pdf", "page": "", "exact_quote": "5 mg/L", "reason": "reported"}]}, candidates)
    assert valid["evidence_status"] == "evidence_found"
    assert valid["evidence"][0]["quote_verified"] is True
    assert valid["evidence"][0]["locator_status"] == "candidate_locator"
    missing = paperqa.verify({"answer": "no", "decision": "exclude", "uncertainty": "", "evidence": [{"chunk_id": candidates[0]["chunk_id"], "source": "paper.pdf", "page": "", "exact_quote": "fabricated", "reason": "none"}]}, candidates)
    assert missing["evidence_status"] == "no_evidence"
    assert missing["decision"] == "uncertain"
    assert "不表示" in missing["uncertainty"]
    blank = paperqa.verify({
        "answer": "yes", "decision": "include", "uncertainty": "",
        "evidence": [{
            "chunk_id": candidates[0]["chunk_id"], "source": "paper.pdf",
            "page": "", "exact_quote": " ", "reason": "blank",
        }],
    }, candidates)
    assert blank["evidence_status"] == "no_evidence"
    assert blank["decision"] == "uncertain"


def test_workspace_relative_queue_manifest_paths(tmp_path: Path) -> None:
    manifest_path, job_id, _ = strict_queue_manifest(tmp_path)
    records, bindings = paperqa.load_corpus(manifest_path, job_id, tmp_path)
    assert "target evidence" in records[0]["text"]
    assert bindings[0]["sections_origin"] == "accepted_sidecar"


def test_shallow_extraction_queue_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(json.dumps({
        "queue_version": "1.0", "task_id": "T", "spec_sha256": "b" * 64,
        "jobs": [],
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="queue keys mismatch"):
        paperqa.load_corpus(path)


def test_staged_manifest_allows_hash_only_authority_on_compute_host(tmp_path: Path) -> None:
    manifest_path = strict_staged_manifest(tmp_path)
    records, bindings = paperqa.load_corpus(manifest_path, workspace_root=tmp_path)
    assert records and bindings[0]["paper_id"] == "P"
    assert bindings[0]["parser"] == "mineru"
    assert bindings[0]["source_revalidation"] == "deferred_to_mac"
    assert bindings[0]["sections_origin"] == "accepted_sidecar"


def test_queue_rejects_tampered_or_missing_section_sidecar(tmp_path: Path) -> None:
    manifest_path, job_id, sidecar = strict_queue_manifest(tmp_path)
    assert sidecar is not None
    sidecar.write_text(sidecar.read_text(encoding="utf-8") + "{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="stale section sidecar hash"):
        paperqa.load_corpus(manifest_path, job_id, tmp_path)

    missing_root = tmp_path / "missing"
    missing_manifest, missing_job_id, _ = strict_queue_manifest(
        missing_root, with_sections=False
    )
    with pytest.raises(ValueError, match="lacks section sidecar"):
        paperqa.load_corpus(missing_manifest, missing_job_id, missing_root)


def test_partial_staged_v1_manifest_is_rejected(tmp_path: Path) -> None:
    parsed = tmp_path / "full.md"
    parsed.write_text("candidate", encoding="utf-8")
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({
        "schema_version": 1, "task_id": "T", "document_id": "D",
        "source_ref": "paper.pdf", "parsed_text_ref": "full.md",
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="staged manifest keys mismatch"):
        paperqa.load_corpus(path, workspace_root=tmp_path)


def test_codex_runs_in_isolated_read_only_temporary_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object) -> object:
        cwd = Path(kwargs["cwd"])
        observed["command"] = command
        observed["cwd_files"] = sorted(path.name for path in cwd.iterdir())
        observed["prompt"] = kwargs["input"]
        answer = Path(command[command.index("--output-last-message") + 1])
        answer.write_text(json.dumps({
            "answer": "", "decision": "uncertain", "uncertainty": "insufficient",
            "evidence": [],
        }), encoding="utf-8")
        return object()

    monkeypatch.setattr(paperqa.subprocess, "run", fake_run)
    result = paperqa.ask_luna("target?", [], "/usr/bin/codex", "gpt-test")

    assert result["decision"] == "uncertain"
    assert observed["cwd_files"] == ["schema.json"]
    assert "--sandbox" in observed["command"]
    sandbox_index = observed["command"].index("--sandbox")
    assert observed["command"][sandbox_index + 1] == "read-only"
    assert "不可信数据" in observed["prompt"]
    assert "禁止访问工作目录外" in observed["prompt"]


def test_embedding_requires_separate_authorization_and_loopback_by_default() -> None:
    with pytest.raises(ValueError, match="explicit authorization"):
        paperqa.embedding_vectors(["secret paper text"], "http://127.0.0.1:18091/v1", "m")
    assert paperqa._embedding_endpoint("http://localhost:18091/v1", False).endswith("/embeddings")
    assert paperqa._embedding_endpoint("http://[::1]:18091/v1", False).endswith("/embeddings")
    with pytest.raises(ValueError, match="remote embedding"):
        paperqa._embedding_endpoint("https://embedding.example/v1", False)
    assert paperqa._embedding_endpoint("https://embedding.example/v1", True).endswith("/embeddings")


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"data": [{"index": 0, "embedding": [1.0]}]}, "count mismatch"),
        ({"data": [
            {"index": 0, "embedding": [1.0]},
            {"index": 0, "embedding": [2.0]},
        ]}, "unique and contiguous"),
        ({"data": [
            {"index": 0, "embedding": [1.0]},
            {"index": 1, "embedding": [2.0, 3.0]},
        ]}, "dimensions"),
        ({"data": [
            {"index": 0, "embedding": [1.0]},
            {"index": 1, "embedding": [math.nan]},
        ]}, "finite"),
    ],
)
def test_embedding_response_contract_is_strict(payload: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        paperqa._validate_embedding_response(payload, 2)


def test_direct_pdf_persists_truthful_parsed_text_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "paper.pdf"
    source.write_bytes(b"%PDF-1.4\ntest")

    def fake_parser(path: Path, parse_media: bool = False) -> object:
        assert path == source.resolve()
        assert parse_media is False
        return type("Parsed", (), {"content": {2: "Second page.", 1: "# Results\n5 mg/L."}})()

    fake_parser.parser_version = "test-1.2.3"
    monkeypatch.setattr(paperqa, "parse_pdf_to_pages", fake_parser)
    records, bindings = paperqa.load_corpus(
        source, parsed_output_dir=tmp_path / "derived"
    )
    binding = bindings[0]
    parsed_path = Path(binding["parsed_text_path"])

    assert parsed_path.is_file()
    assert parsed_path != source.resolve()
    assert binding["source_sha256"] == digest(source)
    assert binding["parsed_text_sha256"] == digest(parsed_path)
    assert binding["parsed_text_sha256"] != binding["source_sha256"]
    assert binding["parser_version"] == "test-1.2.3"
    assert binding["sections_origin"] == "direct_pdf_rebuilt"
    assert Path(binding["sections_path"]).is_file()
    assert binding["sections_sha256"] == digest(Path(binding["sections_path"]))
    assert binding["sections_count"] >= 1
    assert "5 mg/L" in parsed_path.read_text(encoding="utf-8")
    assert records[0]["canonical_section"] == "results"


def test_marker_contract_rejects_candidate_text_hash_drift(tmp_path: Path) -> None:
    candidates, bindings = paperqa.load_corpus(manifest(tmp_path))
    question = "Does it report target data?"
    candidates = paperqa.bm25(question, candidates, 12)
    payload = {
        "schema_version": "1.0", "mode": "retrieval_only", "completed": False,
        "job_id": "J", "question": question,
        "question_sha256": paperqa.text_sha(question),
        "spec_version": "v1", "spec_sha256": "c" * 64,
        "parser": [bindings[0]["parser"]], "retriever": "local-bm25",
        "embedding_base_url": None, "embedding_error": None, "model": None,
        "source_bindings": bindings, "candidate_count": len(candidates),
        "candidates": candidates,
        "result": {"answer": "", "decision": "uncertain",
                   "uncertainty": "retrieval only", "evidence": [],
                   "rejected_evidence": [],
                   "evidence_status": "retrieval_only"},
        "limitations": ["candidate locator only"],
    }
    paperqa.validate_marker_payload(payload)
    payload["candidates"][0]["text"] += " altered"
    with pytest.raises(ValueError, match="text hash mismatch"):
        paperqa.validate_marker_payload(payload)


def test_marker_contract_binds_each_verified_evidence_to_candidate(tmp_path: Path) -> None:
    candidates, bindings = paperqa.load_corpus(manifest(tmp_path))
    question = "Does it report target data?"
    candidates = paperqa.bm25(question, candidates, 12)
    result = paperqa.verify({
        "answer": "yes", "decision": "include", "uncertainty": "",
        "evidence": [{
            "chunk_id": candidates[0]["chunk_id"], "source": "untrusted",
            "page": "untrusted", "exact_quote": "5 mg/L", "reason": "reported value",
        }],
    }, candidates)
    payload = {
        "schema_version": "1.0", "mode": "judged", "completed": True,
        "job_id": "J", "question": question,
        "question_sha256": paperqa.text_sha(question),
        "spec_version": "v1", "spec_sha256": "d" * 64,
        "parser": [bindings[0]["parser"]], "retriever": "local-bm25",
        "embedding_base_url": None, "embedding_error": None, "model": "gpt-test",
        "source_bindings": bindings, "candidate_count": len(candidates),
        "candidates": candidates, "result": result,
        "limitations": ["candidate locator only"],
    }
    expected_job = {
        "job_id": "J", "paper_id": bindings[0]["paper_id"],
        "source_sha256": bindings[0]["source_sha256"],
        "source_revalidation": bindings[0]["source_revalidation"],
        "source_kind": bindings[0]["source_format"],
        "parsed_text_sha256": bindings[0]["parsed_text_sha256"],
        "parser": bindings[0]["parser"], "parser_version": bindings[0]["parser_version"],
        "sections_path": bindings[0]["sections_path"],
        "sections_sha256": bindings[0]["sections_sha256"],
        "sections_count": bindings[0]["sections_count"],
        "sections_schema_version": bindings[0]["sections_schema_version"],
        "spec_version": "v1", "spec_sha256": "d" * 64,
        "paperqa_question": question,
        "paperqa_question_sha256": paperqa.text_sha(question),
    }
    paperqa.validate_paperqa_marker(payload, expected_job=expected_job)
    payload["result"]["evidence"][0]["char_start"] += 1
    with pytest.raises(ValueError, match="differs from its candidate"):
        paperqa.validate_marker_payload(payload)
