from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import paper_section_chunking as sections


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_structure_routing_retains_original_heading_path_and_special_content() -> None:
    text = (
        "# Introduction\nContext.\n"
        "## A journal-specific section\nKeep this nonstandard content.\n"
        "# Results\n## Influence of pH on Cr(VI)\nObserved values.\n"
        "## Table 1\n| pH | value |\n|---|---|\n| 7 | 5 |\n"
        "## Figure 2\n![plot](figure2.png)\n"
    )
    chunks = sections.chunk_markdown(text, limit=1000, overlap=100)
    by_heading = {chunk["original_heading"]: chunk for chunk in chunks}

    assert by_heading["A journal-specific section"]["canonical_section"] == "other"
    assert by_heading["A journal-specific section"]["heading_path"] == [
        "Introduction", "A journal-specific section"
    ]
    assert by_heading["Influence of pH on Cr(VI)"]["canonical_section"] == "results"
    assert by_heading["Table 1"]["canonical_section"] == "table"
    assert by_heading["Table 1"]["table_or_figure"] == "table"
    assert by_heading["Figure 2"]["canonical_section"] == "figure"
    assert by_heading["Figure 2"]["table_or_figure"] == "figure"


def test_chunks_bind_exact_complete_decoded_content() -> None:
    text = "\r\n# Results\r\n" + ("value 5 mg/L. " * 160) + "\r\n"
    chunks = sections.chunk_markdown(text, limit=1000, overlap=125)

    assert chunks[0]["char_start"] == 0
    assert max(chunk["char_end"] for chunk in chunks) == len(text)
    for chunk in chunks:
        exact = text[chunk["char_start"]:chunk["char_end"]]
        assert chunk["text"] == exact
        assert chunk["text_sha256"] == hashlib.sha256(exact.encode()).hexdigest()
    sections.verify_chunks(text, chunks, allow_overlap=True)


def test_chunk_validation_rejects_reordered_ranges_and_forged_routing() -> None:
    text = "# Results\n" + ("reported concentration. " * 150)
    chunks = sections.chunk_markdown(text, limit=1000, overlap=100)
    assert len(chunks) > 1

    forged = json.loads(json.dumps(chunks))
    forged[0]["canonical_section"] = "other"
    with pytest.raises(ValueError, match="differs from Markdown structure"):
        sections.verify_chunks(text, forged, allow_overlap=True)

    reordered = json.loads(json.dumps(chunks))
    moving_keys = {
        "char_start", "char_end", "text", "text_sha256", "sha256",
        "table_or_figure",
    }
    left = {key: reordered[0][key] for key in moving_keys}
    for key in moving_keys:
        reordered[0][key] = reordered[1][key]
        reordered[1][key] = left[key]
    with pytest.raises(ValueError, match="ranges must follow|omit Markdown"):
        sections.verify_chunks(text, reordered, allow_overlap=True)


def test_chunk_limits_match_extraction_contract_bounds() -> None:
    with pytest.raises(ValueError, match="between 1000"):
        sections.chunk_markdown("text", limit=100_001, overlap=0)
    with pytest.raises(ValueError, match="overlap"):
        sections.chunk_markdown("text", limit=30_000, overlap=20_001)
    with pytest.raises(ValueError, match="overlap"):
        sections.chunk_markdown("text", limit=1000, overlap=501)


def test_jsonl_round_trip_and_tamper_detection(tmp_path: Path) -> None:
    markdown = tmp_path / "full.md"
    markdown.write_bytes(
        b"\r\n# Results\r\n" + (b"A = 5 mg/L. " * 220) + b"\r\n"
    )
    output = tmp_path / "sections.jsonl"
    source_sha = "a" * 64

    _, count = sections.write_sections_jsonl(
        markdown, output, paper_id="P-1", source_file="paper.pdf",
        source_format="pdf-mineru", source_sha256=source_sha,
        limit=1000, overlap=0,
    )
    records = sections.validate_sections_jsonl(
        output, markdown, source_sha256=source_sha,
        parsed_text_sha256=sha(markdown), expected_count=count,
    )
    assert records[0]["parsed_char_count"] == len(markdown.read_bytes().decode("utf-8"))
    position = 0
    for record in records:
        assert record["char_start"] == position
        position = record["char_end"]
    assert position == records[0]["parsed_char_count"]

    records[0]["text"] += "tampered"
    output.write_text("\n".join(json.dumps(row) for row in records) + "\n")
    with pytest.raises(ValueError, match="exact Markdown substring"):
        sections.validate_sections_jsonl(
            output, markdown, source_sha256=source_sha,
            parsed_text_sha256=sha(markdown), expected_count=count,
        )


def test_split_cli_only_structures_accepted_markdown(tmp_path: Path) -> None:
    markdown = tmp_path / "full.md"
    markdown.write_text("# Methods\nCollected ten soil samples.\n", encoding="utf-8")
    output = tmp_path / "chunks.jsonl"
    command = [
        sys.executable, str(SCRIPTS / "split_paper_sections.py"),
        "--input", str(markdown), "--output", str(output),
        "--paper-id", "P-2", "--source-file", "accepted/P-2.pdf",
        "--source-format", "pdf-mineru", "--source-sha256", "b" * 64,
        "--parsed-text-sha256", sha(markdown),
    ]
    completed = subprocess.run(command, text=True, capture_output=True, check=True)
    summary = json.loads(completed.stdout)
    record = json.loads(output.read_text(encoding="utf-8").splitlines()[0])

    assert summary["record_count"] == 1
    assert record["canonical_section"] == "materials_methods"
    assert record["source_file"] == "accepted/P-2.pdf"
    assert record["parsed_text_sha256"] == sha(markdown)
