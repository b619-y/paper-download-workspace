#!/usr/bin/env python3
"""Deterministic, structure-aware chunks for parsed research-paper Markdown.

The original PDF/XML remains authoritative.  These records are derived
locators: every chunk is an exact substring of the accepted ``full.md`` and
retains its character offsets, original heading path, and a coarse routing
label.  Unknown headings are retained as ``other`` rather than discarded.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any


SECTION_SCHEMA_VERSION = "1.0"
MAX_CHUNK_CHARS = 100_000
MAX_CHUNK_OVERLAP = 20_000
HEADING_RE = re.compile(r"(?m)^(#{1,6})[ \t]+(.+?)[ \t]*\r?$")
CHUNK_KEYS = {
    "chunk_id", "section_index", "part_index", "char_start", "char_end",
    "canonical_section", "original_heading", "heading_path", "table_or_figure",
    "text", "text_sha256", "sha256",
}
SECTION_RECORD_KEYS = CHUNK_KEYS | {
    "schema_version", "paper_id", "source_file", "source_format",
    "source_sha256", "parsed_text_sha256", "parsed_char_count",
}


def json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def text_sha256(value: str) -> str:
    """Hash the exact UTF-8 text represented by character offsets."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def classify_heading(heading: str) -> str:
    """Return a routing label without treating it as an inclusion decision."""
    value = " ".join(str(heading or "").casefold().replace("&", " and ").split())
    rules = (
        ("abstract", ("abstract", "summary", "摘要", "概要")),
        ("introduction", ("introduction", "background", "rationale", "引言", "背景")),
        ("materials_methods", (
            "material", "method", "experimental", "sampling", "sample collection",
            "chemical analysis", "analytical", "材料", "方法", "实验", "采样", "样品",
        )),
        ("results", (
            "result", "finding", "observation", "concentration", "distribution",
            "influence of", "effect of", "determination", "结果", "发现", "浓度", "分布",
        )),
        ("discussion", ("discussion", "implication", "mechanism", "interpretation", "讨论", "机制")),
        ("conclusion", ("conclusion", "concluding", "结论", "总结")),
        ("supplement", ("supplement", "supporting information", "additional file", "补充", "附录")),
        ("references", ("references", "bibliography", "参考文献")),
    )
    for label, needles in rules:
        if any(needle in value for needle in needles):
            return label
    if re.match(r"^(table|tab\.?|表)\s*\w*", value):
        return "table"
    if re.match(r"^(figure|fig\.?|图)\s*\w*", value):
        return "figure"
    return "other"


def content_kind(heading: str, text: str) -> str:
    """Identify table/figure-heavy blocks while retaining mixed prose."""
    heading_value = str(heading or "").strip().casefold()
    has_table = bool(
        re.search(r"(?m)^\s*\|.+\|\s*$", text)
        or re.match(r"^(table|tab\.?|表)\s*\w*", heading_value)
    )
    has_figure = bool(
        re.search(r"(?mi)^\s*(figure|fig\.?|图)\s*\w+", text)
        or re.match(r"^(figure|fig\.?|图)\s*\w*", heading_value)
        or re.search(r"!\[[^\]]*\]\([^\)]+\)", text)
    )
    if has_table and has_figure:
        return "mixed"
    if has_table:
        return "table"
    if has_figure:
        return "figure"
    return ""


def _section_spans(text: str) -> list[dict[str, Any]]:
    matches = list(HEADING_RE.finditer(text))
    if not matches:
        return [{
            "start": 0, "end": len(text), "original_heading": "",
            "heading_path": [], "canonical_section": "other",
        }]

    spans: list[dict[str, Any]] = []
    if matches[0].start() > 0 and text[:matches[0].start()].strip():
        spans.append({
            "start": 0, "end": matches[0].start(), "original_heading": "",
            "heading_path": [], "canonical_section": "other",
        })
    path: list[str] = []
    for index, match in enumerate(matches):
        level = len(match.group(1))
        heading = " ".join(match.group(2).split())
        path = path[: level - 1] + [heading]
        spans.append({
            # Whitespace before the first heading belongs to that first section
            # when it contains no independent preamble.  This keeps the union of
            # chunk ranges equal to the complete decoded Markdown.
            "start": 0 if index == 0 and not spans else match.start(),
            "end": matches[index + 1].start() if index + 1 < len(matches) else len(text),
            "original_heading": heading,
            "heading_path": list(path),
            "canonical_section": classify_heading(heading),
        })
    return spans


def _split_span(text: str, start: int, end: int, limit: int, overlap: int) -> list[tuple[int, int]]:
    output: list[tuple[int, int]] = []
    cursor = start
    while cursor < end:
        stop = min(end, cursor + limit)
        if stop < end:
            candidates = (
                text.rfind("\n\n", cursor, stop),
                text.rfind("\n", cursor, stop),
                text.rfind(". ", cursor, stop),
                text.rfind("。", cursor, stop),
            )
            split = max(candidates)
            if split > cursor + limit // 2:
                stop = split + (2 if text.startswith("\n\n", split) else 1)
        if stop > cursor:
            output.append((cursor, stop))
        if stop >= end:
            break
        cursor = max(cursor + 1, stop - overlap)
    return output


def chunk_markdown(text: str, limit: int = 12000, overlap: int = 800) -> list[dict[str, Any]]:
    """Split complete Markdown at headings first, then split oversized sections."""
    if (isinstance(limit, bool) or not isinstance(limit, int)
            or not 1000 <= limit <= MAX_CHUNK_CHARS):
        raise ValueError(
            f"limit must be an integer between 1000 and {MAX_CHUNK_CHARS}"
        )
    if (isinstance(overlap, bool) or not isinstance(overlap, int)
            or overlap < 0 or overlap > MAX_CHUNK_OVERLAP
            or overlap > limit // 2):
        raise ValueError(
            f"overlap must be between 0 and min({MAX_CHUNK_OVERLAP}, limit // 2)"
        )

    chunks: list[dict[str, Any]] = []
    for section_index, section in enumerate(_section_spans(text), 1):
        part_ranges = _split_span(text, section["start"], section["end"], limit, overlap)
        for part_index, (start, end) in enumerate(part_ranges, 1):
            value = text[start:end]
            chunks.append({
                "chunk_id": f"chunk-{len(chunks) + 1:05d}",
                "section_index": section_index,
                "part_index": part_index,
                "char_start": start,
                "char_end": end,
                "canonical_section": section["canonical_section"],
                "original_heading": section["original_heading"],
                "heading_path": section["heading_path"],
                "table_or_figure": content_kind(section["original_heading"], value),
                "text": value,
                "text_sha256": text_sha256(value),
                # Compatibility alias for the first unpublished draft of this
                # module.  Both fields deliberately hash raw UTF-8 text rather
                # than JSON string serialization.
                "sha256": text_sha256(value),
            })
    verify_chunks(text, chunks, allow_overlap=overlap > 0)
    return chunks


def verify_chunks(
    text: str,
    chunks: list[dict[str, Any]],
    *,
    allow_overlap: bool = False,
) -> None:
    """Reject altered, duplicate, out-of-range, or content-dropping chunk sets."""
    if text.strip() and not chunks:
        raise ValueError("non-empty Markdown produced no section chunks")
    seen: set[str] = set()
    previous_section = 0
    previous_part = 0
    previous_start = -1
    coverage_end = 0
    sections = _section_spans(text)
    for number, chunk in enumerate(chunks, 1):
        if set(chunk) != CHUNK_KEYS:
            raise ValueError("section chunk keys do not match schema")
        chunk_id = str(chunk.get("chunk_id") or "")
        if chunk_id != f"chunk-{number:05d}" or chunk_id in seen:
            raise ValueError("section chunk IDs must be unique and sequential")
        seen.add(chunk_id)
        start, end = chunk.get("char_start"), chunk.get("char_end")
        if (isinstance(start, bool) or isinstance(end, bool)
                or not isinstance(start, int) or not isinstance(end, int)
                or start < 0 or end <= start or end > len(text)):
            raise ValueError(f"invalid section chunk range: {chunk_id}")
        value = chunk.get("text")
        if not isinstance(value, str) or value != text[start:end]:
            raise ValueError(f"section chunk is not an exact Markdown substring: {chunk_id}")
        expected_text_sha = text_sha256(value)
        if chunk.get("text_sha256") != expected_text_sha:
            raise ValueError(f"section chunk text_sha256 mismatch: {chunk_id}")
        if chunk.get("sha256") != expected_text_sha:
            raise ValueError(f"section chunk hash mismatch: {chunk_id}")
        if chunk.get("canonical_section") not in {
            "abstract", "introduction", "materials_methods", "results", "discussion",
            "conclusion", "supplement", "references", "table", "figure", "other",
        }:
            raise ValueError(f"invalid canonical section: {chunk_id}")
        if not isinstance(chunk.get("heading_path"), list):
            raise ValueError(f"heading_path must be an array: {chunk_id}")
        if any(not isinstance(value, str) or not value.strip()
               for value in chunk["heading_path"]):
            raise ValueError(f"heading_path entries must be non-empty strings: {chunk_id}")
        if not isinstance(chunk.get("original_heading"), str):
            raise ValueError(f"original_heading must be a string: {chunk_id}")
        if chunk["original_heading"]:
            if not chunk["heading_path"] or chunk["heading_path"][-1] != chunk["original_heading"]:
                raise ValueError(f"heading_path must end with original_heading: {chunk_id}")
        elif chunk["heading_path"]:
            raise ValueError(f"empty original_heading requires an empty heading_path: {chunk_id}")
        if chunk.get("table_or_figure") not in {"", "table", "figure", "mixed"}:
            raise ValueError(f"invalid table_or_figure: {chunk_id}")
        section_index, part_index = chunk.get("section_index"), chunk.get("part_index")
        if (isinstance(section_index, bool) or not isinstance(section_index, int)
                or isinstance(part_index, bool) or not isinstance(part_index, int)
                or section_index < 1 or part_index < 1):
            raise ValueError(f"invalid section/part index: {chunk_id}")
        if section_index == previous_section:
            if part_index != previous_part + 1:
                raise ValueError(f"part_index must be sequential within a section: {chunk_id}")
        elif section_index == previous_section + 1:
            if part_index != 1:
                raise ValueError(f"new section must start at part_index 1: {chunk_id}")
        else:
            raise ValueError(f"section_index must be sequential: {chunk_id}")
        previous_section, previous_part = section_index, part_index
        if section_index > len(sections):
            raise ValueError(f"section_index is outside the Markdown structure: {chunk_id}")
        expected_section = sections[section_index - 1]
        if start < expected_section["start"] or end > expected_section["end"]:
            raise ValueError(f"chunk crosses its bound Markdown section: {chunk_id}")
        for key in ("canonical_section", "original_heading", "heading_path"):
            if chunk[key] != expected_section[key]:
                raise ValueError(f"chunk {key} differs from Markdown structure: {chunk_id}")
        if chunk["table_or_figure"] != content_kind(chunk["original_heading"], value):
            raise ValueError(f"chunk table_or_figure differs from its content: {chunk_id}")
        if start <= previous_start:
            raise ValueError(f"chunk ranges must follow JSONL order: {chunk_id}")
        if start > coverage_end:
            raise ValueError("section chunks omit Markdown characters")
        if start < coverage_end and not allow_overlap:
            raise ValueError("section chunks overlap")
        previous_start = start
        coverage_end = max(coverage_end, end)
    if coverage_end < len(text):
        raise ValueError("section chunks omit trailing Markdown characters")


def section_records(
    text: str, *, paper_id: str, source_file: str, source_format: str,
    source_sha256: str, parsed_text_sha256: str, limit: int = 12000,
    overlap: int = 0,
) -> list[dict[str, Any]]:
    if not str(paper_id).strip():
        raise ValueError("paper_id must not be empty")
    if not str(source_file).strip():
        raise ValueError("source_file must not be empty")
    if not str(source_format).strip():
        raise ValueError("source_format must not be empty")
    if not re.fullmatch(r"[0-9a-f]{64}", str(source_sha256)):
        raise ValueError("source_sha256 must be lowercase SHA-256")
    if not re.fullmatch(r"[0-9a-f]{64}", str(parsed_text_sha256)):
        raise ValueError("parsed_text_sha256 must be lowercase SHA-256")
    bindings = {
        "schema_version": SECTION_SCHEMA_VERSION,
        "paper_id": paper_id,
        "source_file": source_file,
        "source_format": source_format,
        "source_sha256": source_sha256,
        "parsed_text_sha256": parsed_text_sha256,
        "parsed_char_count": len(text),
    }
    return [{**bindings, **chunk} for chunk in chunk_markdown(text, limit, overlap)]


def write_sections_jsonl(
    markdown_path: Path, output_path: Path, *, paper_id: str, source_file: str,
    source_format: str, source_sha256: str, limit: int = 12000,
    overlap: int = 0,
) -> tuple[str, int]:
    # Decode bytes directly so CRLF is not silently normalised: offsets and
    # hashes bind the exact decoded file content.
    text = markdown_path.read_bytes().decode("utf-8")
    if not text.strip():
        raise ValueError("accepted parsed Markdown is empty")
    parsed_sha = file_sha256(markdown_path)
    records = section_records(
        text, paper_id=paper_id, source_file=source_file,
        source_format=source_format, source_sha256=source_sha256,
        parsed_text_sha256=parsed_sha, limit=limit, overlap=overlap,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{output_path.name}.", dir=output_path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output_path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return file_sha256(output_path), len(records)


def read_sections_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid sections JSONL at line {number}: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"sections JSONL line {number} must be an object")
            records.append(value)
    return records


def validate_sections_jsonl(
    path: Path, markdown_path: Path, *, source_sha256: str,
    parsed_text_sha256: str, expected_count: int | None = None,
    paper_id: str | None = None, source_file: str | None = None,
    source_format: str | None = None,
) -> list[dict[str, Any]]:
    if file_sha256(markdown_path) != parsed_text_sha256:
        raise ValueError("parsed Markdown SHA-256 changed before section validation")
    text = markdown_path.read_bytes().decode("utf-8")
    records = read_sections_jsonl(path)
    chunks: list[dict[str, Any]] = []
    stable_binding: tuple[Any, ...] | None = None
    for record in records:
        if set(record) != SECTION_RECORD_KEYS:
            raise ValueError("section JSONL record keys do not match schema")
        if record.get("schema_version") != SECTION_SCHEMA_VERSION:
            raise ValueError("unsupported section chunk schema version")
        if record.get("source_sha256") != source_sha256:
            raise ValueError("section chunk source SHA-256 mismatch")
        if record.get("parsed_text_sha256") != parsed_text_sha256:
            raise ValueError("section chunk parsed-text SHA-256 mismatch")
        if record.get("parsed_char_count") != len(text):
            raise ValueError("section chunk parsed_char_count mismatch")
        binding = (
            record.get("paper_id"), record.get("source_file"),
            record.get("source_format"), record.get("source_sha256"),
            record.get("parsed_text_sha256"), record.get("parsed_char_count"),
        )
        if any(not isinstance(value, str) or not value.strip()
               for value in binding[:3]):
            raise ValueError("section chunk paper/source binding must not be empty")
        if stable_binding is None:
            stable_binding = binding
        elif binding != stable_binding:
            raise ValueError("section chunk document bindings are inconsistent")
        if paper_id is not None and record.get("paper_id") != paper_id:
            raise ValueError("section chunk paper_id mismatch")
        if source_file is not None and record.get("source_file") != source_file:
            raise ValueError("section chunk source_file mismatch")
        if source_format is not None and record.get("source_format") != source_format:
            raise ValueError("section chunk source_format mismatch")
        chunks.append({
            key: record.get(key) for key in (
                "chunk_id", "section_index", "part_index", "char_start", "char_end",
                "canonical_section", "original_heading", "heading_path",
                "table_or_figure", "text", "text_sha256", "sha256",
            )
        })
    if expected_count is not None and len(records) != expected_count:
        raise ValueError("section chunk count mismatch")
    verify_chunks(text, chunks)
    return records
