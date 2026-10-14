#!/usr/bin/env python3
"""Split one accepted parsed-paper Markdown file into hash-bound JSONL chunks.

This command intentionally does not parse or upload PDFs.  PDF/XML parsing and
MinerU execution belong to the workspace document-processing queue; this step
only structures an already accepted ``full.md`` artifact.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from paper_section_chunking import (
    file_sha256,
    validate_sections_jsonl,
    write_sections_jsonl,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="accepted UTF-8 full.md")
    parser.add_argument("--output", required=True, type=Path, help="destination JSONL")
    parser.add_argument("--paper-id", required=True)
    parser.add_argument(
        "--source-file",
        required=True,
        help="authoritative PDF/XML workspace reference (content is not read or uploaded)",
    )
    parser.add_argument("--source-format", required=True, help="for example pdf, xml, or pdf-mineru")
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument(
        "--parsed-text-sha256",
        default="",
        help="optional expected SHA-256 for full.md; mismatch aborts before writing",
    )
    parser.add_argument("--chunk-chars", type=int, default=12000)
    parser.add_argument(
        "--chunk-overlap", type=int, default=0,
        help="default 0 keeps the durable section sidecar non-overlapping",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_sha = str(args.source_sha256).strip().casefold()
    if not re.fullmatch(r"[0-9a-f]{64}", source_sha):
        raise SystemExit("--source-sha256 must be a 64-character SHA-256")
    if not args.input.is_file():
        raise SystemExit(f"accepted parsed Markdown not found: {args.input}")
    parsed_sha = file_sha256(args.input)
    if args.parsed_text_sha256 and args.parsed_text_sha256.casefold() != parsed_sha:
        raise SystemExit("--parsed-text-sha256 does not match --input")

    output_sha, count = write_sections_jsonl(
        args.input,
        args.output,
        paper_id=args.paper_id,
        source_file=args.source_file,
        source_format=args.source_format,
        source_sha256=source_sha,
        limit=args.chunk_chars,
        overlap=args.chunk_overlap,
    )
    validate_sections_jsonl(
        args.output,
        args.input,
        source_sha256=source_sha,
        parsed_text_sha256=parsed_sha,
        expected_count=count,
        paper_id=args.paper_id,
        source_file=args.source_file,
        source_format=args.source_format,
    )
    print(json.dumps({
        "output": str(args.output),
        "record_count": count,
        "output_sha256": output_sha,
        "parsed_text_sha256": parsed_sha,
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
