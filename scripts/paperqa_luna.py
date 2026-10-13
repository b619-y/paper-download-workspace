#!/usr/bin/env python3
"""PaperQA2 PDF parsing + local BM25 retrieval + Codex Luna evidence judging.

The lexical retriever is a safe fallback while the separately managed embedding
service is not configured. Luna never generates embeddings in this workflow.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import tempfile
from collections import Counter
from pathlib import Path

from paperqa_pypdf import parse_pdf_to_pages


TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]*|[\u4e00-\u9fff]")


def tokens(text: str) -> list[str]:
    raw = [item.casefold() for item in TOKEN_RE.findall(text)]
    chinese = [item for item in raw if "\u4e00" <= item <= "\u9fff"]
    return raw + [a + b for a, b in zip(chinese, chinese[1:])]


def chunk_text(text: str, limit: int = 1800, overlap: int = 250) -> list[str]:
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return []
    chunks = []
    start = 0
    while start < len(text):
        end = min(len(text), start + limit)
        if end < len(text):
            split = max(text.rfind(". ", start, end), text.rfind("。", start, end))
            if split > start + limit // 2:
                end = split + 1
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start = max(start + 1, end - overlap)
    return chunks


def load_chunks(pdf_dir: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for pdf in sorted(pdf_dir.rglob("*.pdf")):
        parsed = parse_pdf_to_pages(pdf, parse_media=False)
        if not isinstance(parsed.content, dict):
            continue
        for page, page_text in parsed.content.items():
            if not isinstance(page_text, str):
                continue
            for index, content in enumerate(chunk_text(page_text), start=1):
                records.append({
                    "chunk_id": f"{pdf.name}:p{page}:c{index}",
                    "source": pdf.name,
                    "source_path": str(pdf.resolve()),
                    "page": int(page) if str(page).isdigit() else str(page),
                    "text": content,
                })
    return records


def bm25(query: str, records: list[dict[str, object]], top_k: int) -> list[dict[str, object]]:
    query_tokens = tokens(query)
    docs = [tokens(str(record["text"])) for record in records]
    if not query_tokens or not docs:
        return []
    doc_freq = Counter(term for doc in docs for term in set(doc))
    avg_len = sum(map(len, docs)) / len(docs)
    scores: list[tuple[float, int]] = []
    for index, doc in enumerate(docs):
        tf = Counter(doc)
        score = 0.0
        for term in query_tokens:
            if not tf[term]:
                continue
            idf = math.log(1 + (len(docs) - doc_freq[term] + 0.5) / (doc_freq[term] + 0.5))
            denom = tf[term] + 1.5 * (1 - 0.75 + 0.75 * len(doc) / max(avg_len, 1))
            score += idf * tf[term] * 2.5 / denom
        scores.append((score, index))
    selected = sorted(scores, reverse=True)[:top_k]
    output = []
    for score, index in selected:
        item = dict(records[index])
        item["retrieval_score"] = round(score, 6)
        output.append(item)
    return output


def schema() -> dict[str, object]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["answer", "decision", "uncertainty", "evidence"],
        "properties": {
            "answer": {"type": "string"},
            "decision": {"type": "string", "enum": ["include", "exclude", "uncertain"]},
            "uncertainty": {"type": "string"},
            "evidence": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["chunk_id", "source", "page", "exact_quote", "reason"],
                    "properties": {
                        "chunk_id": {"type": "string"},
                        "source": {"type": "string"},
                        "page": {"type": ["integer", "string"]},
                        "exact_quote": {"type": "string"},
                        "reason": {"type": "string"}
                    }
                }
            }
        }
    }


def ask_luna(question: str, candidates: list[dict[str, object]], codex: str, model: str) -> dict[str, object]:
    prompt = (
        "你是论文全文证据筛选器。只可使用下方候选段落回答。"
        "若证据不足，decision 必须为 uncertain。exact_quote 必须逐字复制自对应段落，"
        "不得改写；不要把未检索到证据解释为不存在。\n\n"
        f"问题：{question}\n\n候选段落：\n"
        + json.dumps(candidates, ensure_ascii=False)
    )
    with tempfile.TemporaryDirectory(prefix="paperqa-luna-") as temp:
        temp_path = Path(temp)
        schema_path = temp_path / "schema.json"
        answer_path = temp_path / "answer.json"
        schema_path.write_text(json.dumps(schema(), ensure_ascii=False), encoding="utf-8")
        command = [
            codex, "exec", "--model", model, "--ephemeral", "--sandbox", "read-only",
            "--skip-git-repo-check", "--output-schema", str(schema_path),
            "--output-last-message", str(answer_path), "-",
        ]
        subprocess.run(command, input=prompt, text=True, check=True)
        return json.loads(answer_path.read_text(encoding="utf-8"))


def verify(result: dict[str, object], candidates: list[dict[str, object]]) -> dict[str, object]:
    by_id = {str(item["chunk_id"]): item for item in candidates}
    verified = []
    rejected = []
    for evidence in result.get("evidence", []):
        if not isinstance(evidence, dict):
            continue
        chunk = by_id.get(str(evidence.get("chunk_id", "")))
        quote = str(evidence.get("exact_quote", ""))
        if chunk and quote and quote in str(chunk["text"]):
            evidence["quote_verified"] = True
            evidence["source_path"] = chunk["source_path"]
            verified.append(evidence)
        else:
            evidence["quote_verified"] = False
            rejected.append(evidence)
    result["evidence"] = verified
    result["rejected_evidence"] = rejected
    if not verified:
        result["decision"] = "uncertain"
        result["uncertainty"] = (
            str(result.get("uncertainty", "")) + " 无证据通过原文逐字子串校验。"
        ).strip()
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdf-dir", required=True, type=Path)
    parser.add_argument("--question", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--top-k", type=int, default=12)
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--codex", default="/usr/bin/codex")
    parser.add_argument("--retrieval-only", action="store_true")
    args = parser.parse_args()

    records = load_chunks(args.pdf_dir)
    candidates = bm25(args.question, records, args.top_k)
    payload: dict[str, object] = {
        "parser": "paperqa-pypdf",
        "retriever": "local-bm25-fallback",
        "embedding_status": "separate_not_used",
        "model": None if args.retrieval_only else args.model,
        "question": args.question,
        "candidate_count": len(candidates),
        "candidates": candidates,
    }
    if not args.retrieval_only:
        payload["result"] = verify(ask_luna(args.question, candidates, args.codex, args.model), candidates)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
