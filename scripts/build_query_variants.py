#!/usr/bin/env python3
"""Compile a concept brief into auditable multi-database query variants."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


SOURCES = (
    "openalex", "crossref", "europe_pmc", "pubmed", "semantic_scholar",
    "core", "arxiv", "wos", "scopus",
)
VARIANTS = {
    "recall": ("anchors", "synonyms", "acronyms", "spelling_variants", "expansions"),
    "balanced": ("anchors", "synonyms", "acronyms", "spelling_variants"),
    "precision_pilot": ("anchors", "synonyms"),
}
FIDELITY = {
    "openalex": "approximate", "crossref": "approximate",
    "europe_pmc": "field_translated", "pubmed": "candidate_requires_live_validation",
    "semantic_scholar": "candidate_requires_live_validation", "core": "candidate_requires_live_validation",
    "arxiv": "field_translated", "wos": "field_translated", "scopus": "field_translated",
}


def unique(values: list[Any], *, strip_quotes: bool = True) -> list[str]:
    out: list[str] = []
    for value in values:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if strip_quotes:
            text = text.strip('"')
        if text and text.casefold() not in {item.casefold() for item in out}:
            out.append(text)
    return out


def generic_term(term: str) -> str:
    return f'"{term}"' if re.search(r"\s", term) else term


def generic_block(terms: list[str]) -> str:
    return "(" + " OR ".join(generic_term(term) for term in terms) + ")"


def pubmed_term(term: str) -> str:
    return f'"{term}"[tiab]' if re.search(r"\s", term) else f"{term}[tiab]"


def source_query(source: str, blocks: list[dict[str, Any]]) -> str:
    if source == "pubmed":
        rendered = []
        for block in blocks:
            free = [pubmed_term(term) for term in block["terms"]]
            mesh = [f'"{term}"[Mesh]' for term in block.get("mesh", [])]
            rendered.append("(" + " OR ".join(unique(mesh + free, strip_quotes=False)) + ")")
        return " AND ".join(rendered)
    if source == "europe_pmc":
        rendered = [
            "(" + " OR ".join(f"TITLE_ABS:{generic_term(term)}" for term in block["terms"]) + ")"
            for block in blocks
        ]
        return " AND ".join(rendered)
    if source == "arxiv":
        rendered = [
            "(" + " OR ".join(f"all:{generic_term(term)}" for term in block["terms"]) + ")"
            for block in blocks
        ]
        return " AND ".join(rendered)
    boolean = " AND ".join(generic_block(block["terms"]) for block in blocks)
    if source == "wos":
        return f"TS=({boolean})"
    if source == "scopus":
        return f"TITLE-ABS-KEY({boolean})"
    if source in ("openalex", "crossref"):
        # Their current adapters use relevance-ranked free-text endpoints, not strict field Boolean.
        return " ".join(generic_term(block["terms"][0]) for block in blocks)
    return boolean


def validate_brief(brief: dict[str, Any]) -> None:
    if not str(brief.get("question") or "").strip():
        raise ValueError("brief.question is required")
    concepts = brief.get("concepts")
    if not isinstance(concepts, list) or not concepts:
        raise ValueError("brief.concepts must be a non-empty list")
    ids: set[str] = set()
    for index, concept in enumerate(concepts, 1):
        cid = str(concept.get("id") or "").strip()
        if not cid or not re.fullmatch(r"[A-Za-z0-9_-]+", cid):
            raise ValueError(f"concept {index} needs a safe id")
        if cid in ids:
            raise ValueError(f"duplicate concept id: {cid}")
        ids.add(cid)
        role = concept.get("role")
        if role not in ("search", "screen", "optional"):
            raise ValueError(f"concept {cid} has invalid role: {role}")
        if role in ("search", "optional") and not unique(concept.get("anchors") or []):
            raise ValueError(f"concept {cid} requires at least one anchor")
    if not any(c.get("role") == "search" for c in concepts):
        raise ValueError("at least one concept must have role=search")


def blocks_for(brief: dict[str, Any], variant: str) -> list[dict[str, Any]]:
    roles = {"search", "optional"} if variant == "precision_pilot" else {"search"}
    blocks = []
    for concept in brief["concepts"]:
        if concept.get("role") not in roles:
            continue
        terms: list[Any] = []
        for field in VARIANTS[variant]:
            terms.extend(concept.get(field) or [])
        terms = unique(terms)
        mesh = unique(((concept.get("controlled") or {}).get("mesh") or []))
        blocks.append({
            "id": concept["id"], "name": concept.get("name") or concept["id"],
            "role": concept["role"], "terms": terms, "mesh": mesh,
        })
    return blocks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brief", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    brief_path = Path(args.brief).expanduser().resolve()
    brief = json.loads(brief_path.read_text(encoding="utf-8"))
    try:
        validate_brief(brief)
    except ValueError as exc:
        parser.error(str(exc))
    requested = brief.get("target_sources") or list(SOURCES)
    unknown = [source for source in requested if source not in SOURCES]
    if unknown:
        parser.error(f"unknown target_sources: {unknown}")
    out = Path(args.output_dir).expanduser().resolve()
    if out.exists() and any(out.iterdir()):
        parser.error(f"output directory must be absent or empty: {out}")
    (out / "configs").mkdir(parents=True, exist_ok=True)

    plan: dict[str, Any] = {
        "query_id": brief.get("query_id") or out.name,
        "question": brief["question"], "framework": brief.get("framework") or "custom",
        "limits": brief.get("limits") or {}, "known_records": brief.get("known_records") or [],
        "concepts": brief["concepts"], "variants": {},
        "final_variant": None,
        "warning": "Candidate queries require live testing; approximate sources are not Boolean-equivalent.",
    }
    matrix: list[dict[str, str]] = []
    for variant in VARIANTS:
        blocks = blocks_for(brief, variant)
        source_entries: dict[str, Any] = {}
        config_sources: dict[str, Any] = {}
        for source in requested:
            query = source_query(source, blocks)
            source_entries[source] = {"query": query, "fidelity": FIDELITY[source]}
            config_sources[source] = {"query": query}
            matrix.append({
                "variant": variant, "source": source, "query": query,
                "fidelity": FIDELITY[source],
                "purpose": {
                    "recall": "systematic_search_candidate",
                    "balanced": "noise_comparison_candidate",
                    "precision_pilot": "seed_discovery_only",
                }[variant],
            })
        plan["variants"][variant] = {"blocks": blocks, "sources": source_entries}
        config = {
            "query_id": f"{plan['query_id']}-{variant}",
            "original_query": brief["question"],
            "source_order": requested,
            "sources": config_sources,
        }
        (out / "configs" / f"{variant}.json").write_text(
            json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
        )
    (out / "query_plan.json").write_text(
        json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    with (out / "query_matrix.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["variant", "source", "query", "fidelity", "purpose"])
        writer.writeheader()
        writer.writerows(matrix)
    strategy = (
        f"# Search strategy: {plan['query_id']}\n\n"
        f"## Question\n\n{brief['question']}\n\n"
        "## Status\n\nCandidate variants generated; live counts, known-record recall, noise sampling, "
        "revision history, final selection and PRESS review status remain to be completed.\n"
    )
    (out / "search_strategy.md").write_text(strategy, encoding="utf-8")
    print(json.dumps({
        "output_dir": str(out), "variants": list(VARIANTS), "sources": requested,
        "matrix_rows": len(matrix), "final_variant": None,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

