#!/usr/bin/env python3
"""Read-only, task-scoped workflow status inspection."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PRE_DECISIONS = {"download", "exclude", "uncertain"}
QUERY_VARIANTS = {"recall", "balanced", "precision_pilot"}
BATCH_STATUSES = {"READY", "CLAIMED", "RUNNING", "REVIEW", "ACCEPTED", "BLOCKED", "FAILED"}
FULLTEXT_STATUSES = {"ready", "fulltext_unavailable", "identity_unverified"}
FULLTEXT_DECISIONS = {"include", "exclude", "uncertain", "not_assessed"}
DOWNLOAD_SUCCESS = {"downloaded", "reused_verified"}
DOWNLOAD_FAILURE = {
    "not_found", "paywalled", "access_denied", "identity_mismatch", "failed",
    "skipped_duplicate",
}
DOWNLOAD_FIELDS = {
    "item_id", "paper_id", "status", "title", "doi", "pdf_path", "identity_verified",
}
FULLTEXT_FIELDS = [
    "item_id", "paper_id", "title", "doi", "fulltext_path", "fulltext_status",
    "criterion_id", "criterion_text", "decision", "evidence_quote", "evidence_page",
    "evidence_section", "evidence_kind", "evidence_figure", "evidence_image_path",
    "evidence_object_ref", "reviewer", "reviewed_at", "notes",
]
TASK_FIELDS = {
    "task_id", "title", "research_question", "inclusion_criteria",
    "exclusion_criteria", "target_evidence", "requires_fulltext_screening",
}


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def csv_info(path: Path) -> dict[str, Any]:
    info: dict[str, Any] = {"path": path, "header": [], "rows": [], "error": ""}
    if not path.is_file():
        info["error"] = "missing"
        return info
    try:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            info["header"] = reader.fieldnames or []
            info["rows"] = [
                {str(key): (value or "").strip() for key, value in row.items()}
                for row in reader
            ]
    except (OSError, UnicodeError, csv.Error) as exc:
        info["error"] = f"unreadable:{exc.__class__.__name__}"
    if not info["header"] and not info["error"]:
        info["error"] = "missing_header"
    return info


def rel(path: Path | None, root: Path) -> str | None:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())


def inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def result(root: Path, stage: str, reason: str, next_action: str,
           evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "workspace": str(root), "stage": stage, "reason": reason,
        "next_action": next_action, "evidence": evidence,
    }


def normalize_doi(value: str) -> str:
    value = value.strip().casefold()
    value = re.sub(r"^https?://(dx\.)?doi\.org/", "", value)
    value = re.sub(r"^doi\s*:\s*", "", value)
    return value.strip().strip(".,; ")


def normalize_title(value: str) -> str:
    value = unicodedata.normalize("NFKC", value).casefold()
    return "".join(character for character in value if character.isalnum())


def normalize_year(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip().casefold()


def normalize_source_text(value: str) -> str:
    """Normalize harmless CSV whitespace while preserving source-record content."""
    return " ".join(unicodedata.normalize("NFKC", value).split())


def resolve_workspace_path(root: Path, value: str) -> Path | None:
    text = value.strip()
    if not text:
        return None
    path = Path(text).expanduser()
    return (path if path.is_absolute() else root / path).resolve()


def identity_key(row: dict[str, str]) -> str:
    doi = normalize_doi(row.get("doi", ""))
    if doi:
        return f"doi:{doi}"
    title = normalize_title(row.get("title", ""))
    return f"title:{title}:{normalize_year(row.get('year', ''))}" if title else ""


def manifest_key(value: dict[str, Any]) -> str:
    """Stable identity for a copied run manifest (formatting/path independent)."""
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def run_manifests(task_root: Path, query_id: str | None = None) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for path in task_root.glob("**/run_manifest.json"):
        doc = read_json(path)
        current = str(doc.get("query_id") or "").strip()
        if not doc or (query_id is not None and current != query_id):
            continue
        runs.append({"path": path.resolve(), "doc": doc, "query_id": current, "run_key": manifest_key(doc)})
    return runs


def newest_run(runs: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not runs:
        return None

    def timestamp(run: dict[str, Any]) -> float:
        for field in ("finished_at", "started_at"):
            value = str(run["doc"].get(field) or "").strip()
            if not value:
                continue
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.timestamp()
            except ValueError:
                continue
        return run["path"].stat().st_mtime

    return max(
        runs,
        key=lambda run: (
            timestamp(run),
            run["path"].stat().st_mtime_ns,
        ),
    )


def batch_handoff(root: Path, batch_id: str) -> dict[str, Any]:
    directory = root / "02_batches" / batch_id / "inputs/search_screening"
    manifest_path = directory / "run_manifest.json"
    doc = read_json(manifest_path)
    summary = read_json(directory / "handoff_summary.json")
    request = read_json(root / "02_batches" / batch_id / "REQUEST.json")
    return {
        "manifest_path": manifest_path,
        "manifest_exists": manifest_path.is_file(),
        "manifest": doc,
        "query_id": str(doc.get("query_id") or "").strip(),
        "run_key": manifest_key(doc) if doc else "",
        "summary": summary,
        "request": request,
    }


def task_dirs(runtime: Path) -> list[Path]:
    directory = runtime / "tasks"
    if not directory.is_dir():
        return []
    return sorted(path.resolve() for path in directory.iterdir() if path.is_dir())


def task_query_ids(task_root: Path) -> set[str]:
    ids: set[str] = set()
    for pattern in ("**/query_plan.json", "**/configs/*.json", "**/run_manifest.json"):
        for path in task_root.glob(pattern):
            query_id = str(read_json(path).get("query_id") or "").strip()
            if query_id:
                ids.add(query_id)
    return ids


def handoff_query_id(root: Path, batch_id: str) -> str:
    return batch_handoff(root, batch_id)["query_id"]


def query_owners(root: Path, query_id: str) -> list[Path]:
    return [
        task_root for task_root in task_dirs(root / "runtime")
        if query_id and query_id in task_query_ids(task_root)
    ]


def handoff_metadata_task_id(handoff: dict[str, Any]) -> str:
    for document in (handoff.get("manifest"), handoff.get("summary"), handoff.get("request")):
        if isinstance(document, dict):
            task_id = str(document.get("task_id") or "").strip()
            if task_id:
                return task_id
    return ""


def handoff_metadata_task_ids(handoff: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for document in (handoff.get("manifest"), handoff.get("summary"), handoff.get("request")):
        if isinstance(document, dict):
            value = str(document.get("task_id") or "").strip()
            if value:
                values.add(value)
    return values


def handoff_metadata_batch_ids(handoff: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for document in (handoff.get("manifest"), handoff.get("request")):
        if isinstance(document, dict):
            value = str(document.get("batch_id") or "").strip()
            if value:
                values.add(value)
    summary = handoff.get("summary")
    if isinstance(summary, dict):
        value = str(summary.get("batch_id") or "").strip()
        if value:
            values.add(value)
        nested = summary.get("batch")
        if isinstance(nested, dict):
            value = str(nested.get("batch_id") or "").strip()
            if value:
                values.add(value)
    return values


def task_for_unpointed_batch(root: Path, batch_id: str) -> Path | None:
    """Conservative legacy inference: unique query owner and exact copied run."""
    handoff = batch_handoff(root, batch_id)
    query_id = handoff["query_id"]
    run_key = handoff["run_key"]
    owners = query_owners(root, query_id)
    if len(owners) != 1 or not run_key:
        return None
    owner = owners[0]
    metadata_tasks = handoff_metadata_task_ids(handoff)
    if len(metadata_tasks) > 1 or (metadata_tasks and metadata_tasks != {owner.name}):
        return None
    metadata_batches = handoff_metadata_batch_ids(handoff)
    if metadata_batches and metadata_batches != {batch_id}:
        return None
    matching_runs = [run for run in run_manifests(owner, query_id) if run["run_key"] == run_key]
    return owner if len(matching_runs) == 1 else None


def resolve_task(root: Path, requested_task: str | None,
                 requested_batch: str | None) -> tuple[Path | None, str | None, dict[str, Any], dict[str, Any] | None]:
    runtime = root / "runtime"
    active_path = runtime / "ACTIVE_TASK.json"
    active_exists = active_path.is_file()
    active = read_json(active_path) if active_exists else {}
    errors: list[str] = []
    active_id = str(active.get("task_id") or "").strip()
    root_text = str(active.get("task_root") or "").strip()
    active_root: Path | None = None
    if active_exists:
        if not active:
            errors.append("ACTIVE_TASK.json is unreadable or empty")
        if not active_id:
            errors.append("ACTIVE_TASK.task_id is missing")
        if not root_text:
            errors.append("ACTIVE_TASK.task_root is missing")
        else:
            raw = Path(root_text).expanduser()
            active_root = (raw if raw.is_absolute() else root / raw).resolve()
            if not inside(active_root, runtime):
                errors.append("ACTIVE_TASK.task_root must remain inside runtime/")
            elif active_root.name != active_id:
                errors.append("ACTIVE_TASK task_id and task_root directory name disagree")
            elif not active_root.is_dir():
                errors.append("ACTIVE_TASK.task_root directory does not exist")
        batch_ids = active.get("batch_ids")
        if not isinstance(batch_ids, list) or any(not isinstance(x, str) or not x.strip() for x in batch_ids):
            errors.append("ACTIVE_TASK.batch_ids must be a list of non-empty strings")
        elif len({value.strip() for value in batch_ids}) != len(batch_ids):
            errors.append("ACTIVE_TASK.batch_ids must not contain duplicates")
    evidence: dict[str, Any] = {
        "active_task_path": rel(active_path, root) if active_exists else None,
        "active_task": active if active_exists else None,
        "active_task_errors": errors,
        "requested_task_id": requested_task,
        "requested_batch_id": requested_batch,
    }
    if errors and not requested_task:
        return None, None, evidence, result(
            root, "TASK_STATE_ERROR", "The current-task pointer is invalid.",
            "Repair runtime/ACTIVE_TASK.json before changing workflow state.", evidence,
        )

    directories = task_dirs(runtime)
    if requested_task:
        matches = [path for path in directories if path.name == requested_task]
        if active_id == requested_task and active_root and active_root not in matches:
            matches.append(active_root)
        if len(matches) > 1:
            evidence["task_candidates"] = [rel(path, root) for path in matches]
            return None, None, evidence, result(
                root, "AMBIGUOUS", f"Multiple roots claim task_id={requested_task}.",
                "Repair duplicate roots or the active pointer.", evidence,
            )
        evidence["selection_source"] = "--task-id"
        return (matches[0] if matches else runtime / "tasks" / requested_task), requested_task, evidence, None
    if active_id and active_root:
        evidence["selection_source"] = "ACTIVE_TASK.json"
        return active_root, active_id, evidence, None
    if requested_batch:
        inferred = task_for_unpointed_batch(root, requested_batch)
        if inferred is not None:
            evidence.update({
                "selection_source": "--batch-id unique copied run",
                "batch_handoff_query_id": handoff_query_id(root, requested_batch),
            })
            return inferred, inferred.name, evidence, None
    if len(directories) > 1:
        evidence["task_candidates"] = [rel(path, root) for path in directories]
        return None, None, evidence, result(
            root, "AMBIGUOUS", "Multiple tasks exist without a unique selection.",
            "Use --task-id or create runtime/ACTIVE_TASK.json; do not combine task artifacts.", evidence,
        )
    if len(directories) == 1:
        evidence["selection_source"] = "only task"
        return directories[0], directories[0].name, evidence, None
    evidence["selection_source"] = "no task"
    return None, None, evidence, None


def validate_brief(path: Path, task_id: str) -> tuple[dict[str, Any], list[str]]:
    brief = read_json(path)
    if not path.is_file():
        return {}, ["missing task_brief.json"]
    if not brief:
        return {}, ["task_brief.json is unreadable or empty"]
    issues: list[str] = []
    missing = sorted(field for field in TASK_FIELDS if field not in brief)
    if missing:
        issues.append("missing fields: " + ", ".join(missing))
    if str(brief.get("task_id") or "").strip() != task_id:
        issues.append("task_brief.task_id does not match the selected task")
    for field in ("title", "research_question"):
        if not str(brief.get(field) or "").strip():
            issues.append(f"{field} must be non-empty")
    for field in ("inclusion_criteria", "exclusion_criteria", "target_evidence"):
        value = brief.get(field)
        if not isinstance(value, list) or not any(str(item).strip() for item in value):
            issues.append(f"{field} must be a non-empty list")
    if "requires_fulltext_screening" in brief and not isinstance(brief["requires_fulltext_screening"], bool):
        issues.append("requires_fulltext_screening must be boolean")
    return brief, issues


def query_gate(task_root: Path, root: Path) -> dict[str, Any]:
    directories = sorted({path.parent.resolve() for path in task_root.glob("**/query_brief.json")})
    gate: dict[str, Any] = {
        "candidate_directories": [rel(path, root) for path in directories],
        "directory": None, "valid": False, "issues": [], "final_variant": None,
        "config": None, "config_query_id": None,
    }
    if not directories:
        gate["issues"] = ["no query directory contains query_brief.json"]
        return gate
    if len(directories) > 1:
        gate.update({"ambiguous": True, "issues": ["multiple query directories exist"]})
        return gate
    directory = directories[0]
    gate["directory"] = rel(directory, root)
    paths = {
        "query_brief": directory / "query_brief.json",
        "query_plan": directory / "query_plan.json",
        "query_matrix": directory / "query_matrix.csv",
        "search_strategy": directory / "search_strategy.md",
    }
    gate["paths"] = {key: rel(path, root) for key, path in paths.items()}
    brief = read_json(paths["query_brief"])
    plan = read_json(paths["query_plan"])
    issues: list[str] = []
    if not brief:
        issues.append("query_brief.json is unreadable or empty")
    if not plan:
        issues.append("query_plan.json is missing, unreadable, or empty")
    matrix = csv_info(paths["query_matrix"])
    if matrix["error"]:
        issues.append(f"query_matrix.csv {matrix['error']}")
    elif not matrix["rows"]:
        issues.append("query_matrix.csv has no data rows")
    try:
        strategy = paths["search_strategy"].read_text(encoding="utf-8-sig").strip()
    except (OSError, UnicodeError):
        strategy = ""
    if not strategy:
        issues.append("search_strategy.md is missing or empty")
    variant = str(plan.get("final_variant") or "").strip()
    gate["final_variant"] = variant or None
    if variant not in QUERY_VARIANTS:
        issues.append("final_variant must select recall, balanced, or precision_pilot")
    config_path = directory / "configs" / f"{variant}.json" if variant else None
    config = read_json(config_path) if config_path else {}
    gate["config"] = rel(config_path, root) if config_path else None
    gate["config_query_id"] = str(config.get("query_id") or "").strip() or None
    if variant and not config:
        issues.append(f"configs/{variant}.json is missing, unreadable, or empty")
    elif variant and not gate["config_query_id"]:
        issues.append(f"configs/{variant}.json must contain a non-empty query_id")
    config_sources = config.get("sources")
    if not isinstance(config_sources, dict) or not config_sources:
        issues.append(f"configs/{variant}.json sources must be a non-empty object")
        config_sources = {}
    source_order = config.get("source_order")
    if source_order is None:
        planned_sources = list(config_sources)
    elif not isinstance(source_order, list) or not source_order or any(
        not isinstance(source, str) or not source.strip() for source in source_order
    ):
        issues.append(f"configs/{variant}.json source_order must be a non-empty string list")
        planned_sources = []
    else:
        planned_sources = [source.strip() for source in source_order]
        if len(set(planned_sources)) != len(planned_sources):
            issues.append(f"configs/{variant}.json source_order contains duplicates")
    if planned_sources and set(planned_sources) != set(config_sources):
        issues.append("config source_order and sources keys must match exactly")
    source_queries: dict[str, str] = {}
    for source in planned_sources:
        source_config = config_sources.get(source)
        query_text = str(source_config.get("query") or "").strip() if isinstance(source_config, dict) else ""
        source_queries[source] = query_text
        if not query_text:
            issues.append(f"config source {source} must have a non-empty query")
    validation = plan.get("validation")
    if not isinstance(validation, dict):
        validation = {}
        issues.append("validation is missing")
    if validation.get("live_counts_recorded") is not True:
        issues.append("validation.live_counts_recorded must be true")
    if validation.get("known_record_check") not in {"passed", "not_applicable"}:
        issues.append("validation.known_record_check must be passed or not_applicable")
    if validation.get("noise_sample_reviewed") is not True:
        issues.append("validation.noise_sample_reviewed must be true")
    if not str(validation.get("selection_rationale") or "").strip():
        issues.append("validation.selection_rationale must be non-empty")
    gate.update({
        "validation": validation,
        "planned_sources": planned_sources,
        "source_queries": source_queries,
        "issues": issues,
        "valid": not issues,
        "_config": config,
    })
    return gate


def search_gate(task_root: Path, query: dict[str, Any], root: Path) -> dict[str, Any]:
    query_id = str(query.get("config_query_id") or "").strip()
    run_records = run_manifests(task_root, query_id)
    candidates = [run["path"] for run in run_records]
    selected_run = newest_run(run_records)
    selected = selected_run["path"] if selected_run else None
    gate: dict[str, Any] = {
        "candidate_manifests": [rel(path, root) for path in candidates],
        "manifest": rel(selected, root), "exists": selected is not None,
        "valid": False, "zero_results": False, "issues": [], "_screening_rows": [],
        "run_key": selected_run["run_key"] if selected_run else None,
        "run_instances": [
            {"manifest": rel(run["path"], root), "run_key": run["run_key"]}
            for run in run_records
        ],
    }
    if selected is None:
        gate["issues"] = [f"no run_manifest.json matches query_id={query_id!r}"]
        return gate
    manifest = selected_run["doc"] if selected_run else read_json(selected)
    gate["_manifest"] = manifest
    gate["_manifest_path"] = selected
    gate["query_id"] = str(manifest.get("query_id") or "").strip() or None
    issues: list[str] = []
    if not manifest:
        issues.append("run_manifest.json is unreadable or empty")
    if manifest.get("complete") is not True:
        issues.append("run_manifest.complete is not true")
    requested = manifest.get("requested_sources")
    sources = manifest.get("sources")
    if not isinstance(requested, list) or not requested or any(not str(item).strip() for item in requested):
        issues.append("requested_sources must be a non-empty list")
        requested = []
    if not isinstance(sources, dict):
        issues.append("sources must be an object")
        sources = {}
    planned_sources = list(query.get("planned_sources") or [])
    planned_queries = dict(query.get("source_queries") or {})
    requested_names = [str(source).strip() for source in requested]
    if len(set(requested_names)) != len(requested_names):
        issues.append("requested_sources contains duplicates")
    if requested_names != planned_sources:
        issues.append(
            f"run requested_sources {requested_names} != config planned sources {planned_sources}"
        )
    if set(sources) != set(requested_names):
        issues.append(
            f"run sources keys {sorted(sources)} != requested_sources {sorted(set(requested_names))}"
        )
    source_states: dict[str, str] = {}
    fetched_sum = 0
    fetched_known = True
    for source in requested:
        name = str(source).strip()
        source_info = sources.get(name)
        if not isinstance(source_info, dict):
            issues.append(f"requested source {name} is absent")
            source_states[name] = "missing"
            fetched_known = False
            continue
        status = str(source_info.get("status") or "").strip()
        source_states[name] = status or "missing"
        if status != "complete":
            issues.append(f"requested source {name} status is {status or 'missing'}")
        native_query = str(source_info.get("native_query") or "").strip()
        if native_query != planned_queries.get(name, ""):
            issues.append(
                f"source {name} native_query does not match the selected config query"
            )
        count = source_info.get("fetched_count")
        if isinstance(count, int) and count >= 0:
            fetched_sum += count
        else:
            fetched_known = False
            issues.append(f"requested source {name} fetched_count must be a non-negative integer")
            count = None
        pages = source_info.get("pages")
        if not isinstance(pages, int) or pages < 1:
            issues.append(f"requested source {name} pages must be a positive integer")
        source_records = csv_info(selected.parent / name / "records.csv")
        if source_records["error"]:
            issues.append(f"{name}/records.csv {source_records['error']}")
        elif count is not None and len(source_records["rows"]) != count:
            issues.append(
                f"{name}/records.csv rows {len(source_records['rows'])} != fetched_count {count}"
            )
        raw_dir = selected.parent / name / "raw"
        raw_files = [path for path in raw_dir.iterdir() if path.is_file()] if raw_dir.is_dir() else []
        if isinstance(pages, int) and pages >= 1 and len(raw_files) < pages:
            issues.append(f"requested source {name} has {len(raw_files)} raw pages, expected at least {pages}")
    gate.update({"requested_sources": requested, "source_status": source_states})
    specs = {
        "all_records": (selected.parent / "all_records.csv", "raw_record_count"),
        "deduplicated_records": (selected.parent / "deduplicated_records.csv", "deduplicated_record_count"),
        "pre_screening": (selected.parent / "pre_screening.csv", "deduplicated_record_count"),
    }
    infos: dict[str, dict[str, Any]] = {}
    for name, (path, count_field) in specs.items():
        info = csv_info(path)
        infos[name] = info
        expected = manifest.get(count_field)
        if info["error"]:
            issues.append(f"{path.name} {info['error']}")
        if not isinstance(expected, int) or expected < 0:
            issues.append(f"{count_field} must be a non-negative integer")
        elif not info["error"] and len(info["rows"]) != expected:
            issues.append(f"{path.name} rows {len(info['rows'])} != {count_field} {expected}")
    raw_count = manifest.get("raw_record_count")
    dedup_count = manifest.get("deduplicated_record_count")
    if isinstance(raw_count, int) and fetched_known and fetched_sum != raw_count:
        issues.append(f"source fetched_count sum {fetched_sum} != raw_record_count {raw_count}")
    if isinstance(raw_count, int) and isinstance(dedup_count, int) and dedup_count > raw_count:
        issues.append("deduplicated_record_count exceeds raw_record_count")
    dedup, screening = infos["deduplicated_records"], infos["pre_screening"]
    if not dedup["error"] and not screening["error"]:
        if "record_id" not in dedup["header"]:
            issues.append("deduplicated_records.csv lacks record_id")
        if "record_id" not in screening["header"]:
            issues.append("pre_screening.csv lacks record_id")
        left = [row.get("record_id", "").strip() for row in dedup["rows"]]
        right = [row.get("record_id", "").strip() for row in screening["rows"]]
        if any(not value for value in left + right):
            issues.append("record coverage contains a blank record_id")
        if len(set(left)) != len(left) or len(set(right)) != len(right):
            issues.append("record coverage contains duplicate record_id")
        if set(left) != set(right):
            issues.append("pre_screening does not exactly cover deduplicated record IDs")
        dedup_by_id = {row.get("record_id", "").strip(): row for row in dedup["rows"]}
        screening_by_id = {row.get("record_id", "").strip(): row for row in screening["rows"]}
        for record_id in sorted(set(dedup_by_id) & set(screening_by_id)):
            source_row = dedup_by_id[record_id]
            screen_row = screening_by_id[record_id]
            comparisons = {
                "doi": (normalize_doi(source_row.get("doi", "")), normalize_doi(screen_row.get("doi", ""))),
                "title": (normalize_title(source_row.get("title", "")), normalize_title(screen_row.get("title", ""))),
                "year": (normalize_year(source_row.get("year", "")), normalize_year(screen_row.get("year", ""))),
            }
            for field in (
                "search_query", "source_database", "source_record", "abstract", "authors",
                "journal", "landing_url",
            ):
                comparisons[field] = (
                    normalize_source_text(source_row.get(field, "")),
                    normalize_source_text(screen_row.get(field, "")),
                )
            for field, (expected_value, actual_value) in comparisons.items():
                if expected_value != actual_value:
                    issues.append(f"pre_screening record_id={record_id} {field} differs from deduplicated record")
    gate["counts"] = {
        "raw_manifest": raw_count, "all_records_rows": len(infos["all_records"]["rows"]),
        "deduplicated_manifest": dedup_count,
        "deduplicated_rows": len(infos["deduplicated_records"]["rows"]),
        "pre_screening_rows": len(infos["pre_screening"]["rows"]),
    }
    gate["paths"] = {name: rel(info["path"], root) for name, info in infos.items()}
    gate.update({
        "issues": issues, "valid": not issues,
        "zero_results": not issues and dedup_count == 0,
        "_screening_rows": infos["pre_screening"]["rows"],
    })
    return gate


def screening_gate(rows: list[dict[str, str]]) -> dict[str, Any]:
    issues: list[str] = []
    decisions: dict[str, str] = {}
    counts = {"download": 0, "exclude": 0, "uncertain": 0, "duplicates": 0}
    for number, row in enumerate(rows, 2):
        decision = row.get("decision", "").strip().casefold()
        if decision not in PRE_DECISIONS:
            issues.append(f"row {number} has invalid decision {decision!r}")
            continue
        counts[decision] += 1
        key = identity_key(row)
        if not key:
            issues.append(f"row {number} lacks DOI and title identity")
            continue
        previous = decisions.get(key)
        if previous and previous != decision:
            issues.append(f"duplicate identity conflicts: {key} ({previous} vs {decision})")
        elif previous:
            counts["duplicates"] += 1
        else:
            decisions[key] = decision
        if decision == "download" and not (
            row.get("matched_criteria", "").strip() or row.get("reason", "").strip()
        ):
            issues.append(f"row {number} selected for download without matched_criteria or reason")
        if decision == "uncertain" and not row.get("needs_fulltext_reason", "").strip():
            issues.append(f"row {number} selected for uncertain without needs_fulltext_reason")
        if decision == "exclude" and not row.get("exclusion_reason", "").strip():
            issues.append(f"row {number} excluded without exclusion_reason")
    selected = counts["download"] + counts["uncertain"]
    return {
        "valid": not issues, "issues": issues, "counts": counts,
        "selected_count": selected,
        "all_excluded": not issues and bool(rows) and selected == 0,
    }


def batch_index(root: Path) -> tuple[dict[str, dict[str, str]], list[str]]:
    info = csv_info(root / "00_control/BATCH_INDEX.csv")
    if info["error"]:
        return {}, [f"BATCH_INDEX.csv {info['error']}"]
    issues: list[str] = []
    if not {"batch_id", "status"}.issubset(info["header"]):
        issues.append("BATCH_INDEX.csv lacks batch_id/status")
    index: dict[str, dict[str, str]] = {}
    for row in info["rows"]:
        batch_id = row.get("batch_id", "").strip()
        if not batch_id:
            issues.append("BATCH_INDEX contains a blank batch_id")
        elif batch_id in index:
            issues.append(f"BATCH_INDEX contains duplicate {batch_id}")
        else:
            index[batch_id] = row
    return index, issues


def inspect_batch(root: Path, batch_id: str, index: dict[str, dict[str, str]]) -> dict[str, Any]:
    directory = root / "02_batches" / batch_id
    status_path = directory / "STATUS.json"
    status_doc = read_json(status_path)
    index_row = index.get(batch_id)
    status = str(status_doc.get("status") or "UNKNOWN").strip().upper()
    index_status = str((index_row or {}).get("status") or "UNKNOWN").strip().upper()
    issues: list[str] = []
    if not directory.is_dir():
        issues.append("batch directory is missing")
    if not status_path.is_file() or not status_doc:
        issues.append("STATUS.json is missing or unreadable")
    if str(status_doc.get("batch_id") or "").strip() != batch_id:
        issues.append("STATUS.json batch_id mismatch")
    if index_row is None:
        issues.append("batch is missing from BATCH_INDEX")
    elif status != index_status:
        issues.append(f"STATUS status {status} != index status {index_status}")
    if status not in BATCH_STATUSES:
        issues.append(f"unknown status {status}")
    if index_row is not None and str(status_doc.get("claimed_by") or "").strip() != str(index_row.get("claimed_by") or "").strip():
        issues.append("STATUS claimed_by differs from index")
    manifest = csv_info(directory / "MANIFEST.csv")
    if manifest["error"]:
        issues.append(f"MANIFEST.csv {manifest['error']}")
    queue = directory / "outputs/fulltext_screening_queue.csv"
    handoff = batch_handoff(root, batch_id)
    return {
        "batch_id": batch_id, "status": status, "index_status": index_status,
        "status_path": rel(status_path, root), "manifest_path": rel(directory / "MANIFEST.csv", root),
        "manifest_rows": len(manifest["rows"]), "_manifest": manifest["rows"], "issues": issues,
        "premature_fulltext_queue": queue.is_file() and status != "ACCEPTED",
        "handoff_query_id": handoff["query_id"] or None,
        "handoff_run_key": handoff["run_key"] or None,
        "handoff_task_id": handoff_metadata_task_id(handoff) or None,
        "_handoff": handoff,
    }


def metadata_mismatches(left: dict[str, str], right: dict[str, str], *,
                        left_name: str, right_name: str) -> list[str]:
    issues: list[str] = []
    comparisons = {
        "paper_id": (left.get("paper_id", "").strip(), right.get("paper_id", "").strip()),
        "title": (normalize_title(left.get("title", "")), normalize_title(right.get("title", ""))),
        "doi": (normalize_doi(left.get("doi", "")), normalize_doi(right.get("doi", ""))),
    }
    for field, (left_value, right_value) in comparisons.items():
        if left_value != right_value:
            issues.append(f"{left_name} {field} differs from {right_name}")
    return issues


def acceptance_gate(root: Path, batch: dict[str, Any]) -> dict[str, Any]:
    directory = root / "02_batches" / batch["batch_id"]
    results_path = directory / "outputs/download_results.csv"
    acceptance_path = root / "03_review" / f"{batch['batch_id']}-acceptance.md"
    info = csv_info(results_path)
    issues: list[str] = []
    if info["error"]:
        issues.append(f"download_results.csv {info['error']}")
    missing_fields = sorted(DOWNLOAD_FIELDS - set(info["header"]))
    if missing_fields:
        issues.append("download_results.csv missing fields: " + ", ".join(missing_fields))
    try:
        acceptance_text = acceptance_path.read_text(encoding="utf-8-sig").strip()
    except (OSError, UnicodeError):
        acceptance_text = ""
    if not acceptance_text:
        issues.append("coordinator acceptance record is missing or empty")
    elif not re.search(
        rf"(?<![\w-]){re.escape(batch['batch_id'])}(?![\w-])", acceptance_text,
    ):
        issues.append("coordinator acceptance record does not identify this batch")
    library_info = csv_info(root / "04_library_index/PAPER_INDEX.csv")
    library_rows = [] if library_info["error"] else [
        row for row in library_info["rows"]
        if row.get("source_batch_id", "").strip() == batch["batch_id"]
    ]

    manifest_by_id: dict[str, dict[str, str]] = {}
    for row in batch["_manifest"]:
        item_id = row.get("item_id", "").strip()
        if not item_id or item_id in manifest_by_id:
            issues.append("MANIFEST item_id values must be non-empty and unique")
        else:
            manifest_by_id[item_id] = row
    results_by_id: dict[str, dict[str, str]] = {}
    fulltext_paths: dict[str, str] = {}
    success_count = failure_count = 0
    for number, row in enumerate(info["rows"], 2):
        item_id = row.get("item_id", "").strip()
        if not item_id:
            issues.append(f"download result row {number} has blank item_id")
            continue
        if item_id in results_by_id:
            issues.append(f"download_results.csv duplicates item_id={item_id}")
            continue
        results_by_id[item_id] = row
        manifest_row = manifest_by_id.get(item_id)
        if manifest_row is not None:
            issues.extend(metadata_mismatches(
                row, manifest_row,
                left_name=f"download result {item_id}", right_name="MANIFEST",
            ))
        status = row.get("status", "").strip().casefold()
        if not status:
            issues.append(f"download result {item_id} has blank status")
        elif status not in DOWNLOAD_SUCCESS | DOWNLOAD_FAILURE:
            issues.append(f"download result {item_id} has unknown status {status!r}")
        if status in DOWNLOAD_SUCCESS:
            success_count += 1
            if row.get("identity_verified", "").strip().casefold() != "true":
                issues.append(f"successful download result {item_id} is not identity_verified=true")
            paper_id = row.get("paper_id", "").strip()
            doi = normalize_doi(row.get("doi", ""))
            title = normalize_title(row.get("title", ""))
            matches: list[dict[str, str]] = []
            for library_row in library_rows:
                if paper_id and library_row.get("paper_id", "").strip() != paper_id:
                    continue
                if doi and normalize_doi(library_row.get("doi", "")) != doi:
                    continue
                if not paper_id and not doi and normalize_title(library_row.get("title", "")) != title:
                    continue
                matches.append(library_row)
            if not matches:
                issues.append(f"successful download result {item_id} is not registered in PAPER_INDEX")
                continue
            if len(matches) > 1:
                issues.append(f"successful download result {item_id} maps to multiple PAPER_INDEX rows")
                continue
            library_row = matches[0]
            issues.extend(metadata_mismatches(
                library_row, manifest_row or row,
                left_name=f"PAPER_INDEX row for {item_id}", right_name="MANIFEST",
            ))
            if library_row.get("identity_verified", "").strip().casefold() != "true":
                issues.append(f"PAPER_INDEX row for {item_id} is not identity_verified=true")
            result_path = resolve_workspace_path(root, row.get("pdf_path", ""))
            library_path = resolve_workspace_path(root, library_row.get("pdf_path", ""))
            if library_path is None or not library_path.is_file():
                issues.append(f"successful download result {item_id} has no persisted PDF/XML full text")
            elif library_path.suffix.casefold() not in {".pdf", ".xml"}:
                issues.append(f"successful download result {item_id} persisted path is not PDF or XML")
            elif library_path.stat().st_size <= 0:
                issues.append(f"successful download result {item_id} persisted PDF/XML full text is empty")
            else:
                if result_path is None:
                    issues.append(
                        f"download result {item_id} does not persist its accepted PDF/XML path"
                    )
                elif result_path != library_path:
                    issues.append(f"download result {item_id} path differs from PAPER_INDEX")
                fulltext_paths[item_id] = str(library_path)
        else:
            failure_count += 1
    if success_count and library_info["error"]:
        issues.append(f"PAPER_INDEX.csv {library_info['error']}")
    if set(manifest_by_id) != set(results_by_id):
        issues.append(
            f"download result coverage mismatch: missing={sorted(set(manifest_by_id)-set(results_by_id))} "
            f"extra={sorted(set(results_by_id)-set(manifest_by_id))}"
        )
    return {
        "valid": not issues,
        "issues": issues,
        "download_results": rel(results_path, root),
        "acceptance_record": rel(acceptance_path, root) if acceptance_path.is_file() else None,
        "result_rows": len(info["rows"]),
        "successful": success_count,
        "failed_or_unavailable": failure_count,
        "_results": results_by_id,
        "_fulltext_paths": fulltext_paths,
    }


def queue_gate(root: Path, batch: dict[str, Any]) -> dict[str, Any]:
    path = root / "02_batches" / batch["batch_id"] / "outputs/fulltext_screening_queue.csv"
    info = csv_info(path)
    gate: dict[str, Any] = {
        "path": rel(path, root) if path.is_file() else None, "exists": path.is_file(),
        "valid": False, "issues": [], "rows": len(info["rows"]),
        "pending_ready": 0, "unresolved": 0,
    }
    if not path.is_file():
        gate["issues"] = ["fulltext_screening_queue.csv is missing"]
        return gate
    issues: list[str] = []
    if info["error"]:
        issues.append(f"queue {info['error']}")
    if info["header"] != FULLTEXT_FIELDS:
        issues.append("queue header does not match the contract")
    manifest_ids = [row.get("item_id", "").strip() for row in batch["_manifest"]]
    manifest_by_id = {
        row.get("item_id", "").strip(): row for row in batch["_manifest"]
        if row.get("item_id", "").strip()
    }
    results_by_id = (batch.get("_acceptance") or {}).get("_results", {})
    accepted_paths = (batch.get("_acceptance") or {}).get("_fulltext_paths", {})
    if any(not value for value in manifest_ids) or len(set(manifest_ids)) != len(manifest_ids):
        issues.append("MANIFEST item_id values must be non-empty and unique")
    queue_ids: set[str] = set()
    pairs: set[tuple[str, str]] = set()
    criteria_by_item: dict[str, set[str]] = {}
    pending = unresolved = 0
    for number, row in enumerate(info["rows"], 2):
        item_id = row.get("item_id", "").strip()
        criterion = row.get("criterion_id", "").strip()
        state = row.get("fulltext_status", "").strip().casefold()
        decision = row.get("decision", "").strip().casefold()
        if not item_id or not criterion:
            issues.append(f"queue row {number} lacks item_id/criterion_id")
        pair = (item_id, criterion)
        if pair in pairs:
            issues.append(f"queue row {number} duplicates {pair}")
        pairs.add(pair)
        if item_id:
            queue_ids.add(item_id)
            criteria_by_item.setdefault(item_id, set()).add(criterion)
        manifest_row = manifest_by_id.get(item_id)
        if manifest_row is not None:
            issues.extend(metadata_mismatches(
                row, manifest_row,
                left_name=f"queue row {number}", right_name="MANIFEST",
            ))
        result_row = results_by_id.get(item_id)
        if result_row is None:
            issues.append(f"queue row {number} has no matching download result")
        else:
            result_status = result_row.get("status", "").strip().casefold()
            result_path_text = result_row.get("pdf_path", "").strip()
            queue_path_text = row.get("fulltext_path", "").strip()
            accepted_path_text = accepted_paths.get(item_id, "")
            result_path = resolve_workspace_path(root, accepted_path_text)
            queue_path = resolve_workspace_path(root, queue_path_text)
            if result_status in DOWNLOAD_SUCCESS:
                if state != "ready":
                    issues.append(f"queue row {number} successful download must be fulltext_status=ready")
                if result_path is None or queue_path is None or result_path != queue_path:
                    issues.append(f"queue row {number} fulltext_path differs from accepted PDF/XML path")
                elif not queue_path.is_file():
                    issues.append(f"queue row {number} fulltext_path does not exist")
            else:
                if state != "fulltext_unavailable":
                    issues.append(f"queue row {number} failed result must be fulltext_unavailable")
                if result_path_text or queue_path_text:
                    failure_path = resolve_workspace_path(root, result_path_text)
                    if failure_path is None or queue_path is None or failure_path != queue_path:
                        issues.append(f"queue row {number} fulltext_path differs from failure result pdf_path")
                    elif not queue_path.is_file():
                        issues.append(f"queue row {number} failure path does not exist")
        if state not in FULLTEXT_STATUSES:
            issues.append(f"queue row {number} has invalid fulltext_status {state!r}")
        if decision and decision not in FULLTEXT_DECISIONS:
            issues.append(f"queue row {number} has invalid decision {decision!r}")
        if state != "ready" and decision != "not_assessed":
            issues.append(f"queue row {number} non-ready text must be not_assessed")
        if state == "ready" and decision in {"", "not_assessed"}:
            pending += 1
        if state != "ready":
            unresolved += 1
        if state == "ready" and decision in {"include", "exclude", "uncertain"}:
            if not row.get("reviewer", "").strip() or not row.get("reviewed_at", "").strip():
                issues.append(f"queue row {number} {decision} lacks reviewer/reviewed_at")
        if decision in {"include", "exclude"}:
            if state != "ready":
                issues.append(f"queue row {number} cannot {decision} non-ready text")
            if not row.get("evidence_quote", "").strip():
                issues.append(f"queue row {number} {decision} lacks evidence_quote")
            locators = [row.get(field, "").strip() for field in (
                "evidence_page", "evidence_section", "evidence_figure",
                "evidence_image_path", "evidence_object_ref",
            )]
            if not any(locators):
                issues.append(f"queue row {number} {decision} lacks evidence locator")
    if set(manifest_ids) != queue_ids:
        issues.append(
            f"queue coverage mismatch: missing={sorted(set(manifest_ids)-queue_ids)} "
            f"extra={sorted(queue_ids-set(manifest_ids))}"
        )
    criterion_sets = {tuple(sorted(values)) for values in criteria_by_item.values()}
    if len(criterion_sets) > 1:
        issues.append("queue does not apply the same criterion set to every manifest item")
    if batch["_manifest"] and not info["rows"]:
        issues.append("queue has no rows")
    gate.update({"valid": not issues, "issues": issues, "pending_ready": pending, "unresolved": unresolved})
    return gate


def manifest_config_issues(manifest: dict[str, Any], query: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    requested = manifest.get("requested_sources")
    requested_names = [str(value).strip() for value in requested] if isinstance(requested, list) else []
    planned = list(query.get("planned_sources") or [])
    if not isinstance(requested, list) or not requested_names or any(not value for value in requested_names):
        issues.append("requested_sources must be a non-empty source list")
    if len(set(requested_names)) != len(requested_names):
        issues.append("requested_sources contains duplicates")
    if requested_names != planned:
        issues.append(f"requested_sources {requested_names} != selected config sources {planned}")
    if manifest.get("complete") is not True:
        issues.append("complete is not true")
    raw_sources = manifest.get("sources")
    if not isinstance(raw_sources, dict):
        issues.append("sources must be an object")
        sources: dict[str, Any] = {}
    else:
        sources = raw_sources
    if set(sources) != set(requested_names):
        issues.append(f"sources keys {sorted(sources)} != requested_sources {sorted(set(requested_names))}")
    planned_queries = dict(query.get("source_queries") or {})
    for source in planned:
        source_info = sources.get(source)
        if not isinstance(source_info, dict):
            issues.append(f"source {source} is missing or invalid")
            continue
        if str(source_info.get("status") or "").strip() != "complete":
            issues.append(f"source {source} status is not complete")
        native = str(source_info.get("native_query") or "").strip() if isinstance(source_info, dict) else ""
        if native != planned_queries.get(source, ""):
            issues.append(f"source {source} native_query differs from selected config")
    return issues


def related_batches(root: Path, task_root: Path, task_id: str,
                    active: dict[str, Any] | None, requested_batch: str | None,
                    query: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    """Resolve batch ownership without trusting a colliding query_id alone."""
    index, issues = batch_index(root)
    pointer_ids: list[str] = []
    if active and isinstance(active.get("batch_ids"), list):
        pointer_ids = [str(value).strip() for value in active["batch_ids"] if str(value).strip()]

    query_id = str(query.get("config_query_id") or "").strip()
    query_valid = bool(query.get("valid"))
    task_runs = run_manifests(task_root, query_id) if query_id else []
    task_runs_by_key: dict[str, list[dict[str, Any]]] = {}
    for run in task_runs:
        task_runs_by_key.setdefault(run["run_key"], []).append(run)

    ownership: dict[str, str] = {}
    for pointer_id in pointer_ids:
        ownership[pointer_id] = "ACTIVE_TASK.batch_ids"
        handoff = batch_handoff(root, pointer_id)
        metadata_tasks = handoff_metadata_task_ids(handoff)
        if len(metadata_tasks) > 1:
            issues.append(
                f"ACTIVE_TASK batch {pointer_id} handoff metadata has conflicting task_ids={sorted(metadata_tasks)}"
            )
        elif metadata_tasks and metadata_tasks != {task_id}:
            issues.append(
                f"ACTIVE_TASK batch {pointer_id} handoff task_id={next(iter(metadata_tasks))} != {task_id}"
            )
        metadata_batches = handoff_metadata_batch_ids(handoff)
        if metadata_batches and metadata_batches != {pointer_id}:
            issues.append(
                f"ACTIVE_TASK batch {pointer_id} handoff metadata has mismatched batch_ids={sorted(metadata_batches)}"
            )
        if handoff["manifest_exists"] and not handoff["manifest"]:
            issues.append(
                f"ACTIVE_TASK batch {pointer_id} copied run_manifest.json is unreadable or empty"
            )
            continue
        if not handoff["manifest_exists"]:
            continue  # Explicit pointer supports legacy/direct batches without search evidence.
        if not query_valid:
            issues.append(
                f"ACTIVE_TASK batch {pointer_id} has copied search evidence but the current task "
                "has no validated selected query"
            )
            continue
        if handoff["query_id"] != query_id:
            issues.append(
                f"ACTIVE_TASK batch {pointer_id} query_id={handoff['query_id']} != selected query_id={query_id}"
            )
            continue
        config_issues = manifest_config_issues(handoff["manifest"], query)
        if config_issues:
            issues.extend(
                f"ACTIVE_TASK batch {pointer_id} copied manifest {issue}" for issue in config_issues
            )
            continue
        matching_runs = task_runs_by_key.get(handoff["run_key"], [])
        if len(matching_runs) != 1:
            issues.append(
                f"ACTIVE_TASK batch {pointer_id} copied manifest does not uniquely match a current task run"
            )
            continue
        source_manifest = str(handoff["summary"].get("search_manifest") or "").strip()
        if source_manifest:
            source_path = Path(source_manifest).expanduser()
            if not source_path.is_absolute():
                source_path = root / source_path
            # The workspace may have been synced or moved across hosts, so a stale
            # absolute provenance path is informational.  If it resolves locally,
            # however, it must identify the exact run whose manifest was copied.
            if source_path.is_file() and source_path.resolve() != matching_runs[0]["path"]:
                issues.append(
                    f"ACTIVE_TASK batch {pointer_id} handoff_summary points to a different search run"
                )

    inferred_ids: list[str] = []
    batches_root = root / "02_batches"
    if query_valid and query_id and batches_root.is_dir():
        owners = query_owners(root, query_id)
        globally_unique = len(owners) == 1 and owners[0].resolve() == task_root.resolve()
        if globally_unique:
            for directory in sorted(batches_root.iterdir()):
                if not directory.is_dir() or directory.name in pointer_ids:
                    continue
                handoff = batch_handoff(root, directory.name)
                if handoff["query_id"] != query_id or not handoff["run_key"]:
                    continue
                metadata_tasks = handoff_metadata_task_ids(handoff)
                if len(metadata_tasks) > 1 or (metadata_tasks and metadata_tasks != {task_id}):
                    continue
                metadata_batches = handoff_metadata_batch_ids(handoff)
                if metadata_batches and metadata_batches != {directory.name}:
                    continue
                if manifest_config_issues(handoff["manifest"], query):
                    continue
                if len(task_runs_by_key.get(handoff["run_key"], [])) == 1:
                    inferred_ids.append(directory.name)
                    ownership[directory.name] = "globally unique query_id + exact copied run"

    ids = list(dict.fromkeys(pointer_ids + inferred_ids))
    if requested_batch:
        if requested_batch not in ids:
            return [], issues + [f"batch {requested_batch} is not safely attributable to task {task_id}"]
        ids = [requested_batch]
    batches = [inspect_batch(root, item, index) for item in ids]
    for batch in batches:
        batch["ownership"] = ownership.get(batch["batch_id"], "unknown")
    return batches, issues


def public_batch(batch: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in batch.items() if not key.startswith("_")}


def public_gate(gate: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in gate.items() if not key.startswith("_")}


def route_batches(root: Path, evidence: dict[str, Any], batches: list[dict[str, Any]],
                  requires_fulltext: bool, explicit_batch: str | None) -> dict[str, Any] | None:
    if not batches:
        return None
    for batch in batches:
        if batch["status"] != "ACCEPTED":
            continue
        acceptance = acceptance_gate(root, batch)
        batch["_acceptance"] = acceptance
        batch["acceptance"] = public_gate(acceptance)
        if not acceptance["valid"]:
            batch["issues"].append("ACCEPTED batch lacks valid download/acceptance evidence")
            continue
        if requires_fulltext:
            batch["queue"] = queue_gate(root, batch)
    evidence["batches"] = [public_batch(batch) for batch in batches]

    actionable: list[dict[str, Any]] = []
    for batch in batches:
        status = batch["status"]
        if batch["issues"] or status in {"READY", "CLAIMED", "RUNNING", "REVIEW", "BLOCKED", "FAILED"}:
            actionable.append(batch)
        elif status == "ACCEPTED" and requires_fulltext:
            queue = batch.get("queue", {})
            if not queue.get("valid") or queue.get("pending_ready"):
                actionable.append(batch)
    evidence["batches"] = [public_batch(batch) for batch in batches]
    if len(actionable) > 1 and not explicit_batch:
        evidence["actionable_batch_ids"] = [batch["batch_id"] for batch in actionable]
        return result(
            root, "AMBIGUOUS_BATCH", "Multiple batches for the current search run require an action.",
            "Re-run with --batch-id <id>; do not update multiple batches implicitly.", evidence,
        )
    if actionable:
        batch = actionable[0]
        status = batch["status"]
        evidence["selected_batch_id"] = batch["batch_id"]
        if batch["issues"]:
            return result(
                root, "BATCH_STATE_ERROR", "The batch violates ownership, index, acceptance, or evidence contracts.",
                "Repair the recorded batch evidence; do not re-enqueue or advance it.", evidence,
            )
        if status == "BLOCKED":
            return result(
                root, "STAGE_2_DOWNLOAD_BLOCKED", "The batch is explicitly BLOCKED.",
                "Resolve its recorded blocker, then use an authorized transition to RUNNING or READY.", evidence,
            )
        if status == "FAILED":
            return result(
                root, "STAGE_2_DOWNLOAD_FAILED", "The batch is explicitly FAILED.",
                "Diagnose the failure; a coordinator may reset this same batch to READY for a justified retry.", evidence,
            )
        if status in {"READY", "CLAIMED", "RUNNING"}:
            action = (
                "Atomically claim this batch, then download and verify its manifest."
                if status == "READY" else
                "Continue this claimed batch and submit verified results to REVIEW."
            )
            return result(root, "STAGE_2_DOWNLOAD", f"The batch is {status}.", action, evidence)
        if status == "REVIEW":
            return result(
                root, "STAGE_2_DOWNLOAD_REVIEW", "The batch awaits coordinator acceptance.",
                "Validate coverage, identities, hashes, pages and failures, then accept or return it.", evidence,
            )
        queue = batch.get("queue", {})
        if not queue.get("exists"):
            return result(
                root, "STAGE_3_CREATE_FULLTEXT_QUEUE",
                "The accepted batch requires screening and has no full-text queue.",
                "Create a queue from the persisted accepted PDF/XML full texts.", evidence,
            )
        if not queue.get("valid"):
            return result(
                root, "STAGE_3_FULLTEXT_SCREENING_INVALID",
                "The full-text queue violates identity, path, coverage, decision, or evidence rules.",
                "Repair the existing queue without discarding valid screening work.", evidence,
            )
        return result(
            root, "STAGE_3_FULLTEXT_SCREENING",
            "The accepted batch still has ready full texts awaiting decisions.",
            "Screen the remaining ready rows and record verifiable evidence and reviewer attribution.", evidence,
        )
    if requires_fulltext:
        return result(
            root, "COMPLETE_OR_HUMAN_REVIEW",
            "Current-run accepted batches have valid queues with no ready row pending.",
            "Review uncertain/unavailable records and approve the evidence decisions.", evidence,
        )
    return result(
        root, "COMPLETE_DOWNLOAD",
        "Current-run batches are accepted with verified acceptance evidence.",
        "Review the accepted download report; no further automated stage is required.", evidence,
    )


def inspect(root: Path, task_id: str | None = None,
            batch_id: str | None = None) -> dict[str, Any]:
    root = root.expanduser().resolve()
    task_root, selected_id, selection, early = resolve_task(root, task_id, batch_id)
    if early:
        return early
    evidence: dict[str, Any] = {"task_selection": selection}
    if task_root is None or selected_id is None:
        return result(
            root, "NOT_STARTED", "No workflow task has been initialized.",
            "Create runtime/tasks/<task_id>/task_brief.json and runtime/ACTIVE_TASK.json.", evidence,
        )
    evidence.update({"task_id": selected_id, "task_root": rel(task_root, root)})
    brief_path = task_root / "task_brief.json"
    brief, brief_issues = validate_brief(brief_path, selected_id)
    evidence.update({
        "task_brief": rel(brief_path, root) if brief_path.is_file() else None,
        "task_brief_issues": brief_issues,
    })
    if brief_issues:
        return result(
            root, "STAGE_MINUS_1_TASK_BRIEF", "The selected task has no valid task brief.",
            "Create or repair its task brief before query design.", evidence,
        )
    requires_fulltext = brief.get("requires_fulltext_screening") is True
    evidence["requires_fulltext_screening"] = requires_fulltext
    query = query_gate(task_root, root)
    evidence["query"] = public_gate(query)
    selected_pointer = selection.get("active_task")
    active = (
        selected_pointer
        if isinstance(selected_pointer, dict)
        and str(selected_pointer.get("task_id") or "").strip() == selected_id
        and not selection.get("active_task_errors")
        else None
    )
    batches, index_issues = related_batches(
        root, task_root, selected_id, active, batch_id, query,
    )
    evidence["batch_index_issues"] = index_issues
    evidence["all_related_batches"] = [public_batch(batch) for batch in batches]
    if index_issues and (batch_id or batches or (active and active.get("batch_ids"))):
        return result(
            root, "BATCH_STATE_ERROR", "The requested batch linkage/index is invalid.",
            "Repair task linkage or BATCH_INDEX before continuing.", evidence,
        )
    if query.get("ambiguous"):
        return result(
            root, "AMBIGUOUS", "The selected task contains multiple query directories.",
            "Select or consolidate one query directory before continuing.", evidence,
        )
    if not query["valid"]:
        routed = route_batches(root, evidence, batches, requires_fulltext, batch_id)
        if routed is not None:
            return routed
        return result(
            root, "STAGE_MINUS_1_QUERY_DESIGN", "The task has not passed the validated-query gate.",
            "Complete one query directory, final_variant, and all live-validation evidence.", evidence,
        )
    search = search_gate(task_root, query, root)
    screening_rows = search.get("_screening_rows", [])
    evidence["search"] = public_gate(search)
    if batch_id:
        routed = route_batches(root, evidence, batches, requires_fulltext, batch_id)
        if routed is not None:
            return routed
    latest_run_key = search.get("run_key")
    current_batches = [
        batch for batch in batches
        if latest_run_key and batch.get("handoff_run_key") == latest_run_key
    ]
    evidence["current_run_batch_ids"] = [batch["batch_id"] for batch in current_batches]
    direct_batches = [batch for batch in batches if not batch.get("handoff_run_key")]
    evidence["direct_pointer_batch_ids"] = [batch["batch_id"] for batch in direct_batches]
    # An explicitly pointed direct/legacy batch is active work even after a query
    # is designed.  Completed direct batches do not mask a newer search run.
    priority_batches = current_batches + [
        batch for batch in direct_batches
        if batch["status"] != "ACCEPTED" or batch["issues"] or not search["exists"]
    ]
    priority_batches = list({batch["batch_id"]: batch for batch in priority_batches}.values())
    if priority_batches:
        routed = route_batches(root, evidence, priority_batches, requires_fulltext, batch_id)
        if routed is not None:
            return routed
    if not search["exists"]:
        return result(
            root, "STAGE_0_API_SEARCH", "The validated query has no matching search run.",
            "Run its selected config through every requested API and exhaust pagination.", evidence,
        )
    if not search["valid"]:
        return result(
            root, "STAGE_0_SEARCH_INCOMPLETE",
            "The search failed completeness, source, CSV-count, or coverage checks.",
            "Repair/resume the same run; do not send partial records to screening.", evidence,
        )
    if search["zero_results"]:
        return result(
            root, "NO_RESULTS", "The complete and internally consistent search returned zero records.",
            "Review translations/live counts and revise and revalidate the query.", evidence,
        )
    screening = screening_gate(screening_rows)
    evidence["screening"] = screening
    if not screening["valid"]:
        return result(
            root, "STAGE_1_TITLE_ABSTRACT_SCREENING",
            "pre_screening.csv is incomplete or would fail handoff validation.",
            "Repair decisions, identities, conflicts and selected-record reasons.", evidence,
        )
    if screening["all_excluded"]:
        return result(
            root, "COMPLETE_NO_DOWNLOAD_TARGETS",
            "Every deduplicated record was validly excluded at title/abstract screening.",
            "Do not create an empty batch; review exclusions or close this search run.", evidence,
        )
    return result(
        root, "STAGE_1_HANDOFF_TO_DOWNLOAD",
        "The latest search run passes screening and has no batch for this run instance.",
        "Handoff download+uncertain records to one READY batch and record its ID in ACTIVE_TASK.", evidence,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="paper workspace root")
    parser.add_argument("--task-id", help="select one task explicitly")
    parser.add_argument("--batch-id", help="select one related batch explicitly")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args()
    root = Path(args.root).expanduser().resolve()
    if not (root / "00_control/WORKSPACE.json").is_file():
        parser.error(f"not a paper download workspace: {root}")
    status = inspect(root, task_id=args.task_id, batch_id=args.batch_id)
    if args.json:
        print(json.dumps(status, ensure_ascii=False, indent=2))
    else:
        print(f"stage: {status['stage']}")
        print(f"reason: {status['reason']}")
        print(f"next_action: {status['next_action']}")
        print("evidence:")
        print(json.dumps(status["evidence"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
