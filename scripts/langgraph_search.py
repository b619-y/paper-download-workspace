#!/usr/bin/env python3
"""Isolate scholarly-search partitions and publish one deterministic run.

This module is intentionally independent of LangGraph.  A graph coordinator can
map ``run_source_partition`` over the configured sources and reduce the returned
JSON-serializable dictionaries with ``publish_search_run``.  The existing
``search_literature.py`` remains the only API adapter implementation.
"""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterable

try:  # Works both as ``python scripts/...`` and when imported as ``scripts.*``.
    from search_literature import (
        CSV_FIELDS,
        PRE_SCREENING_FIELDS,
        SOURCES,
        merge_records,
        pre_screening_rows,
        source_options,
        translate_query,
        utc_now,
        write_csv,
    )
except ModuleNotFoundError:  # pragma: no cover - exercised by package-style callers.
    from scripts.search_literature import (  # type: ignore[no-redef]
        CSV_FIELDS,
        PRE_SCREENING_FIELDS,
        SOURCES,
        merge_records,
        pre_screening_rows,
        source_options,
        translate_query,
        utc_now,
        write_csv,
    )


PARTITION_BINDING_VERSION = "search-partition-binding-v1"
PARTITION_REQUEST_VERSION = "search-partition-request-v1"
PARTITION_RECEIPT_VERSION = "search-partition-receipt-v1"
PUBLICATION_VERSION = "deterministic-search-run-v1"
ALLOWED_SOURCE_STATUSES = {
    "complete", "incomplete", "blocked_missing_credentials", "failed",
}
SENSITIVE_CONFIG_KEYS = {
    "api_key", "apikey", "key", "token", "access_token", "insttoken",
    "password", "secret", "authorization", "cookie",
}


class SearchOrchestrationError(RuntimeError):
    """Raised when a search partition or publication fails closed."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _tree_sha256(root: Path) -> str:
    """Hash regular files and their relative names, rejecting link traversal."""
    if root.is_symlink() or not root.is_dir():
        raise SearchOrchestrationError(f"source tree is not a regular directory: {root}")
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise SearchOrchestrationError(f"source tree contains a symlink: {path}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise SearchOrchestrationError(f"source tree contains a non-file entry: {path}")
        relative = path.relative_to(root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(bytes.fromhex(_sha256_file(path)))
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise SearchOrchestrationError(f"required JSON file is missing or unsafe: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SearchOrchestrationError(f"cannot read JSON file {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SearchOrchestrationError(f"JSON root must be an object: {path}")
    return value


def _contains_sensitive_key(value: Any) -> str | None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).strip().casefold().replace("-", "_")
            if normalized in SENSITIVE_CONFIG_KEYS:
                return str(key)
            found = _contains_sensitive_key(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _contains_sensitive_key(child)
            if found:
                return found
    return None


def _load_config(config_path: str | os.PathLike[str]) -> tuple[Path, dict[str, Any], str]:
    path = Path(config_path).expanduser().resolve()
    config = _read_json(path)
    sensitive = _contains_sensitive_key(config)
    if sensitive:
        raise SearchOrchestrationError(
            f"search config contains credential-like key {sensitive!r}; use environment variables"
        )
    query_id = str(config.get("query_id") or "").strip()
    original_query = str(config.get("original_query") or "").strip()
    source_order = config.get("source_order")
    sources = config.get("sources")
    if not query_id:
        raise SearchOrchestrationError("config.query_id must be non-empty")
    if not original_query:
        raise SearchOrchestrationError("config.original_query must be non-empty")
    if not isinstance(source_order, list) or not source_order:
        raise SearchOrchestrationError("config.source_order must be a non-empty list")
    planned = [str(value).strip() for value in source_order]
    if any(not value for value in planned) or len(set(planned)) != len(planned):
        raise SearchOrchestrationError("config.source_order contains blanks or duplicates")
    unknown = [source for source in planned if source not in SOURCES]
    if unknown:
        raise SearchOrchestrationError(f"config.source_order contains unknown sources: {unknown}")
    if not isinstance(sources, dict):
        raise SearchOrchestrationError("config.sources must be an object")
    if set(sources) != set(planned):
        raise SearchOrchestrationError(
            "config.sources keys must exactly match config.source_order"
        )
    for source in planned:
        options = sources.get(source)
        if not isinstance(options, dict) or not str(options.get("query") or "").strip():
            raise SearchOrchestrationError(f"config source {source} must have a non-empty query")
    return path, config, _sha256_file(path)


def _expected_native_query(config: dict[str, Any], source: str) -> str:
    original = str(config["original_query"]).strip()
    options = source_options(config, source)
    return str(options.get("query") or translate_query(source, original)).strip()


def _read_csv(path: Path, fields: list[str]) -> list[dict[str, str]]:
    if path.is_symlink() or not path.is_file():
        raise SearchOrchestrationError(f"CSV is missing or unsafe: {path}")
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != fields:
                raise SearchOrchestrationError(
                    f"CSV header differs from the canonical schema: {path}"
                )
            return [{field: str(row.get(field) or "") for field in fields} for row in reader]
    except (OSError, UnicodeError, csv.Error) as exc:
        raise SearchOrchestrationError(f"cannot read CSV {path}: {exc}") from exc


def _read_records(path: Path) -> list[dict[str, str]]:
    return _read_csv(path, CSV_FIELDS)


def _partition_raw_page(partition: Path, source: str, value: str) -> tuple[Path, Path]:
    raw_root = (partition / source / "raw").resolve()
    raw_path = Path(value).expanduser()
    if not raw_path.is_absolute():
        raw_path = partition / raw_path
    raw_path = raw_path.resolve()
    try:
        relative = raw_path.relative_to(raw_root)
    except ValueError as exc:
        raise SearchOrchestrationError(
            f"source {source} record raw_page escapes its raw partition"
        ) from exc
    if raw_path.is_symlink() or not raw_path.is_file():
        raise SearchOrchestrationError(f"source {source} record raw_page is missing or unsafe")
    return raw_path, relative


def _validate_partition_manifest(
    partition_dir: Path,
    config: dict[str, Any],
    source: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, str]]]:
    manifest_path = partition_dir / "run_manifest.json"
    manifest = _read_json(manifest_path)
    if str(manifest.get("query_id") or "").strip() != str(config["query_id"]).strip():
        raise SearchOrchestrationError(f"partition {source} query_id does not match config")
    if str(manifest.get("original_query") or "").strip() != str(config["original_query"]).strip():
        raise SearchOrchestrationError(f"partition {source} original_query does not match config")
    if manifest.get("requested_sources") != [source]:
        raise SearchOrchestrationError(
            f"partition {source} requested_sources must equal [{source!r}]"
        )
    sources = manifest.get("sources")
    if not isinstance(sources, dict) or set(sources) != {source}:
        raise SearchOrchestrationError(f"partition {source} manifest sources are not isolated")
    info = sources[source]
    if not isinstance(info, dict):
        raise SearchOrchestrationError(f"partition {source} source summary must be an object")
    status = str(info.get("status") or "").strip()
    if status not in ALLOWED_SOURCE_STATUSES:
        raise SearchOrchestrationError(f"partition {source} has invalid status {status!r}")
    native_query = str(info.get("native_query") or "").strip()
    if native_query != _expected_native_query(config, source):
        raise SearchOrchestrationError(f"partition {source} native_query does not match config")
    if manifest.get("complete") is not (status == "complete"):
        raise SearchOrchestrationError(f"partition {source} complete flag contradicts source status")
    max_records = manifest.get("max_records_per_source")
    if (not isinstance(max_records, int) or isinstance(max_records, bool)
            or max_records < 0):
        raise SearchOrchestrationError(
            f"partition {source} max_records_per_source must be a non-negative integer"
        )

    source_dir = partition_dir / source
    raw_dir = source_dir / "raw"
    if source_dir.is_symlink() or not source_dir.is_dir():
        raise SearchOrchestrationError(f"partition {source} source directory is missing or unsafe")
    if raw_dir.is_symlink() or not raw_dir.is_dir():
        raise SearchOrchestrationError(f"partition {source} raw directory is missing or unsafe")

    records_path = source_dir / "records.csv"
    rows: list[dict[str, str]]
    if records_path.is_file() and not records_path.is_symlink():
        rows = _read_records(records_path)
    elif status in {"complete", "incomplete"}:
        raise SearchOrchestrationError(f"partition {source} status {status} lacks records.csv")
    else:
        rows = []

    fetched = info.get("fetched_count")
    if status in {"complete", "incomplete"}:
        if not isinstance(fetched, int) or isinstance(fetched, bool) or fetched < 0:
            raise SearchOrchestrationError(
                f"partition {source} fetched_count must be a non-negative integer"
            )
        if len(rows) != fetched:
            raise SearchOrchestrationError(
                f"partition {source} records rows {len(rows)} != fetched_count {fetched}"
            )
        pages = info.get("pages")
        if not isinstance(pages, int) or isinstance(pages, bool) or pages < 1:
            raise SearchOrchestrationError(f"partition {source} pages must be positive")
        raw_files = [path for path in raw_dir.rglob("*") if path.is_file()]
        if len(raw_files) < pages:
            raise SearchOrchestrationError(
                f"partition {source} has {len(raw_files)} raw pages, expected at least {pages}"
            )
    else:
        fetched = 0

    if manifest.get("raw_record_count") != fetched:
        raise SearchOrchestrationError(f"partition {source} raw_record_count is inconsistent")
    for row in rows:
        if row.get("source_database") != source:
            raise SearchOrchestrationError(f"partition {source} contains a foreign source row")
        if row.get("search_query") != str(config["original_query"]).strip():
            raise SearchOrchestrationError(f"partition {source} row search_query differs from config")
        if row.get("native_query") != native_query:
            raise SearchOrchestrationError(f"partition {source} row native_query differs from config")
        if not str(row.get("raw_page") or "").strip():
            raise SearchOrchestrationError(f"partition {source} row lacks raw_page")
        _partition_raw_page(partition_dir, source, str(row["raw_page"]))

    top_rows = _read_records(partition_dir / "all_records.csv")
    if top_rows != rows:
        raise SearchOrchestrationError(
            f"partition {source} all_records.csv differs from source records.csv"
        )
    expected_deduplicated = merge_records(rows)
    deduplicated = _read_records(partition_dir / "deduplicated_records.csv")
    if deduplicated != expected_deduplicated:
        raise SearchOrchestrationError(
            f"partition {source} deduplicated_records.csv is inconsistent"
        )
    expected_screening = pre_screening_rows(expected_deduplicated)
    screening = _read_csv(partition_dir / "pre_screening.csv", PRE_SCREENING_FIELDS)
    if screening != expected_screening:
        raise SearchOrchestrationError(f"partition {source} pre_screening.csv is inconsistent")
    if manifest.get("deduplicated_record_count") != len(expected_deduplicated):
        raise SearchOrchestrationError(
            f"partition {source} deduplicated_record_count is inconsistent"
        )
    if manifest.get("abstract_present_count") != sum(
        bool(row.get("abstract")) for row in expected_deduplicated
    ):
        raise SearchOrchestrationError(
            f"partition {source} abstract_present_count is inconsistent"
        )
    reported_total = info.get("reported_total")
    if status == "complete" and isinstance(reported_total, int) and reported_total != fetched:
        raise SearchOrchestrationError(
            f"partition {source} complete count differs from reported_total"
        )
    return manifest, info, rows


def _partition_result(
    partition_dir: Path,
    config: dict[str, Any],
    config_sha256: str,
    source: str,
) -> dict[str, Any]:
    manifest, info, rows = _validate_partition_manifest(partition_dir, config, source)
    manifest_path = partition_dir / "run_manifest.json"
    records_path = partition_dir / source / "records.csv"
    status = str(info["status"])
    records_sha = _sha256_file(records_path) if records_path.is_file() else ""
    tree_sha = _tree_sha256(partition_dir / source)
    binding = {
        "version": PARTITION_BINDING_VERSION,
        "source": source,
        "config_sha256": config_sha256,
        "max_records": int(manifest["max_records_per_source"]),
        "manifest_sha256": _sha256_file(manifest_path),
        "source_tree_sha256": tree_sha,
        "records_sha256": records_sha,
        "native_query_sha256": _sha256_text(str(info["native_query"])),
    }
    return {
        "source": source,
        "status": status,
        "complete": status == "complete",
        "partition_dir": str(partition_dir),
        "manifest_path": "run_manifest.json",
        "records_path": f"{source}/records.csv",
        "config_sha256": config_sha256,
        "manifest_sha256": binding["manifest_sha256"],
        "records_sha256": records_sha,
        "source_tree_sha256": tree_sha,
        "query_id": str(manifest["query_id"]),
        "max_records": int(manifest["max_records_per_source"]),
        "fetched_count": len(rows),
        "pages": int(info.get("pages") or 0),
        "binding": binding,
    }


def _partition_request(
    config: dict[str, Any], config_sha256: str, source: str, max_records: int
) -> dict[str, Any]:
    return {
        "version": PARTITION_REQUEST_VERSION,
        "source": source,
        "query_id": str(config["query_id"]),
        "config_sha256": config_sha256,
        "max_records": max_records,
    }


def _partition_receipt(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": PARTITION_RECEIPT_VERSION,
        "source": result["source"],
        "query_id": result["query_id"],
        "config_sha256": result["config_sha256"],
        "max_records": result["max_records"],
        "manifest_sha256": result["manifest_sha256"],
        "records_sha256": result["records_sha256"],
        "source_tree_sha256": result["source_tree_sha256"],
        "binding": result["binding"],
    }


def _resume_partition(
    root: Path,
    config: dict[str, Any],
    config_sha256: str,
    source: str,
    max_records: int,
) -> dict[str, Any] | None:
    """Recover a completed pre-checkpoint partition without repeating its API call."""
    expected_request = _partition_request(config, config_sha256, source, max_records)
    candidates = sorted(
        (
            path for path in root.iterdir()
            if path.name.startswith(f"{source}-") and (path.is_dir() or path.is_symlink())
        ),
        key=lambda path: path.name,
    )
    if not candidates:
        return None

    reusable: list[dict[str, Any]] = []
    problems: list[str] = []
    for candidate in candidates:
        if candidate.is_symlink() or not candidate.is_dir():
            problems.append(f"unsafe partition entry {candidate.name}")
            continue
        request_path = candidate / "partition_request.json"
        try:
            request = _read_json(request_path)
        except SearchOrchestrationError as exc:
            problems.append(f"{candidate.name}: {exc}")
            continue
        if request != expected_request:
            problems.append(f"{candidate.name}: request binding differs from this attempt")
            continue
        try:
            result = _partition_result(candidate, config, config_sha256, source)
        except SearchOrchestrationError as exc:
            problems.append(f"{candidate.name}: incomplete or invalid partition ({exc})")
            continue
        if result["max_records"] != max_records:
            problems.append(f"{candidate.name}: max_records differs from its request")
            continue
        expected_receipt = _partition_receipt(result)
        receipt_path = candidate / "partition_receipt.json"
        if receipt_path.exists() or receipt_path.is_symlink():
            try:
                receipt = _read_json(receipt_path)
            except SearchOrchestrationError as exc:
                problems.append(f"{candidate.name}: {exc}")
                continue
            if receipt != expected_receipt:
                problems.append(f"{candidate.name}: receipt binding is stale")
                continue
        else:
            # The API subprocess finished, but the process died before the graph
            # checkpoint/receipt write.  Adopt only the fully validated output.
            _atomic_write_json(receipt_path, expected_receipt)
        reusable.append(result)

    if problems:
        raise SearchOrchestrationError(
            f"cannot safely resume source {source}: " + "; ".join(problems)
        )
    if len(reusable) != 1:
        raise SearchOrchestrationError(
            f"cannot safely resume source {source}: found {len(reusable)} valid partitions"
        )
    return reusable[0]


def run_source_partition(
    config_path: str | os.PathLike[str],
    source: str,
    work_root: str | os.PathLike[str],
    max_records: int = 0,
    python_executable: str = sys.executable,
) -> dict[str, Any]:
    """Run one configured source in a unique directory and return its binding.

    Source-level API failures are normal results because ``--allow-incomplete``
    makes ``search_literature.py`` emit an auditable manifest.  Process failures
    and malformed/tampered output raise :class:`SearchOrchestrationError`.
    """
    config_file, config, config_sha = _load_config(config_path)
    source = str(source).strip()
    if source not in config["source_order"]:
        raise SearchOrchestrationError(f"source {source!r} is not planned by the config")
    if not isinstance(max_records, int) or isinstance(max_records, bool) or max_records < 0:
        raise SearchOrchestrationError("max_records must be a non-negative integer")
    executable = str(python_executable or "").strip()
    if not executable:
        raise SearchOrchestrationError("python_executable must be non-empty")

    root = Path(work_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or not root.is_dir():
        raise SearchOrchestrationError(f"work_root is not a regular directory: {root}")
    lock_dir = root / ".locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{source}.lock"
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        resumed = _resume_partition(
            root, config, config_sha, source, max_records
        )
        if resumed is not None:
            return resumed

        partition_dir = Path(tempfile.mkdtemp(prefix=f"{source}-", dir=root))
        _atomic_write_json(
            partition_dir / "partition_request.json",
            _partition_request(config, config_sha, source, max_records),
        )
        command = [
            executable,
            str(Path(__file__).with_name("search_literature.py")),
            "--config", str(config_file),
            "--source", source,
            "--output-dir", str(partition_dir),
            "--max-records", str(max_records),
            "--allow-incomplete",
        ]
        try:
            completed = subprocess.run(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
        except OSError as exc:
            raise SearchOrchestrationError(
                f"could not start search_literature.py for source {source}: {exc}"
            ) from exc
        if completed.returncode != 0:
            # Do not persist or echo child output: it is not part of the trusted
            # state contract and an arbitrary Python launcher could print keys.
            raise SearchOrchestrationError(
                f"search_literature.py failed for source {source} with exit code "
                f"{completed.returncode}"
            )
        result = _partition_result(partition_dir, config, config_sha, source)
        if result["max_records"] != max_records:
            raise SearchOrchestrationError(
                f"partition {source} max_records differs from the invocation"
            )
        _atomic_write_json(
            partition_dir / "partition_receipt.json",
            _partition_receipt(result),
        )
        return result


def _validated_source_result(
    result: dict[str, Any],
    config: dict[str, Any],
    config_sha256: str,
) -> tuple[str, Path, dict[str, Any], dict[str, Any], list[dict[str, str]]]:
    if not isinstance(result, dict):
        raise SearchOrchestrationError("each source result must be an object")
    source = str(result.get("source") or "").strip()
    if source not in config["source_order"]:
        raise SearchOrchestrationError(f"source result {source!r} is not planned")
    if result.get("config_sha256") != config_sha256:
        raise SearchOrchestrationError(f"source result {source} has a stale config binding")
    if result.get("manifest_path") != "run_manifest.json":
        raise SearchOrchestrationError(f"source result {source} has an unsafe manifest path")
    if result.get("records_path") != f"{source}/records.csv":
        raise SearchOrchestrationError(f"source result {source} has an unsafe records path")
    raw_partition = result.get("partition_dir")
    if not isinstance(raw_partition, str) or not raw_partition.strip():
        raise SearchOrchestrationError(f"source result {source} lacks partition_dir")
    partition = Path(raw_partition).expanduser().resolve()
    if partition.is_symlink() or not partition.is_dir():
        raise SearchOrchestrationError(f"source result {source} partition is missing or unsafe")
    manifest, info, rows = _validate_partition_manifest(partition, config, source)

    manifest_sha = _sha256_file(partition / "run_manifest.json")
    records_path = partition / source / "records.csv"
    records_sha = _sha256_file(records_path) if records_path.is_file() else ""
    tree_sha = _tree_sha256(partition / source)
    expected_binding = {
        "version": PARTITION_BINDING_VERSION,
        "source": source,
        "config_sha256": config_sha256,
        "max_records": int(manifest["max_records_per_source"]),
        "manifest_sha256": manifest_sha,
        "source_tree_sha256": tree_sha,
        "records_sha256": records_sha,
        "native_query_sha256": _sha256_text(str(info["native_query"])),
    }
    if result.get("binding") != expected_binding:
        raise SearchOrchestrationError(f"source result {source} binding summary is stale")
    scalar_checks = {
        "status": str(info["status"]),
        "complete": str(info["status"]) == "complete",
        "manifest_sha256": manifest_sha,
        "records_sha256": records_sha,
        "source_tree_sha256": tree_sha,
        "query_id": str(manifest["query_id"]),
        "fetched_count": len(rows),
        "pages": int(info.get("pages") or 0),
    }
    for key, expected in scalar_checks.items():
        if result.get(key) != expected:
            raise SearchOrchestrationError(f"source result {source} field {key} is stale")
    if "max_records" in result and result.get("max_records") != int(
        manifest["max_records_per_source"]
    ):
        raise SearchOrchestrationError(f"source result {source} field max_records is stale")
    return source, partition, manifest, info, rows


def _canonical_rows(
    rows: Iterable[dict[str, str]],
    partition: Path,
    source: str,
    output_dir: Path,
) -> list[dict[str, str]]:
    canonical: list[dict[str, str]] = []
    for original in rows:
        row = dict(original)
        raw_value = str(row.get("raw_page") or "").strip()
        if not raw_value:
            raise SearchOrchestrationError(f"source {source} record lacks raw_page")
        _, relative = _partition_raw_page(partition, source, raw_value)
        destination = output_dir / source / "raw" / relative
        row["raw_page"] = destination.relative_to(output_dir).as_posix()
        canonical.append(row)
    return canonical


def _safe_source_summary(
    info: dict[str, Any],
    *,
    partition_manifest_sha256: str,
    partition_source_tree_sha256: str,
) -> dict[str, Any]:
    status = str(info["status"])
    summary: dict[str, Any] = {
        "status": status,
        "native_query": str(info["native_query"]),
        "started_at": str(info.get("started_at") or ""),
        "finished_at": str(info.get("finished_at") or ""),
        "reported_total": info.get("reported_total") if isinstance(info.get("reported_total"), int) else None,
        "fetched_count": int(info.get("fetched_count") or 0),
        "complete": status == "complete",
        "pages": int(info.get("pages") or 0),
        "note": str(info.get("note") or ""),
        "partition_manifest_sha256": partition_manifest_sha256,
        "partition_source_tree_sha256": partition_source_tree_sha256,
    }
    if status in {"blocked_missing_credentials", "failed"}:
        # Never copy arbitrary exception text into durable state.
        summary["error_code"] = status
    return summary


def _artifact_entry(path: Path, rows: int | None = None) -> dict[str, Any]:
    value: dict[str, Any] = {"sha256": _sha256_file(path)}
    if rows is not None:
        value["rows"] = rows
    return value


def _validate_existing_publication(
    output_dir: Path,
    config: dict[str, Any],
    config_sha256: str,
    orchestration_id: str,
    search_attempt: int,
    partition_bindings: list[dict[str, Any]],
    publication_fingerprint: str,
    max_records_per_source: int,
) -> dict[str, Any]:
    if output_dir.is_symlink() or not output_dir.is_dir():
        raise SearchOrchestrationError(f"publication target is not a regular directory: {output_dir}")
    manifest_path = output_dir / "run_manifest.json"
    manifest = _read_json(manifest_path)
    identity = (
        manifest.get("orchestration_id") == orchestration_id
        and manifest.get("search_attempt") == search_attempt
        and manifest.get("config_sha256") == config_sha256
    )
    if not identity:
        raise SearchOrchestrationError(
            "publication target already exists with a different orchestration binding"
        )
    if manifest.get("publication_version") != PUBLICATION_VERSION:
        raise SearchOrchestrationError("existing publication has an unsupported version")
    if manifest.get("partition_bindings") != partition_bindings:
        raise SearchOrchestrationError(
            "publication target already exists with different partition bindings"
        )
    if manifest.get("publication_fingerprint") != publication_fingerprint:
        raise SearchOrchestrationError(
            "publication target already exists with a different publication fingerprint"
        )
    if manifest.get("max_records_per_source") != max_records_per_source:
        raise SearchOrchestrationError(
            "publication target already exists with a different max_records binding"
        )
    if manifest.get("query_id") != config["query_id"]:
        raise SearchOrchestrationError("existing publication query_id differs from config")
    if manifest.get("requested_sources") != config["source_order"]:
        raise SearchOrchestrationError("existing publication source order differs from config")
    sources = manifest.get("sources")
    if not isinstance(sources, dict) or list(sources) != list(config["source_order"]):
        raise SearchOrchestrationError("existing publication sources are incomplete or reordered")
    expected_complete = all(
        isinstance(sources.get(source), dict)
        and sources[source].get("status") == "complete"
        for source in config["source_order"]
    )
    if manifest.get("complete") is not expected_complete:
        raise SearchOrchestrationError("existing publication complete flag is inconsistent")

    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise SearchOrchestrationError("existing publication lacks artifact bindings")
    basic = (
        "all_records.csv", "deduplicated_records.csv", "pre_screening.csv",
    )
    for relative in basic:
        entry = artifacts.get(relative)
        path = output_dir / relative
        if (not isinstance(entry, dict) or path.is_symlink() or not path.is_file()
                or entry.get("sha256") != _sha256_file(path)):
            raise SearchOrchestrationError(f"existing publication artifact is missing or stale: {relative}")
    source_artifacts = artifacts.get("source_records")
    raw_artifacts = artifacts.get("source_raw")
    if not isinstance(source_artifacts, dict) or not isinstance(raw_artifacts, dict):
        raise SearchOrchestrationError("existing publication lacks source artifact bindings")
    for source in config["source_order"]:
        records = output_dir / source / "records.csv"
        raw = output_dir / source / "raw"
        record_entry = source_artifacts.get(source)
        raw_entry = raw_artifacts.get(source)
        if (not isinstance(record_entry, dict) or records.is_symlink() or not records.is_file()
                or record_entry.get("sha256") != _sha256_file(records)):
            raise SearchOrchestrationError(f"existing publication source records are stale: {source}")
        if (not isinstance(raw_entry, dict) or raw.is_symlink() or not raw.is_dir()
                or raw_entry.get("sha256") != _tree_sha256(raw)):
            raise SearchOrchestrationError(f"existing publication raw pages are stale: {source}")
        rows = _read_records(records)
        if record_entry.get("rows") != len(rows):
            raise SearchOrchestrationError(f"existing publication source row count is stale: {source}")
        for row in rows:
            raw_page = output_dir / str(row.get("raw_page") or "")
            try:
                raw_page.resolve().relative_to(raw.resolve())
            except ValueError as exc:
                raise SearchOrchestrationError(
                    f"existing publication raw_page escapes source {source}"
                ) from exc
            if raw_page.is_symlink() or not raw_page.is_file():
                raise SearchOrchestrationError(
                    f"existing publication raw_page is missing for source {source}"
                )
    return {
        "output_dir": str(output_dir),
        "manifest_path": "run_manifest.json",
        "manifest_sha256": _sha256_file(manifest_path),
        "reused": True,
        "complete": bool(manifest["complete"]),
        "raw_record_count": int(manifest["raw_record_count"]),
        "deduplicated_record_count": int(manifest["deduplicated_record_count"]),
        "abstract_present_count": int(manifest.get("abstract_present_count") or 0),
        "source_status": {
            source: str(sources[source]["status"]) for source in config["source_order"]
        },
        "orchestration_id": orchestration_id,
        "search_attempt": search_attempt,
        "config_sha256": config_sha256,
        "publication_fingerprint": publication_fingerprint,
    }


def publish_search_run(
    config_path: str | os.PathLike[str],
    source_results: Iterable[dict[str, Any]],
    output_dir: str | os.PathLike[str],
    orchestration_id: str,
    search_attempt: int,
) -> dict[str, Any]:
    """Validate isolated partitions, merge deterministically, and publish atomically."""
    _, config, config_sha = _load_config(config_path)
    orchestration_id = str(orchestration_id or "").strip()
    if not orchestration_id:
        raise SearchOrchestrationError("orchestration_id must be non-empty")
    if (not isinstance(search_attempt, int) or isinstance(search_attempt, bool)
            or search_attempt < 1):
        raise SearchOrchestrationError("search_attempt must be a positive integer")

    requested_output = Path(output_dir).expanduser()
    if not requested_output.name:
        raise SearchOrchestrationError("output_dir must name a run directory")
    parent = requested_output.parent.resolve()
    target = parent / requested_output.name
    parent.mkdir(parents=True, exist_ok=True)

    results = list(source_results)
    if len(results) != len(config["source_order"]):
        raise SearchOrchestrationError("source results do not cover every planned source exactly once")
    indexed: dict[str, dict[str, Any]] = {}
    for result in results:
        if not isinstance(result, dict):
            raise SearchOrchestrationError("each source result must be an object")
        source = str(result.get("source") or "").strip()
        if source in indexed:
            raise SearchOrchestrationError(f"duplicate source result: {source}")
        indexed[source] = result
    if set(indexed) != set(config["source_order"]):
        raise SearchOrchestrationError("source result names differ from config.source_order")

    validated: dict[
        str, tuple[str, Path, dict[str, Any], dict[str, Any], list[dict[str, str]]]
    ] = {}
    for source in config["source_order"]:
        validated[source] = _validated_source_result(indexed[source], config, config_sha)

    partition_bindings = [
        dict(indexed[source]["binding"]) for source in config["source_order"]
    ]
    max_records_values = {
        int(binding["max_records"]) for binding in partition_bindings
    }
    if len(max_records_values) != 1:
        raise SearchOrchestrationError(
            "source partitions use different max_records values"
        )
    max_records_per_source = next(iter(max_records_values))
    publication_fingerprint = _sha256_json({
        "publication_version": PUBLICATION_VERSION,
        "orchestration_id": orchestration_id,
        "search_attempt": search_attempt,
        "config_sha256": config_sha,
        "partition_bindings": partition_bindings,
    })
    if target.exists() or target.is_symlink():
        return _validate_existing_publication(
            target,
            config,
            config_sha,
            orchestration_id,
            search_attempt,
            partition_bindings,
            publication_fingerprint,
            max_records_per_source,
        )

    temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}.tmp-", dir=parent))
    published = False
    try:
        all_rows: list[dict[str, str]] = []
        source_summaries: dict[str, dict[str, Any]] = {}
        source_record_counts: dict[str, int] = {}
        source_raw_hashes: dict[str, str] = {}
        partition_tree_before = {
            source: str(indexed[source]["source_tree_sha256"])
            for source in config["source_order"]
        }

        for source in config["source_order"]:
            _, partition, _, info, rows = validated[source]
            source_from = partition / source
            source_to = temporary / source
            shutil.copytree(source_from, source_to, symlinks=False)
            if _tree_sha256(source_from) != partition_tree_before[source]:
                raise SearchOrchestrationError(
                    f"source partition changed while publishing: {source}"
                )
            canonical_rows = _canonical_rows(rows, partition, source, temporary)
            # blocked/failed partitions do not receive records.csv from the API
            # runner; publish an empty canonical table so the artifact is explicit.
            write_csv(source_to / "records.csv", CSV_FIELDS, canonical_rows)
            all_rows.extend(canonical_rows)
            source_record_counts[source] = len(canonical_rows)
            source_raw_hashes[source] = _tree_sha256(source_to / "raw")
            source_summaries[source] = _safe_source_summary(
                info,
                partition_manifest_sha256=str(indexed[source]["manifest_sha256"]),
                partition_source_tree_sha256=partition_tree_before[source],
            )

        deduplicated = merge_records(all_rows)
        screening = pre_screening_rows(deduplicated)
        write_csv(temporary / "all_records.csv", CSV_FIELDS, all_rows)
        write_csv(temporary / "deduplicated_records.csv", CSV_FIELDS, deduplicated)
        write_csv(temporary / "pre_screening.csv", PRE_SCREENING_FIELDS, screening)
        complete = all(
            source_summaries[source]["status"] == "complete"
            for source in config["source_order"]
        )
        started_values = [
            str(source_summaries[source].get("started_at") or "")
            for source in config["source_order"]
            if str(source_summaries[source].get("started_at") or "")
        ]
        finished_values = [
            str(source_summaries[source].get("finished_at") or "")
            for source in config["source_order"]
            if str(source_summaries[source].get("finished_at") or "")
        ]
        artifacts: dict[str, Any] = {
            "all_records.csv": _artifact_entry(temporary / "all_records.csv", len(all_rows)),
            "deduplicated_records.csv": _artifact_entry(
                temporary / "deduplicated_records.csv", len(deduplicated)
            ),
            "pre_screening.csv": _artifact_entry(
                temporary / "pre_screening.csv", len(screening)
            ),
            "source_records": {
                source: _artifact_entry(
                    temporary / source / "records.csv", source_record_counts[source]
                )
                for source in config["source_order"]
            },
            "source_raw": {
                source: {"sha256": source_raw_hashes[source]}
                for source in config["source_order"]
            },
        }
        manifest = {
            "publication_version": PUBLICATION_VERSION,
            "orchestration_id": orchestration_id,
            "search_attempt": search_attempt,
            "config_sha256": config_sha,
            "max_records_per_source": max_records_per_source,
            "partition_bindings": partition_bindings,
            "publication_fingerprint": publication_fingerprint,
            "query_id": config["query_id"],
            "original_query": config["original_query"],
            "run_id": target.name,
            "started_at": min(started_values) if started_values else utc_now(),
            "finished_at": max(finished_values) if finished_values else utc_now(),
            "requested_sources": list(config["source_order"]),
            "sources": source_summaries,
            "raw_record_count": len(all_rows),
            "deduplicated_record_count": len(deduplicated),
            "abstract_present_count": sum(bool(row.get("abstract")) for row in deduplicated),
            "complete": complete,
            "artifacts": artifacts,
        }
        (temporary / "run_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

        try:
            temporary.rename(target)
            published = True
        except FileExistsError:
            # A concurrent identical publisher may have won.  It is reusable
            # only after the same fail-closed checks as a pre-existing target.
            return _validate_existing_publication(
                target,
                config,
                config_sha,
                orchestration_id,
                search_attempt,
                partition_bindings,
                publication_fingerprint,
                max_records_per_source,
            )
    finally:
        if not published and temporary.exists():
            shutil.rmtree(temporary)

    manifest_path = target / "run_manifest.json"
    return {
        "output_dir": str(target),
        "manifest_path": "run_manifest.json",
        "manifest_sha256": _sha256_file(manifest_path),
        "reused": False,
        "complete": complete,
        "raw_record_count": len(all_rows),
        "deduplicated_record_count": len(deduplicated),
        "abstract_present_count": sum(bool(row.get("abstract")) for row in deduplicated),
        "source_status": {
            source: str(source_summaries[source]["status"])
            for source in config["source_order"]
        },
        "orchestration_id": orchestration_id,
        "search_attempt": search_attempt,
        "config_sha256": config_sha,
        "publication_fingerprint": publication_fingerprint,
    }


__all__ = [
    "SearchOrchestrationError",
    "publish_search_run",
    "run_source_partition",
]
