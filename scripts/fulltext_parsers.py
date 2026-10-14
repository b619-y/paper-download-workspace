#!/usr/bin/env python3
"""Queue-friendly single-step full-text parsers.

PDF jobs use MinerU API v4.  One call submits *or* polls/materializes; it never
waits in a long polling loop.  XML jobs are converted locally.  All durable
records bind the parsed output to the SHA-256 of the accepted source document.
"""
from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol
import xml.etree.ElementTree as ET

import paper_section_chunking as section_chunking


MINERU_API_BASE = "https://mineru.net/api/v4"
MINERU_TOKEN_ENV_WHITELIST = (
    "MINERU_API_TOKEN", "MINERU_API_TOKEN_2", "MINERU_API_TOKEN_3", "MINERU_API_TOKEN_4",
)
MAX_PDF_BYTES = 200 * 1024 * 1024
MAX_PDF_PAGES = 200
MIN_MARKDOWN_BYTES = 200
MAX_ARCHIVE_MEMBERS = 10_000
MAX_ARCHIVE_COMPRESSED_BYTES = 512 * 1024 * 1024
MAX_ARCHIVE_UNCOMPRESSED_BYTES = 1024 * 1024 * 1024
MAX_ARCHIVE_MEMBER_BYTES = 512 * 1024 * 1024
MAX_ARCHIVE_COMPRESSION_RATIO = 1000
MAX_MARKDOWN_BYTES = 128 * 1024 * 1024
SECTION_CHUNKS_FILENAME = "section_chunks.jsonl"
ERROR_MARKERS = (
    "parsing failed", "access denied", "403 forbidden", "404 not found",
    "<!doctype html", "<html", "just a moment",
)
HARD_REMOTE_FAILURE = re.compile(
    r"pages?.*(exceed|limit)|unsupported|invalid file|file type|file size|too large|"
    r"encrypted|password|retry limit", re.I,
)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _guard(lease_guard: Callable[[], None] | None) -> None:
    if lease_guard is not None:
        lease_guard()


def _publish(
    temporary: Path, destination: Path,
    lease_guard: Callable[[], None] | None,
) -> None:
    """Publish through the worker's transactional lease fence when available."""
    owner = getattr(lease_guard, "__self__", None)
    publisher = getattr(owner, "publish", None)
    if callable(publisher):
        publisher(temporary, destination)
        return
    _guard(lease_guard)
    os.replace(temporary, destination)


def atomic_json(
    path: Path, value: dict[str, Any], *, lease_guard: Callable[[], None] | None = None,
) -> None:
    atomic_text(
        path, json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        lease_guard=lease_guard,
    )


def atomic_text(
    path: Path, value: str, *, lease_guard: Callable[[], None] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp-{uuid.uuid4().hex}")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        _publish(temporary, path, lease_guard)
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def read_checkpoint(path: Path) -> dict[str, Any]:
    """Read an existing checkpoint strictly so corruption cannot resubmit work."""
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"checkpoint is unreadable or invalid JSON: {exc}") from exc
    if not isinstance(value, dict) or not value:
        raise ValueError("checkpoint must be a non-empty JSON object")
    return value


def prepare_mineru_retry(
    output_dir: Path, *, job_id: str, source_sha256: str, policy: str,
    actor: str, reason: str,
) -> None:
    """Durably apply an audited remote-checkpoint policy before queue retry."""
    if policy == "resume":
        return
    if policy not in {"refresh-archive", "resubmit"}:
        raise ValueError(f"unsupported MinerU retry policy: {policy}")
    state_path = output_dir / "state.json"
    try:
        state = read_checkpoint(state_path)
    except ValueError as exc:
        raise ValueError(f"MinerU retry checkpoint is invalid: {exc}") from exc
    if not state:
        raise ValueError("MinerU retry policy requires an existing state.json")
    if state.get("job_id") != job_id or state.get("source_sha256") != source_sha256:
        raise ValueError("MinerU retry checkpoint binding mismatch")
    previous_batch = str(state.get("remote_batch_id") or "")
    history = state.get("retry_history")
    if not isinstance(history, list):
        history = []
    history.append({
        "at": now(), "actor": actor, "policy": policy,
        "reason": " ".join(str(reason).split())[:500],
        "previous_remote_batch_id": previous_batch,
    })
    state["retry_history"] = history[-50:]
    state.pop("remote_zip_url", None)
    state.pop("remote_done_at", None)
    if policy == "refresh-archive":
        if not previous_batch:
            raise ValueError("refresh-archive requires an existing remote_batch_id")
        state["status"] = "submitted"
    else:
        for field in (
            "remote_batch_id", "remote_upload_url", "remote_data_id",
            "submitted_at", "uploaded_at", "last_remote_state", "polled_at",
            "submit_intent_id", "submit_intent_at", "submit_http_status", "submit_error",
        ):
            state.pop(field, None)
        state["status"] = "retry_resubmit_pending"
    atomic_json(state_path, state)


def redact(value: str) -> str:
    """Remove every configured token value before persisting an error."""
    clean = str(value)
    for name in MINERU_TOKEN_ENV_WHITELIST:
        secret = os.environ.get(name, "")
        if secret:
            clean = clean.replace(secret, "[REDACTED]")
    clean = re.sub(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+", r"\1[REDACTED]", clean)
    return clean[:1000]


def valid_markdown(path: Path) -> tuple[bool, str]:
    if not path.is_file():
        return False, "missing full.md"
    size = path.stat().st_size
    if size < MIN_MARKDOWN_BYTES:
        return False, f"full.md is too small ({size} bytes)"
    head = path.read_text(encoding="utf-8", errors="ignore")[:4096].casefold()
    marker = next((item for item in ERROR_MARKERS if item in head), "")
    if marker:
        return False, f"full.md contains error marker {marker!r}"
    return True, "ok"


def best_effort_pdf_pages(path: Path) -> int | None:
    command = shutil.which("pdfinfo") or "/opt/homebrew/bin/pdfinfo"
    if not Path(command).is_file() and shutil.which("pdfinfo") is None:
        return None
    try:
        proc = subprocess.run(
            [command, str(path)], capture_output=True, text=True, timeout=60, check=False,
        )
        match = re.search(r"^Pages:\s+(\d+)", proc.stdout, re.MULTILINE)
        return int(match.group(1)) if match else None
    except (OSError, subprocess.SubprocessError):
        return None


def validate_pdf(path: Path, expected_sha256: str) -> tuple[bool, str, int | None]:
    if not path.is_file():
        return False, "source PDF is missing", None
    size = path.stat().st_size
    if size > MAX_PDF_BYTES:
        return False, f"source PDF exceeds the 200 MB MinerU gate ({size} bytes)", None
    with path.open("rb") as handle:
        if handle.read(4) != b"%PDF":
            return False, "source does not have a PDF header", None
    actual = sha256(path)
    if actual != expected_sha256:
        return False, "source SHA-256 no longer matches the accepted download", None
    pages = best_effort_pdf_pages(path)
    if pages is not None and pages > MAX_PDF_PAGES:
        return False, f"source has {pages} pages; split it to at most 200 pages before MinerU", pages
    return True, "ok", pages


@dataclass(frozen=True)
class ParserOutcome:
    status: str  # waiting_remote | review | blocked | failed
    code: str = ""
    message: str = ""
    retryable: bool = False
    poll_after_seconds: int = 30
    parsed_text_path: str = ""
    parsed_text_sha256: str = ""
    sections_path: str = ""
    sections_sha256: str = ""
    sections_count: int = 0
    state_path: str = ""
    state_sha256: str = ""
    result_path: str = ""
    result_sha256: str = ""


class HTTPTransport(Protocol):
    def post_json(self, url: str, headers: dict[str, str], payload: dict[str, Any], timeout: int) \
            -> tuple[int, dict[str, Any], str]: ...

    def get_json(self, url: str, headers: dict[str, str], timeout: int) \
            -> tuple[int, dict[str, Any], str]: ...

    def upload_file(self, url: str, path: Path, timeout: int) -> int: ...

    def download_file(self, url: str, path: Path, timeout: int) -> int: ...


class UrllibTransport:
    """Small stdlib transport; upload/download bodies are streamed from/to disk."""

    @staticmethod
    def _json_request(
        method: str, url: str, headers: dict[str, str], payload: dict[str, Any] | None,
        timeout: int,
    ) -> tuple[int, dict[str, Any], str]:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", errors="replace")
                status = response.status
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            status = exc.code
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {}
        return status, body if isinstance(body, dict) else {}, raw[:1000]

    def post_json(self, url: str, headers: dict[str, str], payload: dict[str, Any], timeout: int) \
            -> tuple[int, dict[str, Any], str]:
        return self._json_request("POST", url, headers, payload, timeout)

    def get_json(self, url: str, headers: dict[str, str], timeout: int) \
            -> tuple[int, dict[str, Any], str]:
        return self._json_request("GET", url, headers, None, timeout)

    def upload_file(self, url: str, path: Path, timeout: int) -> int:
        parsed = urllib.parse.urlsplit(url)
        connection_class = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        connection = connection_class(parsed.hostname, parsed.port, timeout=timeout)
        target = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
        connection.putrequest("PUT", target)
        connection.putheader("Content-Length", str(path.stat().st_size))
        connection.endheaders()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                connection.send(chunk)
        response = connection.getresponse()
        status = response.status
        response.read()
        connection.close()
        return status

    def download_file(self, url: str, path: Path, timeout: int) -> int:
        request = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response, path.open("wb") as handle:
                length = response.headers.get("Content-Length")
                if length:
                    try:
                        if int(length) > MAX_ARCHIVE_COMPRESSED_BYTES:
                            raise ValueError(
                                "archive Content-Length exceeds compressed-size limit")
                    except ValueError as exc:
                        path.unlink(missing_ok=True)
                        raise ValueError("invalid or oversized archive Content-Length") from exc
                received = 0
                for chunk in iter(lambda: response.read(1024 * 1024), b""):
                    received += len(chunk)
                    if received > MAX_ARCHIVE_COMPRESSED_BYTES:
                        path.unlink(missing_ok=True)
                        raise ValueError("archive download exceeds compressed-size limit")
                    handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
                return response.status
        except urllib.error.HTTPError as exc:
            return exc.code


def _token(preferred_name: str = "") -> tuple[str, str]:
    names = ([preferred_name] if preferred_name else []) + list(MINERU_TOKEN_ENV_WHITELIST)
    seen: set[str] = set()
    for name in names:
        if name in seen or name not in MINERU_TOKEN_ENV_WHITELIST:
            continue
        seen.add(name)
        value = (os.environ.get(name) or "").strip()
        if value:
            return name, value
    raise RuntimeError(
        "no MinerU token found in the allowed environment variables: "
        + ", ".join(MINERU_TOKEN_ENV_WHITELIST)
    )


def _auth(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def safe_extract_zip(archive: Path, destination: Path) -> int:
    """Extract a bounded archive after traversal, type, CRC, and bomb checks."""
    if archive.stat().st_size > MAX_ARCHIVE_COMPRESSED_BYTES:
        raise RuntimeError(
            f"MinerU archive exceeds {MAX_ARCHIVE_COMPRESSED_BYTES} compressed bytes")
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as zipped:
        members = zipped.infolist()
        if len(members) > MAX_ARCHIVE_MEMBERS:
            raise RuntimeError(
                f"MinerU archive has {len(members)} members; limit is {MAX_ARCHIVE_MEMBERS}")
        total_declared = sum(max(0, int(member.file_size)) for member in members)
        if total_declared > MAX_ARCHIVE_UNCOMPRESSED_BYTES:
            raise RuntimeError(
                "MinerU archive declared uncompressed size exceeds "
                f"{MAX_ARCHIVE_UNCOMPRESSED_BYTES} bytes")
        base = destination.resolve()
        seen: set[str] = set()
        for member in members:
            normalized = member.filename.replace("\\", "/")
            if normalized.startswith("/") or ".." in normalized.split("/"):
                raise RuntimeError(f"unsafe path in MinerU archive: {member.filename!r}")
            normalized = normalized.rstrip("/")
            if not normalized:
                continue
            if normalized in seen:
                raise RuntimeError(f"duplicate path in MinerU archive: {member.filename!r}")
            seen.add(normalized)
            unix_mode = (member.external_attr >> 16) & 0o170000
            if unix_mode == 0o120000:
                raise RuntimeError(f"symbolic links are not allowed in MinerU archive: {member.filename!r}")
            if member.flag_bits & 0x1:
                raise RuntimeError(f"encrypted members are not allowed in MinerU archive: {member.filename!r}")
            if member.file_size > MAX_ARCHIVE_MEMBER_BYTES:
                raise RuntimeError(
                    f"MinerU archive member exceeds {MAX_ARCHIVE_MEMBER_BYTES} bytes: "
                    f"{member.filename!r}")
            ratio = member.file_size / max(1, member.compress_size)
            if member.file_size and ratio > MAX_ARCHIVE_COMPRESSION_RATIO:
                raise RuntimeError(
                    f"MinerU archive member compression ratio {ratio:.1f} exceeds "
                    f"{MAX_ARCHIVE_COMPRESSION_RATIO}: {member.filename!r}")
            target = (destination / normalized).resolve()
            if target != base and base not in target.parents:
                raise RuntimeError(f"archive member escapes destination: {member.filename!r}")
        extracted = 0
        for member in members:
            normalized = member.filename.replace("\\", "/").rstrip("/")
            if not normalized:
                continue
            target = destination / normalized
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with zipped.open(member) as source, target.open("xb") as sink:
                member_bytes = 0
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    member_bytes += len(chunk)
                    extracted += len(chunk)
                    if member_bytes > MAX_ARCHIVE_MEMBER_BYTES:
                        raise RuntimeError(
                            f"MinerU archive member exceeded extraction limit: {member.filename!r}")
                    if extracted > MAX_ARCHIVE_UNCOMPRESSED_BYTES:
                        raise RuntimeError("MinerU archive exceeded total extraction limit")
                    sink.write(chunk)
                if member_bytes != member.file_size:
                    raise RuntimeError(
                        f"MinerU archive member size mismatch: {member.filename!r}")
    return len(members)


def _write_sections(
    *, markdown_path: Path, output_dir: Path, paper_id: str, source_file: str,
    source_format: str, source_sha256: str,
    lease_guard: Callable[[], None] | None,
) -> tuple[Path, str, int]:
    """Generate chunks off to the side, then fence their final promotion."""
    temporary = output_dir / f".sections-{uuid.uuid4().hex}.jsonl"
    destination = output_dir / SECTION_CHUNKS_FILENAME
    try:
        sections_sha, sections_count = section_chunking.write_sections_jsonl(
            markdown_path,
            temporary,
            paper_id=paper_id,
            source_file=source_file,
            source_format=source_format,
            source_sha256=source_sha256,
        )
        _publish(temporary, destination, lease_guard)
    finally:
        temporary.unlink(missing_ok=True)
    return destination, sections_sha, sections_count


def _materialize_mineru(
    transport: HTTPTransport, zip_url: str, output_dir: Path, state: dict[str, Any],
    state_path: Path, *, source_sha256: str, processor_version: str,
    paper_id: str, source_file: str,
    lease_guard: Callable[[], None] | None = None,
) -> ParserOutcome:
    output_dir.mkdir(parents=True, exist_ok=True)
    part = output_dir / f"mineru_result.zip.part-{uuid.uuid4().hex}"
    archive = output_dir / "mineru_result.zip"
    _guard(lease_guard)
    try:
        status = transport.download_file(zip_url, part, timeout=600)
    except ValueError as exc:
        part.unlink(missing_ok=True)
        return ParserOutcome("failed", "archive_too_large", redact(str(exc)), False)
    if status not in (200, 206):
        part.unlink(missing_ok=True)
        if status in (408, 429) or status >= 500:
            return ParserOutcome(
                "waiting_remote", "archive_download_failed",
                f"MinerU archive HTTP {status}", True, poll_after_seconds=60,
            )
        if status in (401, 403, 404):
            return ParserOutcome(
                "blocked", "archive_url_expired",
                f"MinerU archive URL returned HTTP {status}; use an explicit refresh-archive retry",
                False,
            )
        return ParserOutcome(
            "failed", "archive_download_failed", f"MinerU archive HTTP {status}", False)
    if not part.is_file() or part.stat().st_size > MAX_ARCHIVE_COMPRESSED_BYTES:
        part.unlink(missing_ok=True)
        return ParserOutcome(
            "failed", "archive_too_large",
            f"MinerU archive exceeds {MAX_ARCHIVE_COMPRESSED_BYTES} compressed bytes",
            False,
        )
    try:
        _publish(part, archive, lease_guard)
    finally:
        part.unlink(missing_ok=True)
    extract_tmp = Path(tempfile.mkdtemp(prefix="mineru-extract-", dir=output_dir))
    try:
        _guard(lease_guard)
        member_count = safe_extract_zip(archive, extract_tmp)
        _guard(lease_guard)
        candidates = sorted(extract_tmp.rglob("*.md"))
        source_md = next((item for item in candidates if item.name.casefold() == "full.md"), None)
        if source_md is None:
            source_md = candidates[0] if candidates else None
        if source_md is None:
            raise RuntimeError("MinerU archive contains no Markdown")
        if source_md.stat().st_size > MAX_MARKDOWN_BYTES:
            raise RuntimeError(
                f"MinerU Markdown exceeds {MAX_MARKDOWN_BYTES} bytes")
        full_md = output_dir / "full.md"
        atomic_text(
            full_md, source_md.read_text(encoding="utf-8", errors="replace"),
            lease_guard=lease_guard,
        )
        valid, reason = valid_markdown(full_md)
        if not valid:
            raise RuntimeError(reason)
        images = output_dir / "images"
        images.mkdir(exist_ok=True)
        image_count = 0
        for item in extract_tmp.rglob("*"):
            if not item.is_file() or item.suffix.casefold() not in {".png", ".jpg", ".jpeg", ".webp", ".gif"}:
                continue
            target = images / f"{image_count:04d}-{item.name}"
            temporary = target.with_suffix(target.suffix + ".part")
            shutil.copyfile(item, temporary)
            try:
                _publish(temporary, target, lease_guard)
            finally:
                temporary.unlink(missing_ok=True)
            image_count += 1
        parsed_sha = sha256(full_md)
        sections_path, sections_sha, sections_count = _write_sections(
            markdown_path=full_md,
            output_dir=output_dir,
            paper_id=paper_id,
            source_file=source_file,
            source_format="pdf",
            source_sha256=source_sha256,
            lease_guard=lease_guard,
        )
        state.update({
            "status": "done", "source_sha256": source_sha256,
            "processor": "mineru", "processor_version": processor_version,
            "parsed_text_path": "full.md", "parsed_text_sha256": parsed_sha,
            "sections_path": SECTION_CHUNKS_FILENAME, "sections_sha256": sections_sha,
            "sections_count": sections_count,
            "archive_sha256": sha256(archive), "archive_members": member_count,
            "image_count": image_count, "completed_at": now(),
        })
        # Signed URLs are useful only while pending and should not remain in the final record.
        state.pop("remote_upload_url", None)
        state.pop("remote_zip_url", None)
        atomic_json(state_path, state, lease_guard=lease_guard)
        state_sha = sha256(state_path)
        result_path = output_dir / "result.json"
        result = {
            "status": "succeeded", "source_sha256": source_sha256,
            "processor": "mineru", "processor_version": processor_version,
            "job_id": state.get("job_id", ""), "document_id": state.get("document_id", ""),
            "paper_id": paper_id, "source_file": source_file, "source_format": "pdf",
            "remote_batch_id": state.get("remote_batch_id", ""),
            "remote_data_id": state.get("remote_data_id", ""),
            "parsed_text_path": "full.md", "parsed_text_sha256": parsed_sha,
            "sections_path": SECTION_CHUNKS_FILENAME, "sections_sha256": sections_sha,
            "sections_count": sections_count,
            "state_path": "state.json", "state_sha256": state_sha,
            "completed_at": now(),
        }
        atomic_json(result_path, result, lease_guard=lease_guard)
        return ParserOutcome(
            "review", parsed_text_path=str(full_md), parsed_text_sha256=parsed_sha,
            sections_path=str(sections_path), sections_sha256=sections_sha,
            sections_count=sections_count,
            state_path=str(state_path), state_sha256=state_sha,
            result_path=str(result_path), result_sha256=sha256(result_path),
        )
    except (OSError, zipfile.BadZipFile, RuntimeError) as exc:
        return ParserOutcome("failed", "invalid_archive", redact(str(exc)), False)
    finally:
        shutil.rmtree(extract_tmp, ignore_errors=True)


def mineru_step(
    *, source: Path, expected_sha256: str, output_dir: Path, job_id: str,
    document_id: str, processor_version: str, external_upload_authorized: bool,
    paper_id: str = "",
    model_version: str = "vlm", enable_table: bool = True, enable_formula: bool = True,
    language: str = "en", transport: HTTPTransport | None = None,
    lease_guard: Callable[[], None] | None = None,
) -> ParserOutcome:
    """Advance one PDF by one remote state transition, then return promptly."""
    transport = transport or UrllibTransport()
    state_path = output_dir / "state.json"
    try:
        state = read_checkpoint(state_path)
    except ValueError as exc:
        return ParserOutcome("blocked", "checkpoint_invalid", redact(str(exc)), False)
    ok, reason, pages = validate_pdf(source, expected_sha256)
    if not ok:
        return ParserOutcome("blocked", "source_gate_failed", reason)
    if state and state.get("source_sha256") not in (None, "", expected_sha256):
        return ParserOutcome("blocked", "source_binding_mismatch", "state.json belongs to another source")
    if state and state.get("job_id") not in (None, "", job_id):
        return ParserOutcome("blocked", "job_binding_mismatch", "state.json belongs to another job")
    if state.get("status") in {"submit_intent", "submit_outcome_unknown"}:
        return ParserOutcome(
            "blocked", "submit_outcome_unknown",
            "MinerU submission outcome is unknown; use an explicit audited resubmit retry",
        )
    remote_batch_id = str(state.get("remote_batch_id") or "")
    remote_data_id = str(state.get("remote_data_id") or "")
    if remote_batch_id and remote_data_id != job_id:
        return ParserOutcome(
            "blocked", "remote_result_identity_mismatch",
            "state.json remote batch lacks the exact job data_id binding",
        )
    if remote_data_id and not remote_batch_id:
        return ParserOutcome(
            "blocked", "checkpoint_invalid",
            "state.json has remote_data_id without remote_batch_id",
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    full_md = output_dir / "full.md"
    result_path = output_dir / "result.json"
    valid, _ = valid_markdown(full_md)
    if valid and state.get("status") == "done" and result_path.is_file():
        result = read_json(result_path)
        if (result.get("source_sha256") == expected_sha256
                and result.get("job_id") == job_id
                and result.get("document_id") == document_id
                and result.get("parsed_text_sha256") == sha256(full_md)):
            parsed_sha = sha256(full_md)
            sections_path = output_dir / SECTION_CHUNKS_FILENAME
            sections_sha = str(result.get("sections_sha256") or "")
            sections_count = result.get("sections_count")
            sections_valid = False
            if (sections_path.is_file() and sections_sha
                    and isinstance(sections_count, int) and not isinstance(sections_count, bool)
                    and sha256(sections_path) == sections_sha):
                try:
                    section_chunking.validate_sections_jsonl(
                        sections_path, full_md,
                        source_sha256=expected_sha256,
                        parsed_text_sha256=parsed_sha,
                        expected_count=sections_count,
                    )
                    sections_valid = True
                except (OSError, ValueError):
                    sections_valid = False
            if not sections_valid:
                sections_path, sections_sha, sections_count = _write_sections(
                    markdown_path=full_md,
                    output_dir=output_dir,
                    paper_id=paper_id or document_id,
                    source_file=source.name,
                    source_format="pdf",
                    source_sha256=expected_sha256,
                    lease_guard=lease_guard,
                )
                state.update({
                    "paper_id": paper_id or document_id,
                    "source_file": source.name,
                    "source_format": "pdf",
                    "sections_path": SECTION_CHUNKS_FILENAME,
                    "sections_sha256": sections_sha,
                    "sections_count": sections_count,
                })
                atomic_json(state_path, state, lease_guard=lease_guard)
                state_sha = sha256(state_path)
                result.update({
                    "paper_id": paper_id or document_id,
                    "source_file": source.name,
                    "source_format": "pdf",
                    "sections_path": SECTION_CHUNKS_FILENAME,
                    "sections_sha256": sections_sha,
                    "sections_count": sections_count,
                    "state_path": "state.json",
                    "state_sha256": state_sha,
                })
                atomic_json(result_path, result, lease_guard=lease_guard)
            return ParserOutcome(
                "review", parsed_text_path=str(full_md), parsed_text_sha256=parsed_sha,
                sections_path=str(sections_path), sections_sha256=sections_sha,
                sections_count=int(sections_count),
                state_path=str(state_path), state_sha256=sha256(state_path),
                result_path=str(result_path), result_sha256=sha256(result_path),
            )

    state.update({
        "source_sha256": expected_sha256, "source_bytes": source.stat().st_size,
        "source_pages": pages, "processor": "mineru", "processor_version": processor_version,
        "job_id": job_id, "document_id": document_id,
        "paper_id": paper_id or document_id, "source_file": source.name,
        "source_format": "pdf",
        "model_version": model_version, "enable_table": bool(enable_table),
        "enable_formula": bool(enable_formula), "language": language,
    })
    preferred_token = str(state.get("token_env") or "")
    try:
        token_env, token = _token(preferred_token)
    except RuntimeError as exc:
        return ParserOutcome("blocked", "missing_mineru_token", str(exc))

    try:
        if state.get("remote_zip_url"):
            return _materialize_mineru(
                transport, str(state["remote_zip_url"]), output_dir, state, state_path,
                source_sha256=expected_sha256, processor_version=processor_version,
                paper_id=paper_id or document_id, source_file=source.name,
                lease_guard=lease_guard,
            )

        if state.get("status") == "upload_pending":
            if not external_upload_authorized:
                return ParserOutcome(
                    "blocked", "external_upload_not_authorized",
                    "resuming the external PDF upload requires --external-upload-authorized",
                )
            _guard(lease_guard)
            upload_status = transport.upload_file(str(state["remote_upload_url"]), source, timeout=600)
            _guard(lease_guard)
            if upload_status not in (200, 201, 204):
                return ParserOutcome(
                    "failed", "upload_failed", f"MinerU upload HTTP {upload_status}", True)
            state.update({"status": "submitted", "uploaded_at": now()})
            atomic_json(state_path, state, lease_guard=lease_guard)
            return ParserOutcome("waiting_remote", poll_after_seconds=30)

        batch_id = str(state.get("remote_batch_id") or "")
        if not batch_id:
            if not external_upload_authorized:
                return ParserOutcome(
                    "blocked", "external_upload_not_authorized",
                    "sending this PDF to MinerU requires --external-upload-authorized",
                )
            payload = {
                "files": [{"name": source.name, "data_id": job_id}],
                "model_version": model_version,
                "enable_formula": bool(enable_formula),
                "enable_table": bool(enable_table),
                "language": language,
            }
            # Persist intent before the paid POST.  If the process loses the
            # response, a later worker must not silently create another batch.
            state.update({
                "status": "submit_intent", "submit_intent_id": uuid.uuid4().hex,
                "submit_intent_at": now(), "token_env": token_env,
            })
            atomic_json(state_path, state, lease_guard=lease_guard)
            _guard(lease_guard)
            try:
                http_status, body, text = transport.post_json(
                    f"{MINERU_API_BASE}/file-urls/batch", _auth(token), payload, timeout=60,
                )
            except (OSError, ValueError, KeyError, urllib.error.URLError) as exc:
                state.update({
                    "status": "submit_outcome_unknown",
                    "submit_error": redact(f"{type(exc).__name__}: {exc}"),
                })
                atomic_json(state_path, state, lease_guard=lease_guard)
                return ParserOutcome(
                    "blocked", "submit_outcome_unknown",
                    "MinerU submission may have reached the server; explicit audited resubmit is required",
                    False,
                )
            _guard(lease_guard)
            data = body.get("data") if isinstance(body.get("data"), dict) else {}
            urls = data.get("file_urls") if isinstance(data, dict) else []
            batch_id = str(data.get("batch_id") or "") if isinstance(data, dict) else ""
            if http_status != 200 or body.get("code") not in (0, 200, "0", "200", None) or not batch_id or not urls:
                ambiguous = http_status in (408, 429) or http_status >= 500 or http_status == 200
                state.update({
                    "status": "submit_outcome_unknown" if ambiguous else "submit_rejected",
                    "submit_http_status": http_status,
                    "submit_error": redact(str(body.get("msg") or text[:300])),
                })
                atomic_json(state_path, state, lease_guard=lease_guard)
                return ParserOutcome(
                    "blocked" if ambiguous else "failed",
                    "submit_outcome_unknown" if ambiguous else "submit_failed",
                    redact(f"MinerU submit HTTP {http_status}: {body.get('msg') or text[:300]}"),
                    False,
                )
            # Persist the paid remote identity before any upload or return.  A crash can
            # repeat the PUT, but can never silently create a second MinerU batch.
            state.update({
                "status": "upload_pending", "remote_batch_id": batch_id,
                "remote_data_id": job_id,
                "remote_upload_url": str(urls[0]), "token_env": token_env,
                "submitted_at": now(),
            })
            state.pop("submit_intent_id", None)
            state.pop("submit_intent_at", None)
            state.pop("submit_http_status", None)
            state.pop("submit_error", None)
            atomic_json(state_path, state, lease_guard=lease_guard)
            _guard(lease_guard)
            upload_status = transport.upload_file(str(urls[0]), source, timeout=600)
            _guard(lease_guard)
            if upload_status not in (200, 201, 204):
                return ParserOutcome(
                    "failed", "upload_failed", f"MinerU upload HTTP {upload_status}", True)
            state.update({"status": "submitted", "uploaded_at": now()})
            atomic_json(state_path, state, lease_guard=lease_guard)
            return ParserOutcome("waiting_remote", poll_after_seconds=30)

        _guard(lease_guard)
        http_status, body, text = transport.get_json(
            f"{MINERU_API_BASE}/extract-results/batch/{batch_id}", _auth(token), timeout=60,
        )
        _guard(lease_guard)
        if http_status != 200:
            if http_status in (408, 429) or http_status >= 500:
                return ParserOutcome(
                    "waiting_remote", "poll_failed",
                    redact(f"MinerU poll HTTP {http_status}: {text[:300]}"),
                    True, poll_after_seconds=60,
                )
            return ParserOutcome(
                "failed", "poll_failed", redact(f"MinerU poll HTTP {http_status}: {text[:300]}"),
                False,
            )
        if body.get("code") not in (0, 200, "0", "200", None):
            message = redact(str(body.get("msg") or body.get("code") or "MinerU API error"))
            auth_error = str(body.get("code") or "").casefold() in {"a0202", "401"}
            return ParserOutcome(
                "blocked" if auth_error else "failed",
                "mineru_auth_failed" if auth_error else "poll_api_error",
                message,
                False if auth_error else True,
            )
        data = body.get("data") if isinstance(body.get("data"), dict) else {}
        results = data.get("extract_result") if isinstance(data, dict) else []
        if not isinstance(results, list) or not results:
            return ParserOutcome("waiting_remote", poll_after_seconds=30)
        matches = [
            item for item in results
            if isinstance(item, dict) and str(item.get("data_id") or "") == job_id
        ]
        if len(results) != 1 or len(matches) != 1:
            return ParserOutcome(
                "blocked", "remote_result_identity_mismatch",
                "MinerU response must contain exactly one extract_result matching this job data_id",
                False,
            )
        result = matches[0]
        remote_status = str(result.get("state") or "").casefold()
        if remote_status not in {"done", "failed"}:
            state.update({"status": "submitted", "last_remote_state": remote_status, "polled_at": now()})
            atomic_json(state_path, state, lease_guard=lease_guard)
            return ParserOutcome("waiting_remote", poll_after_seconds=30)
        if remote_status == "failed":
            message = redact(str(result.get("err_msg") or "MinerU parsing failed"))
            return ParserOutcome(
                "blocked", "remote_parse_failed", message, False,
            )
        zip_url = str(result.get("full_zip_url") or "")
        if not zip_url:
            return ParserOutcome("failed", "missing_archive_url", "MinerU reported done without full_zip_url", True)
        state.update({"status": "remote_done", "remote_zip_url": zip_url, "remote_done_at": now()})
        atomic_json(state_path, state, lease_guard=lease_guard)
        return _materialize_mineru(
            transport, zip_url, output_dir, state, state_path,
            source_sha256=expected_sha256, processor_version=processor_version,
            paper_id=paper_id or document_id, source_file=source.name,
            lease_guard=lease_guard,
        )
    except (OSError, ValueError, KeyError, urllib.error.URLError) as exc:
        if state.get("remote_batch_id"):
            return ParserOutcome(
                "waiting_remote", "transport_error",
                redact(f"{type(exc).__name__}: {exc}"), True, poll_after_seconds=60,
            )
        return ParserOutcome("failed", "transport_error", redact(f"{type(exc).__name__}: {exc}"), True)


def _local_name(element: ET.Element) -> str:
    return element.tag.rsplit("}", 1)[-1].casefold()


def _clean_text(element: ET.Element | None) -> str:
    if element is None:
        return ""
    return re.sub(r"\s+", " ", "".join(element.itertext())).strip()


def _first(root: ET.Element, names: set[str]) -> ET.Element | None:
    return next((node for node in root.iter() if _local_name(node) in names), None)


def _render_table(table: ET.Element) -> list[str]:
    rows: list[list[str]] = []
    for tr in table.iter():
        if _local_name(tr) != "tr":
            continue
        cells = [_clean_text(cell).replace("|", "\\|") for cell in list(tr)
                 if _local_name(cell) in {"td", "th", "entry"}]
        if cells:
            rows.append(cells)
    if not rows:
        raw = _clean_text(table)
        return [f"> Unstructured table text: {raw}"] if raw else []
    width = max(len(row) for row in rows)
    rows = [row + [""] * (width - len(row)) for row in rows]
    rendered = ["| " + " | ".join(rows[0]) + " |", "| " + " | ".join(["---"] * width) + " |"]
    rendered.extend("| " + " | ".join(row) + " |" for row in rows[1:])
    return rendered


def xml_to_markdown_step(
    *, source: Path, expected_sha256: str, output_dir: Path, job_id: str, document_id: str,
    processor_version: str, paper_id: str = "",
    lease_guard: Callable[[], None] | None = None,
) -> ParserOutcome:
    """Convert JATS/Elsevier-like XML without inventing missing structure or data."""
    if not source.is_file():
        return ParserOutcome("blocked", "source_missing", "source XML is missing")
    if sha256(source) != expected_sha256:
        return ParserOutcome(
            "blocked", "source_binding_mismatch",
            "source SHA-256 no longer matches the accepted download",
        )
    try:
        _guard(lease_guard)
        root = ET.parse(source).getroot()
        _guard(lease_guard)
    except (ET.ParseError, OSError) as exc:
        return ParserOutcome("failed", "invalid_xml", redact(str(exc)), False)
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / "state.json"
    result_path = output_dir / "result.json"
    full_md = output_dir / "full.md"

    title = _clean_text(_first(root, {"article-title", "title"})) or "Untitled full-text XML"
    lines = [
        "<!-- parser: xml_to_markdown -->",
        f"<!-- processor_version: {processor_version} -->",
        f"<!-- source_sha256: {expected_sha256} -->",
        f"<!-- document_id: {document_id} -->",
        "",
        f"# {title}", "",
    ]
    body_candidate = _first(root, {"body"})
    body = body_candidate if body_candidate is not None else root
    emitted_paragraphs = 0
    for node in body.iter():
        name = _local_name(node)
        if name in {"title", "section-title"}:
            text = _clean_text(node)
            if text and text != title:
                lines.extend([f"## {text}", ""])
        elif name in {"p", "para"}:
            # Parent paragraphs are rare; repeated nested text is avoided by emitting
            # only nodes without paragraph descendants.
            if any(_local_name(child) in {"p", "para"} for child in node.iter() if child is not node):
                continue
            text = _clean_text(node)
            if text:
                lines.extend([text, ""])
                emitted_paragraphs += 1
        elif name in {"table-wrap", "table"}:
            table_lines = _render_table(node)
            if table_lines:
                label = _clean_text(_first(node, {"label"}))
                caption = _clean_text(_first(node, {"caption"}))
                heading = " ".join(item for item in (label, caption) if item)
                lines.extend([f"### {heading or 'Table'}", "", *table_lines, ""])
        elif name in {"fig", "figure"}:
            label = _clean_text(_first(node, {"label"}))
            caption = _clean_text(_first(node, {"caption"}))
            if label or caption:
                lines.extend([f"### {' '.join(item for item in (label, caption) if item)}", ""])
    if emitted_paragraphs == 0:
        return ParserOutcome("failed", "xml_without_body_text", "XML contains no extractable body paragraphs", False)
    atomic_text(
        full_md, "\n".join(lines).rstrip() + "\n", lease_guard=lease_guard,
    )
    valid, reason = valid_markdown(full_md)
    if not valid:
        return ParserOutcome("failed", "invalid_markdown", reason, False)
    parsed_sha = sha256(full_md)
    sections_path, sections_sha, sections_count = _write_sections(
        markdown_path=full_md,
        output_dir=output_dir,
        paper_id=paper_id or document_id,
        source_file=source.name,
        source_format="xml",
        source_sha256=expected_sha256,
        lease_guard=lease_guard,
    )
    state = {
        "status": "done", "source_sha256": expected_sha256, "processor": "xml_to_markdown",
        "processor_version": processor_version, "job_id": job_id, "document_id": document_id,
        "paper_id": paper_id or document_id, "source_file": source.name,
        "source_format": "xml",
        "parsed_text_path": "full.md", "parsed_text_sha256": parsed_sha,
        "sections_path": SECTION_CHUNKS_FILENAME, "sections_sha256": sections_sha,
        "sections_count": sections_count,
        "paragraph_count": emitted_paragraphs, "completed_at": now(),
    }
    atomic_json(state_path, state, lease_guard=lease_guard)
    state_sha = sha256(state_path)
    result = {
        "status": "succeeded", "source_sha256": expected_sha256,
        "processor": "xml_to_markdown", "processor_version": processor_version,
        "job_id": job_id, "document_id": document_id, "parsed_text_path": "full.md",
        "paper_id": paper_id or document_id, "source_file": source.name,
        "source_format": "xml",
        "parsed_text_sha256": parsed_sha, "state_path": "state.json",
        "sections_path": SECTION_CHUNKS_FILENAME, "sections_sha256": sections_sha,
        "sections_count": sections_count,
        "state_sha256": state_sha, "completed_at": now(),
    }
    atomic_json(result_path, result, lease_guard=lease_guard)
    return ParserOutcome(
        "review", parsed_text_path=str(full_md), parsed_text_sha256=parsed_sha,
        sections_path=str(sections_path), sections_sha256=sections_sha,
        sections_count=sections_count,
        state_path=str(state_path), state_sha256=state_sha,
        result_path=str(result_path), result_sha256=sha256(result_path),
    )
