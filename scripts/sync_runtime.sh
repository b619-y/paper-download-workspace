#!/usr/bin/env bash
set -euo pipefail

# Public-repository safe defaults: the local runtime is derived from this
# checkout, while every remote coordinate must be supplied explicitly.
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
workspace_root="$(cd -- "$script_dir/.." && pwd -P)"
mac_root="${PAPER_SYNC_MAC_ROOT:-$workspace_root/runtime}"
remote_host="${PAPER_SYNC_REMOTE_HOST:-}"
remote_root="${PAPER_SYNC_REMOTE_ROOT:-}"

usage() {
  cat >&2 <<'EOF'
用法:
  PAPER_SYNC_REMOTE_HOST=<ssh-target> PAPER_SYNC_REMOTE_ROOT=<absolute-runtime-path> \
    scripts/sync_runtime.sh push-workflow [--apply]
  PAPER_SYNC_REMOTE_HOST=<ssh-target> PAPER_SYNC_REMOTE_ROOT=<absolute-runtime-path> \
    scripts/sync_runtime.sh push-processing-inputs <task_id> [--apply]
  PAPER_SYNC_REMOTE_HOST=<ssh-target> PAPER_SYNC_REMOTE_ROOT=<absolute-runtime-path> \
    scripts/sync_runtime.sh pull-processing-results <task_id> [--apply]

默认只做 dry-run；只有显式追加 --apply 才会改动目标端。
push-inputs / pull-results 旧广域模式已禁用，请使用上述限定范围模式。

处理文件实际传输还需本机显式放行：
  export PAPER_PROCESSING_TRANSFER_POLICY=allow-remote-worker
若暂存目录含 PDF/XML，还需第二道单次全文放行：
  export PAPER_FULLTEXT_TRANSFER_POLICY=allow-remote-worker
EOF
}

mode="${1:-}"
task_id=""
apply_requested=false

case "$mode" in
  push-inputs|pull-results)
    echo "已禁用旧广域同步模式 ${mode}；请改用按 task 限定的安全模式。" >&2
    exit 6
    ;;
  push-workflow)
    if [[ $# -gt 2 ]]; then usage; exit 2; fi
    case "${2:-}" in
      "") ;;
      --apply) apply_requested=true ;;
      *) usage; exit 2 ;;
    esac
    ;;
  push-processing-inputs|pull-processing-results)
    if [[ $# -lt 2 || $# -gt 3 ]]; then usage; exit 2; fi
    task_id="$2"
    if [[ ! "$task_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
      echo "task_id 必须是路径安全的 ASCII 标识符" >&2
      exit 2
    fi
    case "${3:-}" in
      "") ;;
      --apply) apply_requested=true ;;
      *) usage; exit 2 ;;
    esac
    ;;
  *) usage; exit 2 ;;
esac

validate_configuration() {
  if [[ -z "$remote_host" || -z "$remote_root" ]]; then
    echo "拒绝同步：必须显式设置 PAPER_SYNC_REMOTE_HOST 和 PAPER_SYNC_REMOTE_ROOT。" >&2
    exit 2
  fi
  # Port and other SSH options belong in ~/.ssh/config.  Keeping this value to
  # a target token prevents remote-shell metacharacter injection.
  if [[ ! "$remote_host" =~ ^[A-Za-z0-9][A-Za-z0-9._@-]{0,254}$ ]]; then
    echo "PAPER_SYNC_REMOTE_HOST 只能是安全的 SSH target token" >&2
    exit 2
  fi
  if [[ ! "$remote_root" =~ ^/[A-Za-z0-9._/-]+$ \
        || "$remote_root" == "/" \
        || "$remote_root" == *"//"* \
        || "$remote_root" == *"/../"* \
        || "$remote_root" == */.. \
        || "$remote_root" == *"/./"* \
        || "$remote_root" == */. ]]; then
    echo "PAPER_SYNC_REMOTE_ROOT 必须是不含空格、点跳转或元字符的绝对路径，且不能是 /" >&2
    exit 2
  fi
  if [[ ! -d "$mac_root" || -L "$mac_root" ]]; then
    echo "本机 runtime 根目录缺失或是符号链接：$mac_root" >&2
    exit 2
  fi
}

validate_configuration

if [[ "$apply_requested" != true ]]; then
  echo "预览模式；确认方向、范围和文件清单后追加 --apply"
fi

require_processing_policy() {
  [[ "$apply_requested" == true ]] || return 0
  local value="${PAPER_PROCESSING_TRANSFER_POLICY:-}"
  local policy_file="${PAPER_PROCESSING_TRANSFER_POLICY_FILE:-$mac_root/PROCESSING_TRANSFER_POLICY}"
  local line=""
  if [[ -z "$value" && -r "$policy_file" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
      line="${line%%#*}"
      line="${line//[[:space:]]/}"
      if [[ -n "$line" ]]; then value="$line"; break; fi
    done < "$policy_file"
  fi
  if [[ "$value" != "allow-remote-worker" ]]; then
    echo "已阻止实际传输：请先确认授权/版权/隐私政策，再设置 PAPER_PROCESSING_TRANSFER_POLICY=allow-remote-worker。" >&2
    exit 4
  fi
}

require_fulltext_policy_if_needed() {
  local source_root="$1"
  [[ "$apply_requested" == true ]] || return 0
  local found
  found="$(find "$source_root" -type f \( -iname '*.pdf' -o -iname '*.xml' -o -iname '*.caj' \) -print -quit)"
  [[ -n "$found" ]] || return 0
  if [[ "${PAPER_FULLTEXT_TRANSFER_POLICY:-}" != "allow-remote-worker" ]]; then
    echo "已阻止全文传输：暂存输入含 PDF/XML/CAJ，需额外设置 PAPER_FULLTEXT_TRANSFER_POLICY=allow-remote-worker。" >&2
    exit 4
  fi
}

make_temp_file() {
  mktemp "${TMPDIR:-/tmp}/paper-sync.XXXXXX"
}

snapshot_file="$(make_temp_file)"
file_list="$(make_temp_file)"
staging_dir=""

cleanup() {
  if [[ -n "$staging_dir" && -d "$staging_dir" ]]; then
    python3 - "$staging_dir" "$mac_root" <<'PY'
import shutil
import sys
from pathlib import Path

target = Path(sys.argv[1]).resolve()
runtime = Path(sys.argv[2]).resolve()
staging_root = runtime / ".sync-staging"
try:
    target.relative_to(staging_root)
except ValueError:
    raise SystemExit("refusing to clean a path outside runtime/.sync-staging")
if target.name.startswith("pull-"):
    shutil.rmtree(target, ignore_errors=True)
PY
  fi
  python3 - "$snapshot_file" "$file_list" <<'PY'
import os
import sys
for raw in sys.argv[1:]:
    try:
        os.unlink(raw)
    except FileNotFoundError:
        pass
PY
}
trap cleanup EXIT

# Build a source allow-list and a content snapshot, and compare every existing
# same-path destination file by SHA-256.  The scanner uses lstat semantics,
# rejects links/special files, handles mixed-case database names, and excludes
# mutable legacy completion.json receipts.  rsync receives only this list, so
# an object created after the scan is not silently added to the transfer.
prepare_snapshot() {
  local transfer_mode="$1"
  local local_tree="$2"
  local remote_tree="$3"
  local verify_complete="${4:-false}"
  local transfer_context="${5:-}"
  python3 - "$transfer_mode" "$local_tree" "$remote_host" "$remote_tree" \
    "$snapshot_file" "$file_list" "$verify_complete" "$transfer_context" <<'PY'
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path, PurePosixPath

mode, local_raw, host, remote_raw, snapshot_raw, list_raw, verify_raw, context = sys.argv[1:]
verify_complete = verify_raw == "true"
RESULT_ROOTS = {"paperqa", "data_extraction", "document_processing"}
SENSITIVE_SUFFIXES = {".pdf", ".xml", ".caj"}
SQLITE_MAGIC = b"SQLite format 3\x00"
ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


def positive_limit(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


MAX_RESULT_FILES = positive_limit("PAPER_SYNC_MAX_RESULT_FILES", 50000)
MAX_RESULT_FILE_BYTES = positive_limit("PAPER_SYNC_MAX_RESULT_FILE_BYTES", 536870912)
MAX_RESULT_TOTAL_BYTES = positive_limit("PAPER_SYNC_MAX_RESULT_TOTAL_BYTES", 8589934592)


def excluded_name(name: str) -> bool:
    lowered = name.casefold()
    return (
        ".sqlite" in lowered
        or ".db" in lowered
        or lowered.endswith((
            "-wal", "-shm", "-journal", ".part", ".partial", ".tmp",
            ".temp", ".download", ".crdownload", ".lock",
        ))
        or ".part." in lowered
        or ".partial." in lowered
        or ".tmp." in lowered
        or lowered == ".ds_store"
    )


def extraction_result_path_allowed(relative: Path, *, directory: bool) -> bool:
    parts = relative.parts
    if not parts or parts[0] != "data_extraction":
        return True
    if len(parts) == 1:
        return directory
    if not ID_RE.fullmatch(parts[1]):
        return False
    if len(parts) == 2:
        return directory
    branch = parts[2]
    if branch == "attempts":
        if len(parts) == 3:
            return directory
        if not ID_RE.fullmatch(parts[3]):
            return False
        return directory if len(parts) == 4 else len(parts) >= 5
    if branch == "receipts":
        if len(parts) == 3:
            return directory
        return (
            not directory
            and len(parts) == 4
            and relative.suffix.casefold() == ".json"
            and ID_RE.fullmatch(relative.stem) is not None
        )
    return False


def result_path_allowed(relative: Path, *, directory: bool) -> bool:
    if not relative.parts or relative.parts[0] not in RESULT_ROOTS:
        return False
    if relative.parts[0] == "data_extraction":
        return extraction_result_path_allowed(relative, directory=directory)
    if relative.parts[0] == "paperqa":
        if directory:
            return len(relative.parts) == 1
        return (
            len(relative.parts) == 2
            and relative.suffix.casefold() == ".json"
            and ID_RE.fullmatch(relative.stem) is not None
        )
    return True


def excluded_path(relative: Path, *, directory: bool = False) -> bool:
    if any(excluded_name(part) for part in relative.parts):
        return True
    lowered_name = relative.name.casefold()
    if mode in {"push_workflow", "pull_results"} and relative.suffix.casefold() in SENSITIVE_SUFFIXES:
        return True
    if mode == "push_workflow":
        parts = tuple(part.casefold() for part in relative.parts)
        if lowered_name == "acceptance.json" or "delivery" in parts:
            return True
        if lowered_name == "review_queue.csv":
            return True
        if len(parts) >= 3 and parts[0] == "tasks" and parts[2] == "extraction" and lowered_name in {
            "data_extraction_queue.json", "data_extraction_queue.csv"
        }:
            return True
    if mode == "pull_results" and lowered_name == "completion.json":
        return True
    if mode == "pull_results" and not result_path_allowed(relative, directory=directory):
        return True
    return False


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            value.update(chunk)
    return value.hexdigest()


def safe_relative_text(relative: Path) -> str:
    value = relative.as_posix()
    if not value or value.startswith("/") or "\\" in value:
        raise RuntimeError(f"unsafe transfer path: {value!r}")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise RuntimeError(f"control character in transfer path: {value!r}")
    if any(part in {"", ".", ".."} for part in PurePosixPath(value).parts):
        raise RuntimeError(f"non-canonical transfer path: {value!r}")
    return value


def scan(root: Path) -> tuple[bool, dict[str, dict[str, int | str]]]:
    try:
        root_stat = root.lstat()
    except FileNotFoundError:
        return False, {}
    if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
        raise RuntimeError(f"sync root must be a real directory: {root}")
    output: dict[str, dict[str, int | str]] = {}
    total_bytes = 0
    for directory, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        base = Path(directory)
        base_relative = base.relative_to(root)
        kept_dirs = []
        for name in sorted(dirnames):
            path = base / name
            relative = path.relative_to(root)
            if mode == "push_workflow" and not base_relative.parts and name != "tasks":
                continue
            if mode == "pull_results" and len(relative.parts) == 1 and name not in RESULT_ROOTS:
                continue
            if excluded_path(relative, directory=True):
                continue
            entry_stat = path.lstat()
            if stat.S_ISLNK(entry_stat.st_mode) or not stat.S_ISDIR(entry_stat.st_mode):
                raise RuntimeError(f"link/special directory is not allowed: {relative}")
            safe_relative_text(relative)
            kept_dirs.append(name)
        dirnames[:] = kept_dirs
        for name in sorted(filenames):
            path = base / name
            relative = path.relative_to(root)
            if mode == "push_workflow" and not base_relative.parts and name != "ACTIVE_TASK.json":
                continue
            if mode == "pull_results" and (not relative.parts or relative.parts[0] not in RESULT_ROOTS):
                continue
            if excluded_path(relative):
                continue
            entry_stat = path.lstat()
            if stat.S_ISLNK(entry_stat.st_mode) or not stat.S_ISREG(entry_stat.st_mode):
                raise RuntimeError(f"link/special file is not allowed: {relative}")
            with path.open("rb") as handle:
                if handle.read(len(SQLITE_MAGIC)) == SQLITE_MAGIC:
                    raise RuntimeError(f"SQLite content is not allowed under a disguised name: {relative}")
            size = entry_stat.st_size
            if mode == "pull_results":
                if size > MAX_RESULT_FILE_BYTES:
                    raise RuntimeError(f"result exceeds per-file byte limit: {relative}")
                total_bytes += size
                if len(output) + 1 > MAX_RESULT_FILES:
                    raise RuntimeError("result exceeds file-count limit")
                if total_bytes > MAX_RESULT_TOTAL_BYTES:
                    raise RuntimeError("result exceeds total-byte limit")
            output[safe_relative_text(relative)] = {"sha256": digest(path), "size": size}
    return True, output


def validated_document_allowlist(
    root: Path, files: dict[str, dict[str, int | str]], task_id: str
) -> dict[str, dict[str, int | str]]:
    if not ID_RE.fullmatch(task_id):
        raise RuntimeError("push_documents requires a path-safe task context")
    documents = sorted({path.split("/", 1)[0] for path in files})
    if not documents:
        return {}
    allowed: set[str] = set()
    sha_re = re.compile(r"[0-9a-f]{64}\Z")

    def staged_ref(value, document: str, label: str) -> str:
        if not isinstance(value, str) or not value:
            raise RuntimeError(f"{document}/manifest.json has invalid {label}")
        pure = PurePosixPath(value)
        expected_prefix = ("runtime", "inputs", task_id, "documents", document)
        if (pure.is_absolute() or "\\" in value or
                any(part in {"", ".", ".."} for part in pure.parts) or
                tuple(pure.parts[:5]) != expected_prefix or len(pure.parts) < 7):
            raise RuntimeError(f"{document}/manifest.json {label} escapes its staged document")
        return "/".join(pure.parts[5:])

    def bind_file(manifest, document: str, ref_key: str, sha_key: str, bytes_key: str) -> None:
        relative_inside = staged_ref(manifest.get(ref_key), document, ref_key)
        transfer_path = f"{document}/{relative_inside}"
        checksum = manifest.get(sha_key)
        expected_size = manifest.get(bytes_key)
        if not isinstance(checksum, str) or not sha_re.fullmatch(checksum):
            raise RuntimeError(f"{document}/manifest.json has invalid {sha_key}")
        if isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size <= 0:
            raise RuntimeError(f"{document}/manifest.json has invalid {bytes_key}")
        entry = files.get(transfer_path)
        if entry != {"sha256": checksum, "size": expected_size}:
            raise RuntimeError(f"{document}/manifest.json binding mismatch for {transfer_path}")
        allowed.add(transfer_path)

    for document in documents:
        if not ID_RE.fullmatch(document):
            raise RuntimeError(f"local snapshot has invalid document id: {document!r}")
        manifest_relative = f"{document}/manifest.json"
        if manifest_relative not in files:
            raise RuntimeError(f"staged document lacks manifest.json: {document}")
        manifest_path = root / document / "manifest.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"unreadable staged manifest: {manifest_relative}: {exc}") from exc
        if not isinstance(manifest, dict):
            raise RuntimeError(f"staged manifest must be an object: {manifest_relative}")
        expected_scalars = {
            "schema_version": 3,
            "status": "accepted",
            "task_id": task_id,
            "document_id": document,
        }
        for key, expected in expected_scalars.items():
            if manifest.get(key) != expected:
                raise RuntimeError(f"{manifest_relative} has invalid {key}")
        if manifest.get("parsed_text_ref") != manifest.get("parsed_text_path"):
            raise RuntimeError(f"{manifest_relative} parsed text aliases disagree")
        bind_file(manifest, document, "parsed_text_ref", "parsed_text_sha256", "parsed_text_bytes")
        if manifest.get("section_chunks_ref") != manifest.get("section_chunks_path"):
            raise RuntimeError(f"{manifest_relative} section chunk aliases disagree")
        bind_file(
            manifest, document, "section_chunks_ref", "section_chunks_sha256",
            "section_chunks_bytes",
        )
        if (isinstance(manifest.get("section_chunks_count"), bool)
                or not isinstance(manifest.get("section_chunks_count"), int)
                or manifest["section_chunks_count"] <= 0
                or not isinstance(manifest.get("section_chunks_schema_version"), str)
                or not manifest["section_chunks_schema_version"].strip()):
            raise RuntimeError(f"{manifest_relative} has invalid section chunk metadata")
        copied = manifest.get("fulltext_copied")
        if not isinstance(copied, bool):
            raise RuntimeError(f"{manifest_relative} has invalid fulltext_copied")
        if copied:
            if manifest.get("source_staged_ref") != manifest.get("source_path"):
                raise RuntimeError(f"{manifest_relative} staged source aliases disagree")
            bind_file(manifest, document, "source_staged_ref", "source_sha256", "source_bytes")
        elif manifest.get("source_staged_ref") is not None:
            raise RuntimeError(f"{manifest_relative} declares a staged source without fulltext_copied")
        else:
            checksum = manifest.get("source_sha256")
            source_bytes = manifest.get("source_bytes")
            if (not isinstance(checksum, str) or not sha_re.fullmatch(checksum)
                    or isinstance(source_bytes, bool) or not isinstance(source_bytes, int)
                    or source_bytes <= 0):
                raise RuntimeError(f"{manifest_relative} has invalid reference-only source binding")
        allowed.add(manifest_relative)

    extras = sorted(set(files) - allowed)
    if extras:
        raise RuntimeError("staged documents contain undeclared files: " + ", ".join(extras[:50]))
    allowed_directories = {document for document in documents}
    for item in allowed:
        pure = PurePosixPath(item)
        for depth in range(1, len(pure.parts)):
            allowed_directories.add("/".join(pure.parts[:depth]))
    actual_directories: set[str] = set()
    for directory, dirnames, _ in os.walk(root, topdown=True, followlinks=False):
        base = Path(directory)
        for name in dirnames:
            actual_directories.add((base / name).relative_to(root).as_posix())
    extra_directories = sorted(actual_directories - allowed_directories)
    if extra_directories:
        raise RuntimeError(
            "staged documents contain undeclared directories: "
            + ", ".join(extra_directories[:50])
        )
    return {path: files[path] for path in sorted(allowed)}


remote_program = r'''
import hashlib, json, os, re, stat, sys
from pathlib import Path, PurePosixPath
mode, raw, max_files_raw, max_file_raw, max_total_raw = sys.argv[1:]
MAX_RESULT_FILES = int(max_files_raw)
MAX_RESULT_FILE_BYTES = int(max_file_raw)
MAX_RESULT_TOTAL_BYTES = int(max_total_raw)
RESULT_ROOTS = {"paperqa", "data_extraction", "document_processing"}
SENSITIVE_SUFFIXES = {".pdf", ".xml", ".caj"}
SQLITE_MAGIC = b"SQLite format 3\x00"
ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
def excluded_name(name):
    lowered = name.casefold()
    return (".sqlite" in lowered or ".db" in lowered or
            lowered.endswith(("-wal", "-shm", "-journal", ".part", ".partial", ".tmp", ".temp", ".download", ".crdownload", ".lock")) or
            ".part." in lowered or ".partial." in lowered or ".tmp." in lowered or lowered == ".ds_store")
def extraction_result_path_allowed(relative, directory=False):
    parts = relative.parts
    if not parts or parts[0] != "data_extraction": return True
    if len(parts) == 1: return directory
    if not ID_RE.fullmatch(parts[1]): return False
    if len(parts) == 2: return directory
    branch = parts[2]
    if branch == "attempts":
        if len(parts) == 3: return directory
        if not ID_RE.fullmatch(parts[3]): return False
        return directory if len(parts) == 4 else len(parts) >= 5
    if branch == "receipts":
        if len(parts) == 3: return directory
        return (not directory and len(parts) == 4 and
                relative.suffix.casefold() == ".json" and
                ID_RE.fullmatch(relative.stem) is not None)
    return False
def result_path_allowed(relative, directory=False):
    if not relative.parts or relative.parts[0] not in RESULT_ROOTS: return False
    if relative.parts[0] == "data_extraction":
        return extraction_result_path_allowed(relative, directory=directory)
    if relative.parts[0] == "paperqa":
        if directory: return len(relative.parts) == 1
        return (len(relative.parts) == 2 and relative.suffix.casefold() == ".json" and
                ID_RE.fullmatch(relative.stem) is not None)
    return True
def excluded_path(relative, directory=False):
    if any(excluded_name(part) for part in relative.parts): return True
    lowered_name = relative.name.casefold()
    if mode in {"push_workflow", "pull_results"} and relative.suffix.casefold() in SENSITIVE_SUFFIXES: return True
    if mode == "push_workflow":
        parts = tuple(part.casefold() for part in relative.parts)
        if lowered_name == "acceptance.json" or "delivery" in parts or lowered_name == "review_queue.csv": return True
        if (len(parts) >= 3 and parts[0] == "tasks" and parts[2] == "extraction" and
                lowered_name in {"data_extraction_queue.json", "data_extraction_queue.csv"}): return True
    if mode == "pull_results" and lowered_name == "completion.json": return True
    if mode == "pull_results" and not result_path_allowed(relative, directory=directory): return True
    return False
def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""): value.update(chunk)
    return value.hexdigest()
def safe_text(relative):
    value = relative.as_posix()
    if not value or value.startswith("/") or "\\" in value or any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise RuntimeError(f"unsafe transfer path: {value!r}")
    if any(part in {"", ".", ".."} for part in PurePosixPath(value).parts): raise RuntimeError(f"non-canonical path: {value!r}")
    return value
def scan(root):
    try: root_stat = root.lstat()
    except FileNotFoundError: return False, {}
    if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode): raise RuntimeError(f"sync root must be a real directory: {root}")
    output = {}; total_bytes = 0
    for directory, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        base = Path(directory); base_relative = base.relative_to(root); kept = []
        for name in sorted(dirnames):
            path = base / name; relative = path.relative_to(root)
            if mode == "push_workflow" and not base_relative.parts and name != "tasks": continue
            if mode == "pull_results" and len(relative.parts) == 1 and name not in RESULT_ROOTS: continue
            if excluded_path(relative, directory=True): continue
            entry = path.lstat()
            if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode): raise RuntimeError(f"link/special directory is not allowed: {relative}")
            safe_text(relative); kept.append(name)
        dirnames[:] = kept
        for name in sorted(filenames):
            path = base / name; relative = path.relative_to(root)
            if mode == "push_workflow" and not base_relative.parts and name != "ACTIVE_TASK.json": continue
            if mode == "pull_results" and (not relative.parts or relative.parts[0] not in RESULT_ROOTS): continue
            if excluded_path(relative): continue
            entry = path.lstat()
            if stat.S_ISLNK(entry.st_mode) or not stat.S_ISREG(entry.st_mode): raise RuntimeError(f"link/special file is not allowed: {relative}")
            with path.open("rb") as handle:
                if handle.read(len(SQLITE_MAGIC)) == SQLITE_MAGIC:
                    raise RuntimeError(f"SQLite content is not allowed under a disguised name: {relative}")
            size = entry.st_size
            if mode == "pull_results":
                if size > MAX_RESULT_FILE_BYTES: raise RuntimeError(f"result exceeds per-file byte limit: {relative}")
                total_bytes += size
                if len(output) + 1 > MAX_RESULT_FILES: raise RuntimeError("result exceeds file-count limit")
                if total_bytes > MAX_RESULT_TOTAL_BYTES: raise RuntimeError("result exceeds total-byte limit")
            output[safe_text(relative)] = {"sha256": digest(path), "size": size}
    return True, output
root = Path(raw)
exists, files = scan(root)
payload = {"exists": exists, "files": files}
if mode == "push_workflow":
    state_path = root / ".paper-sync" / "workflow-state.json"
    if state_path.is_symlink(): raise RuntimeError("workflow state must not be a symlink")
    if state_path.exists():
        if not state_path.is_file(): raise RuntimeError("workflow state must be a regular file")
        payload["workflow_state"] = json.loads(state_path.read_text(encoding="utf-8"))
    else:
        payload["workflow_state"] = None
print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
'''

try:
    local_exists, local_files = scan(Path(local_raw))
    if mode == "push_documents":
        local_files = validated_document_allowlist(Path(local_raw), local_files, context)
    completed = subprocess.run(
        ["ssh", host, "python3", "-", mode, remote_raw, str(MAX_RESULT_FILES),
         str(MAX_RESULT_FILE_BYTES), str(MAX_RESULT_TOTAL_BYTES)], input=remote_program,
        text=True, capture_output=True, check=True,
    )
    remote_payload = json.loads(completed.stdout or "{}")
    expected_remote_keys = {"exists", "files", "workflow_state"} if mode == "push_workflow" else {"exists", "files"}
    if set(remote_payload) != expected_remote_keys or not isinstance(remote_payload["files"], dict):
        raise RuntimeError("remote snapshot has an invalid shape")
    remote_exists = bool(remote_payload["exists"])
    remote_files = remote_payload["files"]
    for path, entry in remote_files.items():
        if not isinstance(path, str) or not isinstance(entry, dict) or set(entry) != {"sha256", "size"}:
            raise RuntimeError("remote snapshot contains an invalid file entry")
        checksum, size = entry["sha256"], entry["size"]
        relative = Path(*PurePosixPath(path).parts)
        if safe_relative_text(relative) != path or excluded_path(relative):
            raise RuntimeError(f"remote snapshot contains a forbidden path: {path!r}")
        if mode == "pull_results" and relative.parts[0] not in RESULT_ROOTS:
            raise RuntimeError(f"remote snapshot path is outside result roots: {path!r}")
        if mode == "push_workflow" and not (
            path == "ACTIVE_TASK.json" or relative.parts[0] == "tasks"
        ):
            raise RuntimeError(f"remote snapshot path is outside workflow roots: {path!r}")
        if mode == "push_documents" and not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", relative.parts[0]
        ):
            raise RuntimeError(f"remote snapshot has an invalid document id: {path!r}")
        if mode == "push_documents" and len(relative.parts) < 2:
            raise RuntimeError(f"remote snapshot has a root-level document file: {path!r}")
        if not isinstance(checksum, str) or len(checksum) != 64 or any(ch not in "0123456789abcdef" for ch in checksum):
            raise RuntimeError(f"remote snapshot contains an invalid SHA-256: {path!r}")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise RuntimeError(f"remote snapshot contains an invalid byte size: {path!r}")
    workflow_state_files = None
    if mode == "push_workflow":
        state = remote_payload["workflow_state"]
        if state is not None:
            if (not isinstance(state, dict)
                    or set(state) != {"schema_version", "files"}
                    or state.get("schema_version") != "paper-workflow-sync-state/v1"
                    or not isinstance(state.get("files"), dict)):
                raise RuntimeError("remote workflow CAS state is invalid")
            workflow_state_files = state["files"]
            for path, entry in workflow_state_files.items():
                relative = Path(*PurePosixPath(path).parts)
                if (not isinstance(path, str) or safe_relative_text(relative) != path
                        or excluded_path(relative)
                        or not (path == "ACTIVE_TASK.json" or relative.parts[0] == "tasks")
                        or not isinstance(entry, dict) or set(entry) != {"sha256", "size"}
                        or not isinstance(entry["sha256"], str)
                        or len(entry["sha256"]) != 64
                        or any(ch not in "0123456789abcdef" for ch in entry["sha256"])
                        or isinstance(entry["size"], bool) or not isinstance(entry["size"], int)
                        or entry["size"] < 0):
                    raise RuntimeError("remote workflow CAS state contains a forbidden entry")
    if mode == "pull_results" and not remote_exists:
        raise RuntimeError(f"remote result directory is missing: {remote_raw}")
    if mode in {"push_workflow", "push_documents"} and not local_exists:
        raise RuntimeError(f"local source directory is missing: {local_raw}")

    if mode == "push_documents":
        local_by_doc: dict[str, dict[str, dict[str, int | str]]] = {}
        remote_by_doc: dict[str, dict[str, dict[str, int | str]]] = {}
        for path, entry in local_files.items():
            document = path.split("/", 1)[0]
            local_by_doc.setdefault(document, {})[path] = entry
        for path, entry in remote_files.items():
            document = path.split("/", 1)[0]
            remote_by_doc.setdefault(document, {})[path] = entry
        conflicts = [doc for doc in local_by_doc if doc in remote_by_doc and local_by_doc[doc] != remote_by_doc[doc]]
        if verify_complete:
            conflicts.extend(doc for doc in local_by_doc if remote_by_doc.get(doc) != local_by_doc[doc])
    elif mode == "push_workflow":
        conflicts = []
        if verify_complete:
            conflicts.extend(path for path, entry in local_files.items() if remote_files.get(path) != entry)
        else:
            for path, entry in local_files.items():
                current = remote_files.get(path)
                previous = None if workflow_state_files is None else workflow_state_files.get(path)
                if current == entry:
                    continue
                if workflow_state_files is None:
                    if current is not None:
                        conflicts.append(path)
                    continue
                if current is None:
                    if previous is not None:
                        conflicts.append(path)
                    continue
                if previous is None or current != previous:
                    conflicts.append(path)
    else:
        conflicts = [path for path, entry in local_files.items() if path in remote_files and remote_files[path] != entry]
    conflicts = sorted(set(conflicts))
    if conflicts:
        raise RuntimeError("same-path content conflict or incomplete transfer: " + ", ".join(conflicts[:50]))

    source_files = remote_files if mode == "pull_results" else local_files
    if mode == "pull_results":
        if len(remote_files) > MAX_RESULT_FILES:
            raise RuntimeError("remote snapshot exceeds file-count limit")
        if any(entry["size"] > MAX_RESULT_FILE_BYTES for entry in remote_files.values()):
            raise RuntimeError("remote snapshot exceeds per-file byte limit")
        if sum(entry["size"] for entry in remote_files.values()) > MAX_RESULT_TOTAL_BYTES:
            raise RuntimeError("remote snapshot exceeds total-byte limit")

    payload = {
        "schema_version": "paper-runtime-sync-snapshot/v2",
        "mode": mode,
        "files": source_files,
    }
    Path(snapshot_raw).write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    Path(list_raw).write_text("".join(path + "\n" for path in sorted(source_files)), encoding="utf-8")
except (OSError, RuntimeError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
    detail = getattr(exc, "stderr", "") or str(exc)
    print(f"sync snapshot/conflict preflight failed: {detail}", file=sys.stderr)
    raise SystemExit(3)
PY
}

safe_rsync=(
  -rlt --itemize-changes --ignore-existing
  --no-links --no-specials --no-devices
  --files-from="$file_list" --relative
)

run_rsync() {
  local source="$1"
  local destination="$2"
  local transfer_mode="${3:-push}"
  local options=("${safe_rsync[@]}")
  if [[ "$transfer_mode" == "pull" ]]; then
    options+=("--max-size=${PAPER_SYNC_MAX_RESULT_FILE_BYTES:-536870912}")
  fi
  if [[ "$apply_requested" != true ]]; then options+=(--dry-run); fi
  rsync "${options[@]}" "$source" "$destination"
}

publish_workflow_snapshot() {
  local generation
  generation="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
  local remote_stage="$remote_root/.paper-sync/incoming/$generation"

  # Create only the exact control/staging path after checking that no control
  # component is a symlink.  The configured runtime root itself is explicit
  # operator input; everything below it is owned by this protocol.
  ssh "$remote_host" python3 - "$remote_root" "$generation" <<'PY'
import os, re, stat, sys
from pathlib import Path
root = Path(sys.argv[1]); generation = sys.argv[2]
if not re.fullmatch(r"[0-9a-f]{32}", generation): raise SystemExit("invalid workflow generation")
root.mkdir(parents=True, exist_ok=True)
entry = root.lstat()
if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode): raise SystemExit("unsafe remote runtime root")
current = root
for name in (".paper-sync", "incoming", generation):
    current = current / name
    try: entry = current.lstat()
    except FileNotFoundError: current.mkdir(mode=0o700); entry = current.lstat()
    if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
        raise SystemExit(f"unsafe workflow control directory: {current}")
PY

  run_rsync "$mac_root/" "$remote_host:$remote_stage/"
  rsync -lt --no-links --no-specials --no-devices \
    "$snapshot_file" "$remote_host:$remote_stage/.snapshot.json"

  # CAS is repeated under an exclusive remote protocol lock.  Coordinator
  # files are atomically replaced one by one, with ACTIVE_TASK.json published
  # last so it never points to a task that has not yet been transferred.
  ssh "$remote_host" python3 - "$remote_root" "$generation" <<'PY'
import hashlib, json, os, re, shutil, stat, sys, uuid
from pathlib import Path, PurePosixPath

root = Path(sys.argv[1]); generation = sys.argv[2]
control = root / ".paper-sync"
stage = control / "incoming" / generation
state_path = control / "workflow-state.json"
lock_path = control / "workflow.lock"
SQLITE_MAGIC = b"SQLite format 3\x00"

def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""): value.update(chunk)
    return value.hexdigest()

def regular_entry(path):
    try: entry = path.lstat()
    except FileNotFoundError: return None
    if stat.S_ISLNK(entry.st_mode) or not stat.S_ISREG(entry.st_mode):
        raise RuntimeError(f"workflow target is link/special: {path}")
    with path.open("rb") as handle:
        if handle.read(len(SQLITE_MAGIC)) == SQLITE_MAGIC:
            raise RuntimeError(f"workflow target contains SQLite content: {path}")
    return {"sha256": digest(path), "size": entry.st_size}

def safe_relative(raw):
    pure = PurePosixPath(raw)
    if (not raw or pure.is_absolute() or "\\" in raw
            or any(part in {"", ".", ".."} for part in pure.parts)
            or not (raw == "ACTIVE_TASK.json" or pure.parts[0] == "tasks")):
        raise RuntimeError(f"unsafe workflow path: {raw!r}")
    return pure

def safe_parent(relative):
    current = root
    for part in relative.parts[:-1]:
        current = current / part
        try: entry = current.lstat()
        except FileNotFoundError: current.mkdir(mode=0o700); continue
        if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
            raise RuntimeError(f"unsafe workflow parent: {current}")
    return current

for directory in (root, control, control / "incoming", stage):
    entry = directory.lstat()
    if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
        raise RuntimeError(f"unsafe workflow protocol directory: {directory}")

lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
os.close(lock_fd)
try:
    snapshot_path = stage / ".snapshot.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    if (not isinstance(snapshot, dict)
            or snapshot.get("schema_version") != "paper-runtime-sync-snapshot/v2"
            or snapshot.get("mode") != "push_workflow"
            or not isinstance(snapshot.get("files"), dict)):
        raise RuntimeError("invalid staged workflow snapshot")
    expected = snapshot["files"]
    for raw, value in expected.items():
        safe_relative(raw)
        if (not isinstance(value, dict) or set(value) != {"sha256", "size"}
                or not isinstance(value["sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", value["sha256"])
                or isinstance(value["size"], bool) or not isinstance(value["size"], int)
                or value["size"] < 0):
            raise RuntimeError(f"invalid staged workflow entry: {raw!r}")

    actual = {}
    for directory, dirnames, filenames in os.walk(stage, topdown=True, followlinks=False):
        base = Path(directory)
        for name in dirnames:
            child = base / name; entry = child.lstat()
            if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
                raise RuntimeError(f"staged workflow has link/special directory: {child}")
        for name in filenames:
            child = base / name
            if child == snapshot_path: continue
            relative = child.relative_to(stage).as_posix()
            value = regular_entry(child)
            actual[relative] = value
    if actual != expected:
        raise RuntimeError("staged workflow does not match its snapshot")

    state = None
    if state_path.exists() or state_path.is_symlink():
        entry = state_path.lstat()
        if stat.S_ISLNK(entry.st_mode) or not stat.S_ISREG(entry.st_mode):
            raise RuntimeError("workflow CAS state is not a regular file")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if (not isinstance(state, dict) or set(state) != {"schema_version", "files"}
                or state.get("schema_version") != "paper-workflow-sync-state/v1"
                or not isinstance(state.get("files"), dict)):
            raise RuntimeError("invalid workflow CAS state")
    previous_files = {} if state is None else state["files"]

    for raw, wanted in expected.items():
        relative = safe_relative(raw)
        current = regular_entry(root.joinpath(*relative.parts))
        previous = previous_files.get(raw)
        if current == wanted: continue
        if state is None:
            if current is not None:
                raise RuntimeError(f"unmanaged remote workflow conflict: {raw}")
        elif current is None:
            if previous is not None:
                raise RuntimeError(f"remote workflow changed since last coordinator sync: {raw}")
        elif previous is None or current != previous:
            raise RuntimeError(f"remote workflow changed since last coordinator sync: {raw}")

    ordered = sorted((raw for raw in expected if raw != "ACTIVE_TASK.json"))
    if "ACTIVE_TASK.json" in expected: ordered.append("ACTIVE_TASK.json")
    for raw in ordered:
        relative = safe_relative(raw)
        source = stage.joinpath(*relative.parts)
        target = root.joinpath(*relative.parts)
        if regular_entry(target) == expected[raw]: continue
        parent = safe_parent(relative)
        temporary = parent / (".paper-sync-" + uuid.uuid4().hex + ".tmp")
        try:
            with source.open("rb") as input_handle:
                fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                with os.fdopen(fd, "wb") as output_handle:
                    shutil.copyfileobj(input_handle, output_handle, length=1 << 20)
                    output_handle.flush(); os.fsync(output_handle.fileno())
            if regular_entry(temporary) != expected[raw]:
                raise RuntimeError(f"workflow temporary copy mismatch: {raw}")
            os.replace(temporary, target)
        finally:
            try: temporary.unlink()
            except FileNotFoundError: pass

    merged = dict(previous_files)
    merged.update(expected)
    new_state = {"schema_version": "paper-workflow-sync-state/v1", "files": merged}
    state_temp = control / ("workflow-state." + uuid.uuid4().hex + ".tmp")
    with state_temp.open("x", encoding="utf-8") as handle:
        json.dump(new_state, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n"); handle.flush(); os.fsync(handle.fileno())
    os.replace(state_temp, state_path)
finally:
    try: lock_path.unlink()
    except FileNotFoundError: pass
    if stage.is_dir() and not stage.is_symlink(): shutil.rmtree(stage)
PY
}

write_sync_receipt() {
  local receipt_mode="$1"
  local receipt_task="$2"
  [[ "$apply_requested" == true ]] || return 0
  python3 - "$mac_root" "$snapshot_file" "$receipt_mode" "$receipt_task" \
    "$remote_host" "$remote_root" <<'PY'
import hashlib, json, os, sys, uuid
from datetime import datetime, timezone
from pathlib import Path
runtime, snapshot_path, mode, task_id, host, remote_root = sys.argv[1:]
raw = Path(snapshot_path).read_bytes()
snapshot = json.loads(raw)
directory = Path(runtime) / "sync_receipts" / "v1" / mode / (task_id or "workspace")
directory.mkdir(parents=True, exist_ok=True)
now = datetime.now(timezone.utc).isoformat(timespec="seconds")
name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex + ".json"
receipt = {
    "schema_version": "paper-runtime-sync-receipt/v1",
    "mode": mode,
    "task_id": task_id,
    "completed_at": now,
    "remote_host": host,
    "remote_root": remote_root,
    "snapshot_sha256": hashlib.sha256(raw).hexdigest(),
    "file_count": len(snapshot.get("files", {})),
}
path = directory / name
fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
with os.fdopen(fd, "w", encoding="utf-8") as handle:
    json.dump(receipt, handle, ensure_ascii=False, indent=2, sort_keys=True)
    handle.write("\n")
    handle.flush(); os.fsync(handle.fileno())
print(f"sync receipt: {path}")
PY
}

verify_and_publish_pull() {
  local staged="$1"
  local destination="$2"
  python3 - "$staged" "$destination" "$snapshot_file" <<'PY'
import hashlib, json, os, stat, sys
from pathlib import Path, PurePosixPath

stage, destination, snapshot_path = map(Path, sys.argv[1:])
payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
if payload.get("schema_version") != "paper-runtime-sync-snapshot/v2" or payload.get("mode") != "pull_results":
    raise SystemExit("invalid pull snapshot")
expected = payload.get("files")
if not isinstance(expected, dict): raise SystemExit("invalid pull snapshot files")

def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""): value.update(block)
    return value.hexdigest()

def regular(path, label):
    entry = path.lstat()
    if stat.S_ISLNK(entry.st_mode) or not stat.S_ISREG(entry.st_mode):
        raise RuntimeError(f"{label} is a link/special file: {path}")

def safe_parent(root, relative_parent):
    current = root
    if current.exists():
        entry = current.lstat()
        if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
            raise RuntimeError(f"destination root is unsafe: {current}")
    else:
        current.mkdir(mode=0o700)
    for part in relative_parent.parts:
        current = current / part
        try:
            entry = current.lstat()
        except FileNotFoundError:
            current.mkdir(mode=0o700)
            continue
        if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
            raise RuntimeError(f"destination parent is unsafe: {current}")

actual = {}
for directory, dirnames, filenames in os.walk(stage, topdown=True, followlinks=False):
    base = Path(directory)
    for name in list(dirnames):
        path = base / name; entry = path.lstat()
        if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
            raise RuntimeError(f"staged result has link/special directory: {path}")
    for name in filenames:
        path = base / name; regular(path, "staged result")
        relative = path.relative_to(stage).as_posix()
        with path.open("rb") as handle:
            if handle.read(16) == b"SQLite format 3\x00":
                raise RuntimeError(f"staged result contains SQLite content: {relative}")
        actual[relative] = {"sha256": digest(path), "size": path.stat().st_size}
if actual != expected:
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    changed = sorted(path for path in set(actual) & set(expected) if actual[path] != expected[path])
    raise RuntimeError(f"post-pull snapshot mismatch: missing={missing[:10]} extra={extra[:10]} changed={changed[:10]}")

created = []
try:
    for raw, entry in sorted(expected.items()):
        if not isinstance(entry, dict) or set(entry) != {"sha256", "size"}:
            raise RuntimeError(f"invalid snapshot entry: {raw}")
        checksum = entry["sha256"]
        expected_size = entry["size"]
        relative = PurePosixPath(raw)
        if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            raise RuntimeError(f"unsafe snapshot path: {raw!r}")
        source = stage.joinpath(*relative.parts)
        target = destination.joinpath(*relative.parts)
        regular(source, "staged result")
        safe_parent(destination, Path(*relative.parts[:-1]))
        try:
            regular(target, "existing local result")
        except FileNotFoundError:
            os.link(source, target, follow_symlinks=False)
            created.append(target)
        if target.stat().st_size != expected_size or digest(target) != checksum:
            raise RuntimeError(f"published result hash mismatch: {raw}")

    if destination.exists():
        for directory, dirnames, filenames in os.walk(destination, topdown=True, followlinks=False):
            base = Path(directory)
            for name in list(dirnames):
                path = base / name; entry = path.lstat()
                if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
                    raise RuntimeError(f"local result has link/special directory: {path}")
            for name in filenames:
                regular(base / name, "local result")
    for raw, entry in sorted(expected.items()):
        checksum = entry["sha256"]
        target = destination.joinpath(*PurePosixPath(raw).parts)
        regular(target, "published local result")
        with target.open("rb") as handle:
            if handle.read(16) == b"SQLite format 3\x00":
                raise RuntimeError(f"published result contains SQLite content: {raw}")
        if target.stat().st_size != entry["size"] or digest(target) != checksum:
            raise RuntimeError(f"final published result hash mismatch: {raw}")
except Exception:
    for path in reversed(created):
        try: path.unlink()
        except FileNotFoundError: pass
    raise
PY
}

case "$mode" in
  push-workflow)
    [[ -f "$mac_root/ACTIVE_TASK.json" && ! -L "$mac_root/ACTIVE_TASK.json" ]] || {
      echo "缺少普通文件 $mac_root/ACTIVE_TASK.json" >&2; exit 2; }
    [[ -d "$mac_root/tasks" && ! -L "$mac_root/tasks" ]] || {
      echo "缺少普通目录 $mac_root/tasks" >&2; exit 2; }
    prepare_snapshot push_workflow "$mac_root" "$remote_root"
    if [[ ! -s "$file_list" ]]; then echo "没有可同步的 workflow 文件" >&2; exit 2; fi
    if [[ "$apply_requested" == true ]]; then
      publish_workflow_snapshot
      prepare_snapshot push_workflow "$mac_root" "$remote_root" true
      write_sync_receipt push-workflow ""
    else
      # Preview only: no remote staging directory or lock is created.
      run_rsync "$mac_root/" "$remote_host:$remote_root/"
    fi
    ;;
  push-processing-inputs)
    source_dir="$mac_root/inputs/$task_id/documents"
    remote_destination="$remote_root/inputs/$task_id/documents"
    [[ -d "$source_dir" && ! -L "$source_dir" ]] || {
      echo "缺少普通处理输入目录：$source_dir" >&2; exit 2; }
    require_processing_policy
    require_fulltext_policy_if_needed "$source_dir"
    prepare_snapshot push_documents "$source_dir" "$remote_destination" false "$task_id"
    if [[ ! -s "$file_list" ]]; then echo "处理输入中没有可同步的普通文件" >&2; exit 2; fi
    if [[ "$apply_requested" == true ]]; then ssh "$remote_host" mkdir -p -- "$remote_destination"; fi
    run_rsync "$source_dir/" "$remote_host:$remote_destination/"
    if [[ "$apply_requested" == true ]]; then
      prepare_snapshot push_documents "$source_dir" "$remote_destination" true "$task_id"
      write_sync_receipt push-processing-inputs "$task_id"
    fi
    ;;
  pull-processing-results)
    remote_task_results="$remote_root/results/$task_id"
    local_task_results="$mac_root/results/$task_id"
    require_processing_policy
    prepare_snapshot pull_results "$local_task_results" "$remote_task_results"
    if [[ ! -s "$file_list" ]]; then echo "远程任务没有可拉取的版本化结果" >&2; exit 2; fi
    if [[ "$apply_requested" != true ]]; then
      run_rsync "$remote_host:$remote_task_results/" "$local_task_results/" pull
      exit 0
    fi
    mkdir -p "$mac_root/results"
    [[ ! -L "$mac_root/results" ]] || { echo "本机 results 根不能是符号链接" >&2; exit 3; }
    staging_root="$mac_root/.sync-staging"
    mkdir -p "$staging_root"
    [[ ! -L "$staging_root" ]] || { echo "同步 staging 根不能是符号链接" >&2; exit 3; }
    staging_dir="$(mktemp -d "$staging_root/pull-$task_id.XXXXXX")"
    run_rsync "$remote_host:$remote_task_results/" "$staging_dir/" pull
    verify_and_publish_pull "$staging_dir" "$local_task_results"
    write_sync_receipt pull-processing-results "$task_id"
    ;;
esac
