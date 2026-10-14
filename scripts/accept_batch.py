#!/usr/bin/env python3
"""验收一个 `REVIEW` 批次：校验 → 写论文库索引/待人工清单 → 置 `ACCEPTED`。

这是**协调Agent**的动作（`--actor-role coordinator`），下载Agent不能跑它。
以前这步全靠手工：对着 `download_results.csv` 逐行抄进 `PAPER_INDEX.csv`、
再把缺口抄进 `MANUAL_DOWNLOAD_BACKLOG.csv`，既慢又容易抄错批次号/路径。

它做什么
--------
1. **校验**（任一失败即中止，不写任何东西）：
   * `outputs/download_results.csv` 覆盖 `MANIFEST.csv` 全部 `item_id`；
   * 每个成功条目的文件真实存在，且 PDF/XML 重过 §7 闸门（复用 `build_download_results`）；
   * `outputs/files/` 内没有 `.part` / `.partial` / `.partial.json` / `.crdownload` 残留；
   * 缺 `outputs/manual_backlog.csv` 时自动补生成。
2. **写入**（持 `00_control/.queue.lock`，按批次幂等；重复跑不会重复登记）：
   * `04_library_index/PAPER_INDEX.csv` ← 成功条目；
   * `03_review/MANUAL_DOWNLOAD_BACKLOG.csv` ← 缺口条目；
   * `03_review/<batch_id>-acceptance.md` ← 验收记录；
   * 在同一批次锁临界区内校验并置 `ACCEPTED`；`update_batch_status.py`
     不允许绕过本验收闸门直接写 `ACCEPTED`。
   * 五个权威目标先写入带 SHA-256 的 prepared transaction；任一
     `os.replace` 之间中断时，重跑同一命令会在读状态前自动向前补齐。

    python3 accept_batch.py <batch_id> --actor-role coordinator --dry-run
    python3 accept_batch.py <batch_id> --actor-role coordinator

已 ACCEPTED 但交付件后来被证明错配时，用 `--reaccept` 重过闸门，
替换本批次在论文库索引和待人工清单里的行，并重写验收记录。状态保持 ACCEPTED。
"""
from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import io
import json
import os
import re
import shutil
import importlib.util
import subprocess
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_ROOT = Path.cwd()
PAPER_INDEX = Path("04_library_index/PAPER_INDEX.csv")
MANUAL_BACKLOG = Path("03_review/MANUAL_DOWNLOAD_BACKLOG.csv")
#: 必须与 04_library_index/PAPER_INDEX.csv 的**列顺序**完全一致（追加行是按本列表写的；
#: 顺序不一致会把值写进错误的列）。新增列只能追加在末尾。
PAPER_FIELDS = ["paper_id", "title", "doi", "pdf_path", "supplement_paths", "sha256",
                "source_batch_id", "identity_verified", "accepted_at", "notes",
                "mineru_md_path", "xml_md_path", "source_item_id", "document_id",
                "source_kind", "processed_text_sha256", "processing_method",
                "processing_version", "processing_accepted_at", "section_chunks_path",
                "section_chunks_sha256", "section_chunks_count",
                "section_chunks_schema_version"]
BACKLOG_FIELDS = ["batch_id", "item_id", "paper_id", "doi", "title", "status", "reason_code",
                  "reason", "attempted_channels", "needs", "owner", "reported_at",
                  "resolved_at", "resolution_batch_id", "notes"]
DOWNLOAD_RESULT_FIELDS = [
    "item_id", "paper_id", "status", "title", "doi", "source_url", "pdf_path",
    "supplement_paths", "sha256", "file_bytes", "page_count", "identity_verified",
    "downloaded_at", "notes",
]
MANIFEST_FIELDS = [
    "item_id", "paper_id", "title", "authors", "year", "journal", "doi",
    "landing_url", "priority", "download_reason", "requested_files",
    "target_filename", "suggested_filename", "source_record", "local_existing_path",
    "queue_action", "existing_batch_id", "notes",
]
MANUAL_STATUSES = {"not_found", "paywalled", "access_denied", "identity_mismatch", "failed"}
SAFE_BATCH_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
TRANSACTION_VERSION = 1
TRANSACTION_DIR = Path("00_control/.acceptance_transactions")
COMMON_IDENTITY_TOKENS = {
    "abstract", "article", "conclusion", "download", "fulltext", "introduction",
    "journal", "method", "methods", "paper", "research", "result", "results",
    "study", "supplement", "water", "soil", "groundwater", "environmental",
}


def validate_batch_id(value: str) -> str:
    """Reject path traversal and ambiguous filesystem spellings before path joins."""
    if (not SAFE_BATCH_ID_RE.fullmatch(value or "") or value in {".", ".."}
            or ".." in value or value.casefold().endswith(".committed")
            or Path(value).name != value):
        raise SystemExit(
            "batch_id 只能包含 ASCII 字母、数字、点、下划线和连字符，"
            "且不得包含路径分隔符、'..' 或保留后缀 '.committed'")
    return value


def fsync_directory(path: Path) -> None:
    """Persist directory-entry updates where the host filesystem supports it."""
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    """Durably replace one file with bytes written in the same directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def render_csv_bytes(fields: list[str], rows: list[dict[str, str]]) -> bytes:
    """Render the exact UTF-8-with-BOM CSV representation used by workspace indexes."""
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8-sig")


def transaction_target_paths(batch_id: str) -> list[str]:
    """Return the fixed, ordered roll-forward set; status files are commit markers."""
    validate_batch_id(batch_id)
    return [
        PAPER_INDEX.as_posix(),
        MANUAL_BACKLOG.as_posix(),
        f"03_review/{batch_id}-acceptance.md",
        f"02_batches/{batch_id}/STATUS.json",
        "00_control/BATCH_INDEX.csv",
    ]


def acceptance_transaction_dir(root: Path, batch_id: str) -> Path:
    validate_batch_id(batch_id)
    candidate = root / TRANSACTION_DIR / "pending" / batch_id
    resolved_parent = candidate.parent.resolve(strict=False)
    resolved_root = root.resolve()
    if resolved_parent == resolved_root or resolved_root not in resolved_parent.parents:
        raise RuntimeError(f"验收事务目录越界：{candidate}")
    return candidate


def committed_transaction_dir(root: Path, batch_id: str) -> Path:
    validate_batch_id(batch_id)
    candidate = root / TRANSACTION_DIR / "committed" / batch_id
    resolved_parent = candidate.parent.resolve(strict=False)
    resolved_root = root.resolve()
    if resolved_parent == resolved_root or resolved_root not in resolved_parent.parents:
        raise RuntimeError(f"已完成验收事务目录越界：{candidate}")
    return candidate


def pending_transaction_batch_ids(root: Path) -> list[str]:
    """List pending batch journals without trusting directory names as paths."""
    pending_root = root / TRANSACTION_DIR / "pending"
    if not pending_root.exists():
        return []
    if pending_root.is_symlink() or not pending_root.is_dir():
        raise RuntimeError(f"验收 pending 路径不是普通目录：{pending_root}")
    batch_ids: list[str] = []
    for child in pending_root.iterdir():
        if child.is_symlink() or not child.is_dir():
            raise RuntimeError(f"验收 pending 中存在非法项：{child}")
        try:
            batch_ids.append(validate_batch_id(child.name))
        except SystemExit as exc:
            raise RuntimeError(f"验收 pending 目录名非法：{child.name!r}") from exc
    return sorted(batch_ids)


def _safe_transaction_target(root: Path, batch_id: str, relative: str) -> Path:
    expected = set(transaction_target_paths(batch_id))
    if relative not in expected:
        raise RuntimeError(f"验收事务包含非法目标：{relative!r}")
    candidate = root / Path(relative)
    resolved_root = root.resolve()
    resolved_candidate = candidate.resolve(strict=False)
    if resolved_candidate == resolved_root or resolved_root not in resolved_candidate.parents:
        raise RuntimeError(f"验收事务目标越界：{relative!r}")
    return candidate


def snapshot_dependency(path: Path, label: str,
                        expected_sha256: str = "") -> dict[str, object]:
    """Bind a prepared transaction to one immutable-on-replay input file."""
    path = path.expanduser()
    if not path.is_file():
        raise RuntimeError(f"验收事务依赖不存在：{label} -> {path}")
    resolved = path.resolve(strict=True)
    digest = file_sha256(path)
    if expected_sha256 and digest != expected_sha256:
        raise RuntimeError(f"验收事务依赖在准备前已变化：{label}")
    return {
        "label": label,
        "path": str(path),
        "resolved_path": str(resolved),
        "sha256": digest,
        "size": path.stat().st_size,
    }


def build_acceptance_dependencies(
        batch: Path, success_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    """Snapshot authoritative inputs needed to justify the prepared outputs."""
    dependencies = [
        snapshot_dependency(batch / "MANIFEST.csv", "manifest"),
        snapshot_dependency(batch / "outputs/download_results.csv", "download_results"),
        snapshot_dependency(batch / "outputs/manual_backlog.csv", "manual_backlog"),
    ]
    for row in success_rows:
        item_id = row.get("item_id", "")
        payload = Path(row.get("pdf_path") or "")
        dependencies.append(snapshot_dependency(
            payload,
            f"fulltext:{item_id}",
            row.get("_validated_payload_sha256", ""),
        ))
    return dependencies


def verify_transaction_dependencies(raw_dependencies: object) -> None:
    """Fail closed if any source used during validation changed before replay."""
    if not isinstance(raw_dependencies, list) or len(raw_dependencies) < 3:
        raise RuntimeError("验收事务缺少必需依赖快照")
    labels: set[str] = set()
    for dependency in raw_dependencies:
        if not isinstance(dependency, dict):
            raise RuntimeError("验收事务依赖项格式错误")
        label = dependency.get("label")
        path_text = dependency.get("path")
        resolved_text = dependency.get("resolved_path")
        digest = dependency.get("sha256")
        size = dependency.get("size")
        if (not isinstance(label, str) or not label or label in labels
                or not isinstance(path_text, str) or not path_text
                or not isinstance(resolved_text, str) or not resolved_text
                or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
                or not isinstance(size, int) or size < 0):
            raise RuntimeError("验收事务依赖项缺少必需字段或重复")
        labels.add(label)
        path = Path(path_text)
        try:
            current_resolved = path.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise RuntimeError(f"验收事务依赖已丢失：{label}") from exc
        if (not path.is_file() or str(current_resolved) != resolved_text
                or path.stat().st_size != size or file_sha256(path) != digest):
            raise RuntimeError(f"验收事务依赖已变化：{label}")
    if not {"manifest", "download_results", "manual_backlog"} <= labels:
        raise RuntimeError("验收事务依赖快照不完整")


def prepare_acceptance_transaction(
        root: Path, batch_id: str, target_payloads: dict[str, bytes],
        dependencies: list[dict[str, object]]) -> Path:
    """Write every payload and its digest before publishing the prepared manifest."""
    expected = transaction_target_paths(batch_id)
    if list(target_payloads) != expected:
        raise RuntimeError("验收事务的目标集或顺序与合同不一致")

    transaction_dir = acceptance_transaction_dir(root, batch_id)
    committed_dir = committed_transaction_dir(root, batch_id)
    transaction_parent = transaction_dir.parent
    committed_parent = committed_dir.parent
    transaction_parent.mkdir(parents=True, exist_ok=True)
    committed_parent.mkdir(parents=True, exist_ok=True)
    pending_ids = pending_transaction_batch_ids(root)
    if pending_ids and pending_ids != [batch_id]:
        raise RuntimeError(
            f"其他批次尚有待恢复验收事务：{pending_ids}")
    if committed_dir.is_symlink():
        raise RuntimeError(f"已完成验收事务目录不得是符号链接：{committed_dir}")
    if committed_dir.exists():
        if not committed_dir.is_dir():
            raise RuntimeError(f"已完成验收事务路径不是目录：{committed_dir}")
        shutil.rmtree(committed_dir)
        fsync_directory(committed_parent)
    if transaction_dir.is_symlink():
        raise RuntimeError(f"验收事务目录不得是符号链接：{transaction_dir}")
    if transaction_dir.exists():
        # A directory without prepared.json can only be an interrupted prepare:
        # replay never starts before the manifest has been atomically published.
        if (transaction_dir / "prepared.json").exists():
            raise RuntimeError(f"存在未恢复的验收事务：{transaction_dir}")
        shutil.rmtree(transaction_dir)
        fsync_directory(transaction_parent)
    transaction_dir.mkdir()
    fsync_directory(transaction_parent)

    entries: list[dict[str, object]] = []
    for ordinal, relative in enumerate(expected):
        payload = target_payloads[relative]
        suffix = Path(relative).suffix or ".bin"
        payload_name = f"{ordinal:02d}-{Path(relative).stem}{suffix}"
        atomic_write_bytes(transaction_dir / payload_name, payload)
        entries.append({
            "relative_path": relative,
            "payload": payload_name,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size": len(payload),
        })

    manifest = {
        "version": TRANSACTION_VERSION,
        "batch_id": batch_id,
        "prepared_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "dependencies": dependencies,
        "targets": entries,
    }
    manifest_bytes = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    # Publishing prepared.json is the prepare/commit boundary. No target is
    # touched before this durable atomic write completes.
    atomic_write_bytes(transaction_dir / "prepared.json", manifest_bytes)
    return transaction_dir


def _load_prepared_transaction(
        root: Path, batch_id: str, transaction_dir: Path) \
        -> tuple[list[tuple[dict[str, object], bytes]], list[dict[str, object]]]:
    manifest_path = transaction_dir / "prepared.json"
    if manifest_path.is_symlink():
        raise RuntimeError("验收事务清单不得是符号链接")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"验收事务清单无法读取：{exc}") from exc
    if manifest.get("version") != TRANSACTION_VERSION:
        raise RuntimeError("验收事务版本不受支持")
    if manifest.get("batch_id") != batch_id:
        raise RuntimeError("验收事务 batch_id 与路径不一致")
    raw_dependencies = manifest.get("dependencies")
    verify_transaction_dependencies(raw_dependencies)
    assert isinstance(raw_dependencies, list)
    targets = manifest.get("targets")
    if not isinstance(targets, list):
        raise RuntimeError("验收事务 targets 必须是数组")
    relative_paths = [entry.get("relative_path") if isinstance(entry, dict) else None
                      for entry in targets]
    if relative_paths != transaction_target_paths(batch_id):
        raise RuntimeError("验收事务目标集或顺序被篡改")

    verified: list[tuple[dict[str, object], bytes]] = []
    for entry in targets:
        if not isinstance(entry, dict):
            raise RuntimeError("验收事务目标项格式错误")
        relative = entry.get("relative_path")
        payload_name = entry.get("payload")
        digest = entry.get("sha256")
        size = entry.get("size")
        if (not isinstance(relative, str) or not isinstance(payload_name, str)
                or not isinstance(digest, str) or not isinstance(size, int)):
            raise RuntimeError("验收事务目标项缺少必需字段")
        _safe_transaction_target(root, batch_id, relative)
        if Path(payload_name).name != payload_name or payload_name in {".", ".."}:
            raise RuntimeError(f"验收事务载荷路径非法：{payload_name!r}")
        payload_path = transaction_dir / payload_name
        if payload_path.is_symlink() or not payload_path.is_file():
            raise RuntimeError(f"验收事务载荷不存在：{payload_name}")
        payload = payload_path.read_bytes()
        if len(payload) != size or hashlib.sha256(payload).hexdigest() != digest:
            raise RuntimeError(f"验收事务载荷哈希不一致：{payload_name}")
        verified.append((entry, payload))
    return verified, raw_dependencies


def install_transaction_target(root: Path, batch_id: str,
                               entry: dict[str, object], payload: bytes) -> None:
    """Install one verified target; split out so tests can inject a crash."""
    relative = entry["relative_path"]
    if not isinstance(relative, str):
        raise RuntimeError("验收事务目标路径类型错误")
    target = _safe_transaction_target(root, batch_id, relative)
    atomic_write_bytes(target, payload)


def recover_prepared_acceptance(root: Path, batch_id: str) -> bool:
    """Roll a prepared acceptance transaction forward, idempotently.

    This function must run while holding ``00_control/.queue.lock`` and before
    reading either status file. It verifies *all* payloads first, then replaces
    targets in a fixed order. A second crash simply causes the next run to
    replay the same hashed bytes.
    """
    transaction_dir = acceptance_transaction_dir(root, batch_id)
    if not transaction_dir.exists():
        return False
    if transaction_dir.is_symlink() or not transaction_dir.is_dir():
        raise RuntimeError(f"验收事务路径不是普通目录：{transaction_dir}")
    manifest_path = transaction_dir / "prepared.json"
    if not manifest_path.exists():
        # Interrupted preparation is safe to discard: target replacement is
        # never entered until prepared.json has been durably published.
        shutil.rmtree(transaction_dir)
        fsync_directory(transaction_dir.parent)
        return False

    verified, dependencies = _load_prepared_transaction(root, batch_id, transaction_dir)
    for ordinal, (entry, payload) in enumerate(verified):
        # Re-check immediately before publishing either status file, so source
        # mutation cannot turn a partial data update into a visible ACCEPTED
        # generation. A failed check leaves the prepared journal in place and
        # blocks all later accepts until the source is restored or audited.
        if ordinal == 3:
            verify_transaction_dependencies(dependencies)
        relative = entry["relative_path"]
        target = _safe_transaction_target(root, batch_id, str(relative))
        digest = str(entry["sha256"])
        if target.is_symlink() or not target.is_file() or file_sha256(target) != digest:
            install_transaction_target(root, batch_id, entry, payload)
        if target.is_symlink() or not target.is_file() or file_sha256(target) != digest:
            raise RuntimeError(f"验收事务目标写入后校验失败：{relative}")

    verify_transaction_dependencies(dependencies)

    # Atomically retire the pending name. If the process dies during optional
    # cleanup, no future run can mistake a half-deleted payload set for a
    # recoverable prepared transaction.
    committed_dir = committed_transaction_dir(root, batch_id)
    if committed_dir.exists():
        if committed_dir.is_symlink() or not committed_dir.is_dir():
            raise RuntimeError(f"已完成验收事务路径异常：{committed_dir}")
        shutil.rmtree(committed_dir)
        fsync_directory(committed_dir.parent)
    os.replace(transaction_dir, committed_dir)
    fsync_directory(transaction_dir.parent)
    fsync_directory(committed_dir.parent)
    try:
        shutil.rmtree(committed_dir)
        fsync_directory(committed_dir.parent)
    except OSError:
        # The authoritative targets are already fully verified and the pending
        # name is gone. A leftover committed directory is harmless and will be
        # removed before the next reaccept transaction.
        pass
    return True


def recover_all_prepared_acceptances(root: Path) -> list[str]:
    """Recover the one globally serialized pending acceptance, if present.

    ``.queue.lock`` serializes transaction preparation, so correct operation
    can leave at most one pending journal. Multiple journals cannot be safely
    ordered from full-file snapshots and therefore fail closed instead of
    guessing and risking lost global-index updates.
    """
    batch_ids = pending_transaction_batch_ids(root)
    if len(batch_ids) > 1:
        raise RuntimeError(
            "同时存在多个验收 pending 事务，无法安全确定全局快照顺序："
            f"{batch_ids}")
    recovered: list[str] = []
    for pending_batch_id in batch_ids:
        if recover_prepared_acceptance(root, pending_batch_id):
            recovered.append(pending_batch_id)
    return recovered


def load_builder():
    """复用 build_download_results 的闸门实现，避免两套校验逻辑漂移。"""
    spec = importlib.util.spec_from_file_location(
        "build_download_results", HERE / "build_download_results.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def assert_header_matches(path: Path, fields: list[str]) -> None:
    """追加前必须确认磁盘上的列顺序与代码里的 fields 完全一致。

    2026-09-17 事故教训：CSV 加了列但代码没同步时，`DictWriter` 会按自己的顺序写值，
    **静默把内容写进错误的列**。这里宁可响亮失败。
    """
    if not path.is_file() or path.stat().st_size == 0:
        return
    with path.open(encoding="utf-8-sig", newline="") as fh:
        header = next(csv.reader(fh), [])
    if header != fields:
        raise SystemExit(
            f"{path.name} 的表头与代码不一致，拒绝追加（否则会写错列）：\n"
            f"  文件: {header}\n  代码: {fields}\n"
            f"请先同步 accept_batch.py 的字段列表或修正该 CSV。")


def backup_once(path: Path) -> None:
    """首次改动前留一份备份（索引被写坏的代价太高）。"""
    if path.is_file() and path.stat().st_size > 0:
        bak = path.with_suffix(path.suffix + ".bak")
        if not bak.exists():
            shutil.copy2(path, bak)


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    assert_header_matches(path, fields)
    backup_once(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def normalized_title(value: str) -> str:
    """题名身份比对只容忍大小写和排版空白差异，不做模糊匹配。"""
    return " ".join((value or "").split()).casefold()


def normalized_doi(value: str) -> str:
    """将 DOI URL/标签写法统一后比对，仍要求 DOI 本体完全相同。"""
    value = (value or "").strip().casefold()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if value.startswith(prefix):
            value = value[len(prefix):].strip()
            break
    return value


def row_fingerprint(row: dict[str, str]) -> str:
    """给 CSV 行生成稳定快照，用于防止校验后、写回前被替换。"""
    payload = json.dumps(row, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def identity_token_errors(batch: Path,
                          tokens: dict[str, tuple[str, ...]]) -> list[str]:
    """Identity token 必须是硬标识，不能是空值、短词或普通学术词。"""
    errors: list[str] = []
    token_path = batch / "outputs/identity_tokens.json"
    if token_path.is_file():
        try:
            raw_doc = json.loads(token_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return [f"{batch.name} identity_tokens.json 无法解析：{exc}"]
        if not isinstance(raw_doc, dict) or not raw_doc:
            errors.append(f"{batch.name} identity_tokens.json 必须是非空对象")
        else:
            for paper_id, values in raw_doc.items():
                if not isinstance(values, list) or not values:
                    errors.append(
                        f"{batch.name} identity_tokens.json {paper_id or '<empty>'} "
                        "必须是非空 token 数组")
    for paper_id, values in tokens.items():
        if not values:
            errors.append(
                f"{batch.name} identity_tokens.json {paper_id or '<empty>'} token 列表为空")
        for raw in values:
            token = str(raw).strip()
            folded = token.casefold()
            if not token:
                reason = "为空"
            elif len(token) < 8:
                reason = "过短（少于 8 字符）"
            elif (folded in COMMON_IDENTITY_TOKENS
                  or re.fullmatch(r"[a-z\s._:/-]+", folded)):
                reason = "过常见，不是可唯一定位的硬标识"
            else:
                continue
            errors.append(
                f"{batch.name} identity_tokens.json {paper_id or '<empty>'} token {token!r} {reason}")
    return errors


def validate_manual_backlog(batch: Path, gaps: list[dict[str, str]]) \
        -> tuple[list[str], list[dict[str, str]]]:
    """校验批次待人工表与已验证的缺口一一对应。"""
    path = batch / "outputs/manual_backlog.csv"
    if not path.is_file():
        return [f"{batch.name} manual_backlog.csv 未生成"], []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames or []
        rows = list(reader)
    missing_fields = [field for field in BACKLOG_FIELDS if field not in fields]
    if missing_fields:
        return [f"{batch.name} manual_backlog.csv 缺少必需列：{missing_fields}"], rows

    expected_pairs = Counter((row.get("item_id", ""), row.get("status", "")) for row in gaps)
    actual_pairs = Counter((row.get("item_id", ""), row.get("status", "")) for row in rows)
    errors: list[str] = []
    if expected_pairs != actual_pairs:
        errors.append(
            f"{batch.name} manual_backlog.csv 与 gaps 的 item_id/status 覆盖不一致："
            f"期望 {dict(expected_pairs)}，实际 {dict(actual_pairs)}")
    gap_by_id = {row.get("item_id", ""): row for row in gaps}
    for row in rows:
        item_id = row.get("item_id", "")
        expected = gap_by_id.get(item_id)
        if row.get("batch_id", "") != batch.name:
            errors.append(f"{batch.name} manual_backlog.csv {item_id} batch_id 不一致")
        if expected is None:
            continue
        mismatches = []
        if (row.get("paper_id") or "").strip() != (expected.get("paper_id") or "").strip():
            mismatches.append("paper_id")
        if normalized_title(row.get("title") or "") != normalized_title(expected.get("title") or ""):
            mismatches.append("title")
        if normalized_doi(row.get("doi") or "") != normalized_doi(expected.get("doi") or ""):
            mismatches.append("doi")
        if mismatches:
            errors.append(
                f"{batch.name} manual_backlog.csv {item_id} 身份不一致：{','.join(mismatches)}")
    return errors, rows


def persist_validated_results(path: Path, success_rows: list[dict[str, str]],
                              validated_rows: list[dict[str, str]] | None = None) -> bool:
    """把验收时确认的全文路径原子写回结果表。

    XML-only 记录原先的 ``pdf_path`` 是空的；如果只把 XML 路径写进
    PAPER_INDEX，后续 fulltext queue 仍会读 download_results.csv 并误判为
    unavailable。因此在整个批次验证无误后，把已规范化的路径和
    ``[XML_FULL]`` 类型标记回写到权威结果表。
    """
    validated_rows = validated_rows if validated_rows is not None else success_rows
    if not validated_rows:
        return False
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames or []
        rows = list(reader)
    missing_fields = [field for field in DOWNLOAD_RESULT_FIELDS if field not in fields]
    if missing_fields:
        raise ValueError(
            f"{path.name} 缺少必需列，拒绝回写：{missing_fields}")
    current_ids = [row.get("item_id", "") for row in rows]
    validated_ids = [row.get("item_id", "") for row in validated_rows]
    if Counter(current_ids) != Counter(validated_ids):
        raise RuntimeError(f"{path.name} 在验收校验后发生了行集变化，拒绝写回")
    current_by_id = {row.get("item_id", ""): row for row in rows}
    for validated in validated_rows:
        item_id = validated.get("item_id", "")
        expected_fingerprint = validated.get("_validated_source_fingerprint", "")
        if not expected_fingerprint or row_fingerprint(current_by_id[item_id]) != expected_fingerprint:
            raise RuntimeError(
                f"{path.name} {item_id or '<unknown>'} 在验收校验后被修改，拒绝写回")

    replacements: dict[str, dict[str, str]] = {}
    for row in success_rows:
        item_id = row.get("item_id", "")
        payload = Path(row.get("pdf_path") or "")
        expected_sha256 = row.get("_validated_payload_sha256", "")
        if not payload.is_file() or payload.stat().st_size <= 0:
            raise RuntimeError(f"{item_id} 全文在验收校验后丢失或变空：{payload}")
        if not expected_sha256 or file_sha256(payload) != expected_sha256:
            raise RuntimeError(f"{item_id} 全文在验收校验后内容发生变化：{payload}")
        replacements[item_id] = {
            key: value for key, value in row.items() if not key.startswith("_validated_")
        }
    merged = [replacements.get(row.get("item_id", ""), row) for row in rows]
    if merged == rows:
        return False
    backup_once(path)
    atomic_write_bytes(path, render_csv_bytes(fields, merged))
    return True


def validate(root: Path, batch: Path, builder) -> tuple[list[str], list[dict], list[dict]]:
    """返回 (errors, success_rows, gap_rows)。"""
    errors: list[str] = []
    manifest_path = batch / "MANIFEST.csv"
    manifest = read_csv(manifest_path)
    if not manifest:
        return [f"{batch.name} MANIFEST.csv 缺失或无数据行"], [], []
    with manifest_path.open(encoding="utf-8-sig", newline="") as handle:
        manifest_fields = csv.DictReader(handle).fieldnames or []
    missing_manifest_fields = [field for field in MANIFEST_FIELDS if field not in manifest_fields]
    if missing_manifest_fields:
        return [f"{batch.name} MANIFEST.csv 缺少必需列：{missing_manifest_fields}"], [], []
    blank_keys = [str(index) for index, row in enumerate(manifest, 2)
                  if not (row.get("item_id") or "").strip()
                  or not (row.get("paper_id") or "").strip()]
    if blank_keys:
        return [f"{batch.name} MANIFEST.csv 第 {blank_keys[:5]} 行 item_id/paper_id 不得为空"], [], []
    results_path = batch / "outputs/download_results.csv"
    results = read_csv(results_path)
    if not results:
        return [f"{batch.name} 缺 outputs/download_results.csv（先跑 build_download_results.py）"], [], []
    with results_path.open(encoding="utf-8-sig", newline="") as handle:
        result_fields = csv.DictReader(handle).fieldnames or []
    missing_fields = [field for field in DOWNLOAD_RESULT_FIELDS if field not in result_fields]
    if missing_fields:
        return [f"{batch.name} download_results.csv 缺少必需列：{missing_fields}"], [], []
    m_ids = [r.get("item_id", "") for r in manifest]
    r_ids = [r.get("item_id", "") for r in results]
    if set(m_ids) != set(r_ids):
        missing = sorted(set(m_ids) - set(r_ids))
        extra = sorted(set(r_ids) - set(m_ids))
        errors.append(f"{batch.name} download_results.csv 与 MANIFEST 不一致：缺 {missing[:5]} 多 {extra[:5]}")
    duplicate_manifest = sorted(item_id for item_id, count in Counter(m_ids).items() if count > 1)
    duplicate_results = sorted(item_id for item_id, count in Counter(r_ids).items() if count > 1)
    if duplicate_manifest:
        errors.append(f"{batch.name} MANIFEST.csv 有重复 item_id：{duplicate_manifest[:5]}")
    if duplicate_results:
        errors.append(f"{batch.name} download_results.csv 有重复 item_id：{duplicate_results[:5]}")

    files_dir = batch / "outputs/files"
    strays = [p.name for p in files_dir.glob("*")
              if p.suffix in (".part", ".crdownload") or ".partial" in p.name]
    if strays:
        errors.append(f"{batch.name} outputs/files 有分片残留：{strays[:5]}")

    tokens = builder.load_tokens(batch)
    errors.extend(identity_token_errors(batch, tokens))
    manifest_by_id = {r.get("item_id", ""): r for r in manifest}
    targets = {r.get("paper_id", ""): (r.get("suggested_filename") or r.get("target_filename") or "")
               for r in manifest}
    success, gaps = [], []
    for row in results:
        item_id = row.get("item_id", "") or "<unknown>"
        expected = manifest_by_id.get(row.get("item_id", ""))
        if expected is not None:
            mismatches = []
            if (row.get("paper_id") or "").strip() != (expected.get("paper_id") or "").strip():
                mismatches.append("paper_id")
            if normalized_title(row.get("title") or "") != normalized_title(expected.get("title") or ""):
                mismatches.append("title")
            if normalized_doi(row.get("doi") or "") != normalized_doi(expected.get("doi") or ""):
                mismatches.append("doi")
            if mismatches:
                errors.append(
                    f"{batch.name} {item_id} 与 MANIFEST 身份不一致：{','.join(mismatches)}")
                continue
        status = (row.get("status") or "").strip()
        allowed_statuses = set(builder.SUCCESS) | MANUAL_STATUSES
        if status not in allowed_statuses:
            errors.append(
                f"{batch.name} {item_id} status {status!r} 为空或不在允许集合 "
                f"{sorted(allowed_statuses)}")
            continue
        if status in builder.SUCCESS:
            path_text = (row.get("pdf_path") or "").strip()
            payload: Path | None = None
            payload_kind = "pdf"

            if path_text:
                payload = Path(path_text).expanduser()
                if not payload.is_absolute():
                    payload = root / payload
                if payload.suffix.lower() == ".xml":
                    payload_kind = "xml"
            else:
                # XML_FULL 条目在 download_results.csv 中没有 pdf_path；只有在
                # 标准目录确实找到全文 XML 并重过闸门时，才能视为成功。
                payload_kind = "xml"
                payload = builder.find_xml(
                    batch / "outputs/tdm-xml", row.get("paper_id", ""),
                    targets.get(row.get("paper_id", ""), ""), row.get("doi", ""))

            if not payload.is_file():
                detail = f"交付路径不存在：{payload}" if path_text else "未填写 pdf_path，且未找到全文 XML"
                errors.append(f"{batch.name} {item_id} {detail}")
                continue
            try:
                payload_size = payload.stat().st_size
            except OSError as exc:
                errors.append(f"{batch.name} {item_id} 无法读取交付件：{payload}（{exc}）")
                continue
            if payload_size <= 0:
                errors.append(f"{batch.name} {item_id} 交付件为空文件：{payload}")
                continue

            if payload_kind == "xml":
                gate = builder.gate_xml(payload, row.get("doi", ""), row.get("title", ""))
            else:
                gate = builder.gate_pdf(
                    payload, row.get("doi", ""), row.get("title", ""),
                    tokens.get(row.get("paper_id", ""), ()))
            if not (gate.get("ok") and gate.get("identity_verified")):
                errors.append(
                    f"{batch.name} {item_id} 交付件未过闸门/身份校验："
                    f"{gate.get('reason') or gate.get('how') or '未知原因'}")
                continue

            payload_sha256 = file_sha256(payload)
            accepted = dict(row)
            accepted["_validated_source_fingerprint"] = row_fingerprint(row)
            accepted["_validated_payload_sha256"] = payload_sha256
            accepted["pdf_path"] = str(payload)
            accepted["identity_verified"] = "true"
            if "file_bytes" in accepted:
                accepted["file_bytes"] = str(payload_size)
            if "sha256" in accepted:
                accepted["sha256"] = str(gate.get("sha256") or payload_sha256)
            if payload_kind == "xml":
                notes = row.get("notes") or ""
                if not notes.startswith("[XML_FULL]"):
                    notes = "[XML_FULL] 全文 XML（铁律 1），该条无 PDF；" + notes
                accepted["notes"] = notes[:300]
            success.append(accepted)
        else:
            gap = dict(row)
            gap["_validated_source_fingerprint"] = row_fingerprint(row)
            gaps.append(gap)
    return errors, success, gaps


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("batch_id")
    parser.add_argument("--actor-role", choices=["worker", "coordinator"], required=True)
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reaccept", action="store_true",
                        help="重验收已 ACCEPTED 的批次：替换本批次索引行和验收记录")
    args = parser.parse_args()

    if args.actor_role != "coordinator":
        raise SystemExit("只有协调Agent可以验收批次（--actor-role coordinator）")

    batch_id = validate_batch_id(args.batch_id)
    root = Path(args.root).expanduser().resolve()
    batch = root / "02_batches" / batch_id
    batches_root = (root / "02_batches").resolve(strict=False)
    if (root not in batches_root.parents or batch.is_symlink()
            or batch.resolve(strict=False).parent != batches_root):
        raise SystemExit(f"批次路径越出工作区：{batch}")

    reaccept = bool(args.reaccept)
    lock_path = root / "00_control/.queue.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        # A prior process may have died after replacing only some of the five
        # authoritative files. Recover before reading either status file.
        try:
            recovered_batch_ids = recover_all_prepared_acceptances(root)
        except (OSError, RuntimeError) as exc:
            print(f"  ERROR 未能恢复已准备的验收事务：{exc}")
            print("验收中止：事务已保留，修复存储问题后重跑同一命令")
            return 1
        if recovered_batch_ids:
            print(
                "  已自动补齐上次中断的验收事务："
                + ", ".join(recovered_batch_ids))

        if not (batch / "MANIFEST.csv").is_file():
            raise SystemExit(f"批次不存在：{batch}")
        index_path = root / "00_control/BATCH_INDEX.csv"
        with index_path.open(encoding="utf-8-sig", newline="") as handle:
            index_reader = csv.DictReader(handle)
            index_fields = index_reader.fieldnames or []
            index = list(index_reader)
        required_index_fields = {"batch_id", "status", "updated_at", "accepted_at"}
        if not required_index_fields <= set(index_fields):
            raise SystemExit(
                f"BATCH_INDEX.csv 缺少状态字段：{sorted(required_index_fields - set(index_fields))}")
        index_row = next((row for row in index if row.get("batch_id") == batch_id), None)
        if index_row is None:
            raise SystemExit(f"BATCH_INDEX.csv 里没有 {batch_id}")

        status_path = batch / "STATUS.json"
        if not status_path.is_file():
            raise SystemExit(f"{batch_id} 缺 STATUS.json")
        status_doc = json.loads(status_path.read_text(encoding="utf-8"))
        if status_doc.get("batch_id") != batch_id:
            raise SystemExit(
                f"{batch_id} STATUS.json 的 batch_id 不一致："
                f"{status_doc.get('batch_id')!r}")
        index_status = (index_row.get("status") or "").strip()
        batch_status = (status_doc.get("status") or "").strip()
        if index_status != batch_status:
            raise SystemExit(
                f"{batch_id} 状态不一致：BATCH_INDEX={index_status}, STATUS.json={batch_status}")

        existing_papers = read_csv(root / PAPER_INDEX)
        existing_backlog = read_csv(root / MANUAL_BACKLOG)
        if batch_status == "ACCEPTED" and not reaccept:
            # An all-gap accepted batch legitimately has no PAPER_INDEX rows;
            # require its backlog/record instead of treating a bare status flip
            # as a completed acceptance.
            has_paper_rows = any(
                row.get("source_batch_id") == batch_id for row in existing_papers)
            has_backlog_rows = any(
                row.get("batch_id") == batch_id for row in existing_backlog)
            acceptance_record = root / "03_review" / f"{batch_id}-acceptance.md"
            if not acceptance_record.is_file() or not (has_paper_rows or has_backlog_rows):
                raise SystemExit(
                    f"{batch_id} 已标 ACCEPTED，但验收索引/记录不完整；"
                    "请用 --reaccept 重过闸门")
            print(f"{batch_id}: 已是 ACCEPTED，无需重复验收")
            return 0
        if reaccept and batch_status != "ACCEPTED":
            raise SystemExit(
                f"{batch_id} 当前状态 {batch_status}，--reaccept 只用于已 ACCEPTED 的批次")
        if not reaccept and batch_status != "REVIEW":
            raise SystemExit(f"{batch_id} 当前状态 {batch_status}，只有 REVIEW 可以验收")

        # 在同一 queue lock 临界区内重新读取并校验所有交付件。
        builder = load_builder()
        errors, success, gaps = validate(root, batch, builder)
        print(f"批次 {batch_id}")
        print(f"  条目: {len(success) + len(gaps)} | 成功 {len(success)} | 缺口 {len(gaps)}")
        for err in errors:
            print(f"  ERROR {err}")
        if errors:
            print("验收中止：先修掉上面的问题（不写任何文件）")
            return 1

        now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

        paper_rows = [{
            "paper_id": row.get("paper_id", ""), "title": row.get("title", ""),
            "doi": row.get("doi", ""), "pdf_path": row.get("pdf_path", ""),
            "supplement_paths": row.get("supplement_paths", ""),
            "sha256": row.get("sha256", ""), "source_batch_id": batch_id,
            "identity_verified": row.get("identity_verified", ""), "accepted_at": now,
            "notes": (row.get("notes", "") or "")[:300],
            # 下载验收只登记权威原文及其来源绑定。解析产物必须由
            # validate_document_processing.py 重验源/Markdown SHA 后回填，
            # 不得因验收时目录中恰好存在 full.md 就自动信任它。
            "mineru_md_path": "", "xml_md_path": "",
            "source_item_id": row.get("item_id", ""),
            "document_id": "",
            "source_kind": (
                "xml" if Path(row.get("pdf_path", "")).suffix.casefold() == ".xml" else "pdf"
            ),
            "processed_text_sha256": "", "processing_method": "",
            "processing_version": "", "processing_accepted_at": "",
            "section_chunks_path": "", "section_chunks_sha256": "",
            "section_chunks_count": "", "section_chunks_schema_version": "",
        } for row in success]

        if args.dry_run:
            print(f"  [dry-run] 将登记 {len(paper_rows)} 条到 {PAPER_INDEX}")
            print(f"  [dry-run] 将并入 {len(gaps)} 条到 {MANUAL_BACKLOG}")
            print("  [dry-run] 将置为 ACCEPTED")
            return 0

        try:
            subprocess.run(
                [sys.executable, str(HERE / "make_manual_backlog.py"),
                 batch_id, "--root", str(root)],
                check=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            print(f"  ERROR manual_backlog.csv 生成失败：{exc}")
            print("验收中止：未更新索引或状态")
            return 1
        backlog_errors, backlog_rows = validate_manual_backlog(batch, gaps)
        if backlog_errors:
            for err in backlog_errors:
                print(f"  ERROR {err}")
            print("验收中止：待人工清单未覆盖全部缺口")
            return 1

        try:
            normalized_results = persist_validated_results(
                batch / "outputs/download_results.csv", success, success + gaps)
        except (OSError, ValueError, RuntimeError) as exc:
            print(f"  ERROR 校验后的结果/全文发生变化：{exc}")
            print("验收中止：未更新索引或状态")
            return 1

        known_papers = {(row.get("source_batch_id"), row.get("pdf_path") or row.get("doi"))
                        for row in existing_papers}
        fresh_papers = [r for r in paper_rows
                        if (r["source_batch_id"], r["pdf_path"] or r["doi"]) not in known_papers]
        known_backlog = {(row.get("batch_id"), row.get("item_id"))
                         for row in existing_backlog}
        fresh_backlog = [r for r in backlog_rows
                         if (r["batch_id"], r["item_id"]) not in known_backlog]
        if reaccept:
            paper_kept = [r for r in existing_papers
                          if r.get("source_batch_id") != batch_id]
            backlog_kept = [r for r in existing_backlog
                            if r.get("batch_id") != batch_id]
            final_papers = paper_kept + paper_rows
            final_backlog = backlog_kept + backlog_rows
            n_papers, n_backlog = len(paper_rows), len(backlog_rows)
        else:
            final_papers = existing_papers + fresh_papers
            final_backlog = existing_backlog + fresh_backlog
            n_papers, n_backlog = len(fresh_papers), len(fresh_backlog)
        record = root / "03_review" / f"{batch_id}-acceptance.md"
        record_text = (
            f"# {batch_id} 验收记录\n\n"
            f"- 验收时间：{now}\n- 验收人角色：协调Agent（accept_batch.py）\n"
            f"- 交付成功：{len(success)} 条（其中身份已证实 "
            f"{sum(1 for r in success if r.get('identity_verified') == 'true')} 条）\n"
            f"- 缺口：{len(gaps)} 条（已并入 MANUAL_DOWNLOAD_BACKLOG.csv）\n"
            f"- 本次{'替换登记' if reaccept else '新登记'}：索引 {n_papers} 条 / 待人工 {n_backlog} 条\n"
            + ("- 重新验收：按修正后的 download_results.csv 重过身份、完整性和正文闸门。\n"
               if reaccept else "")
            + "\n"
            "## 缺口明细\n\n"
            + ("\n".join(f"- `{r['item_id']}` {r.get('paper_id','')} — {r.get('status','')}"
                         f"：{(r.get('notes','') or '')[:160]}" for r in gaps)
               or "- 无缺口") + "\n"
        )

        status_now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        note = (f"accept_batch.py {'重新' if reaccept else ''}验收："
                f"成功 {len(success)} / 缺口 {len(gaps)}")
        status_doc["status"] = "ACCEPTED"
        status_doc["updated_at"] = status_now
        status_doc["last_note"] = note
        for row in index:
            if row.get("batch_id") == batch_id:
                row["status"] = "ACCEPTED"
                row["updated_at"] = status_now
                row["accepted_at"] = status_now

        # Prepare all bytes and hashes before publishing any authoritative
        # target. Status files are installed last and a later invocation can
        # safely replay the exact payloads after a crash between replacements.
        assert_header_matches(root / PAPER_INDEX, PAPER_FIELDS)
        assert_header_matches(root / MANUAL_BACKLOG, BACKLOG_FIELDS)
        backup_once(root / PAPER_INDEX)
        backup_once(root / MANUAL_BACKLOG)
        target_payloads = {
            PAPER_INDEX.as_posix(): render_csv_bytes(PAPER_FIELDS, final_papers),
            MANUAL_BACKLOG.as_posix(): render_csv_bytes(BACKLOG_FIELDS, final_backlog),
            f"03_review/{batch_id}-acceptance.md": record_text.encode("utf-8"),
            f"02_batches/{batch_id}/STATUS.json": (
                json.dumps(status_doc, ensure_ascii=False, indent=2) + "\n"
            ).encode("utf-8"),
            "00_control/BATCH_INDEX.csv": render_csv_bytes(index_fields, index),
        }
        try:
            dependencies = build_acceptance_dependencies(batch, success)
            prepare_acceptance_transaction(
                root, batch_id, target_payloads, dependencies)
            recover_prepared_acceptance(root, batch_id)
        except (OSError, RuntimeError) as exc:
            print(f"  ERROR 验收事务写入中断：{exc}")
            print("验收中断：已准备事务保留，重跑同一命令将自动补齐")
            return 1

    print(f"  已登记索引 {n_papers} 条、待人工 {n_backlog} 条")
    if normalized_results:
        print("  已将验证后的全文路径/类型写回 outputs/download_results.csv")
    print(f"  验收记录：{record.relative_to(root)}")
    if reaccept:
        print(f"  {batch_id} 保持 ACCEPTED（索引已按修正结果替换）")
        return 0
    print(f"  {batch_id} → ACCEPTED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
