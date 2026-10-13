#!/usr/bin/env python3
"""把批次的失败条目导出成**协调侧可直接拼接**的 `outputs/manual_backlog.csv`。

为什么需要它：`03_review/MANUAL_DOWNLOAD_BACKLOG.csv` 由协调Agent维护，但"哪些条目要人工、
原因码是什么、下一步谁做什么"这些信息只有下载Agent手上有。以前靠人从 `failures.md` 誊抄，
既慢又容易抄错。现在下载Agent在批次内生成同构 CSV，协调侧只做一次 append。

只读 `outputs/download_results.csv`（必要时读 `logs/failures.md` 补证据），只写
`outputs/manual_backlog.csv`，不动工作区其它文件。

    python3 make_manual_backlog.py <batch_id>
    python3 make_manual_backlog.py --all
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_ROOT = Path("/Users/b/Desktop/论文下载")
#: 与 03_review/MANUAL_DOWNLOAD_BACKLOG.csv 完全同构（协调侧直接 append）
TARGET_FIELDS = [
    "batch_id", "item_id", "paper_id", "doi", "title", "status", "reason_code",
    "reason", "attempted_channels", "needs", "owner", "reported_at", "resolved_at",
    "resolution_batch_id", "notes",
]
#: 只导出"需要人工/换渠道"的状态；downloaded / reused_verified / skipped_duplicate 不导出
MANUAL_STATUSES = {"not_found", "paywalled", "access_denied", "identity_mismatch", "failed"}

#: 原因码 → (需求, 责任方)
NEEDS = {
    "E01": ("换渠道：补路由表或改用其它来源", "协调Agent"),
    "E02": ("用户登录 WebVPN 后由下载Agent重跑", "用户"),
    "E03": ("修复 Elsevier TDM 凭据（不写入工作区）", "用户"),
    "E04": ("联系作者/馆际互借，或确认机构是否订阅该刊", "协调Agent"),
    "E05": ("换渠道：绕开机器人墙（人工或其它镜像）", "协调Agent"),
    "E06": ("换渠道取全文（预览件不算成功）", "协调Agent"),
    "E07": ("下载Agent重跑续传（不要放大超时而应 -C - 续传）", "下载Agent"),
    "E08": ("人工核对身份后重下正确目标", "下载Agent"),
    "E09": ("人工找源（链接失效/无 OA/目录里没有）", "协调Agent"),
    "E10": ("人工取补充材料（介质或权限限制）", "协调Agent"),
    "E11": ("换渠道取正文（仅摘要不算）", "协调Agent"),
    "E12": ("换渠道取真 PDF（拿到的是 HTML/错误页）", "协调Agent"),
    "E13": ("下载Agent择时重跑（慢源/断流，分片已保留）", "下载Agent"),
    "E14": ("需要用户交互完成登录/验证码", "用户"),
    "E15": ("人工确认文件归属后重搬（共享目录争抢）", "下载Agent"),
}


def atomic_write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    """先写 `.tmp` 再 rename，避免中途中断留下半截 CSV。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def reason_code_of(notes: str) -> str:
    m = re.search(r"\b(E\d{2})\b", notes or "")
    return m.group(1) if m else ""


def reason_of(notes: str, code: str) -> str:
    text = (notes or "").strip()
    if code:
        text = re.sub(rf"^{code}\s*[:：]?\s*", "", text)
    # 去掉后续原因码，避免把整段判词塞进 reason 列
    text = re.split(r";\s*E\d{2}", text)[0]
    return text.strip("；; ")[:600]


def load_channels(batch: Path) -> dict[str, str]:
    """可选：`logs/gap_channels.json`（{item_id: "direct → webvpn(browser)"}）补充"已尝试通道"。"""
    f = batch / "logs/gap_channels.json"
    if not f.is_file():
        return {}
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"  warn: {f} 解析失败（{type(exc).__name__}），attempted_channels 将留空", flush=True)
        return {}
    return {str(k): str(v) for k, v in (data or {}).items()}


def build(batch: Path, *, overwrite: bool = True) -> dict:
    results = read_csv(batch / "outputs/download_results.csv")
    if not results:
        raise SystemExit(f"no outputs/download_results.csv in {batch}")
    channels = load_channels(batch)
    now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    out: list[dict[str, str]] = []
    for row in results:
        status = (row.get("status") or "").strip()
        if status not in MANUAL_STATUSES:
            continue
        notes = row.get("notes") or ""
        code = reason_code_of(notes)
        need, owner = NEEDS.get(code, ("人工确认", "协调Agent"))
        out.append({
            "batch_id": batch.name,
            "item_id": row.get("item_id", ""),
            "paper_id": row.get("paper_id", ""),
            "doi": row.get("doi", ""),
            "title": row.get("title", ""),
            "status": status,
            "reason_code": code,
            "reason": reason_of(notes, code),
            "attempted_channels": channels.get(row.get("item_id", ""), "见 logs/failures.md"),
            "needs": need,
            "owner": owner,
            "reported_at": now,
            "resolved_at": "",
            "resolution_batch_id": "",
            "notes": notes[:1000],
        })
    dest = batch / "outputs/manual_backlog.csv"
    if dest.is_file() and not overwrite:
        # 批量模式（--all）默认只增不改：别人的批次里若有手工维护过的清单，不能被顺手重写。
        dest = dest.with_name("manual_backlog.regenerated.csv")
        print(f"  note: {batch.name} 已有 manual_backlog.csv → 改写 {dest.name}"
              f"（确认要覆盖时加 --overwrite）", flush=True)
    atomic_write_csv(dest, TARGET_FIELDS, out)
    return {"batch_id": batch.name, "rows": len(out), "path": str(dest),
            "by_code": {c: sum(1 for r in out if r["reason_code"] == c)
                        for c in sorted({r["reason_code"] for r in out if r["reason_code"]})}}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_id", nargs="?")
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--overwrite", action="store_true",
                        help="--all 模式下允许覆盖已有 manual_backlog.csv（默认改写 *.regenerated.csv）")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if args.all:
        batches = sorted(p for p in (root / "02_batches").iterdir() if p.is_dir())
        overwrite = args.overwrite
    elif args.batch_id:
        batches = [root / "02_batches" / args.batch_id]
        overwrite = True          # 单批次是下载Agent自己的产物，正常覆盖
    else:
        parser.error("give a batch_id or --all")

    for batch in batches:
        info = build(batch, overwrite=overwrite)
        codes = ", ".join(f"{k}×{v}" for k, v in info["by_code"].items()) or "无"
        print(f"{info['batch_id']}: {info['rows']} 条待人工（{codes}） -> {info['path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
