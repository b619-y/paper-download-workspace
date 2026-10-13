#!/usr/bin/env python3
"""把一个批次的产物汇总成 `outputs/download_results.csv`（覆盖 MANIFEST 全部 item_id）。

这是下载Agent交接前的**标准收尾步骤**——以前每个批次都要复制改一份类似脚本，现在统一到这里。

它做什么
--------
1. 读 `MANIFEST.csv`（权威清单）与 skill 的 `outputs/download-manifest*.json`（取来源证据）。
2. 对每个条目逐篇过 §7 闸门 + 身份校验：
   * PDF：`%PDF` 头 + 末尾附近 `%%EOF` + `pdfinfo` 页数 > 2 + 身份证据；
   * XML：`<ce:section>` 计数 > 0 + `<ce:doi>` 与清单 DOI 相等（或 `dc:title` 命中）；
   * 补充材料：合并 `outputs/supplement-manifest.json`（Elsevier mmc 通道）；
   * `reference_existing` / `verify_existing`：去既有批次复核后记 `reused_verified`。
3. 无产物的条目按三层判定：`outputs/gap_reasons.csv`（人工核实结论）→ 可续传分片（E13）→ E09。
4. 需要时顺带导出 `outputs/manual_backlog.csv`（`--manual-backlog`，默认开）。

两个可选输入文件（都放在批次的 `outputs/` 下）
--------------------------------------------
* `identity_tokens.json` — `{"WEB201": ["srnl-sti-2015-00358"]}`，给"正文不打印 DOI"的条目
  （机构报告、EPA 文件）提供硬标识；
* `gap_reasons.csv` — `item_id,status,reason_code,evidence`，人工核实后的缺口结论
  （例如 `WEB067,paywalled,E04,OUP 非 OA；代理 PDF 落回文章落地页`）。

    python3 build_download_results.py <batch_id> [--no-manual-backlog]
    python3 build_download_results.py --all
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_ROOT = Path("/Users/b/Desktop/论文下载")
FIELDS = ["item_id", "paper_id", "status", "title", "doi", "source_url", "pdf_path",
          "supplement_paths", "sha256", "file_bytes", "page_count", "identity_verified",
          "downloaded_at", "notes"]
SUCCESS = ("downloaded", "reused_verified")
FULLWIDTH = {"共": "(", "兲": ")", "（": "(", "）": ")", "［": "[", "］": "]"}
ZERO_WIDTH = ("\u00ad", "\u200b", "\u200c", "\u200d", "\ufeff")
STOP = {"with", "from", "that", "this", "using", "based", "into", "case", "study",
        "analysis", "review", "effect", "effects", "model", "models", "approach",
        "methods", "method", "groundwater", "soil", "water", "between", "during"}


# --------------------------------------------------------------------------- 工具
def norm_text(text: str) -> str:
    for src, dst in FULLWIDTH.items():
        text = text.replace(src, dst)
    for ch in ZERO_WIDTH:
        text = text.replace(ch, "")
    return re.sub(r"\s+", " ", text.lower())


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def poppler_command(name: str) -> str:
    """非交互 Mac shell 的 PATH 可能不含 Homebrew。"""
    found = shutil.which(name)
    if found:
        return found
    mac = Path("/opt/homebrew/bin") / name
    return str(mac) if mac.is_file() else name


def pdf_pages(path: Path) -> int | None:
    try:
        proc = subprocess.run([poppler_command("pdfinfo"), str(path)], capture_output=True, text=True, timeout=60)
        if proc.returncode != 0:
            return None
        m = re.search(r"^Pages:\s+(\d+)", proc.stdout, re.M)
        return int(m.group(1)) if m else None
    except Exception:
        return None


def pdf_title(path: Path) -> str:
    try:
        proc = subprocess.run([poppler_command("pdfinfo"), str(path)], capture_output=True, text=True, timeout=60)
        m = re.search(r"^Title:\s+(.*)$", proc.stdout, re.M)
        return (m.group(1) if m else "").strip()
    except Exception:
        return ""


def pdf_text(path: Path, pages: int = 3) -> str:
    try:
        proc = subprocess.run([poppler_command("pdftotext"), "-f", "1", "-l", str(pages), str(path), "-"],
                              capture_output=True, text=True, timeout=120)
        return norm_text(proc.stdout or "")
    except Exception:
        return ""


def pdf_last_page_readable(path: Path, page: int) -> bool:
    """尾随字节 PDF 必须实际读到最后一页，不能只看旧版增量 EOF。"""
    try:
        proc = subprocess.run([poppler_command("pdftotext"), "-f", str(page), "-l", str(page),
                               str(path), "-"], capture_output=True, timeout=120)
        return proc.returncode == 0
    except Exception:
        return False


def compact_title(value: str) -> str:
    """只用于首页精确中文题名匹配；统一罗马数字、全角和排版空白。"""
    value = unicodedata.normalize("NFKC", value).lower()
    return "".join(ch for ch in value if ch.isalnum())


def title_words(title: str) -> list[str]:
    return [w for w in re.findall(r"[a-z]{5,}", title.lower()) if w not in STOP]


DOI_TOKEN = re.compile(r"10\.\d{4,9}/[^\s<>\"']+", re.I)
EVIDENCE_RANK = {"doi": 0, "doi-normalized": 1, "pdfinfo-title": 3,
                 "chinese-title": 3, "title-words": 4}


def compact_doi(value: str) -> str:
    text = re.sub(r"\s+", "", norm_text(value))
    text = re.sub(r"\(20\d{2}\).*$", "", text)
    return text.rstrip(".,;:)]}")


def dois_in_text(text: str) -> list[str]:
    """从已规范化的 PDF 文本里抽出 DOI。空白先去掉，避免 DOI 被断行拆开。"""
    compact = re.sub(r"\s+", "", norm_text(text))
    found: list[str] = []
    for raw in DOI_TOKEN.findall(compact):
        token = compact_doi(raw)
        if token and token not in found:
            found.append(token)
    return found


def evidence_rank(how: str) -> int:
    if how in EVIDENCE_RANK:
        return EVIDENCE_RANK[how]
    if how.startswith("token:"):
        return 2
    return 9


# --------------------------------------------------------------------------- 闸门
def gate_pdf(path: Path, doi: str, title: str, tokens: tuple[str, ...] = ()) -> dict:
    res = {"ok": False, "pages": None, "sha256": "", "identity_verified": False,
           "how": "", "reason": ""}
    if not path.is_file():
        res["reason"] = "文件不存在"
        return res
    raw = path.read_bytes()
    if raw[:4] != b"%PDF":
        res["reason"] = "文件头不是 %PDF"
        return res
    eof = raw.rfind(b"%%EOF")
    trailing = len(raw) - eof - 5 if eof >= 0 else len(raw)
    if eof < 0 or trailing > 1024:
        res["reason"] = "末尾 1 KiB 内无 %%EOF（疑似截断）"
        return res
    pages = pdf_pages(path)
    if pages is None:
        res["reason"] = "pdfinfo 解析失败"
        return res
    res["pages"] = pages
    res["sha256"] = sha256(path)
    if trailing and not pdf_last_page_readable(path, pages):
        res["reason"] = "PDF 有 EOF 尾随字节且末页不可读取"
        return res
    if pages <= 2:
        res["reason"] = f"仅 {pages} 页（疑似首页预览）"
        return res
    first = pdf_text(path)
    whole = first
    how = ""
    citation_only = False
    if doi:
        target = compact_doi(doi)
        first_compact = re.sub(r"\s+", "", first)
        if doi.lower() in first:
            how = "doi"
        elif target and target in first_compact:
            how = "doi-normalized"      # ASCE 之类把 DOI 排成全角括号或断行
        else:
            # 全文子串会命中参考文献里的别人 DOI。首页已经印着另一篇 DOI 时，
            # 后面的命中只是引用，不能当成这篇文章本身。
            whole = pdf_text(path, pages=999)
            whole_compact = re.sub(r"\s+", "", whole)
            own = [token for token in dois_in_text(first) if token != target]
            if target and target in whole_compact and own:
                citation_only = True
            elif target and target in whole_compact:
                how = "doi-normalized"
    if citation_only:
        res.update(ok=True, identity_verified=False, how="citation-only",
                   reason=f"PDF_FULL {pages}p；目标 DOI 只出现在参考文献，首页 DOI 属于另一篇")
        return res
    if not how:
        for token in tokens:
            if token.lower() in whole:
                how = f"token:{token[:40]}"
                break
    meta = norm_text(pdf_title(path))
    if not how and sum("\u4e00" <= ch <= "\u9fff" for ch in title) >= 4:
        chinese_title = compact_title(title)
        if chinese_title in compact_title(meta) or chinese_title in compact_title(first):
            how = "chinese-title"
    words = title_words(title)
    if not how and meta and words and sum(1 for w in words if w in meta) / len(words) >= 0.8:
        how = "pdfinfo-title"
    if not how and words and sum(1 for w in words if w in first) / len(words) >= 0.8:
        how = "title-words"
    res.update(ok=True, identity_verified=bool(how), how=how or "unverified",
               reason=f"PDF_FULL {pages}p")
    return res


def gate_xml(path: Path, doi: str, title: str) -> dict:
    res = {"ok": False, "sections": 0, "sha256": "", "identity_verified": False,
           "how": "", "reason": ""}
    if not path.is_file():
        res["reason"] = "文件不存在"
        return res
    text = path.read_text(encoding="utf-8", errors="ignore")
    res["sha256"] = sha256(path)
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        res["reason"] = "XML 解析失败"
        return res
    local = lambda node: node.tag.rsplit("}", 1)[-1]
    is_jats = local(root) == "article"
    body = next((node for node in root.iter() if local(node) == "body"), None)
    sections = sum(local(node) == "section" for node in body.iter()) if body is not None else 0
    paragraphs = sum(local(node) in {"p", "para"} for node in body.iter()) if body is not None else 0
    res["sections"] = sections
    if sections <= 0 and paragraphs < 5:
        res["reason"] = "XML 无可核验正文"
        return res
    how = ""
    if is_jats:
        ids = {"".join(node.itertext()).strip().lower() for node in root.iter()
               if local(node) == "article-id" and node.get("pub-id-type") == "doi"}
        if doi and doi.lower() in ids:
            how = "article-id:doi"
    else:
        m = re.search(r"<ce:doi>\s*([^<\s]+)\s*</ce:doi>", text)
        xml_doi = (m.group(1) if m else "").lower()
        if xml_doi and doi and xml_doi == doi.lower():
            how = "ce:doi"
        else:
            mt = re.search(r"<dc:title>(.*?)</dc:title>", text, re.S)
            words = title_words(title)
            if mt and words and sum(1 for w in words if w in norm_text(mt.group(1))) / len(words) >= 0.8:
                how = "dc:title"
    res.update(ok=True, identity_verified=bool(how), how=how or "unverified",
               reason=f"XML_FULL {'JATS' if is_jats else 'Elsevier'} {sections} sections/{paragraphs} paragraphs")
    return res


def find_xml(xml_dir: Path, paper_id: str, target: str, doi: str) -> Path:
    """定位某篇的全文 XML（Elsevier TDM 或 Europe PMC JATS）。

    * 本会话的跑法：`tdm_fetch` 用 target_filename 派生 → `CAP-01.xml` / `WEB042.xml`；
    * 早期批次：清单里的 suggested_filename 是 DOI slug → `10.1016_j.envres.2023.115592.xml`。

    只按 paper_id 找会让"1 页预览 PDF + 完整 XML"的老批次整片误判成 failed（实测 d9aa 6 条）。
    """
    candidates = [
        xml_dir / f"{paper_id}.xml",
        xml_dir / f"{Path(target).stem}.xml",
    ]
    if doi:
        candidates.append(xml_dir / f"{doi.replace('/', '_')}.xml")
    candidates.append(xml_dir.parent / "europepmc-xml" / f"{paper_id}.xml")
    for cand in candidates:
        if cand.is_file():
            return cand
    return candidates[0]


def supplement_gap_note(xml_dir: Path, paper_id: str) -> str:
    """没取到补充材料时，先看该文到底有没有声明——没有声明不是 E10 缺口。"""
    xml = xml_dir / f"{paper_id}.xml"
    if xml.is_file():
        text = xml.read_text(encoding="utf-8", errors="ignore")
        if not re.search(r'<object[^>]*ref="mmc\d+"', text) and "supplementary-material" not in text:
            return "XML 内无 mmc/Supplementary 声明，判定该文无补充材料"
    return "E10:清单要求 supplement，但未取回（检查介质/权限，见 logs/failures.md）"


def supplement_manifest(batch: Path) -> dict[str, tuple[list[str], str]]:
    """{paper_id: (files, why)} —— 来自 Elsevier mmc 通道的补充材料清单。"""
    out: dict[str, tuple[list[str], str]] = {}
    f = batch / "outputs/supplement-manifest.json"
    if not f.is_file():
        return out
    try:
        entries = json.loads(f.read_text(encoding="utf-8")).get("entries", [])
    except Exception:
        return out
    for entry in entries:
        if entry.get("status") not in ("ok", "skipped_valid"):
            continue
        pid = entry.get("paper_id") or ""
        name = Path(entry.get("file", "")).name
        files, why = out.get(pid, ([], ""))
        if name and name not in files:
            files = files + [name]
        extra = "Elsevier 官方 object 端点（TDM 凭据）取回的 mmc"
        out[pid] = (files, f"{why}；{extra}" if why else extra)
    return out


def load_tokens(batch: Path) -> dict[str, tuple[str, ...]]:
    f = batch / "outputs/identity_tokens.json"
    if not f.is_file():
        return {}
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
    except Exception as exc:
        # 坏输入必须出声：静默忽略会让"身份硬标识"悄悄失效，然后整篇被判 identity_mismatch
        print(f"  warn: {f.name} 解析失败（{type(exc).__name__}: {exc}），"
              f"身份硬标识未生效", flush=True)
        return {}
    return {str(k): tuple(str(x) for x in v) for k, v in (data or {}).items()}


def load_gaps(batch: Path) -> dict[str, tuple[str, str]]:
    """读 `outputs/gap_reasons.csv`：item_id,status,reason_code,evidence。"""
    f = batch / "outputs/gap_reasons.csv"
    if not f.is_file():
        return {}
    out: dict[str, tuple[str, str]] = {}
    try:
        with f.open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames and not {"item_id", "status"} <= set(reader.fieldnames):
                print(f"  warn: {f.name} 缺少必需列（现有 {reader.fieldnames}），已忽略",
                      flush=True)
                return {}
            for row in reader:
                iid = (row.get("item_id") or "").strip()
                if not iid:
                    continue
                code = (row.get("reason_code") or "").strip()
                evidence = (row.get("evidence") or "").strip()
                out[iid] = ((row.get("status") or "failed").strip(),
                            f"{code}:{evidence}" if code else evidence)
    except Exception as exc:
        print(f"  warn: {f.name} 读取失败（{type(exc).__name__}: {exc}），缺口结论未生效",
              flush=True)
        return {}
    return out


def plan_sources(batch: Path) -> dict[str, str]:
    """{paper_id: 最像"真来源"的候选 URL}。

    最终 `--verify-only` 写的 manifest 里 `source` 常为空（复核用的 plan 不带 URL），
    直接用它会让 results.csv 的 source_url 退化成 doi.org 落地页——审计时看不出到底走了哪条通道。
    """
    out: dict[str, str] = {}
    for f in sorted((batch / "outputs").glob("plan*.json")):
        try:
            entries = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(entries, list):
            continue
        for entry in entries:
            pid = entry.get("paper_id") or ""
            if not pid or pid in out:
                continue
            for url in (entry.get("webvpn_urls") or []) + (entry.get("direct_urls") or []):
                if url.startswith("http"):
                    out[pid] = url if "doi.org" not in url else url
                    break
            if pid in out:
                continue
            for url in (entry.get("direct_urls") or []) + (entry.get("webvpn_urls") or []):
                if url.startswith("http"):
                    out[pid] = url
                    break
    return out


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


# --------------------------------------------------------------------------- 主流程
def build(batch: Path, *, manual_backlog: bool = True, overwrite: bool = False) -> dict:
    out_dir = batch / "outputs"
    files_dir = out_dir / "files"
    xml_dir = out_dir / "tdm-xml"
    manifest = read_csv(batch / "MANIFEST.csv")
    if not manifest:
        raise SystemExit(f"no MANIFEST.csv in {batch}")
    capg_request = batch / "inputs/capg_request.json"
    require_pdf = (json.loads(capg_request.read_text(encoding="utf-8")).get("required_format") == "pdf"
                   if capg_request.is_file() else False)

    evidence: dict[str, dict] = {}
    for name in sorted(out_dir.glob("download-manifest*.json")):
        try:
            for entry in json.loads(name.read_text(encoding="utf-8")).get("entries", []):
                evidence.setdefault(entry.get("paper_id") or "", entry)
        except Exception:
            pass
    tokens = load_tokens(batch)
    gaps = load_gaps(batch)
    supplements = supplement_manifest(batch)
    plans = plan_sources(batch)
    now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    rows: list[dict[str, str]] = []
    for item in manifest:
        iid = item.get("item_id", "")
        pid = item.get("paper_id", "")
        title = (item.get("title") or "").strip()
        doi = (item.get("doi") or "").strip()
        target = (item.get("suggested_filename") or item.get("target_filename") or "").strip() \
            or f"{pid}.pdf"
        requested = item.get("requested_files") or ""
        prov = evidence.get(pid, {})
        source = prov.get("source") or ""
        if not source or "doi.org" in source:
            source = plans.get(pid) or source or item.get("landing_url") or ""
        notes: list[str] = []

        # --- 既有批次复用 -----------------------------------------------------
        if (item.get("queue_action") or "") in ("reference_existing", "verify_existing"):
            src_batch = (item.get("existing_batch_id") or "").strip()
            cand_dir = batch.parent / src_batch / "outputs/files"
            hits = []
            for cand in sorted(cand_dir.glob("*.pdf")) if cand_dir.is_dir() else []:
                g = gate_pdf(cand, doi, title, tokens.get(pid, ()))
                if g["ok"] and g["identity_verified"]:
                    hits.append((cand, g))
            hit = None
            if hits:
                stem = Path(target).stem.lower()
                hit = sorted(hits, key=lambda item: (
                    evidence_rank(item[1]["how"]),
                    0 if stem and stem in item[0].stem.lower() else 1,
                    item[0].name,
                ))[0]
            if hit:
                f, g = hit
                rows.append({
                    "item_id": iid, "paper_id": pid, "status": "reused_verified",
                    "title": title, "doi": doi, "source_url": str(f), "pdf_path": str(f),
                    "supplement_paths": "", "sha256": g["sha256"],
                    "file_bytes": f.stat().st_size, "page_count": g["pages"],
                    "identity_verified": "true", "downloaded_at": now,
                    "notes": (f"复用既有文件（批次 {src_batch}，未重下、未搬动），复核 §7 闸门通过；"
                              f"身份证据：{g['how']}"),
                })
            else:
                rows.append({
                    "item_id": iid, "paper_id": pid, "status": "not_found", "title": title,
                    "doi": doi, "source_url": str(cand_dir), "pdf_path": "",
                    "supplement_paths": "", "sha256": "", "file_bytes": "", "page_count": "",
                    "identity_verified": "false", "downloaded_at": now,
                    "notes": f"E09:既有批次 {src_batch} 中未找到能通过身份校验的匹配文件",
                })
            continue

        pdf = files_dir / target
        xml = find_xml(xml_dir, pid, target, doi)

        if pdf.is_file():
            g = gate_pdf(pdf, doi, title, tokens.get(pid, ()))
            gx = gate_xml(xml, doi, title) if xml.is_file() else None
            # Elsevier 经常"1 页预览 PDF + 完整 XML"并存：PDF 不过闸门时先看 XML，
            # 拿到 FULL XML 就是完成（铁律 1），不要把整条判成 failed。
            if g["ok"]:
                status = "downloaded" if g["identity_verified"] else "identity_mismatch"
                notes.append(f"{g['reason']}；来源 {source[:100]}")
                notes.append(f"身份证据：{g['how']}")
                payload = pdf
                sha, pages = g["sha256"], (g["pages"] if g["pages"] else "")
                identity = "true" if g["identity_verified"] else "false"
            elif gx and gx["ok"]:
                status = ("failed" if require_pdf else "downloaded") if gx["identity_verified"] else "identity_mismatch"
                notes.append((f"E06:需求是完整 PDF，现有 PDF 未过闸门（{g['reason']}）；"
                              f"{gx['reason']} 仅作补充，仍需 PDF" if require_pdf else
                              f"PDF 未过闸门（E07:{g['reason']}），但同篇 full-text XML 完整 → "
                              f"{gx['reason']}：按铁律 1 视为完成；PDF 视为预览/残片，不得喂 MinerU"))
                notes.append(f"身份证据：{gx['how']}")
                payload = xml
                sha, pages = gx["sha256"], ""
                identity = "true" if gx["identity_verified"] else "false"
            else:
                status = "failed"
                notes.append(f"E07:{g['reason']}；来源 {source[:80]}")
                payload, sha, pages, identity = None, "", "", "false"
            supp_files, supp_note = supplements.get(pid, ([], ""))
            if "supplement" in requested:
                notes.append(f"补充材料已取回：{';'.join(supp_files)}（{supp_note}）"
                             if supp_files else supplement_gap_note(xml_dir, pid))
            if status not in SUCCESS and iid in gaps:
                gap_status, gap_evidence = gaps[iid]
                if gap_status not in SUCCESS:
                    status = gap_status          # 人工核实结论优先于自动判定
                if gap_evidence:
                    notes.append(gap_evidence)
            rows.append({
                "item_id": iid, "paper_id": pid, "status": status, "title": title, "doi": doi,
                "source_url": source, "pdf_path": str(pdf) if payload is not None and payload == pdf else "",
                "supplement_paths": ";".join(supp_files), "sha256": sha,
                "file_bytes": payload.stat().st_size if payload is not None else "",
                "page_count": pages, "identity_verified": identity,
                "downloaded_at": now, "notes": "；".join(notes),
            })
            continue

        if xml.is_file():
            g = gate_xml(xml, doi, title)
            if g["ok"]:
                status = ("failed" if require_pdf else "downloaded") if g["identity_verified"] else "identity_mismatch"
                notes.append((f"E06:需求是完整 PDF；{g['reason']} 仅作补充，仍需 PDF。" if require_pdf else
                              f"{g['reason']}：完整全文 XML 即视为完成（铁律1）；无 PDF。"))
                notes.append(f"身份证据：{g['how']}")
                supp_files, supp_note = supplements.get(pid, ([], ""))
                if "supplement" in requested:
                    notes.append(f"补充材料已取回：{';'.join(supp_files)}（{supp_note}）"
                                 if supp_files else supplement_gap_note(xml_dir, pid))
                rows.append({
                    "item_id": iid, "paper_id": pid, "status": status, "title": title, "doi": doi,
                    "source_url": source, "pdf_path": "", "supplement_paths": ";".join(supp_files),
                    "sha256": g["sha256"], "file_bytes": xml.stat().st_size, "page_count": "",
                    "identity_verified": "true" if g["identity_verified"] else "false",
                    "downloaded_at": now, "notes": "；".join(notes),
                })
                continue
            notes.append(f"E11:{g['reason']}")

        # --- 无产物：人工核实结论 → 可续传分片 → E09 ----------------------------
        if iid in gaps:
            status, why = gaps[iid]
            notes.append(why)
        elif not doi:
            status = "not_found"
            notes.append("E09:清单条目无 DOI，resolve 判为 manual，未进入任何下载通道；"
                         "需在 outputs/gap_reasons.csv 写明人工结论或补 plan_patch.json")
        else:
            partial = files_dir / f"{target}.partial"
            if partial.is_file():
                info = {}
                sidecar = files_dir / f"{target}.partial.json"
                if sidecar.is_file():
                    try:
                        info = json.loads(sidecar.read_text(encoding="utf-8"))
                    except Exception:
                        info = {}
                status = "failed"
                notes.append(f"E13:只拿到可续传分片（{partial.stat().st_size:,} B"
                             + (f"，来源 {info.get('source', '')}" if info.get("source") else "")
                             + "）；重跑会自动优先同一源 `-C -` 续传，慢源可加 `--fetch-timeout`")
            else:
                status = "not_found"
                notes.append("E09:所有候选通道均未产出文件"
                             + (f"（{prov.get('reason')}）" if prov.get("reason") else ""))
        if notes and "E" not in notes[-1][:3]:
            pass
        rows.append({
            "item_id": iid, "paper_id": pid, "status": status, "title": title, "doi": doi,
            "source_url": source, "pdf_path": "", "supplement_paths": "", "sha256": "",
            "file_bytes": "", "page_count": "", "identity_verified": "false",
            "downloaded_at": now, "notes": "；".join(notes),
        })

    dest = out_dir / "download_results.csv"
    # enqueue_download_batch.py creates a header-only placeholder. Filling that
    # placeholder is the first write, not an overwrite of another agent's result.
    has_rows = False
    if dest.is_file():
        with dest.open(encoding="utf-8-sig", newline="") as existing:
            has_rows = any(csv.DictReader(existing))
    if dest.exists() and has_rows and not overwrite:
        # 保护：别人的批次记录不能被顺手重写（2026-09-17 事故后加的闸门）。
        dest = out_dir / "download_results.regenerated.csv"
        print(f"  note: {batch.name} 已有 download_results.csv → 本次写 "
              f"{dest.name}（确认要用新记录覆盖时加 --overwrite）", flush=True)
    if dest.is_file() and has_rows and overwrite and dest.name == "download_results.csv":
        bak = dest.with_suffix(dest.suffix + ".bak")     # 覆盖前留底（记录被写坏的代价太高）
        if not bak.exists():
            shutil.copy2(dest, bak)
    tmp = dest.with_suffix(dest.suffix + ".tmp")         # 原子写：中断不留半截 CSV
    with tmp.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, dest)
    produced = str(dest)

    from collections import Counter
    by_status = Counter(r["status"] for r in rows)
    gaps_out = [r for r in rows if r["status"] not in SUCCESS]
    if manual_backlog:
        import subprocess as sp
        import sys as _sys
        script = Path(__file__).with_name("make_manual_backlog.py")
        sp.run([_sys.executable, str(script), batch.name,
                "--root", str(batch.parent.parent)], check=False)
    return {"batch_id": batch.name, "items": len(rows), "by_status": dict(by_status),
            "path": produced,
            "gaps": [f"{r['item_id']}/{r['paper_id']}({r['status']})" for r in gaps_out],
            "unverified": [r["item_id"] for r in rows
                           if r["status"] == "downloaded" and r["identity_verified"] != "true"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("batch_id", nargs="?")
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--no-manual-backlog", action="store_true")
    parser.add_argument("--overwrite", action="store_true",
                        help="allow overwriting an existing download_results.csv "
                             "(default: write download_results.regenerated.csv instead)")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if args.all:
        batches = sorted(p for p in (root / "02_batches").iterdir()
                         if p.is_dir() and (p / "MANIFEST.csv").is_file())
    elif args.batch_id:
        batches = [root / "02_batches" / args.batch_id]
    else:
        parser.error("give a batch_id or --all")

    for batch in batches:
        info = build(batch, manual_backlog=not args.no_manual_backlog,
                     overwrite=args.overwrite)
        print(f"{info['batch_id']}: {info['items']} 条 | 状态 {info['by_status']}")
        if info["gaps"]:
            print(f"  非成功 {len(info['gaps'])} 条: " + ", ".join(info["gaps"][:8])
                  + (" …" if len(info["gaps"]) > 8 else ""))
        if info["unverified"]:
            print("  ⚠ 已交付但身份未证实: " + ", ".join(info["unverified"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
