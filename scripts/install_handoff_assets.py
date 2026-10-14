#!/usr/bin/env python3
"""Install non-destructive screening-to-download handoff assets."""

from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_ROOT = Path.cwd()
FILES = {
    "01_shared/contracts/SCREENING_DOWNLOAD_HANDOFF.md": """# 检索筛选到下载批次的交接合同

API 检索完成后先对 `pre_screening.csv` 填写 `download`、`exclude` 或 `uncertain`，再运行 `handoff_screening_to_download.py`。脚本默认只交接 `download + uncertain`，按 DOI、否则按规范化题名加年份二次去重，并创建 `READY` 批次。

空白决定、非法决定、同一论文的冲突决定、入选却无理由，以及未获接受的不完整检索都会阻止交接。批次 `inputs/search_screening/` 必须保留运行清单、完整初筛表、最终下载清单和交接摘要。`READY` 表示进入下载队列，不表示下载完成。
""",
    "01_shared/prompts/筛选结果交接Agent通用提示词.txt": """你负责把论文检索与初筛结果交给任务下载Agent。先确认 run_manifest.json 完整，再确认 pre_screening.csv 每行已有合法决定。使用 handoff_screening_to_download.py 二次去重并生成下载清单；默认保留 download 和 uncertain，排除 exclude。任何空白决定、身份重复但决定冲突、缺少入选理由或未披露的不完整检索都必须停止。脚本成功创建 READY 批次后才算交接完成；不得把 READY 说成已经下载。
""",
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    args = parser.parse_args()
    root = Path(args.root).expanduser().resolve()
    if not (root / "00_control/WORKSPACE.json").is_file():
        parser.error(f"not a paper-download workspace: {root}")
    created = kept = 0
    for rel, content in FILES.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            kept += 1
            continue
        path.write_text(content, encoding="utf-8")
        created += 1
    print(f"{root} created={created} kept={kept}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
