#!/usr/bin/env python3
"""Install non-destructive literature-search assets into an existing workspace."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


DEFAULT_ROOT = Path.cwd()
FILES = {
    "01_shared/contracts/LITERATURE_SEARCH_SCHEMA.md": """# 文献检索运行合同

每次检索保留 `run_manifest.json`、各来源原始分页响应、`all_records.csv`、`deduplicated_records.csv` 和 `pre_screening.csv`。

只有顶层 `complete=true` 且计划使用的来源均为 `status=complete`，才能称为完整导出。`reported_total` 是来源报告的命中数，`fetched_count` 是实际写入数。`failed`、`blocked_missing_credentials`、`incomplete` 或 `partial_by_max_records` 必须先处理或在筛选报告中披露。

“全部”仅指该来源在本次原生检索式、索引覆盖范围和访问权限下返回的全部匹配记录。工作区不得保存 API 密钥。
""",
    "01_shared/prompts/论文检索Agent通用提示词.txt": """你是论文检索Agent。先把用户的概念检索式分别翻译成每个数据库的原生语法，再用 paper-download-workspace 的 search_literature.py 分页导出。保留每一页原始响应、来源报告命中数、实际写入数、原生检索式和运行清单。没有设置 max-records 且 run_manifest.json 顶层 complete=true、计划来源均为 complete 后，才可以声称完整导出。缺少密钥、限流、分页中断或数量不一致必须标成 incomplete，不能把部分记录静默交给初筛。API 密钥只从环境变量读取，不写入配置或工作区。检索完成后把 pre_screening.csv 交给论文筛选Agent。
""",
}
EXAMPLE_CONFIG = {
    "query_id": "example-search",
    "original_query": "soil AND (lead OR cadmium)",
    "source_order": ["openalex", "crossref", "europe_pmc", "pubmed"],
    "sources": {
        "openalex": {"query": "soil lead cadmium"},
        "crossref": {"query": "soil lead cadmium"},
        "europe_pmc": {"query": "soil AND (lead OR cadmium)"},
        "pubmed": {"query": "soil AND (lead OR cadmium)"},
    },
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    args = parser.parse_args()
    root = Path(args.root).expanduser().resolve()
    if not (root / "00_control/WORKSPACE.json").is_file():
        parser.error(f"not a paper-download workspace: {root}")
    created = 0
    kept = 0
    payloads = dict(FILES)
    payloads["01_shared/search-config.example.json"] = json.dumps(EXAMPLE_CONFIG, ensure_ascii=False, indent=2) + "\n"
    for rel, content in payloads.items():
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
