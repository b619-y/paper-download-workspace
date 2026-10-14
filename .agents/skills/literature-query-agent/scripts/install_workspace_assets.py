#!/usr/bin/env python3
"""Install non-destructive query-strategy assets into a paper workspace."""

from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_ROOT = Path.cwd()
FILES = {
    "01_shared/contracts/QUERY_STRATEGY_SCHEMA.md": """# 检索式智能体交付合同

检索前保存 `query_brief.json`、`query_plan.json`、`query_matrix.csv`、`configs/<variant>.json` 和 `search_strategy.md`。至少生成 recall、balanced、precision_pilot 三套候选；只有经过数据库实测、种子召回检查和噪声抽样后才能选定执行版。

概念块内部 OR、块之间 AND。结局、比较、研究设计和狭窄场景默认留给筛选，除非它们定义研究问题。OpenAlex/Crossref 自由文本翻译标为 approximate。内部模型审查不等于正式 PRESS 同行评审。
""",
    "01_shared/prompts/检索式Agent通用提示词.txt": """你是检索式智能体。不要直接凭直觉给出一个 Boolean 字符串。先明确研究问题和边界，选择合适框架，再把候选概念分成 search、screen、optional。为 search 概念建立锚词、同义词、缩写、拼写变体、扩展词和经核实的受控词，生成 recall、balanced、precision_pilot 三套方案并翻译成各数据库原生语法。用命中数、已知相关论文召回和噪声抽样迭代；记录每次修改。PubMed 生物医学检索优先使用 pubmed-search-builder-next。最终把选定配置交给论文检索Agent完整分页导出。
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
