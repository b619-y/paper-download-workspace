#!/usr/bin/env python3
"""Install non-destructive screening contracts and prompts into an existing workspace."""

from __future__ import annotations

import argparse
from pathlib import Path


DEFAULT_ROOT = Path("/Users/b/Desktop/论文下载")
FILES = {
    "01_shared/contracts/PRE_SCREENING_SCHEMA.md": """# pre_screening.csv

字段：`record_id,search_query,source_database,source_record,title,abstract,authors,year,journal,doi,landing_url,screening_basis,decision,matched_criteria,exclusion_reason,needs_fulltext_reason,screened_by,screened_at,notes`。

`decision` 只用 `download`、`exclude`、`uncertain`。摘要未报告某项内容不能作为排除依据；需要全文判断时用 `uncertain`。
""",
    "01_shared/contracts/FULLTEXT_SCREENING_SCHEMA.md": """# fulltext_screening_queue.csv

字段：`item_id,paper_id,title,doi,fulltext_path,fulltext_status,criterion_id,criterion_text,decision,evidence_quote,evidence_page,evidence_section,evidence_kind,evidence_figure,evidence_image_path,evidence_object_ref,reviewer,reviewed_at,notes`。

只有 `fulltext_status=ready` 才能作内容判断。`decision` 使用 `include`、`exclude`、`uncertain`、`not_assessed`；全文不可用或身份未核验不等于 `exclude`。若目标数据可能在论文图中，先下载并阅读目标图，再判定；图像证据必须记录图号、图片路径和可用时的对象 ref。
""",
    "01_shared/prompts/论文初筛Agent通用提示词.txt": """你是论文筛选Agent。根据用户确认的检索式、纳入/排除标准，只使用候选记录的题名和摘要进行初筛。输出 pre_screening.csv：download、exclude 或 uncertain。排除必须给出明确的题名/摘要依据；摘要缺失、信息不足或必须读取方法与结果全文时标记 uncertain。高召回任务中保留边界记录进入全文下载。保留检索式、数据库、原始记录、DOI/URL和筛选理由，不得凭记忆补造元数据。只把 download 和需要全文解决的 uncertain 交给下载批次。
""",
    "01_shared/prompts/任务筛选Agent通用提示词.txt": """你是任务筛选Agent。只依据 fulltext_screening_queue.csv 中 fulltext_status=ready 且身份已核验的全文判断是否包含用户需要的数据或内容。逐项输出 include、exclude、uncertain 或 not_assessed，并提供最短充分原文证据、页码或章节和全文路径。扫描件先 OCR。若目标数据可能在曲线、散点、柱状图、热图、谱图、地图、显微图或多 panel 中，必须先根据图题与引用上下文定位目标图，按需下载并阅读可读图像，再完成判断；不得因 XML/正文未列数值就判为不存在。图像证据记录 evidence_kind、图号、图片路径和可用时的对象 ref；必需图无法取得或辨读时标记 uncertain，不得判为 exclude。找不到明确证据时标记 uncertain，不得用常识或网络摘要补全。全文不可用、无法解析或身份未核验不得判为 exclude。关键纳入/排除和关键数据标记为需要人工复核。
""",
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
