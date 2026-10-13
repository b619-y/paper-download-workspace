# 文献检索 API 适配器

`scripts/search_literature.py` 把不同数据库的分页结果保存为统一记录，并直接生成供论文初筛 Agent 填写的 `pre_screening.csv`。

若用户只有自然语言研究问题，先使用 `literature-query-agent` 生成 `recall`、`balanced` 和 `precision_pilot` 三套候选、完成实测并选定执行配置；不要直接把整句问题当作最终检索式。生物医学 PubMed 策略优先由 `pubmed-search-builder-next` 构建和验证。

## 支持来源

| `source` | 检索内容 | 凭据 |
|---|---|---|
| `openalex` | 题名、摘要倒排索引、作者、年份、DOI | 可选 `OPENALEX_API_KEY`；推荐设置 `OPENALEX_MAILTO` |
| `crossref` | Crossref 元数据；摘要是否存在取决于成员上报 | 无密钥；推荐设置 `CROSSREF_MAILTO` |
| `europe_pmc` | Europe PMC 核心元数据与摘要 | 无密钥 |
| `pubmed` | PubMed 题名、摘要和书目信息 | 可选 `NCBI_API_KEY`；推荐设置 `NCBI_EMAIL` |
| `semantic_scholar` | Semantic Scholar Graph API 批量检索 | 推荐 `SEMANTIC_SCHOLAR_API_KEY` 或 `S2_API_KEY`；匿名访问可能限流 |
| `core` | CORE works 检索 | 推荐 `CORE_API_KEY` |
| `arxiv` | arXiv Atom API | 无密钥 |
| `wos` | Web of Science Starter 或 Expanded | `WOS_API_KEY` 或 `CLARIVATE_API_KEY`；可用 `WOS_API_MODE=starter` |
| `scopus` | Scopus Search API | `SCOPUS_API_KEY` 或 `ELSEVIER_API_KEY`；机构令牌可放 `SCOPUS_INSTTOKEN` |

所有密钥只从环境变量读取。不要把密钥写进 JSON 配置、工作区、日志或交接文件。

## 直接运行

```bash
python3 scripts/search_literature.py \
  --query 'soil AND (lead OR cadmium)' \
  --source openalex --source crossref --source europe_pmc \
  --output-dir /path/to/search-run
```

`--source all` 会依次调用全部九个来源。缺少授权来源的密钥时，该来源记为 `blocked_missing_credentials`，其他来源仍继续。默认只调用 OpenAlex、Crossref 和 Europe PMC。

`--max-records N` 只用于试跑；只要数据库报告的命中数超过 N，该来源会标为不完整。生产检索不要设置这个参数。

## 多来源配置

不同数据库的语法并不完全兼容。简单关键词可由脚本自动包装：arXiv 使用 `all:`、WoS 使用 `TS=(...)`、Scopus 使用 `TITLE-ABS-KEY(...)`。复杂检索式应显式提供每个来源的原生查询：

```json
{
  "query_id": "soil-metals-2026-10-13",
  "original_query": "soil AND (lead OR cadmium)",
  "source_order": ["openalex", "crossref", "europe_pmc", "pubmed", "wos", "scopus"],
  "sources": {
    "openalex": {"query": "soil lead cadmium"},
    "crossref": {"query": "soil lead cadmium"},
    "europe_pmc": {"query": "soil AND (lead OR cadmium)"},
    "pubmed": {"query": "soil AND (lead OR cadmium)"},
    "wos": {"query": "TS=(soil AND (lead OR cadmium))", "database": "WOS", "mode": "expanded"},
    "scopus": {"query": "TITLE-ABS-KEY(soil AND (lead OR cadmium))", "view": "COMPLETE"}
  }
}
```

```bash
python3 scripts/search_literature.py --config search-config.json --output-dir /path/to/search-run
```

## 输出与完整性

- `<source>/raw/page-*.json|xml`：API 原始分页响应。
- `<source>/records.csv`：单一来源的规范化记录。
- `all_records.csv`：全部来源的原始合并表，不去重。
- `deduplicated_records.csv`：按 DOI，或规范化题名加年份去重；优先保留较长摘要。
- `pre_screening.csv`：论文初筛 Agent 的工作表。
- `run_manifest.json`：每个来源的原生检索式、报告命中数、实际写入数、分页数、状态和整体完整性。

只有 `run_manifest.json` 的顶层 `complete=true`，且计划使用的来源均为 `status=complete`，才能把结果描述为“已完整导出”。这里的“全部”只指：该来源在本次原生检索式、索引覆盖范围和访问权限下返回的全部匹配记录，不等于穷尽全世界所有论文。

出现 `failed`、`blocked_missing_credentials`、`incomplete`，或者用户主动设置 `--max-records` 导致截断时，应先修复并重跑；若用户选择继续，必须在初筛报告中明确披露缺失来源和截断范围。

## 去重、初筛后交给下载 Agent

`search_literature.py` 已生成跨库去重后的 `deduplicated_records.csv` 和待填写的 `pre_screening.csv`。初筛完成后，不要直接下载全部检索命中项；用交接脚本二次核验身份并创建下载批次：

```bash
python3 scripts/handoff_screening_to_download.py \
  --screening /path/to/search-run/pre_screening.csv \
  --purpose '为目标数据全文复筛取得原文' \
  --title '检索初筛后的全文下载批次'
```

成功时输出批次 ID 和批次目录，批次状态为 `READY`。完整检索证据、初筛表和最终下载清单会复制到该批次 `inputs/search_screening/`。

