# 检索式设计协议

## 输入 `query_brief.json`

```json
{
  "query_id": "soil-metal-remediation",
  "question": "哪些修复技术能够降低农田土壤中铅和镉的生物有效性？",
  "framework": "PECO",
  "target_sources": ["openalex", "crossref", "europe_pmc", "pubmed", "semantic_scholar", "core", "arxiv", "wos", "scopus"],
  "concepts": [
    {
      "id": "soil_metals",
      "name": "soil lead or cadmium contamination",
      "role": "search",
      "rationale": "目标污染物与介质",
      "anchors": ["soil contamination", "lead", "cadmium"],
      "synonyms": ["heavy metal contamination", "potentially toxic element"],
      "acronyms": ["PTE"],
      "spelling_variants": [],
      "expansions": ["trace metal pollution"],
      "controlled": {"mesh": ["Soil Pollutants", "Lead", "Cadmium"]}
    },
    {
      "id": "remediation",
      "name": "remediation",
      "role": "search",
      "rationale": "干预主题",
      "anchors": ["soil remediation"],
      "synonyms": ["immobilization", "stabilization", "phytoremediation"],
      "acronyms": [],
      "spelling_variants": ["immobilisation", "stabilisation"],
      "expansions": ["biochar amendment", "soil washing"],
      "controlled": {"mesh": ["Soil Remediation"]}
    },
    {
      "id": "bioavailability",
      "name": "bioavailability outcome",
      "role": "screen",
      "rationale": "结局可能只在全文报告",
      "anchors": ["bioavailability"],
      "synonyms": [],
      "acronyms": [],
      "spelling_variants": [],
      "expansions": []
    }
  ],
  "limits": {"from_year": null, "to_year": null, "languages": [], "document_types": []},
  "known_records": []
}
```

`role` 只能是：

- `search`：进入主检索式的 AND 概念块。
- `screen`：在题名/摘要或全文筛选，不进入检索式。
- `optional`：只加入 `precision_pilot`，用于试探，不默认用于完整检索。

词汇层：

- `anchors`：不可缺少的规范名称或最直接表达。
- `synonyms`：语义等价的常见名称。
- `acronyms`、`spelling_variants`：缩写和拼写变化；歧义缩写不得单独使用。
- `expansions`：更宽的上位词、旧称、机制或成员名称，仅进入高召回式。
- `controlled.mesh`：经 NLM 权威词表核实的 MeSH 标题；不得由模型臆造。

## 三套变体

| 变体 | 词汇 | 概念块 | 用途 |
|---|---|---|---|
| `recall` | anchors + synonyms + acronyms + spelling + expansions + controlled | `search` | 系统性检索默认候选 |
| `balanced` | anchors + synonyms + acronyms + spelling + controlled | `search` | 降低明显噪声的比较方案 |
| `precision_pilot` | anchors + synonyms + controlled | `search` + `optional` | 发现种子、快速试探；不是默认最终式 |

每个概念块内部用 OR，概念块之间用 AND。不要默认添加 NOT；日期、语言和研究设计限制只有在问题确实要求且有理由时才加入。

## 数据库翻译

- PubMed：MeSH 与逐项 `[tiab]` 自由词。生物医学正式版本由 `pubmed-search-builder-next` 实测和签发；本技能的编译结果只是候选。
- Europe PMC：使用 `TITLE_ABS:` 字段；受控词不假装等价于 MeSH 检索。
- WoS：`TS=(...)`。
- Scopus：`TITLE-ABS-KEY(...)`。
- arXiv：逐项使用 `all:`，并保留 AND/OR 结构。
- Semantic Scholar、CORE：使用其通用 Boolean 查询候选，必须实测接口解释。
- OpenAlex、Crossref：当前下载工作流使用自由文本搜索入口，输出标记为 `approximate`。复杂 Boolean 逻辑需要用多次查询并在本地合并，不能声称与 WoS/Scopus 严格等价。

## 验证记录

`search_strategy.md` 至少记录：

- 每套方案的精确字符串和检索日期；
- 每个数据库的命中数或失败状态；
- 已知相关论文的召回情况，区分开发种子与保留验证集；
- 噪声抽样与造成噪声的术语；
- 每次修改的原因、前后命中数和是否丢失种子；
- 最终选择及尚未解决的风险。

“召回了全部种子”只能称为开发集相对召回，不能等同于真实敏感度。没有种子或独立验证集时，不得宣称高召回已经得到实证验证。

