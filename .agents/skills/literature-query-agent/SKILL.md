---
name: literature-query-agent
description: 将自然语言研究问题拆成可审计的检索概念，自动生成高召回、平衡和高精度试探三套检索式，翻译为 PubMed、OpenAlex、Crossref、Europe PMC、Semantic Scholar、CORE、arXiv、Web of Science 和 Scopus 的查询配置，并用已知相关论文、命中数和抽样结果迭代验证。用于“帮我写检索式”“生成多数据库检索策略”“优化 Boolean 查询”或论文检索前的检索式设计；不用于直接综述结论。
---

# 检索式智能体

把研究问题转成可验证、可复现的多数据库检索策略，再交给 `paper-download-workspace` 执行检索。不要一次性凭直觉输出一个看起来复杂的 Boolean 字符串。

## 路由

- 生物医学且包含 PubMed：先使用 `pubmed-search-builder-next` 完成 PubMed 的概念范围、MeSH/自由词、已知相关记录召回、PRESS 结构化内部审查和实时验证；再从**概念块**翻译到其他数据库，不要机械替换最终 PubMed 字段标签。
- 非生物医学或暂不需要 PubMed：按本技能建立概念块、词汇层和多套候选式，并在目标数据库做命中数、种子召回和噪声抽样。
- 用户只要查几篇代表性论文时可用快速深度；系统综述、范围综述或“全部导出”默认使用标准深度。

完整 JSON 字段和数据库翻译边界见[检索式设计协议](references/query-strategy-protocol.md)。

## 工作流

1. **定义问题与边界**：记录研究问题、综述类型、日期/语言/文献类型限制、目标数据库，以及已知相关论文（若有）。高影响歧义会改变 AND 概念块时必须请用户确认；用户要求直接继续时，记录假设。
2. **选择框架**：干预问题用 PICO，暴露/病因用 PECO，诊断用 PIRD，定性问题用 PICo/SPIDER，范围综述用 PCC，方法学问题用“方法 + 任务 + 必要应用场景”。框架帮助分析，不代表每个元素都要进入查询。
3. **决定概念角色**：只把每篇相关论文都应明确提到、且能被题名/摘要/主题词可靠表达的概念设为 `search`。结局、比较、狭窄场景、研究设计通常放在 `screen`；不确定的块设为 `optional` 并仅用于高精度试探式。
4. **构建词汇层**：每个 `search` 概念填写核心锚词、同义词、缩写、拼写变体和扩展词。受控词必须从权威词表核实；PubMed 同时使用核实后的 MeSH 与 `[tiab]` 自由词。种子论文只能扩充词汇和测召回，不能反过来改变纳入范围。
5. **生成多套检索式**：填写 `query_brief.json`，运行：

```bash
python3 scripts/build_query_variants.py \
  --brief /path/to/query_brief.json \
  --output-dir /path/to/query-plan
```

输出 `recall`、`balanced`、`precision_pilot` 三套方案，以及每套方案的九数据库配置。`recall` 是系统性检索的默认候选；`precision_pilot` 只用于试探和发现种子，不能因为结果少就直接当作完整检索。

6. **实测迭代**：用 `paper-download-workspace/scripts/search_literature.py --config ... --max-records 20 --allow-incomplete` 做隔离试跑，记录各库报告命中数并抽样查看噪声。若有已知相关论文，检查每套式是否召回；漏检时定位是哪个概念块或术语造成。任何修改都记录原因，不静默覆盖旧方案。
7. **确定执行版**：优先选择能召回全部合格种子且噪声可筛选的最宽方案。没有种子时明确写“未估计召回率”。系统综述检索式仍应由信息专家进行正式 PRESS 同行评审；模型内部审查不能冒充该评审。
8. **交给论文检索Agent**：把选定配置传给 `search_literature.py` 做完整分页导出；生产运行不使用 `--max-records`。随后按既有流程去重、题名/摘要初筛并进入下载批次。

## 输出要求

- `query_brief.json`：问题、框架、概念角色、词汇、限制和种子论文。
- `query_plan.json`：各变体、数据库原生式、翻译保真度和未决风险。
- `query_matrix.csv`：便于人工比较的“变体 × 数据库”矩阵。
- `configs/<variant>.json`：可直接传给论文检索脚本。
- `search_strategy.md`：选择理由、实测命中数、种子召回、噪声观察、版本修改和限制。

OpenAlex 与 Crossref 的自由文本接口不等价于 WoS/Scopus 的严格字段 Boolean 查询，必须在 `query_plan.json` 标成 `approximate`，不可声称逐字等价。数据库语法翻译后仍需实测，不因编译脚本成功就宣称检索式有效。

