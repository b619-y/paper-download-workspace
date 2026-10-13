# 检索式智能体交付合同

检索前保存 `query_brief.json`、`query_plan.json`、`query_matrix.csv`、`configs/<variant>.json` 和 `search_strategy.md`。至少生成 recall、balanced、precision_pilot 三套候选；经过数据库实测、种子召回检查和噪声抽样后才能选定执行版。

概念块内部 OR、块之间 AND。结局、比较、研究设计和狭窄场景默认留给筛选。OpenAlex/Crossref 自由文本翻译标为 approximate。内部模型审查不等于正式 PRESS 同行评审。
