# 文献检索运行合同

每次检索保留 `run_manifest.json`、各来源原始分页响应、`all_records.csv`、`deduplicated_records.csv` 和 `pre_screening.csv`。

只有顶层 `complete=true` 且计划使用的来源均为 `status=complete`，才能称为完整导出。`reported_total` 是来源报告的命中数，`fetched_count` 是实际写入数。失败、缺少凭据或截断必须处理或披露。

“全部”仅指该来源在本次原生检索式、索引覆盖范围和访问权限下返回的全部匹配记录。工作区不得保存 API 密钥。
