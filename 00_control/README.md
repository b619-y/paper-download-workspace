# 00_control

> **目录职责：状态、政策与批次索引。**

工作区控制面：身份、政策、批次索引和人工决定日志。

- `WORKSPACE.json`：工作区身份与版本。
- `POLICIES.md`：下载、传输、模型和验收政策。
- `BATCH_INDEX.example.csv`：批次索引空模板。
- `DECISIONS.example.md`：人工决定日志空模板。

真实 `BATCH_INDEX.csv`、`DECISIONS.md`、锁和验收事务只保存在本地，不进 Git。
