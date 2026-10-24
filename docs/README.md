# 文档索引

## 先读

1. [仓库首页](../README.md)：连续 `0–7` 流程、目录图和安全边界。
2. [总控操作手册](../README_总控.md)：初始化、状态入口、计算组件和操作原则。
3. [系统架构](系统架构.md)：权威状态、证据层、队列、验收和角色边界。
4. [团队体系](团队体系.md)：角色分工、交接闸门与归属规则。

## 执行与编排

- [下载后文档处理与数据提取](数据提取流程.md)：MinerU/XML、章节侧车、PaperQA-style 证据定位、Luna 候选提取与人工验收。
- [LangGraph 薄总控](LangGraph总控.md)：可选 checkpoint、interrupt、幂等与多来源 reducer。
- [故障恢复智能体](故障恢复智能体.md)：唯一 `recovery_agent` 的投影修复白名单与安全停止条件。

## 部署与跨机

- [协调端、计算端与 GitHub 同步](同步方案.md)：单向同步、staging、receipt 和冲突处理。
- [Embedding 主机盘点](Embedding主机盘点_2026-10-13.md)：部署位置与资源判断记录。
- [部署验收](部署验收_2026-10-13.md)：计算端、embedding 服务与端到端验收记录。

带日期的文档是部署快照，不是永久合同；当它与代码、`AGENTS.md`、`00_control/POLICIES.md` 或 `01_shared/contracts/` 冲突时，以后者为准。
