# 01_shared

> **目录职责：合同、提示词与配置模板。**

跨主机、跨 Agent 共用的稳定合同和空示例。

- `contracts/`：任务、检索、筛选、下载、文档处理、全文复筛和数据提取 schema。
- `prompts/`：可审计的角色提示词。
- `*.example.json`：不含真实任务数据的配置模板。

这里只放可版本化的合同；任务实例放在 `runtime/tasks/<task_id>/`。
