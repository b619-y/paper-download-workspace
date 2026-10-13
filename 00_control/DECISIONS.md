# Decisions

记录批次重分配、验收、合并、撤销和异常处理决定。

## 2026-10-13

- 工作区代码和模板同步到私有 GitHub；论文全文、索引、密钥和登录态排除。
- Mac 负责检索和授权下载；capg 负责 ASReview、PaperQA2 和 Codex Luna。
- Embedding 独立部署，不由 Luna 代替；候选主机由单独远程任务盘点。
- ASReview 只提供题名/摘要主动学习排序，最终下载决定仍记录 `download/exclude/uncertain` 及理由。
- PaperQA/Luna 输出必须通过逐字引文校验，并回到原始全文复核。
