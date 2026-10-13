# Embedding 主机盘点（2026-10-13）

独立远程任务以只读方式检查了 SSH 配置中的非 capg 候选机，未安装软件、未启动服务、未停止进程。

| 结论 | 主机 | 实测状态 |
|---|---|---|
| 推荐 | `capg2` | 4×L40S，每卡约 45.6 GiB 空闲；Docker 可用并有 NVIDIA runtime。适合 BGE-M3、bge-large-zh-v1.5、E5-large-v2。 |
| 有条件可用 | `capg4` | 4×V100，每卡约 32 GiB 空闲；根盘只剩约 25 GiB，`/data2` 已满，当前账号无 Docker 权限。 |
| 不建议 | `capg3` | 两张可查询 RTX 3090 各约 12 GiB 空闲，另两张查询异常；根盘已满，Python 临时目录异常。 |

中文和长文优先 `BAAI/bge-m3`：多语言、长上下文并可同时提供 dense/sparse/multi-vector。只处理较短中文片段时可考虑 `bge-large-zh-v1.5`；E5-large-v2 官方定位偏英文，不作为中文首选。

用户随后授权部署到 capg2。进一步核查发现 capg2 已有完整 BGE-M3 权重、FlagEmbedding 环境和可复用 Docker 镜像，因此部署复用只读资产，不依赖公网下载。运行配置见 `embedding_service/README.md`。

远程盘点任务：`盘点 Embedding 部署机器`（Codex thread `01a1354a-ecd1-7e91-846d-eb6f01cf3965`）。
