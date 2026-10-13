# capg2 BGE-M3 服务

- 主机：`capg2`
- GPU：默认 GPU 0（L40S）
- 容器：`paper-workspace-bge-m3`
- 模型：`BAAI/bge-m3`，1024 维
- capg2 监听：仅 `127.0.0.1:8091`
- capg 安全隧道：`127.0.0.1:18091` → capg2 `127.0.0.1:8091`
- 接口：`GET /health`、`GET /v1/models`、`POST /v1/embeddings`

服务复用 capg2 已存在的只读 BGE-M3 权重和 Python 环境，不从公网重新下载。模型、虚拟环境和运行日志不进入 GitHub。

PaperQA/Luna 适配器默认访问 capg 的隧道端口并优先使用 BGE-M3；连接失败时 `auto` 模式会记录错误并退回 BM25。生产运行若要求必须使用 embedding，请传 `--retriever embedding`，使服务故障直接失败而不是降级。

验证：

```bash
ssh capg 'curl -sS http://127.0.0.1:18091/health'
ssh capg 'curl -sS http://127.0.0.1:18091/v1/embeddings -H "Content-Type: application/json" -d "{\"input\":[\"土壤重金属污染\",\"soil heavy metal contamination\"]}"'
```

运维时先检查是否已有同名容器和端口占用。不要停止非本工作区的 GPU 进程或容器。
