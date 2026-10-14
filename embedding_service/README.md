# BGE-M3 Embedding 服务

- 模型：`BAAI/bge-m3`，1024 维
- 接口：`GET /health`、`GET /v1/models`、`POST /v1/embeddings`

主机、GPU 设备、容器名、监听端口、隧道拓扑和模型权重路径由不入库的部署配置提供。服务应只监听本机回环或受控私网端点；模型、虚拟环境和运行日志不进入 GitHub。

设置 `PAPER_EMBEDDING_BASE_URL` 只配置地址，不构成传输授权。只有本次任务明确允许把候选文本发给该服务并传 `--embedding-authorized` 后，PaperQA-style 适配器才使用 BGE-M3；非回环端点还必须传 `--allow-remote-embedding`。连接失败时 `auto` 模式会记录错误并退回 BM25。生产运行若要求必须使用 embedding，请同时传 `--retriever embedding --embedding-authorized`，使服务故障直接失败而不是降级。

验证：

```bash
PAPER_EMBEDDING_SERVICE_URL='http://127.0.0.1:<port>'
curl -fsS "${PAPER_EMBEDDING_SERVICE_URL%/}/health"
curl -fsS "${PAPER_EMBEDDING_SERVICE_URL%/}/v1/embeddings" \
  -H 'Content-Type: application/json' \
  -d '{"input":["土壤重金属污染","soil heavy metal contamination"]}'
```

适配器的 `PAPER_EMBEDDING_BASE_URL` 通常设为 `${PAPER_EMBEDDING_SERVICE_URL%/}/v1`。运维时先检查现有服务和端口占用，不要停止非本工作区的 GPU 进程或容器。Embedding 请求只应包含检索所需文本；服务可达不等于获得传输论文全文的授权。
