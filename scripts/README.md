# 脚本索引

生产命令保持在工作区根目录的 `scripts/` 中，避免在不同主机上产生多套入口。下表按连续流程分组，不改变现有 CLI 路径。

| 流程 | 脚本 |
|---|---|
| 工作区与总控 | `init_download_workspace.py`、`workflow_status.py`、`validate_download_workspace.py`、`langgraph_coordinator.py`、`workflow_recovery.py` |
| 1. 检索式 | `build_query_variants.py`、`install_search_assets.py` |
| 2. API 检索与 reducer | `search_literature.py`、`langgraph_search.py` |
| 3. 题名/摘要初筛与交接 | `install_screening_assets.py`、`handoff_screening_to_download.py` |
| 4. 全文下载与验收 | `enqueue_download_batch.py`、`list_download_batches.py`、`claim_download_batch.py`、`update_batch_status.py`、`make_download_csv.py`、`build_download_results.py`、`make_manual_backlog.py`、`accept_batch.py` |
| 5. 文档解析与验收 | `make_document_processing_queue.py`、`document_processing_worker.py`、`processing_queue.py`、`fulltext_parsers.py`、`paper_section_chunking.py`、`split_paper_sections.py`、`accept_document_processing.py` |
| 6. 全文证据复筛 | `make_fulltext_screening_queue.py`、`paperqa_luna.py` |
| 7. 数据提取与交付 | `make_data_extraction_queue.py`、`luna_extract_data.py`、`extraction_common.py`、`merge_extraction_receipts.py`、`validate_extraction_result.py`、`make_extraction_review_queue.py`、`accept_extraction_job.py`、`build_extraction_delivery.py` |
| 跨机与服务入口 | `stage_processing_inputs.py`、`sync_runtime.sh`、`install_handoff_assets.py`、`start_asreview_capg.sh`、`start_asreview_tunnel.sh`、`start_embedding_tunnel_capg.sh` |

所有改变业务状态的操作都必须经过对应脚本；不要为单个批次复制并改名一套脚本。
