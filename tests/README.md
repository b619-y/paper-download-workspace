# 测试索引

```bash
python3 -m pytest -q
```

测试文件与生产边界按同名关系对应：

- `test_workflow_status.py`、`test_workflow_extraction_gates.py`：阶段推导与闸门。
- `test_langgraph_*.py`：checkpoint、interrupt、多来源 reducer 和重放边界。
- `test_search_literature.py`、`test_handoff_screening_to_download.py`：检索完整性、去重与初筛交接。
- `test_accept_batch.py`、`test_update_batch_status.py`：下载批次状态与验收。
- `test_document_processing.py`、`test_paper_section_chunking.py`：MinerU/XML 处理、章节侧车与哈希绑定。
- `test_paperqa_luna.py`、`test_data_extraction.py`、`test_extraction_receipts_hardening.py`：证据定位、提取合同、receipt 与人工验收。
- `test_sync_runtime.py`、`test_stage_processing_inputs.py`：跨机传输的授权、方向和不可变绑定。
- `test_workflow_recovery.py`：有限自动修复、预算、后置条件与路径安全。
