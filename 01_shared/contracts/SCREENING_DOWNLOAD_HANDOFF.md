# 检索筛选到下载批次的交接合同

API 检索完成后先填写 `pre_screening.csv`，再运行 `handoff_screening_to_download.py`。脚本默认只交接 `download + uncertain`，按 DOI、否则按规范化题名加年份二次去重，并创建 `READY` 批次。

空白决定、冲突决定、入选却无理由，以及未获接受的不完整检索都会阻止交接。批次 `inputs/search_screening/` 保留完整证据。`READY` 表示进入下载队列，不表示下载完成。
