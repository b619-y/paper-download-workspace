# download_results.csv

字段：`item_id,paper_id,status,title,doi,source_url,pdf_path,supplement_paths,sha256,file_bytes,page_count,identity_verified,downloaded_at,notes`。结果必须逐一覆盖 MANIFEST 全部 `item_id`，并与对应行的 `paper_id/title/doi` 一致。

- 成功状态只用 `downloaded` 或 `reused_verified`；两者都必须 `identity_verified=true`，交付路径必须指向实际存在、非空、重过身份闸门的 PDF 或全文 XML。
- XML-only 交付验收后在 `pdf_path` 持久化实际 XML 路径，并在 `notes` 中保留 `[XML_FULL]`，供全文队列继续识别。
- 需人工/换渠道的状态只用 `not_found`、`paywalled`、`access_denied`、`identity_mismatch`、`failed`，且必须完整进入 `manual_backlog.csv`。
- 空状态、未知状态、结果缺行、身份错配、空/不存在交付件或缺口清单未覆盖，均阻止 `ACCEPTED`。
