# fulltext_screening_queue.csv

字段：`item_id,paper_id,title,doi,fulltext_path,fulltext_status,criterion_id,criterion_text,decision,evidence_quote,evidence_page,evidence_section,evidence_kind,evidence_figure,evidence_image_path,evidence_object_ref,reviewer,reviewed_at,notes`。

只有 `fulltext_status=ready` 才能作内容判断。`decision` 使用 `include`、`exclude`、`uncertain`、`not_assessed`；全文不可用或身份未核验不等于 `exclude`。若目标数据可能在论文图中，先下载并阅读目标图再判定，并记录图号、图片路径和可用时的对象 ref。
