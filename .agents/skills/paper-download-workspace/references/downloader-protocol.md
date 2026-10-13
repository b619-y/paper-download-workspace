# 下载Agent执行协议

## 开始前

1. 阅读根目录`README_总控.md`、本批次`REQUEST.md`、`MANIFEST.csv`和`STATUS.json`。
2. 使用`claim_download_batch.py`领取，重复领取必须失败。
3. 领取后用`update_batch_status.py`改为`RUNNING`（要带`--worker`，身份必须与领取者一致）。

## 下载输入：用批次自带的投影，不要现场投影

批次里已经有下载就绪的`outputs/zju_papers.csv`（入队时生成，列`doi,paper_id,suggested_filename,title`，只含`queue_action=download`，无BOM）。出版社/DOI 条目可**直接传给**`zju-edge-paper-download/scripts/pipeline.py --csv`；已核实的 CNKI 条目即使出现在该投影中，也按下述知网分支单独完成，不能用管线的 `manual` 状态作最终结论。纯 CNKI 批次跳过 DOI 管线。

只有投影缺失（老批次）或MANIFEST被改过时才重建：

```bash
PDW=~/.codex/skills/paper-download-workspace/scripts
python3 $PDW/make_download_csv.py <batch_id>
```

不要临时写投影代码、不要用`utf-8-sig`重写投影（BOM会让`resolve.py`把整批判成“无DOI → manual”）；不要手改`MANIFEST.csv`来迁就下载脚本。

## 逐篇下载

- 先按来源分流：CNKI 条目若只有题名，先用 `cnki-search` 在国内知网定位实际记录，再用 `cnki-paper-detail` 核对题名、作者、年份、文种和观察到的详情 URL；已有 URL 也要核对身份。然后读取 `cnki-download/SKILL.md` 及其浙大 WebVPN 参考流程，在已授权 Edge 详情页使用原生 `PDF下载`。DOI/出版社条目继续走 `zju-edge-paper-download`。不要凭题名猜详情 URL，也不要把无 DOI 的 CNKI 条目交给 DOI 管线后直接记为 `manual`。CNKI 下载后的文件仍须放入本批次 `outputs/files/` 并纳入同一结果/验收流程。
- 先核验题名、DOI、期刊和年份，再保存文件。
- 优先合法公开来源、出版社页面、机构已有权限和用户已授权的浏览器会话。
- 不绕过付费墙、验证码或访问控制。
- PDF必须检查文件头、可解析页数、首末页、题名或DOI；网页、摘要、封面页和损坏文件不算成功。
- 下载补充材料时保持原扩展名，并在结果表中列出所有路径。
- 文件统一放在本批次`outputs/files/`，不得覆盖同名文件；发生冲突时加入短DOI或item_id。
- 每个成功文件记录SHA-256、大小、页数和来源URL。

## 完成和交接

`download_results.csv`必须覆盖清单全部item_id。没有下载成功的论文也必须有状态和原因。完成后填写`HANDOFF.md`，列出成功数、复用数、失败数、需要人工登录或下载的论文，再把状态改为`REVIEW`。

交接前用`make_manual_backlog.py <batch_id>`导出`outputs/manual_backlog.csv`：它按原因码自动填好`needs`与`owner`，字段与`03_review/MANUAL_DOWNLOAD_BACKLOG.csv`完全同构，协调Agent只需拼接（可选：用`logs/gap_channels.json`补`attempted_channels`）。

下载Agent不能把批次改为`ACCEPTED`，也不能直接修改`04_library_index/PAPER_INDEX.csv`。

若批次目的包含下载后的内容筛选，下载Agent仍只负责下载与身份/完整性核验，不代替任务筛选Agent下内容结论。协调Agent验收后运行 `make_fulltext_screening_queue.py`，再把队列交给任务筛选Agent。只有摘要、全文不可用、身份未核验或解析失败的条目必须保留相应状态，不能写成内容“不符合”。

## CNKI 实测故障与处理

以下只适用于**已核实来源为 CNKI**、且用户已有浙大 Edge/WebVPN 授权会话的条目。浏览器操作细节以 `cnki-download/SKILL.md` 和 `references/zju-webvpn-edge.md` 为准；这里规定批次里怎样判断失败与交付，不把知网当作 DOI 管线的特例硬跑。

| 现象 | 先核实 | 处理与记录 |
|---|---|---|
| 直连知网显示“IP 不在允许范围” | 当前**网页**是否显示浙江大学机构身份；Mac 的 `10.*` 内网地址不证明知网出口有权限 | 复用用户已登录的 WebVPN 门户，输入从知网页面**实际观察到**的详情 URL，再核对机构身份和题名；仍失权则记访问受阻，不能断言文献无全文，也不能拼代理 URL 或搬用 Cookie。 |
| 页面停在“安全验证”或 CAPTCHA | 是否为站点显示的真人验证，而非网络超时或浏览器控制脚本挂起 | 暂停该条目并请用户在 Mac 已授权页面完成验证；不自动绕过、不反复刷新或新建相同批次。验证后复用原批次、重核机构身份及论文详情，再继续原生 `PDF下载`；未完成验证时记“访问待人工验证”，不是“论文无全文”。 |
| 无 DOI 被 `resolve.py` 判为 `manual` | 来源是否已确认为 CNKI | 改走授权的 CNKI 浏览器分支；DOI 管线的 `manual` 不是知网全文结论。纯 CNKI 批次不必运行 DOI 管线。 |
| 点击通用“下载”得到 CAJ | 详情页是否另有明确的 `PDF下载` | 只用原生 `PDF下载` 取默认 PDF；若无权限或无 PDF 选项，逐篇记录原因，不把 CAJ 改扩展名或冒充 PDF。 |
| 点击后无文件、传输停滞或浏览器调用挂起 | 对应下载回执、文件大小是否变化；区分 PDF 传输与 Edge/AppleScript 控制脚本的状态 | 不重复点击正在进行的下载；检查一次页面与进程，只对确认失败的传输有限重试。若控制脚本超时，先确认进程属于本任务，再结束挂起的脚本，不并发重试或误杀用户进程；脚本挂起不等于 CNKI 失权。仍无回执就记“已尝试、未核验”，不报成功。 |
| 某个可选浏览器/CLI 工具因本机凭据或钥匙串锁定无法启动 | 确认报错发生在工具启动阶段，而不是 CNKI 页面或 PDF 传输阶段；检查当前会话是否另有可用的授权浏览器连接 | 直接用可用连接继续本批次；若均不可用，请用户在 Mac 本机恢复会话或凭据，不索取或记录密码，也不绕过保护。保留未完成条目并记录原始错误；工具故障不等于论文无全文。 |
| 文件落在默认 `Downloads` 或文件名不符 | 只定位**本次下载回执对应**的文件，并核对论文身份 | 将任务所属文件移到本批次 `outputs/files/`，避免扫走无关文件及覆盖同名文件；随后重新验证并记录最终路径。 |
| `file` 报页数异常，或 `%%EOF` 后有厂商尾随字节 | PDF 解析器页数、首页题名/作者、末页内容、大小和 SHA-256 | 单一 `file` 页数或 EOF 位置不作截断判据；全部解析与内容核验通过才算完整 PDF，否则记录校验失败。 |

2026-09-28 已从授权知网详情页取得并核验一篇 67 页学位论文及一篇 8 页期刊文；这验证的是**访问路径**，不是今后每篇都具有相同权限。每篇结果仍须写入 `outputs/download_results.csv`；失败原因和已试通道写 `logs/failures.md`。

## 已验证的坑（2026-09-16 批次复盘）

1. **投影BOM**：`resolve.py`用`encoding="utf-8"`读CSV，投影带BOM时`row.get("doi")`返回`None`，整批静默变`manual`。投影一律无BOM；`validate_download_workspace.py`会拦。
2. **不要阻塞等待长任务**：`pipeline.py`把子进程输出整段捕获后才打印，`tee`日志在结束时才出现，`job_output(wait=true)`只会干等到超时。要进度就让命令**无缓冲增量写日志**（`python3 -u`、`stdbuf`，或让脚本自己 flush），然后非阻塞轮询；等待上限控制在30–60秒，超时就去看文件大小/进程状态。
3. **大文件先测速再决定**：53MB的OSTI扫描件实测瞬时16–24KB/s且中途断流重写（`.part`12.6→9.0→10.3MB，净速约2KB/s）。先花60秒量一次净速率和是否回退，再决定值不值得等；`pipeline.py`固定`--max-time 300`且失败即删`.part`，天生无法续传大文件，需用`fetch_direct.py --timeout <秒>`单跑。
4. **补充材料受介质限制**：强制通道只校验并交付PDF/XML。Elsevier的`mmc`多为`doc/docx/jpg/mp4`，S3内容库直链403，需官方object端点（要key+insttoken）；ACS/Nature的SI是PDF，可用`browser_batch.py --job`逐个取。`requested_files`含`supplement`时要先判介质，别默认能拿到。
5. **`browser_batch`的45秒早期中止会假阴性**：文件可能已落盘`~/Downloads`却在波次结束后才settle，于是报`no_download`。收尾时扫一遍`~/Downloads`本轮新增的未认领文件（按`%PDF`+`%%EOF`+页数+题名/水印核对）再下结论。
6. **出版社DOI排版会骗过字符串匹配**：ASCE把DOI排成全角括号`共…兲`，ASCII匹配必然失败而报`needs_review`；先做全角/空白规范化再比对，别急着当身份不符。
7. **无DOI条目要单独补plan**：`resolve.py`会把无DOI行判为`manual`。这类条目（EPA/NEPIS报告等）应在`outputs/plan_patch.json`里补真实直链后单独跑，并在`download_results.csv`显式记一行——静默丢弃比失败更糟。
8. **零候选时必须给落地页**：`resolve` 现在会在"一个候选都没有"时兜底给出 DOI 落地页；`browser_batch` 会用**DOM fallback**（读标签 DOM → 同站 PDF 链接 → 再派发一波）把它变成 PDF。注意代理 URL 里**没有出版社域名**，同站判断必须比 WebVPN token（`_site_key`），只比 host 会把出版社自己的链接全剔掉。
9. **池内也要有增量输出**：`fetch_direct`/`tdm_fetch` 已改 `as_completed` 逐篇打印；`fetch_direct` 还会按 HTTP 状态与 content-type 直接给出 `E04/E05/E09/E12/E13/E07` 级诊断——看到 `E12` 就是"拿到了 HTML 反爬页"，别再当慢源重试。
10. **路由表没覆盖≠没权限**：`10.1061`（ASCE）不在`ROUTING`里，但走`webvpn`代理`ascelibrary.org/doi/pdf/<doi>`实测能拿到浙大全文。遇到默认`browser`且无候选URL的条目，先手工验证一次出版社WebVPN路径，再决定是否报人工。
