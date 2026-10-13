# capg GROBID：从 PDF 提取元信息

- 项目：[GROBID](https://github.com/grobidOrg/grobid)。capg 的安装与说明在 `/data2/sg/dataset/soilagent/repo/tools/grobid/`。
- `POST /api/processHeaderDocument` 可从 PDF 头部返回 TEI XML，包含题名、作者、摘要、DOI 等可识别字段。字段可能缺失或识别错误，须与首页及已有可信记录交叉核对。
- 现有 `grobid_extract.py` 请求 BibTeX，只整理题名、作者、期刊、年份、DOI，**不输出摘要**。需要摘要时请求 `Accept: application/xml`，读取 TEI 的 `teiHeader/profileDesc/abstract`；不要把该脚本的空摘要误判为 PDF 无摘要。

## 调用前检查

服务仅在 capg 上。优先通过 capg 的 IPv6 回环地址检查健康状态，避免宿主机的 Docker IPv4 回环 NAT 规则干扰：

```bash
ssh capg "curl --noproxy '*' -g --connect-timeout 3 -fsS 'http://[::1]:8070/api/isalive'"
```

返回 `true` 才调用。若 IPv6 回环不可用，可用 `docker inspect -f '{{(index .NetworkSettings.Networks "grobid_default").IPAddress}}' soilagent-grobid` 在 capg 查询当前容器 IP，再访问其 `:8070`；不要固定写入某次查到的 `172.*` 地址。若检查失败，记录工具故障，不将其当成论文无元信息或无全文。

## 单篇 PDF

本机 PDF 可通过 SSH 标准输入发给 capg 的 API，无需在 capg 留下额外副本；将下例中的路径换为本批次 `outputs/files/` 里的 PDF：

```bash
ssh capg "curl --noproxy '*' -g --connect-timeout 3 --max-time 90 -fsS -H 'Accept: application/xml' -F 'input=@-;filename=paper.pdf;type=application/pdf' 'http://[::1]:8070/api/processHeaderDocument'" < /path/to/paper.pdf > /path/to/paper.tei.xml
```

如果 PDF 已在 capg，把 `input=@-` 改为其 capg 绝对路径的 `input=@/path/to/paper.pdf`，无需标准输入。TEI 使用命名空间 `http://www.tei-c.org/ns/1.0`；从 `teiHeader` 提取 `titleStmt/title`、`profileDesc/abstract`、`sourceDesc` 中的作者及 DOI `idno[@type='DOI']`。原始 XML 和抽取结果只写入当前任务允许的输出目录，不修改下载清单或验收索引。

2026-09-30 已用桌面论文下载工作区的一篇 PDF 通过 SSH 调用：接口返回可解析的 TEI，含题名、摘要和标识符。
