#!/usr/bin/env python3
"""Search scholarly APIs, preserve raw pages, and emit normalized screening records.

Supported sources: OpenAlex, Crossref, Europe PMC, PubMed, Semantic Scholar,
CORE, arXiv, Web of Science, and Scopus. API credentials are read only from
environment variables; they are never written to the output directory.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


SOURCES = (
    "openalex", "crossref", "europe_pmc", "pubmed", "semantic_scholar",
    "core", "arxiv", "wos", "scopus",
)
DEFAULT_SOURCES = ("openalex", "crossref", "europe_pmc")
CSV_FIELDS = [
    "record_id", "source_database", "source_record", "search_query", "native_query",
    "title", "abstract", "authors", "year", "publication_date", "journal", "doi",
    "landing_url", "document_type", "updated_at", "abstract_status", "raw_page",
]
PRE_SCREENING_FIELDS = [
    "record_id", "search_query", "source_database", "source_record", "title", "abstract",
    "authors", "year", "journal", "doi", "landing_url", "screening_basis", "decision",
    "matched_criteria", "exclusion_reason", "needs_fulltext_reason", "screened_by",
    "screened_at", "notes",
]


class SearchFailure(RuntimeError):
    pass


class MissingCredential(SearchFailure):
    pass


SENSITIVE_QUERY_KEYS = {
    "api_key", "apikey", "key", "token", "access_token", "insttoken",
}


def redact_url(url: str) -> str:
    """Remove credentials from URLs before they reach logs or manifests."""
    parsed = urllib.parse.urlsplit(url)
    redacted = []
    for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True):
        replacement = "[REDACTED]" if key.casefold() in SENSITIVE_QUERY_KEYS else value
        redacted.append((key, replacement))
    return urllib.parse.urlunsplit((
        parsed.scheme, parsed.netloc, parsed.path,
        urllib.parse.urlencode(redacted, doseq=True), parsed.fragment,
    ))


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return "; ".join(filter(None, (clean_text(v) for v in value)))
    if isinstance(value, dict):
        return clean_text(value.get("value") or value.get("content") or value.get("text") or "")
    text = html.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_doi(value: Any) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", text)
    text = re.sub(r"^doi\s*:\s*", "", text)
    return text.strip().strip(".,; ")


def normalize_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


def element_text(element: ET.Element | None) -> str:
    if element is None:
        return ""
    return clean_text("".join(element.itertext()))


def first_year(*values: Any) -> str:
    for value in values:
        match = re.search(r"\b(18|19|20|21)\d{2}\b", clean_text(value))
        if match:
            return match.group(0)
    return ""


def inverted_abstract(index: Any) -> str:
    if not isinstance(index, dict):
        return ""
    positions: list[tuple[int, str]] = []
    for word, offsets in index.items():
        if isinstance(offsets, list):
            positions.extend((int(pos), str(word)) for pos in offsets if isinstance(pos, int))
    return " ".join(word for _, word in sorted(positions))


def record(source: str, source_id: Any, query: str, native_query: str, raw_page: str,
           *, title: Any = "", abstract: Any = "", authors: Any = "", year: Any = "",
           publication_date: Any = "", journal: Any = "", doi: Any = "", url: Any = "",
           document_type: Any = "", updated_at: Any = "") -> dict[str, str]:
    title_text = clean_text(title)
    abstract_text = clean_text(abstract)
    source_text = clean_text(source_id)
    rid = f"{source}:{source_text}" if source_text else f"{source}:title:{normalize_title(title_text)[:80]}"
    return {
        "record_id": rid,
        "source_database": source,
        "source_record": source_text,
        "search_query": query,
        "native_query": native_query,
        "title": title_text,
        "abstract": abstract_text,
        "authors": clean_text(authors),
        "year": first_year(year, publication_date),
        "publication_date": clean_text(publication_date),
        "journal": clean_text(journal),
        "doi": normalize_doi(doi),
        "landing_url": clean_text(url),
        "document_type": clean_text(document_type),
        "updated_at": clean_text(updated_at),
        "abstract_status": "present" if abstract_text else "missing",
        "raw_page": raw_page,
    }


@dataclass
class HttpClient:
    mailto: str
    retries: int = 4
    timeout: int = 90

    def request(self, url: str, *, params: dict[str, Any] | None = None,
                headers: dict[str, str] | None = None, data: bytes | None = None) -> bytes:
        if params:
            encoded = urllib.parse.urlencode(params, doseq=True)
            url = f"{url}{'&' if '?' in url else '?'}{encoded}"
        safe_url = redact_url(url)
        request_headers = {
            "User-Agent": f"paper-download-workspace/1.0 (mailto:{self.mailto or 'unknown@example.invalid'})",
            "Accept": "application/json, application/xml, text/xml, application/atom+xml;q=0.9",
        }
        request_headers.update(headers or {})
        req = urllib.request.Request(url, data=data, headers=request_headers)
        last: Exception | None = None
        for attempt in range(self.retries):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    return response.read()
            except urllib.error.HTTPError as exc:
                last = exc
                if exc.code not in (429, 500, 502, 503, 504) or attempt + 1 == self.retries:
                    detail = exc.read(1000).decode("utf-8", errors="replace")
                    raise SearchFailure(f"HTTP {exc.code} for {safe_url}: {detail[:500]}") from exc
                wait = exc.headers.get("Retry-After")
                time.sleep(float(wait) if wait and wait.isdigit() else min(2 ** attempt, 20))
            except (urllib.error.URLError, TimeoutError) as exc:
                last = exc
                if attempt + 1 == self.retries:
                    raise SearchFailure(f"network failure for {safe_url}: {exc}") from exc
                time.sleep(min(2 ** attempt, 20))
        raise SearchFailure(str(last))

    def json(self, url: str, **kwargs: Any) -> Any:
        return json.loads(self.request(url, **kwargs).decode("utf-8"))


def save_json(raw_dir: Path, page: int, payload: Any) -> str:
    path = raw_dir / f"page-{page:06d}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return str(path)


def save_xml(raw_dir: Path, page: int, payload: bytes) -> str:
    path = raw_dir / f"page-{page:06d}.xml"
    path.write_bytes(payload)
    return str(path)


def remaining_size(max_records: int, current: int, default: int) -> int:
    if max_records <= 0:
        return default
    return max(1, min(default, max_records - current))


def capped(records: list[dict[str, str]], max_records: int) -> tuple[list[dict[str, str]], bool]:
    if max_records > 0 and len(records) >= max_records:
        return records[:max_records], True
    return records, False


def run_openalex(client: HttpClient, query: str, original: str, raw_dir: Path,
                 options: dict[str, Any], max_records: int) -> dict[str, Any]:
    api_key = os.environ.get("OPENALEX_API_KEY", "")
    cursor = "*"
    records: list[dict[str, str]] = []
    page = 0
    total: int | None = None
    while cursor:
        page += 1
        params: dict[str, Any] = {
            "search": query, "cursor": cursor, "per_page": remaining_size(max_records, len(records), 100),
            "select": "id,title,abstract_inverted_index,authorships,publication_year,publication_date,primary_location,doi,type,updated_date",
        }
        headers: dict[str, str] = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        elif client.mailto:
            params["mailto"] = client.mailto
        data = client.json("https://api.openalex.org/works", params=params, headers=headers)
        raw = save_json(raw_dir, page, data)
        total = int((data.get("meta") or {}).get("count") or 0)
        items = data.get("results") or []
        for item in items:
            authors = [((a.get("author") or {}).get("display_name") or "") for a in item.get("authorships") or []]
            location = item.get("primary_location") or {}
            source = location.get("source") or {}
            records.append(record(
                "openalex", item.get("id"), original, query, raw,
                title=item.get("title"), abstract=inverted_abstract(item.get("abstract_inverted_index")),
                authors=authors, year=item.get("publication_year"), publication_date=item.get("publication_date"),
                journal=source.get("display_name"), doi=item.get("doi"), url=location.get("landing_page_url") or item.get("id"),
                document_type=item.get("type"), updated_at=item.get("updated_date"),
            ))
        records, hit_cap = capped(records, max_records)
        if hit_cap:
            return result(records, total, False, page, "partial_by_max_records")
        cursor = (data.get("meta") or {}).get("next_cursor")
        if not items:
            break
    return result(records, total, True, page)


def crossref_date(item: dict[str, Any]) -> str:
    for key in ("published-online", "published-print", "published", "issued", "created"):
        value = item.get(key) or {}
        parts = value.get("date-parts") if isinstance(value, dict) else None
        if parts and parts[0]:
            return "-".join(str(v).zfill(2) if i else str(v) for i, v in enumerate(parts[0]))
    return ""


def run_crossref(client: HttpClient, query: str, original: str, raw_dir: Path,
                 options: dict[str, Any], max_records: int) -> dict[str, Any]:
    cursor = "*"
    records: list[dict[str, str]] = []
    page = 0
    total: int | None = None
    while cursor:
        page += 1
        rows = remaining_size(max_records, len(records), 1000)
        params: dict[str, Any] = {
            "query.bibliographic": query, "cursor": cursor, "rows": rows,
            "select": "DOI,title,abstract,author,published,published-online,published-print,issued,created,container-title,URL,type,indexed",
        }
        if client.mailto:
            params["mailto"] = client.mailto
        data = client.json("https://api.crossref.org/works", params=params)
        raw = save_json(raw_dir, page, data)
        message = data.get("message") or {}
        total = int(message.get("total-results") or 0)
        items = message.get("items") or []
        for item in items:
            authors = [" ".join(filter(None, (a.get("given"), a.get("family")))) for a in item.get("author") or []]
            date = crossref_date(item)
            indexed = item.get("indexed") or {}
            records.append(record(
                "crossref", item.get("DOI"), original, query, raw,
                title=item.get("title"), abstract=item.get("abstract"), authors=authors,
                year=date, publication_date=date, journal=item.get("container-title"), doi=item.get("DOI"),
                url=item.get("URL"), document_type=item.get("type"), updated_at=indexed.get("date-time"),
            ))
        records, hit_cap = capped(records, max_records)
        if hit_cap:
            return result(records, total, False, page, "partial_by_max_records")
        next_cursor = message.get("next-cursor")
        if not items or len(items) < rows or not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return result(records, total, True, page)


def run_europe_pmc(client: HttpClient, query: str, original: str, raw_dir: Path,
                   options: dict[str, Any], max_records: int) -> dict[str, Any]:
    cursor = "*"
    records: list[dict[str, str]] = []
    page = 0
    total: int | None = None
    while cursor:
        page += 1
        size = remaining_size(max_records, len(records), 1000)
        data = client.json("https://www.ebi.ac.uk/europepmc/webservices/rest/search", params={
            "query": query, "format": "json", "resultType": "core", "pageSize": size,
            "cursorMark": cursor,
        })
        raw = save_json(raw_dir, page, data)
        total = int(data.get("hitCount") or 0)
        items = ((data.get("resultList") or {}).get("result") or [])
        for item in items:
            source_id = f"{item.get('source', '')}:{item.get('id', '')}"
            journal_info = item.get("journalInfo") or {}
            journal = (journal_info.get("journal") or {}).get("title")
            records.append(record(
                "europe_pmc", source_id, original, query, raw,
                title=item.get("title"), abstract=item.get("abstractText"), authors=item.get("authorString"),
                year=item.get("pubYear"), publication_date=item.get("firstPublicationDate") or journal_info.get("printPublicationDate"),
                journal=journal, doi=item.get("doi"), url=(f"https://europepmc.org/article/{item.get('source')}/{item.get('id')}"),
                document_type=item.get("pubTypeList"), updated_at=item.get("dateOfCompletion"),
            ))
        records, hit_cap = capped(records, max_records)
        if hit_cap:
            return result(records, total, False, page, "partial_by_max_records")
        next_cursor = data.get("nextCursorMark")
        if not items or not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return result(records, total, True, page)


def parse_pubmed(payload: bytes, original: str, query: str, raw: str) -> list[dict[str, str]]:
    root = ET.fromstring(payload)
    out: list[dict[str, str]] = []
    for article in root.findall(".//PubmedArticle"):
        citation = article.find("MedlineCitation")
        pmid = element_text(citation.find("PMID") if citation is not None else None)
        art = citation.find("Article") if citation is not None else None
        if art is None:
            continue
        title = element_text(art.find("ArticleTitle"))
        abstract_parts = []
        for node in art.findall("./Abstract/AbstractText"):
            label = node.attrib.get("Label") or node.attrib.get("NlmCategory") or ""
            text = element_text(node)
            abstract_parts.append(f"{label}: {text}" if label and text else text)
        author_names = []
        for author in art.findall("./AuthorList/Author"):
            collective = element_text(author.find("CollectiveName"))
            if collective:
                author_names.append(collective)
            else:
                author_names.append(" ".join(filter(None, (element_text(author.find("ForeName")), element_text(author.find("LastName"))))))
        journal = art.find("Journal")
        journal_title = element_text(journal.find("Title") if journal is not None else None)
        date_node = journal.find("JournalIssue/PubDate") if journal is not None else None
        date = " ".join(filter(None, (
            element_text(date_node.find("Year") if date_node is not None else None),
            element_text(date_node.find("Month") if date_node is not None else None),
            element_text(date_node.find("Day") if date_node is not None else None),
            element_text(date_node.find("MedlineDate") if date_node is not None else None),
        )))
        doi = ""
        for aid in article.findall("./PubmedData/ArticleIdList/ArticleId"):
            if aid.attrib.get("IdType", "").lower() == "doi":
                doi = element_text(aid)
                break
        pub_types = [element_text(n) for n in art.findall("./PublicationTypeList/PublicationType")]
        out.append(record(
            "pubmed", pmid, original, query, raw, title=title, abstract=" ".join(abstract_parts),
            authors=author_names, year=date, publication_date=date, journal=journal_title, doi=doi,
            url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/", document_type=pub_types,
        ))
    return out


def run_pubmed(client: HttpClient, query: str, original: str, raw_dir: Path,
               options: dict[str, Any], max_records: int) -> dict[str, Any]:
    params: dict[str, Any] = {"db": "pubmed", "term": query, "retmode": "json", "retmax": 0, "usehistory": "y"}
    if os.environ.get("NCBI_API_KEY"):
        params["api_key"] = os.environ["NCBI_API_KEY"]
    if os.environ.get("NCBI_EMAIL") or client.mailto:
        params["email"] = os.environ.get("NCBI_EMAIL") or client.mailto
    search = client.json("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi", params=params)
    save_json(raw_dir, 0, search)
    info = search.get("esearchresult") or {}
    total = int(info.get("count") or 0)
    webenv = info.get("webenv")
    query_key = info.get("querykey")
    if total and (not webenv or not query_key):
        raise SearchFailure("PubMed ESearch did not return WebEnv/query_key")
    records: list[dict[str, str]] = []
    page = 0
    batch = 500
    target = min(total, max_records) if max_records > 0 else total
    for start in range(0, target, batch):
        page += 1
        fetch_params: dict[str, Any] = {
            "db": "pubmed", "query_key": query_key, "WebEnv": webenv,
            "retstart": start, "retmax": min(batch, target - start), "retmode": "xml",
        }
        if os.environ.get("NCBI_API_KEY"):
            fetch_params["api_key"] = os.environ["NCBI_API_KEY"]
        payload = client.request("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi", params=fetch_params)
        raw = save_xml(raw_dir, page, payload)
        records.extend(parse_pubmed(payload, original, query, raw))
        time.sleep(0.12 if os.environ.get("NCBI_API_KEY") else 0.36)
    return result(records, total, max_records <= 0 or target >= total, page,
                  "partial_by_max_records" if max_records > 0 and target < total else "")


def run_semantic_scholar(client: HttpClient, query: str, original: str, raw_dir: Path,
                         options: dict[str, Any], max_records: int) -> dict[str, Any]:
    token = ""
    records: list[dict[str, str]] = []
    page = 0
    total: int | None = None
    key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY") or os.environ.get("S2_API_KEY") or ""
    headers = {"x-api-key": key} if key else {}
    while True:
        page += 1
        params: dict[str, Any] = {
            "query": query,
            "fields": "paperId,corpusId,externalIds,title,abstract,authors,year,publicationDate,venue,url,publicationTypes",
        }
        if token:
            params["token"] = token
        data = client.json("https://api.semanticscholar.org/graph/v1/paper/search/bulk", params=params, headers=headers)
        raw = save_json(raw_dir, page, data)
        total = int(data.get("total") or 0)
        items = data.get("data") or []
        for item in items:
            external = item.get("externalIds") or {}
            records.append(record(
                "semantic_scholar", item.get("paperId") or item.get("corpusId"), original, query, raw,
                title=item.get("title"), abstract=item.get("abstract"),
                authors=[a.get("name") for a in item.get("authors") or []], year=item.get("year"),
                publication_date=item.get("publicationDate"), journal=item.get("venue"), doi=external.get("DOI"),
                url=item.get("url"), document_type=item.get("publicationTypes"),
            ))
        records, hit_cap = capped(records, max_records)
        if hit_cap:
            return result(records, total, False, page, "partial_by_max_records")
        token = data.get("token") or ""
        if not items or not token:
            break
        if key:
            time.sleep(1.05)
    return result(records, total, True, page, "anonymous_access_may_be_throttled" if not key else "")


def author_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return [clean_text(value)] if clean_text(value) else []
    out = []
    for item in value:
        if isinstance(item, dict):
            out.append(clean_text(item.get("name") or item.get("displayName") or item.get("fullName")))
        else:
            out.append(clean_text(item))
    return [v for v in out if v]


def run_core(client: HttpClient, query: str, original: str, raw_dir: Path,
             options: dict[str, Any], max_records: int) -> dict[str, Any]:
    key = os.environ.get("CORE_API_KEY", "")
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    records: list[dict[str, str]] = []
    page = 0
    offset = 0
    total: int | None = None
    while True:
        page += 1
        limit = remaining_size(max_records, len(records), 100)
        data = client.json("https://api.core.ac.uk/v3/search/works", params={"q": query, "limit": limit, "offset": offset}, headers=headers)
        raw = save_json(raw_dir, page, data)
        total = int(data.get("totalHits") or data.get("total") or 0)
        items = data.get("results") or data.get("data") or []
        for item in items:
            journals = item.get("journals") or []
            journal = journals[0].get("title") if journals and isinstance(journals[0], dict) else item.get("publisher")
            ids = item.get("identifiers") or []
            doi = item.get("doi") or next((v for v in ids if str(v).lower().startswith("10.")), "")
            records.append(record(
                "core", item.get("id") or item.get("coreId"), original, query, raw,
                title=item.get("title"), abstract=item.get("abstract"), authors=author_names(item.get("authors")),
                year=item.get("yearPublished") or item.get("year"), publication_date=item.get("publishedDate"),
                journal=journal, doi=doi, url=item.get("downloadUrl") or item.get("sourceFulltextUrls") or item.get("links"),
                document_type=item.get("documentType"), updated_at=item.get("updatedDate"),
            ))
        records, hit_cap = capped(records, max_records)
        if hit_cap:
            return result(records, total, False, page, "partial_by_max_records")
        offset += len(items)
        if not items or len(items) < limit or (total is not None and offset >= total):
            break
    return result(records, total, True, page, "anonymous_rate_is_low" if not key else "")


def run_arxiv(client: HttpClient, query: str, original: str, raw_dir: Path,
              options: dict[str, Any], max_records: int) -> dict[str, Any]:
    ns = {"a": "http://www.w3.org/2005/Atom", "o": "http://a9.com/-/spec/opensearch/1.1/", "arxiv": "http://arxiv.org/schemas/atom"}
    records: list[dict[str, str]] = []
    page = 0
    start = 0
    total: int | None = None
    while True:
        page += 1
        size = remaining_size(max_records, len(records), 100)
        payload = client.request("https://export.arxiv.org/api/query", params={
            "search_query": query, "start": start, "max_results": size,
            "sortBy": options.get("sortBy", "submittedDate"), "sortOrder": options.get("sortOrder", "descending"),
        })
        raw = save_xml(raw_dir, page, payload)
        root = ET.fromstring(payload)
        total = int(element_text(root.find("o:totalResults", ns)) or 0)
        entries = root.findall("a:entry", ns)
        for item in entries:
            identifier = element_text(item.find("a:id", ns))
            doi = element_text(item.find("arxiv:doi", ns))
            journal_ref = element_text(item.find("arxiv:journal_ref", ns))
            authors = [element_text(node.find("a:name", ns)) for node in item.findall("a:author", ns)]
            categories = [node.attrib.get("term", "") for node in item.findall("a:category", ns)]
            records.append(record(
                "arxiv", identifier.rsplit("/", 1)[-1], original, query, raw,
                title=element_text(item.find("a:title", ns)), abstract=element_text(item.find("a:summary", ns)),
                authors=authors, year=element_text(item.find("a:published", ns)), publication_date=element_text(item.find("a:published", ns)),
                journal=journal_ref, doi=doi, url=identifier, document_type=categories,
                updated_at=element_text(item.find("a:updated", ns)),
            ))
        records, hit_cap = capped(records, max_records)
        if hit_cap:
            return result(records, total, False, page, "partial_by_max_records")
        start += len(entries)
        if not entries or (total is not None and start >= total):
            break
        time.sleep(3.1)
    return result(records, total, True, page)


def nested(obj: Any, *path: str) -> Any:
    current = obj
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def wos_expanded_record(item: dict[str, Any], original: str, query: str, raw: str) -> dict[str, str]:
    summary = nested(item, "static_data", "summary") or {}
    titles = ensure_list(nested(summary, "titles", "title"))
    title = next((t.get("content") for t in titles if isinstance(t, dict) and t.get("type") == "item"), "")
    journal = next((t.get("content") for t in titles if isinstance(t, dict) and t.get("type") == "source"), "")
    names = ensure_list(nested(summary, "names", "name"))
    authors = [clean_text(n.get("display_name") or n.get("full_name")) for n in names if isinstance(n, dict)]
    abstracts = ensure_list(nested(item, "static_data", "fullrecord_metadata", "abstracts", "abstract"))
    abstract_parts: list[str] = []
    for abstract in abstracts:
        text = nested(abstract, "abstract_text", "p") if isinstance(abstract, dict) else abstract
        abstract_parts.extend(clean_text(v) for v in ensure_list(text))
    identifiers = ensure_list(nested(item, "dynamic_data", "cluster_related", "identifiers", "identifier"))
    doi = next((x.get("value") for x in identifiers if isinstance(x, dict) and str(x.get("type", "")).lower() == "doi"), "")
    pub_info = summary.get("pub_info") or {}
    uid = item.get("UID") or item.get("uid")
    return record(
        "wos", uid, original, query, raw, title=title, abstract=abstract_parts, authors=authors,
        year=pub_info.get("pubyear"), publication_date=pub_info.get("sortdate"), journal=journal, doi=doi,
        url=f"https://www.webofscience.com/wos/woscc/full-record/{uid}" if uid else "",
        document_type=summary.get("doctypes"),
    )


def run_wos(client: HttpClient, query: str, original: str, raw_dir: Path,
            options: dict[str, Any], max_records: int) -> dict[str, Any]:
    key = os.environ.get("WOS_API_KEY") or os.environ.get("CLARIVATE_API_KEY") or ""
    if not key:
        raise MissingCredential("Web of Science requires WOS_API_KEY or CLARIVATE_API_KEY")
    mode = str(options.get("mode") or os.environ.get("WOS_API_MODE") or "expanded").lower()
    headers = {"X-ApiKey": key}
    records: list[dict[str, str]] = []
    page = 0
    total: int | None = None
    if mode == "starter":
        page_number = 1
        while True:
            page += 1
            limit = remaining_size(max_records, len(records), 50)
            data = client.json("https://api.clarivate.com/apis/wos-starter/v1/documents", params={
                "q": query, "db": options.get("database", "WOS"), "limit": limit, "page": page_number,
            }, headers=headers)
            raw = save_json(raw_dir, page, data)
            total = int((data.get("metadata") or {}).get("total") or data.get("total") or 0)
            items = data.get("hits") or data.get("documents") or []
            for item in items:
                identifiers = item.get("identifiers") or {}
                source = item.get("source") or {}
                records.append(record(
                    "wos", item.get("uid") or item.get("id"), original, query, raw,
                    title=item.get("title"), abstract=item.get("abstract"), authors=nested(item, "names", "authors"),
                    year=source.get("publishYear"), publication_date=source.get("publishDate"),
                    journal=source.get("sourceTitle"), doi=identifiers.get("doi"), url=item.get("links"),
                    document_type=item.get("types"),
                ))
            records, hit_cap = capped(records, max_records)
            if hit_cap:
                return result(records, total, False, page, "partial_by_max_records")
            if not items or (total is not None and len(records) >= total):
                break
            page_number += 1
    else:
        first = 1
        while True:
            page += 1
            count = remaining_size(max_records, len(records), 100)
            data = client.json("https://wos-api.clarivate.com/api/wos", params={
                "databaseId": options.get("database", "WOS"), "usrQuery": query,
                "count": count, "firstRecord": first,
            }, headers=headers)
            raw = save_json(raw_dir, page, data)
            total = int((data.get("QueryResult") or {}).get("RecordsFound") or 0)
            items = nested(data, "Data", "Records", "records", "REC") or []
            items = ensure_list(items)
            records.extend(wos_expanded_record(item, original, query, raw) for item in items if isinstance(item, dict))
            records, hit_cap = capped(records, max_records)
            if hit_cap:
                return result(records, total, False, page, "partial_by_max_records")
            first += len(items)
            if not items or (total is not None and first > total):
                break
    return result(records, total, True, page, f"mode={mode}")


def run_scopus(client: HttpClient, query: str, original: str, raw_dir: Path,
               options: dict[str, Any], max_records: int) -> dict[str, Any]:
    key = os.environ.get("SCOPUS_API_KEY") or os.environ.get("ELSEVIER_API_KEY") or ""
    if not key:
        raise MissingCredential("Scopus requires SCOPUS_API_KEY or ELSEVIER_API_KEY")
    headers = {"X-ELS-APIKey": key, "Accept": "application/json"}
    insttoken = os.environ.get("SCOPUS_INSTTOKEN") or os.environ.get("ELSEVIER_INSTTOKEN") or ""
    if insttoken:
        headers["X-ELS-Insttoken"] = insttoken
    cursor = "*"
    records: list[dict[str, str]] = []
    page = 0
    total: int | None = None
    while cursor:
        page += 1
        count = remaining_size(max_records, len(records), 25)
        data = client.json("https://api.elsevier.com/content/search/scopus", params={
            "query": query, "view": options.get("view", "COMPLETE"), "count": count,
            "cursor": cursor, "httpAccept": "application/json",
        }, headers=headers)
        raw = save_json(raw_dir, page, data)
        search = data.get("search-results") or {}
        total = int(search.get("opensearch:totalResults") or 0)
        items = search.get("entry") or []
        for item in items:
            authors = item.get("author") or item.get("dc:creator")
            records.append(record(
                "scopus", item.get("eid") or item.get("dc:identifier"), original, query, raw,
                title=item.get("dc:title"), abstract=item.get("dc:description") or item.get("description"),
                authors=author_names(authors), year=item.get("prism:coverDate"), publication_date=item.get("prism:coverDate"),
                journal=item.get("prism:publicationName"), doi=item.get("prism:doi"),
                url=item.get("prism:url") or item.get("link"), document_type=item.get("subtypeDescription"),
            ))
        records, hit_cap = capped(records, max_records)
        if hit_cap:
            return result(records, total, False, page, "partial_by_max_records")
        next_cursor = (search.get("cursor") or {}).get("@next")
        if not items or not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return result(records, total, True, page)


def result(records: list[dict[str, str]], total: int | None, complete: bool,
           pages: int, note: str = "") -> dict[str, Any]:
    if complete and total is not None and len(records) != total:
        complete = False
        note = ";".join(filter(None, (note, f"count_mismatch:{len(records)}!={total}")))
    return {
        "records": records,
        "reported_total": total,
        "fetched_count": len(records),
        "complete": bool(complete),
        "pages": pages,
        "note": note,
    }


ADAPTERS: dict[str, Callable[..., dict[str, Any]]] = {
    "openalex": run_openalex,
    "crossref": run_crossref,
    "europe_pmc": run_europe_pmc,
    "pubmed": run_pubmed,
    "semantic_scholar": run_semantic_scholar,
    "core": run_core,
    "arxiv": run_arxiv,
    "wos": run_wos,
    "scopus": run_scopus,
}


def translate_query(source: str, original: str) -> str:
    query = original.strip()
    if source == "arxiv" and not re.search(r"\b(all|ti|au|abs|cat|jr|id):", query):
        return f"all:{query}"
    if source == "wos" and not re.search(r"\b(TS|TI|AB|AU|SO|DO)=", query, re.I):
        return f"TS=({query})"
    if source == "scopus" and not re.search(r"\b(TITLE-ABS-KEY|TITLE|ABS|KEY|AUTH|DOI)\s*\(", query, re.I):
        return f"TITLE-ABS-KEY({query})"
    return query


def merge_records(records: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    order: list[str] = []
    for row in records:
        key = f"doi:{row['doi']}" if row.get("doi") else f"title:{normalize_title(row.get('title', ''))}:{row.get('year', '')}"
        if not normalize_title(row.get("title", "")) and not row.get("doi"):
            key = f"record:{row.get('record_id')}"
        if key not in merged:
            merged[key] = dict(row)
            order.append(key)
            continue
        target = merged[key]
        for field in CSV_FIELDS:
            incoming = row.get(field, "")
            if field in ("source_database", "source_record", "record_id", "raw_page"):
                values = [v for v in target.get(field, "").split(";") if v]
                if incoming and incoming not in values:
                    values.append(incoming)
                target[field] = ";".join(values)
            elif field == "abstract":
                if len(incoming) > len(target.get(field, "")):
                    target[field] = incoming
            elif not target.get(field) and incoming:
                target[field] = incoming
        target["abstract_status"] = "present" if target.get("abstract") else "missing"
    return [merged[key] for key in order]


def write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def pre_screening_rows(records: list[dict[str, str]]) -> list[dict[str, str]]:
    out = []
    for row in records:
        out.append({
            "record_id": row.get("record_id", ""), "search_query": row.get("search_query", ""),
            "source_database": row.get("source_database", ""), "source_record": row.get("source_record", ""),
            "title": row.get("title", ""), "abstract": row.get("abstract", ""), "authors": row.get("authors", ""),
            "year": row.get("year", ""), "journal": row.get("journal", ""), "doi": row.get("doi", ""),
            "landing_url": row.get("landing_url", ""),
            "screening_basis": "title_abstract" if row.get("abstract") else "title_only",
            "decision": "", "matched_criteria": "", "exclusion_reason": "", "needs_fulltext_reason": "",
            "screened_by": "", "screened_at": "", "notes": "",
        })
    return out


def load_config(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    return json.loads(Path(path).expanduser().read_text(encoding="utf-8"))


def source_options(config: dict[str, Any], source: str) -> dict[str, Any]:
    sources = config.get("sources") or {}
    if isinstance(sources, dict):
        value = sources.get(source) or {}
        return value if isinstance(value, dict) else {}
    return {}


def self_test() -> int:
    assert inverted_abstract({"world": [1], "Hello": [0]}) == "Hello world"
    rows = [
        record("a", "1", "q", "q", "p1", title="A Study", abstract="short", doi="https://doi.org/10.1/X"),
        record("b", "2", "q", "q", "p2", title="A Study", abstract="a much longer abstract", doi="10.1/x"),
    ]
    merged = merge_records(rows)
    assert len(merged) == 1 and merged[0]["abstract"] == "a much longer abstract"
    assert translate_query("wos", "soil AND metal") == "TS=(soil AND metal)"
    assert translate_query("scopus", "soil AND metal") == "TITLE-ABS-KEY(soil AND metal)"
    assert "secret" not in redact_url("https://example.test/?api_key=secret&q=soil")
    assert result([], 1, True, 1)["complete"] is False
    print("self-test OK")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="JSON config with original_query and per-source native queries")
    parser.add_argument("--query", help="original query; source-specific translation is recorded")
    parser.add_argument("--source", action="append", choices=(*SOURCES, "all"), help="repeatable")
    parser.add_argument("--output-dir")
    parser.add_argument("--max-records", type=int, default=0, help="per source; 0 means exhaust pagination")
    parser.add_argument("--allow-incomplete", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    config = load_config(args.config)
    original = (args.query or config.get("original_query") or "").strip()
    if not original:
        parser.error("--query or config.original_query is required")
    if not args.output_dir:
        parser.error("--output-dir is required")
    requested = args.source or config.get("source_order") or list((config.get("sources") or {}).keys()) or list(DEFAULT_SOURCES)
    if "all" in requested:
        requested = list(SOURCES)
    unknown = [name for name in requested if name not in SOURCES]
    if unknown:
        parser.error(f"unknown sources: {unknown}")

    out_dir = Path(args.output_dir).expanduser().resolve()
    if out_dir.exists() and any(out_dir.iterdir()):
        parser.error(f"output directory must be absent or empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    client = HttpClient(mailto=(os.environ.get("CROSSREF_MAILTO") or os.environ.get("OPENALEX_MAILTO") or os.environ.get("NCBI_EMAIL") or ""))
    run_manifest: dict[str, Any] = {
        "query_id": config.get("query_id") or out_dir.name,
        "original_query": original,
        "started_at": utc_now(),
        "requested_sources": requested,
        "max_records_per_source": args.max_records,
        "sources": {},
    }
    all_rows: list[dict[str, str]] = []
    incomplete = False
    for source in requested:
        options = source_options(config, source)
        native_query = str(options.get("query") or translate_query(source, original)).strip()
        source_dir = out_dir / source
        raw_dir = source_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        started = utc_now()
        try:
            response = ADAPTERS[source](client, native_query, original, raw_dir, options, args.max_records)
            rows = response.pop("records")
            write_csv(source_dir / "records.csv", CSV_FIELDS, rows)
            all_rows.extend(rows)
            status = "complete" if response.get("complete") else "incomplete"
            incomplete |= status != "complete"
            run_manifest["sources"][source] = {
                "status": status, "native_query": native_query, "started_at": started,
                "finished_at": utc_now(), **response,
            }
        except MissingCredential as exc:
            incomplete = True
            run_manifest["sources"][source] = {
                "status": "blocked_missing_credentials", "native_query": native_query,
                "started_at": started, "finished_at": utc_now(), "error": str(exc),
            }
        except Exception as exc:
            incomplete = True
            run_manifest["sources"][source] = {
                "status": "failed", "native_query": native_query,
                "started_at": started, "finished_at": utc_now(), "error": str(exc),
            }

    deduped = merge_records(all_rows)
    write_csv(out_dir / "all_records.csv", CSV_FIELDS, all_rows)
    write_csv(out_dir / "deduplicated_records.csv", CSV_FIELDS, deduped)
    write_csv(out_dir / "pre_screening.csv", PRE_SCREENING_FIELDS, pre_screening_rows(deduped))
    run_manifest.update({
        "finished_at": utc_now(), "raw_record_count": len(all_rows),
        "deduplicated_record_count": len(deduped),
        "abstract_present_count": sum(bool(r.get("abstract")) for r in deduped),
        "complete": not incomplete,
    })
    (out_dir / "run_manifest.json").write_text(json.dumps(run_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "output_dir": str(out_dir), "raw_records": len(all_rows), "deduplicated": len(deduped),
        "abstracts": run_manifest["abstract_present_count"], "complete": not incomplete,
        "source_status": {k: v["status"] for k, v in run_manifest["sources"].items()},
    }, ensure_ascii=False))
    return 0 if (not incomplete or args.allow_incomplete) else 2


if __name__ == "__main__":
    raise SystemExit(main())
