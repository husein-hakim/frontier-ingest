from __future__ import annotations

import asyncio
import xml.etree.ElementTree as ET
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urljoin

from frontier_ingest.core.hashing import canonicalize_url, stable_key
from frontier_ingest.core.http import FetchError, ResilientHttpClient
from frontier_ingest.core.raw_store import ContentAddressedRawStore
from frontier_ingest.extraction import extract_json_ld, html_to_text, parse_absolute_timestamp
from frontier_ingest.freshness import decide_freshness
from frontier_ingest.models import (
    CanonicalRecord,
    EvidenceMethod,
    FieldEvidence,
    RecordType,
    SourceRef,
)


@dataclass(frozen=True, slots=True)
class NewsFeedSource:
    name: str
    feed_url: str


DEFAULT_NEWS_SOURCES = (
    NewsFeedSource("OpenAI News", "https://openai.com/news/rss.xml"),
    NewsFeedSource("Google DeepMind", "https://deepmind.google/blog/rss.xml"),
    NewsFeedSource("Hugging Face Blog", "https://huggingface.co/blog/feed.xml"),
    NewsFeedSource("MIT News AI", "https://news.mit.edu/rss/topic/artificial-intelligence2"),
    NewsFeedSource(
        "TechCrunch AI", "https://techcrunch.com/category/artificial-intelligence/feed/"
    ),
)


class NewsSignalsAdapter:
    name = "AI news signals"

    def __init__(
        self,
        http: ResilientHttpClient,
        raw_store: ContentAddressedRawStore,
        sources: tuple[NewsFeedSource, ...] = DEFAULT_NEWS_SOURCES,
    ) -> None:
        self.http = http
        self.raw_store = raw_store
        self.sources = sources
        self.source_errors: dict[str, str] = {}
        self.source_yields: dict[str, int] = {source.name: 0 for source in sources}

    async def collect(self, limit: int) -> AsyncIterator[CanonicalRecord]:
        emitted = 0
        seen: set[str] = set()
        per_source_limit = max(1, (max(1, limit) + len(self.sources) - 1) // len(self.sources))
        source_batches = await asyncio.gather(
            *(self._collect_source(source, per_source_limit) for source in self.sources)
        )
        for source, records in zip(self.sources, source_batches, strict=True):
            self.source_yields[source.name] = len(records)
            for record in records:
                if record.record_key in seen:
                    continue
                seen.add(record.record_key)
                yield record
                emitted += 1
                if emitted >= limit:
                    return

    async def _collect_source(
        self, source: NewsFeedSource, per_source_limit: int
    ) -> list[CanonicalRecord]:
        try:
            feed = await self.http.get(source.feed_url, detect_blocks=True)
        except FetchError as error:
            self.source_errors[source.name] = str(error)
            return []
        await self.raw_store.put(feed.body, ".feed.xml")
        try:
            items = self._feed_items(feed.body, source.feed_url)
        except ET.ParseError as error:
            self.source_errors[source.name] = f"invalid feed XML: {error}"
            return []
        candidates = []
        for item in items:
            published = parse_absolute_timestamp(item["published"])
            decision = decide_freshness(
                published,
                feed.fetched_at,
                "feed_timestamp",
                1.0 if published else 0.0,
            )
            if decision.accepted and published:
                candidates.append((item, published, decision))
            if len(candidates) >= per_source_limit:
                break
        fetched = await asyncio.gather(
            *(
                self._fetch_article(source, item, published, decision)
                for item, published, decision in candidates
            )
        )
        return [record for record in fetched if record is not None]

    async def _fetch_article(
        self,
        source: NewsFeedSource,
        item: dict[str, str],
        published: datetime,
        decision,
    ) -> CanonicalRecord | None:
        url = canonicalize_url(item["url"])
        try:
            article = await self.http.get(url, detect_blocks=True)
        except FetchError as error:
            self.source_errors[f"{source.name}:{url}"] = str(error)
            return None
        content_hash, _ = await self.raw_store.put(article.body, ".html")
        record = self._record(
            source,
            item,
            published,
            decision,
            article.body,
            article.fetched_at,
            content_hash,
        )
        record.validate_evidence_gate()
        return record

    @staticmethod
    def _feed_items(body: bytes, base_url: str) -> list[dict[str, str]]:
        root = ET.fromstring(body)
        items: list[dict[str, str]] = []
        for node in root.findall(".//item"):
            link = (node.findtext("link") or "").strip()
            items.append(
                {
                    "title": (node.findtext("title") or "").strip(),
                    "url": urljoin(base_url, link),
                    "published": (
                        node.findtext("pubDate")
                        or node.findtext("{http://purl.org/dc/elements/1.1/}date")
                        or ""
                    ).strip(),
                    "author": (
                        node.findtext("{http://purl.org/dc/elements/1.1/}creator") or ""
                    ).strip(),
                }
            )
        atom = "{http://www.w3.org/2005/Atom}"
        for node in root.findall(f".//{atom}entry"):
            link_node = next(
                (
                    link
                    for link in node.findall(f"{atom}link")
                    if link.get("rel", "alternate") == "alternate"
                ),
                None,
            )
            items.append(
                {
                    "title": (node.findtext(f"{atom}title") or "").strip(),
                    "url": urljoin(base_url, link_node.get("href", "") if link_node else ""),
                    "published": (
                        node.findtext(f"{atom}published") or node.findtext(f"{atom}updated") or ""
                    ).strip(),
                    "author": (node.findtext(f"{atom}author/{atom}name") or "").strip(),
                }
            )
        return [item for item in items if item["title"] and item["url"]]

    @staticmethod
    def _record(
        source: NewsFeedSource,
        item: dict[str, str],
        published: datetime,
        decision,
        body: bytes,
        fetched_at: datetime,
        content_hash: str,
    ) -> CanonicalRecord:
        json_ld = extract_json_ld(body)
        article_node = next(
            (
                node
                for node in json_ld
                if str(node.get("@type", "")).casefold()
                in {"article", "newsarticle", "blogposting"}
            ),
            {},
        )
        full_text = str(article_node.get("articleBody") or "").strip() or html_to_text(body)
        authors = item["author"]
        if not authors:
            author = article_node.get("author")
            if isinstance(author, dict):
                authors = str(author.get("name") or "")
            elif isinstance(author, list):
                authors = ", ".join(
                    str(entry.get("name") or "") for entry in author if isinstance(entry, dict)
                )
        content = {
            "title": item["title"],
            "publisher": source.name,
            "authors": [name.strip() for name in authors.split(",") if name.strip()],
            "published_at": published.isoformat(),
            "full_text": full_text,
            "freshness_status": decision.status.value,
            "freshness_method": decision.method,
            "freshness_confidence": decision.confidence,
        }
        methods = {
            "title": EvidenceMethod.FEED,
            "publisher": EvidenceMethod.FEED,
            "authors": EvidenceMethod.META,
            "published_at": EvidenceMethod.FEED,
            "full_text": EvidenceMethod.JSON_LD
            if article_node.get("articleBody")
            else EvidenceMethod.PAGE_TEXT,
            "freshness_status": EvidenceMethod.INFERRED,
            "freshness_method": EvidenceMethod.FEED,
            "freshness_confidence": EvidenceMethod.INFERRED,
        }
        evidence = [
            FieldEvidence(
                field,
                method,
                item["url"],
                "feed entry" if method is EvidenceMethod.FEED else "article page",
                confidence=1.0,
            )
            for field, method in methods.items()
        ]
        return CanonicalRecord(
            record_key=stable_key("news", canonicalize_url(item["url"])),
            record_type=RecordType.NEWS,
            source=SourceRef(source.name, item["url"], fetched_at, content_hash),
            content=content,
            evidence=evidence,
        )
