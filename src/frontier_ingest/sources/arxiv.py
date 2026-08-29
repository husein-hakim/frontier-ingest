from __future__ import annotations

import asyncio
import xml.etree.ElementTree as ET
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import aiohttp

from frontier_ingest.core.hashing import stable_key
from frontier_ingest.core.raw_store import ContentAddressedRawStore
from frontier_ingest.models import (
    CanonicalRecord,
    EvidenceMethod,
    FieldEvidence,
    RecordType,
    SourceRef,
)
from frontier_ingest.sources.base import SourceAdapter

ATOM = "{http://www.w3.org/2005/Atom}"


class ArxivAdapter(SourceAdapter):
    name = "arXiv"
    endpoint = "https://export.arxiv.org/api/query"

    def __init__(
        self, raw_store: ContentAddressedRawStore, page_size: int = 100, delay_seconds: float = 3.0
    ) -> None:
        self.raw_store = raw_store
        self.page_size = min(max(page_size, 1), 2000)
        self.delay_seconds = max(delay_seconds, 0.0)

    async def collect(self, limit: int) -> AsyncIterator[CanonicalRecord]:
        if limit <= 0:
            return
        timeout = aiohttp.ClientTimeout(total=60)
        headers = {"User-Agent": "frontier-ingest/0.1 research-assessment contact=local"}
        emitted = 0
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            for start in range(0, limit, self.page_size):
                requested = min(self.page_size, limit - emitted)
                payload, request_url, retrieved_at = await self._fetch_page(
                    session, start, requested
                )
                content_hash, _ = await self.raw_store.put(payload, ".atom.xml")
                entries = ET.fromstring(payload).findall(f"{ATOM}entry")
                if not entries:
                    break
                for entry in entries:
                    record = self._parse_entry(entry, request_url, retrieved_at, content_hash)
                    record.validate_evidence_gate()
                    yield record
                    emitted += 1
                    if emitted >= limit:
                        return
                if emitted < limit:
                    await asyncio.sleep(self.delay_seconds)

    async def _fetch_page(
        self, session: aiohttp.ClientSession, start: int, count: int
    ) -> tuple[bytes, str, datetime]:
        params = {
            "search_query": "cat:cs.AI OR cat:cs.LG OR cat:cs.CL OR cat:cs.CV",
            "start": start,
            "max_results": count,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }
        async with session.get(self.endpoint, params=params) as response:
            response.raise_for_status()
            return await response.read(), str(response.url), datetime.now(UTC)

    def _parse_entry(
        self, entry: ET.Element, request_url: str, retrieved_at: datetime, content_hash: str
    ) -> CanonicalRecord:
        def text(tag: str) -> str:
            node = entry.find(f"{ATOM}{tag}")
            return " ".join((node.text or "").split()) if node is not None else ""

        paper_url = text("id").replace("http://", "https://")
        arxiv_id = paper_url.rsplit("/", 1)[-1].split("v", 1)[0]
        authors = [
            " ".join((node.findtext(f"{ATOM}name") or "").split())
            for node in entry.findall(f"{ATOM}author")
        ]
        content = {
            "title": text("title"),
            "authors": authors,
            "paper_url": paper_url,
            "abstract": text("summary"),
            "published_date": text("published"),
            "github_url": None,
            "github_stars": None,
        }
        evidence = [
            FieldEvidence(field, EvidenceMethod.API, paper_url, f"Atom entry/{pointer}")
            for field, pointer in {
                "title": "title",
                "authors": "author/name",
                "paper_url": "id",
                "abstract": "summary",
                "published_date": "published",
            }.items()
        ]
        return CanonicalRecord(
            record_key=stable_key("arxiv", arxiv_id),
            record_type=RecordType.RESEARCH_PAPER,
            source=SourceRef(self.name, paper_url or request_url, retrieved_at, content_hash),
            content=content,
            evidence=evidence,
        )
