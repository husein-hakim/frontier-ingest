from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from frontier_ingest.core.hashing import stable_key
from frontier_ingest.core.http import ResilientHttpClient
from frontier_ingest.core.raw_store import ContentAddressedRawStore
from frontier_ingest.models import (
    CanonicalRecord,
    EvidenceMethod,
    FieldEvidence,
    RecordType,
    SourceRef,
)
from frontier_ingest.sources.base import SourceAdapter

GITHUB_URL = re.compile(r"https?://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)", re.IGNORECASE)


class HuggingFacePapersAdapter(SourceAdapter):
    name = "Hugging Face Papers"
    list_url = "https://huggingface.co/api/daily_papers"

    def __init__(
        self,
        http: ResilientHttpClient,
        raw_store: ContentAddressedRawStore,
        require_github: bool = True,
        require_stars: bool = False,
        page_size: int = 100,
    ) -> None:
        self.http = http
        self.raw_store = raw_store
        self.require_github = require_github
        self.require_stars = require_stars
        self.page_size = max(1, min(page_size, 100))

    async def collect(self, limit: int) -> AsyncIterator[CanonicalRecord]:
        emitted = 0
        page = 0
        seen: set[str] = set()
        while emitted < limit:
            response = await self.http.get(
                self.list_url,
                params={"p": page, "limit": self.page_size, "sort": "publishedAt"},
                headers={"Accept": "application/json"},
            )
            content_hash, _ = await self.raw_store.put(response.body, ".json")
            payload = json.loads(response.body)
            if not isinstance(payload, list) or not payload:
                break
            new_on_page = 0
            for wrapper in payload:
                paper = wrapper.get("paper", wrapper) if isinstance(wrapper, dict) else {}
                paper_id = str(paper.get("id") or "").strip()
                if not paper_id or paper_id in seen:
                    continue
                seen.add(paper_id)
                new_on_page += 1
                github_url = self._github_url(paper)
                if self.require_github and not github_url:
                    continue
                if self.require_stars and not isinstance(paper.get("githubStars"), int | float):
                    continue
                record = self._record(
                    paper,
                    github_url,
                    content_hash,
                    response.fetched_at,
                )
                record.validate_evidence_gate(optional_fields={"github_url", "github_stars"})
                yield record
                emitted += 1
                if emitted >= limit:
                    return
            if new_on_page == 0:
                break
            page += 1

    @staticmethod
    def _github_url(paper: dict[str, object]) -> str | None:
        direct = str(paper.get("githubRepo") or "").strip()
        candidate = direct or str(paper.get("summary") or "")
        match = GITHUB_URL.search(candidate)
        if not match:
            return None
        repo = match.group(2).rstrip(".,);]").removesuffix(".git")
        return f"https://github.com/{match.group(1)}/{repo}"

    def _record(
        self,
        paper: dict[str, object],
        github_url: str | None,
        content_hash: str,
        fetched_at: datetime,
    ) -> CanonicalRecord:
        paper_id = str(paper["id"])
        paper_url = f"https://arxiv.org/abs/{paper_id}"
        authors = [
            str(author.get("name") or "").strip()
            for author in paper.get("authors", [])
            if isinstance(author, dict) and author.get("name")
        ]
        stars = paper.get("githubStars")
        content = {
            "title": str(paper.get("title") or "").strip(),
            "authors": authors,
            "paper_url": paper_url,
            "abstract": str(paper.get("summary") or "").strip(),
            "published_date": self._iso(str(paper.get("publishedAt") or "")),
            "github_url": github_url,
            "github_stars": int(stars) if isinstance(stars, int | float) else None,
            "github_stars_collected_at": fetched_at.isoformat() if stars is not None else None,
            "github_stars_source": "hugging_face_cache" if stars is not None else None,
        }
        pointers = {
            "title": "paper.title",
            "authors": "paper.authors[].name",
            "paper_url": "paper.id",
            "abstract": "paper.summary",
            "published_date": "paper.publishedAt",
            "github_url": "paper.githubRepo|paper.summary",
            "github_stars": "paper.githubStars",
            "github_stars_collected_at": "HTTP fetch time",
            "github_stars_source": "paper.githubStars",
        }
        evidence = [
            FieldEvidence(
                field=field,
                method=EvidenceMethod.API,
                source_url=f"https://huggingface.co/papers/{paper_id}",
                source_pointer=pointer,
            )
            for field, pointer in pointers.items()
            if content.get(field) not in (None, "", [], {})
        ]
        return CanonicalRecord(
            record_key=stable_key("arxiv", paper_id.split("v", 1)[0]),
            record_type=RecordType.RESEARCH_PAPER,
            source=SourceRef(self.name, paper_url, fetched_at, content_hash),
            content=content,
            evidence=evidence,
        )

    @staticmethod
    def _iso(value: str) -> str:
        if not value:
            return ""
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(UTC).isoformat()
