from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator
from datetime import datetime

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

NEXT_LINK = re.compile(r'<([^>]+)>;\s*rel="next"')


class HuggingFaceSpacesAdapter(SourceAdapter):
    """Collect genuine, individually addressable AI applications from Spaces."""

    name = "Hugging Face Spaces"
    list_url = "https://huggingface.co/api/spaces"

    def __init__(
        self,
        http: ResilientHttpClient,
        raw_store: ContentAddressedRawStore,
        token: str | None = None,
        page_size: int = 100,
    ) -> None:
        self.http = http
        self.raw_store = raw_store
        self.token = token
        self.page_size = max(1, min(page_size, 100))

    async def collect(self, limit: int) -> AsyncIterator[CanonicalRecord]:
        emitted = 0
        next_url: str | None = self.list_url
        params: dict[str, str | int] | None = {
            "limit": self.page_size,
            "full": "true",
            "sort": "likes",
            "direction": -1,
        }
        seen: set[str] = set()
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        while next_url and emitted < limit:
            response = await self.http.get(next_url, params=params, headers=headers)
            params = None
            content_hash, _ = await self.raw_store.put(response.body, ".json")
            payload = json.loads(response.body)
            if not isinstance(payload, list) or not payload:
                return
            for raw in payload:
                if not isinstance(raw, dict) or raw.get("private") is True:
                    continue
                space_id = str(raw.get("id") or "").strip()
                if not space_id or "/" not in space_id or space_id in seen:
                    continue
                seen.add(space_id)
                yield self._record(raw, content_hash, response.fetched_at)
                emitted += 1
                if emitted >= limit:
                    return
            match = NEXT_LINK.search(response.headers.get("link", ""))
            next_url = match.group(1) if match else None

    def _record(
        self,
        raw: dict[str, object],
        content_hash: str,
        fetched_at: datetime,
    ) -> CanonicalRecord:
        space_id = str(raw["id"])
        author, slug = space_id.split("/", 1)
        card = raw.get("cardData") if isinstance(raw.get("cardData"), dict) else {}
        explicit_title = str(card.get("title") or "").strip()
        title = explicit_title or slug.replace("-", " ").replace("_", " ").strip()
        source_url = f"https://huggingface.co/spaces/{space_id}"
        description = str(
            raw.get("short_description") or card.get("short_description") or ""
        ).strip()
        tags = [str(tag) for tag in raw.get("tags", []) if str(tag).strip()]
        content = {
            "productName": title,
            "startupName": author,
            "rawStartupName": author,
            "pricingModel": None,
            "description": description or None,
            "tags": tags,
            "sdk": str(raw.get("sdk") or "").strip() or None,
            "likes": int(raw["likes"]) if isinstance(raw.get("likes"), int | float) else None,
            "createdAt": str(raw.get("createdAt") or "").strip() or None,
            "lastModified": str(raw.get("lastModified") or "").strip() or None,
            "accessModel": "PUBLIC_HOSTED" if raw.get("private") is False else None,
        }
        pointers = {
            "productName": "cardData.title|id.slug",
            "startupName": "id.author",
            "rawStartupName": "id.author",
            "description": "short_description|cardData.short_description",
            "tags": "tags",
            "sdk": "sdk",
            "likes": "likes",
            "createdAt": "createdAt",
            "lastModified": "lastModified",
            "accessModel": "private=false",
        }
        evidence = [
            FieldEvidence(
                field,
                EvidenceMethod.INFERRED
                if field == "productName" and not explicit_title
                else EvidenceMethod.API,
                source_url,
                pointer,
                confidence=0.95 if field == "productName" and not explicit_title else 1.0,
            )
            for field, pointer in pointers.items()
            if content.get(field) not in (None, "", [], {})
        ]
        return CanonicalRecord(
            record_key=stable_key("hf-space", space_id.casefold()),
            record_type=RecordType.PRODUCT,
            source=SourceRef(self.name, source_url, fetched_at, content_hash),
            content=content,
            evidence=evidence,
            schema_version="1.1",
        )
