from __future__ import annotations

import html
import json
import re
from collections.abc import AsyncIterator

from bs4 import BeautifulSoup

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


class YCDirectoryAdapter(SourceAdapter):
    name = "Y Combinator AI Directory"
    base_url = "https://www.ycombinator.com/companies/industry/AI"

    def __init__(
        self,
        http: ResilientHttpClient,
        raw_store: ContentAddressedRawStore,
        record_type: RecordType = RecordType.STARTUP,
    ) -> None:
        if record_type not in {RecordType.STARTUP, RecordType.PRODUCT}:
            raise ValueError("YC adapter produces STARTUP or PRODUCT records")
        self.http = http
        self.raw_store = raw_store
        self.record_type = record_type

    async def collect(self, limit: int) -> AsyncIterator[CanonicalRecord]:
        emitted = 0
        page = 1
        seen: set[str] = set()
        while emitted < limit:
            result = await self.http.get(self.base_url, params={"page": page})
            content_hash, _ = await self.raw_store.put(result.body, ".html")
            companies = self._extract_companies(result.body)
            if not companies:
                break
            new_on_page = 0
            for company in companies:
                slug = str(company.get("slug") or "").strip()
                if not slug or slug in seen:
                    continue
                seen.add(slug)
                new_on_page += 1
                yield self._record(company, content_hash, result.fetched_at)
                emitted += 1
                if emitted >= limit:
                    return
            if new_on_page == 0:
                break
            page += 1

    @staticmethod
    def _extract_companies(body: bytes) -> list[dict[str, object]]:
        soup = BeautifulSoup(body, "html.parser")
        node = soup.find(attrs={"data-page": True})
        if node is None:
            return []
        raw = node.get("data-page")
        if not isinstance(raw, str):
            return []
        payload = json.loads(html.unescape(raw))
        companies = payload.get("props", {}).get("companies", [])
        return [item for item in companies if isinstance(item, dict)]

    def _record(self, company: dict[str, object], content_hash: str, fetched_at) -> CanonicalRecord:
        slug = str(company.get("slug") or "").strip()
        name = str(company.get("name") or "").strip()
        source_url = f"https://www.ycombinator.com/companies/{slug}"
        employee_count = self._int_or_none(company.get("team_size"))
        if employee_count is None:
            employee_count = self._int_or_none(company.get("employee_count"))
        common = {
            "description": str(
                company.get("long_description") or company.get("one_liner") or ""
            ).strip(),
            "website": str(company.get("website") or "").strip() or None,
            "batch": str(company.get("batch_name") or "").upper() or None,
            "status": str(company.get("ycdc_status") or "").strip() or None,
            "tags": [str(tag) for tag in company.get("tags", []) if tag],
        }
        if self.record_type is RecordType.STARTUP:
            content = {
                "entityName": name,
                "data": {"employeeCount": employee_count},
                **common,
            }
            key = stable_key("yc-startup", slug)
        else:
            content = {
                "productName": name,
                "startupName": name,
                "pricingModel": None,
                "tagline": str(company.get("one_liner") or "").strip() or None,
                **common,
            }
            key = stable_key("yc-product", slug)

        evidence = [
            FieldEvidence(
                field=field,
                method=EvidenceMethod.API,
                source_url=source_url,
                source_pointer=f"directory.props.companies[].{pointer}",
            )
            for field, pointer in self._evidence_fields().items()
            if content.get(field) not in (None, "", [], {})
        ]
        return CanonicalRecord(
            record_key=key,
            record_type=self.record_type,
            source=SourceRef(self.name, source_url, fetched_at, content_hash),
            content=content,
            evidence=evidence,
        )

    def _evidence_fields(self) -> dict[str, str]:
        common = {
            "description": "long_description",
            "website": "website",
            "batch": "batch_name",
            "status": "ycdc_status",
            "tags": "tags",
        }
        if self.record_type is RecordType.STARTUP:
            return {"entityName": "name", "data": "team_size", **common}
        return {
            "productName": "name",
            "startupName": "name",
            "tagline": "one_liner",
            **common,
        }

    @staticmethod
    def _int_or_none(value: object) -> int | None:
        if isinstance(value, int):
            return value
        match = re.search(r"\d+", str(value or ""))
        return int(match.group()) if match else None
