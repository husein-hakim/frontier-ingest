from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from frontier_ingest.core.hashing import stable_key
from frontier_ingest.core.http import FetchError, ResilientHttpClient
from frontier_ingest.core.raw_store import ContentAddressedRawStore
from frontier_ingest.extraction import (
    classify_role_family,
    html_to_text,
    infer_remote,
    is_ai_relevant,
    parse_absolute_timestamp,
)
from frontier_ingest.freshness import decide_freshness
from frontier_ingest.models import (
    CanonicalRecord,
    EvidenceMethod,
    FieldEvidence,
    RecordType,
    SourceRef,
)


@dataclass(frozen=True, slots=True)
class JobBoardSource:
    name: str
    url: str
    parser: str
    params: dict[str, str | int]
    assume_utc: bool = False
    legal_note: str | None = None


DEFAULT_JOB_SOURCES = (
    JobBoardSource(
        "Arbeitnow",
        "https://www.arbeitnow.com/api/job-board-api",
        "arbeitnow",
        {"search": "artificial intelligence"},
    ),
    JobBoardSource("Remote OK", "https://remoteok.com/api", "remoteok", {}),
    JobBoardSource(
        "Jobicy",
        "https://jobicy.com/api/v2/remote-jobs",
        "jobicy",
        {"count": 50, "tag": "machine learning"},
        assume_utc=True,
    ),
    JobBoardSource(
        "Himalayas",
        "https://himalayas.app/jobs/api",
        "himalayas",
        {"limit": 100},
    ),
    JobBoardSource(
        "Remotive",
        "https://remotive.com/api/remote-jobs",
        "remotive",
        {"search": "artificial intelligence"},
        assume_utc=True,
        legal_note="Public feed is delayed; attribution and source link are mandatory.",
    ),
)


class JobSignalsAdapter:
    name = "AI job signals"

    def __init__(
        self,
        http: ResilientHttpClient,
        raw_store: ContentAddressedRawStore,
        sources: tuple[JobBoardSource, ...] = DEFAULT_JOB_SOURCES,
    ) -> None:
        self.http = http
        self.raw_store = raw_store
        self.sources = sources
        self.source_errors: dict[str, str] = {}

    async def collect(self, limit: int) -> AsyncIterator[CanonicalRecord]:
        emitted = 0
        seen: set[str] = set()
        per_source_limit = max(1, (max(1, limit) + len(self.sources) - 1) // len(self.sources))
        source_batches = await asyncio.gather(
            *(self._collect_source(source, per_source_limit) for source in self.sources)
        )
        for records in source_batches:
            for record in records:
                if record.record_key in seen:
                    continue
                seen.add(record.record_key)
                yield record
                emitted += 1
                if emitted >= limit:
                    return

    async def _collect_source(
        self, source: JobBoardSource, per_source_limit: int
    ) -> list[CanonicalRecord]:
        records: list[CanonicalRecord] = []
        try:
            response = await self.http.get(
                source.url,
                params=source.params,
                headers={"Accept": "application/json"},
                detect_blocks=True,
            )
        except FetchError as error:
            self.source_errors[source.name] = str(error)
            return records
        content_hash, _ = await self.raw_store.put(response.body, ".json")
        try:
            payload = json.loads(response.body)
        except json.JSONDecodeError as error:
            self.source_errors[source.name] = f"invalid JSON: {error}"
            return records
        for raw in self._items(source.parser, payload):
            normalized = self._normalize(source, raw)
            if not normalized or not is_ai_relevant(normalized["title"], normalized["full_text"]):
                continue
            published_at = normalized["published_at"]
            decision = decide_freshness(
                published_at,
                response.fetched_at,
                normalized["date_method"],
                1.0 if published_at else 0.0,
            )
            if not decision.accepted:
                continue
            record = self._record(
                source,
                normalized,
                stable_key(f"job-{source.name}", str(normalized["source_id"])),
                content_hash,
                response.fetched_at,
                decision,
            )
            record.validate_evidence_gate()
            records.append(record)
            if len(records) >= per_source_limit:
                break
        return records

    @staticmethod
    def _items(parser: str, payload: Any) -> list[dict[str, Any]]:
        if parser == "remoteok" and isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict) and item.get("id")]
        key = {
            "arbeitnow": "data",
            "jobicy": "jobs",
            "himalayas": "jobs",
            "remotive": "jobs",
        }.get(parser)
        items = payload.get(key, []) if key and isinstance(payload, dict) else []
        return [item for item in items if isinstance(item, dict)]

    @staticmethod
    def _normalize(source: JobBoardSource, raw: dict[str, Any]) -> dict[str, Any] | None:
        parser = source.parser
        if parser == "arbeitnow":
            values = {
                "source_id": raw.get("slug"),
                "title": raw.get("title"),
                "company": raw.get("company_name"),
                "url": raw.get("url"),
                "description": raw.get("description"),
                "location": raw.get("location"),
                "remote": raw.get("remote"),
                "published": raw.get("created_at"),
                "date_method": "api.created_at_epoch",
            }
        elif parser == "remoteok":
            values = {
                "source_id": raw.get("id"),
                "title": raw.get("position"),
                "company": raw.get("company"),
                "url": raw.get("url") or raw.get("apply_url"),
                "description": raw.get("description"),
                "location": raw.get("location") or "Remote",
                "remote": True,
                "published": raw.get("date") or raw.get("epoch"),
                "date_method": "api.date",
            }
        elif parser == "jobicy":
            values = {
                "source_id": raw.get("id"),
                "title": raw.get("jobTitle"),
                "company": raw.get("companyName"),
                "url": raw.get("url"),
                "description": raw.get("jobDescription"),
                "location": raw.get("jobGeo") or "Remote",
                "remote": True,
                "published": raw.get("pubDate"),
                "date_method": "api.pubDate",
            }
        elif parser == "himalayas":
            values = {
                "source_id": raw.get("guid"),
                "title": raw.get("title"),
                "company": raw.get("companyName"),
                "url": raw.get("applicationLink") or raw.get("guid"),
                "description": raw.get("description"),
                "location": ", ".join(raw.get("locationRestrictions") or []) or "Remote",
                "remote": True,
                "published": raw.get("pubDate"),
                "date_method": "api.pubDate_epoch",
            }
        elif parser == "remotive":
            values = {
                "source_id": raw.get("id"),
                "title": raw.get("title"),
                "company": raw.get("company_name"),
                "url": raw.get("url"),
                "description": raw.get("description"),
                "location": raw.get("candidate_required_location") or "Remote",
                "remote": True,
                "published": raw.get("publication_date"),
                "date_method": "api.publication_date",
            }
        else:
            return None
        if not values["source_id"] or not values["title"] or not values["url"]:
            return None
        full_text = html_to_text(str(values["description"] or ""))
        return {
            **values,
            "title": str(values["title"]).strip(),
            "company": str(values["company"] or "Unknown").strip(),
            "url": str(values["url"]),
            "location": str(values["location"] or ""),
            "full_text": full_text,
            "published_at": parse_absolute_timestamp(
                values["published"], assume_utc=source.assume_utc
            ),
        }

    @staticmethod
    def _record(
        source: JobBoardSource,
        job: dict[str, Any],
        record_key: str,
        content_hash: str,
        fetched_at: datetime,
        decision,
    ) -> CanonicalRecord:
        content = {
            "company": job["company"],
            "title": job["title"],
            "date": job["published_at"].isoformat(),
            "is_remote": infer_remote(job["location"], job["remote"]),
            "role_family": classify_role_family(job["title"]),
            "location": job["location"],
            "full_text": job["full_text"],
            "freshness_status": decision.status.value,
            "freshness_method": decision.method,
            "freshness_confidence": decision.confidence,
        }
        pointers = {
            "company": "API company field",
            "title": "API title field",
            "date": job["date_method"],
            "is_remote": "API remote/location fields",
            "role_family": "deterministic title classifier",
            "location": "API location field",
            "full_text": "API full description",
            "freshness_status": "24-hour decision",
            "freshness_method": job["date_method"],
            "freshness_confidence": job["date_method"],
        }
        evidence = [
            FieldEvidence(
                field,
                EvidenceMethod.API if field != "role_family" else EvidenceMethod.INFERRED,
                job["url"],
                pointer,
                confidence=1.0 if field != "role_family" else 0.9,
            )
            for field, pointer in pointers.items()
        ]
        return CanonicalRecord(
            record_key=record_key,
            record_type=RecordType.JOB,
            source=SourceRef(source.name, job["url"], fetched_at, content_hash),
            content=content,
            evidence=evidence,
        )
