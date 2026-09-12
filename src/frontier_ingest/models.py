from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class RecordType(StrEnum):
    STARTUP = "STARTUP"
    PRODUCT = "PRODUCT"
    RESEARCH_PAPER = "RESEARCH_PAPER"
    JOB = "JOB"
    NEWS = "NEWS"


class EvidenceMethod(StrEnum):
    API = "api"
    FEED = "feed"
    JSON_LD = "json_ld"
    META = "meta"
    PAGE_TEXT = "page_text"
    LLM = "llm"
    INFERRED = "inferred"


@dataclass(frozen=True, slots=True)
class FieldEvidence:
    field: str
    method: EvidenceMethod
    source_url: str
    source_pointer: str
    excerpt: str | None = None
    confidence: float = 1.0

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("confidence must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class SourceRef:
    name: str
    url: str
    retrieved_at: datetime
    content_hash: str


@dataclass(slots=True)
class CanonicalRecord:
    record_key: str
    record_type: RecordType
    source: SourceRef
    content: dict[str, Any]
    evidence: list[FieldEvidence]
    schema_version: str = "1.1"
    collected_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def validate_evidence_gate(self, optional_fields: set[str] | None = None) -> None:
        optional_fields = optional_fields or set()
        proven = {item.field for item in self.evidence}
        missing = {
            key
            for key, value in self.content.items()
            if value not in (None, "", [], {}) and key not in optional_fields and key not in proven
        }
        if missing:
            raise ValueError(f"fields lack evidence: {sorted(missing)}")

    def to_dict(self) -> dict[str, Any]:
        """Return the public submission contract using the brief's exact casing."""
        return {
            "schemaVersion": self.schema_version,
            "recordKey": self.record_key,
            "recordType": self.record_type.value,
            "source": {
                "name": self.source.name,
                "url": self.source.url,
                "retrievedAt": self.source.retrieved_at.isoformat(),
                "contentHash": self.source.content_hash,
            },
            "content": self.content,
            "evidence": [
                {
                    "field": item.field,
                    "method": item.method.value,
                    "sourceUrl": item.source_url,
                    "sourcePointer": item.source_pointer,
                    "excerpt": item.excerpt,
                    "confidence": item.confidence,
                }
                for item in self.evidence
            ],
            "collectedAt": self.collected_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> CanonicalRecord:
        """Hydrate queue/storage payloads, accepting the pre-1.1 snake_case shape."""

        def value(mapping: dict[str, Any], camel: str, snake: str) -> Any:
            return mapping.get(camel, mapping.get(snake))

        source_payload = payload.get("source") or {}
        evidence_payload = payload.get("evidence") or []
        retrieved_at = value(source_payload, "retrievedAt", "retrieved_at")
        collected_at = value(payload, "collectedAt", "collected_at")
        return cls(
            record_key=str(value(payload, "recordKey", "record_key")),
            record_type=RecordType(str(value(payload, "recordType", "record_type"))),
            source=SourceRef(
                name=str(source_payload["name"]),
                url=str(source_payload["url"]),
                retrieved_at=datetime.fromisoformat(str(retrieved_at)),
                content_hash=str(value(source_payload, "contentHash", "content_hash")),
            ),
            content=dict(payload.get("content") or {}),
            evidence=[
                FieldEvidence(
                    field=str(item["field"]),
                    method=EvidenceMethod(str(item["method"])),
                    source_url=str(value(item, "sourceUrl", "source_url")),
                    source_pointer=str(value(item, "sourcePointer", "source_pointer")),
                    excerpt=item.get("excerpt"),
                    confidence=float(item.get("confidence", 1.0)),
                )
                for item in evidence_payload
                if isinstance(item, dict)
            ],
            schema_version=str(value(payload, "schemaVersion", "schema_version") or "1.0"),
            collected_at=datetime.fromisoformat(str(collected_at)),
        )
