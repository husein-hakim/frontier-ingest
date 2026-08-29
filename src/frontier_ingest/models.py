from __future__ import annotations

from dataclasses import asdict, dataclass, field
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
    schema_version: str = "1.0"
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
        payload = asdict(self)
        payload["record_type"] = self.record_type.value
        payload["source"]["retrieved_at"] = self.source.retrieved_at.isoformat()
        payload["collected_at"] = self.collected_at.isoformat()
        for evidence in payload["evidence"]:
            evidence["method"] = evidence["method"].value
        return payload
