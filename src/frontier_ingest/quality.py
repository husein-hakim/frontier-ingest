from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from frontier_ingest.models import RecordType


@dataclass(slots=True)
class QualityReport:
    record_type: str
    total: int
    unique_keys: int
    valid_source_urls: int
    evidence_complete: int
    errors: Counter[str] = field(default_factory=Counter)

    @property
    def pass_rate(self) -> float:
        return self.evidence_complete / self.total if self.total else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_type": self.record_type,
            "total": self.total,
            "unique_keys": self.unique_keys,
            "valid_source_urls": self.valid_source_urls,
            "evidence_complete": self.evidence_complete,
            "pass_rate": round(self.pass_rate, 4),
            "errors": dict(self.errors),
        }


REQUIRED_FIELDS = {
    RecordType.STARTUP: {"entityName"},
    RecordType.PRODUCT: {"productName", "startupName"},
    RecordType.RESEARCH_PAPER: {
        "title",
        "authors",
        "paper_url",
        "published_date",
        "github_url",
        "github_stars",
    },
    RecordType.JOB: {"company", "title", "date", "is_remote", "role_family", "full_text"},
    RecordType.NEWS: {"title", "publisher", "published_at", "full_text"},
}


def validate_payloads(record_type: RecordType, payloads: list[dict[str, Any]]) -> QualityReport:
    report = QualityReport(record_type.value, len(payloads), 0, 0, 0)
    keys: set[str] = set()
    now = datetime.now(UTC)
    for payload in payloads:
        key = str(payload.get("record_key") or "")
        if key:
            keys.add(key)
        source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
        source_url = str(source.get("url") or "")
        if source_url.startswith(("https://", "http://")):
            report.valid_source_urls += 1
        else:
            report.errors["invalid_source_url"] += 1
        content = payload.get("content") if isinstance(payload.get("content"), dict) else {}
        missing = {
            field
            for field in REQUIRED_FIELDS[record_type]
            if content.get(field) in (None, "", [], {})
        }
        if missing:
            report.errors["missing_required_fields"] += 1
        evidence = payload.get("evidence") if isinstance(payload.get("evidence"), list) else []
        proven = {
            item.get("field")
            for item in evidence
            if isinstance(item, dict) and item.get("source_url")
        }
        populated = {field for field, value in content.items() if value not in (None, "", [], {})}
        if populated <= proven:
            report.evidence_complete += 1
        else:
            report.errors["fields_without_evidence"] += 1
        if record_type in {RecordType.JOB, RecordType.NEWS}:
            date_field = "date" if record_type is RecordType.JOB else "published_at"
            try:
                published = datetime.fromisoformat(str(content[date_field]))
                age = now - published.astimezone(UTC)
                if not timedelta(minutes=-5) <= age <= timedelta(hours=24):
                    report.errors["outside_24h_window"] += 1
            except (KeyError, ValueError, TypeError):
                report.errors["invalid_freshness_date"] += 1
    report.unique_keys = len(keys)
    if report.unique_keys != report.total:
        report.errors["duplicate_keys"] += report.total - report.unique_keys
    return report


def submission_gate(reports: list[QualityReport], strict_counts: bool = True) -> list[str]:
    failures: list[str] = []
    counts = {report.record_type: report.total for report in reports}
    if strict_counts:
        for record_type in (
            RecordType.STARTUP,
            RecordType.PRODUCT,
            RecordType.RESEARCH_PAPER,
        ):
            if counts.get(record_type.value, 0) < 1_000:
                failures.append(f"{record_type.value} has fewer than 1,000 records")
    for report in reports:
        if report.valid_source_urls != report.total:
            failures.append(f"{report.record_type} contains invalid source URLs")
        if report.evidence_complete != report.total:
            failures.append(f"{report.record_type} contains fields without evidence")
        if report.errors.get("missing_required_fields"):
            failures.append(f"{report.record_type} contains incomplete required records")
        if report.errors.get("duplicate_keys"):
            failures.append(f"{report.record_type} contains duplicate keys")
        if report.errors.get("outside_24h_window"):
            failures.append(f"{report.record_type} contains stale signal records")
    return failures
