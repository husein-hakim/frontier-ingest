from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

from frontier_ingest.models import RecordType


@dataclass(slots=True)
class QualityReport:
    record_type: str
    total: int
    unique_keys: int
    unique_source_urls: int
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
            "unique_source_urls": self.unique_source_urls,
            "valid_source_urls": self.valid_source_urls,
            "evidence_complete": self.evidence_complete,
            "pass_rate": round(self.pass_rate, 4),
            "errors": dict(self.errors),
        }


REQUIRED_FIELDS = {
    RecordType.STARTUP: {"entityName", "rawEntityName"},
    RecordType.PRODUCT: {"productName", "startupName", "rawStartupName"},
    RecordType.RESEARCH_PAPER: {
        "title",
        "authors",
        "paper_url",
        "published_date",
        "github_url",
        "github_stars",
    },
    RecordType.JOB: {
        "company",
        "rawCompany",
        "title",
        "date",
        "is_remote",
        "role_family",
        "full_text",
    },
    RecordType.NEWS: {"title", "publisher", "published_at", "full_text"},
}


def validate_payloads(record_type: RecordType, payloads: list[dict[str, Any]]) -> QualityReport:
    report = QualityReport(record_type.value, len(payloads), 0, 0, 0, 0)
    keys: set[str] = set()
    source_urls: set[str] = set()
    now = datetime.now(UTC)
    for payload in payloads:
        if not all(
            field in payload
            for field in ("schemaVersion", "recordKey", "recordType", "collectedAt")
        ):
            report.errors["schema_contract_mismatch"] += 1
        if payload.get("recordType") != record_type.value:
            report.errors["record_type_mismatch"] += 1
        key = str(payload.get("recordKey") or payload.get("record_key") or "")
        if key:
            keys.add(key)
        source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
        if not all(field in source for field in ("name", "url", "retrievedAt", "contentHash")):
            report.errors["schema_contract_mismatch"] += 1
        source_url = str(source.get("url") or "")
        parsed_url = urlsplit(source_url)
        if parsed_url.scheme in {"http", "https"} and bool(parsed_url.hostname):
            report.valid_source_urls += 1
            source_urls.add(source_url)
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
        if any(
            isinstance(item, dict)
            and not all(
                field in item for field in ("field", "method", "sourceUrl", "sourcePointer")
            )
            for item in evidence
        ):
            report.errors["schema_contract_mismatch"] += 1
        proven = {
            item.get("field")
            for item in evidence
            if isinstance(item, dict) and (item.get("sourceUrl") or item.get("source_url"))
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
        if record_type is RecordType.PRODUCT:
            product = str(content.get("productName") or "").casefold().strip()
            startup = str(content.get("startupName") or "").casefold().strip()
            if product and product == startup:
                report.errors["product_company_name_collision"] += 1
            pricing = content.get("pricingModel")
            if pricing is not None and pricing not in {"FREE", "FREEMIUM", "PAID", "ENTERPRISE"}:
                report.errors["invalid_pricing_model"] += 1
        if record_type is RecordType.RESEARCH_PAPER:
            if content.get("github_stars_source") != "github_graphql":
                report.errors["github_stars_not_direct"] += 1
            try:
                measured = datetime.fromisoformat(str(content["github_stars_collected_at"]))
                metric_age = now - measured.astimezone(UTC)
                if not timedelta(minutes=-5) <= metric_age <= timedelta(hours=24):
                    report.errors["github_stars_stale"] += 1
            except (KeyError, ValueError, TypeError):
                report.errors["github_stars_timestamp_invalid"] += 1
    report.unique_keys = len(keys)
    report.unique_source_urls = len(source_urls)
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
        for record_type in (RecordType.JOB, RecordType.NEWS):
            if counts.get(record_type.value, 0) < 1:
                failures.append(
                    f"{record_type.value} has no verified record inside the 24-hour window"
                )
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
        if report.errors.get("invalid_freshness_date"):
            failures.append(f"{report.record_type} contains an invalid freshness date")
        if report.errors.get("schema_contract_mismatch") or report.errors.get(
            "record_type_mismatch"
        ):
            failures.append(f"{report.record_type} does not match the camelCase schema contract")
        if (
            report.record_type
            in {
                RecordType.STARTUP.value,
                RecordType.PRODUCT.value,
                RecordType.RESEARCH_PAPER.value,
            }
            and report.unique_source_urls != report.total
        ):
            failures.append(f"{report.record_type} does not have one genuine source URL per record")
        if report.record_type == RecordType.PRODUCT.value:
            collisions = report.errors.get("product_company_name_collision", 0)
            if report.total and collisions / report.total > 0.05:
                failures.append("PRODUCT appears to contain company records relabelled as products")
            if report.errors.get("invalid_pricing_model"):
                failures.append("PRODUCT contains unsupported pricing classifications")
        if report.record_type == RecordType.RESEARCH_PAPER.value:
            if report.errors.get("github_stars_not_direct"):
                failures.append("RESEARCH_PAPER GitHub stars were not refreshed directly")
            if report.errors.get("github_stars_stale") or report.errors.get(
                "github_stars_timestamp_invalid"
            ):
                failures.append("RESEARCH_PAPER GitHub metrics are not current")
    return failures
