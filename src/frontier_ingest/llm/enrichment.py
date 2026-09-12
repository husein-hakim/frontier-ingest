from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from frontier_ingest.config import Settings
from frontier_ingest.core.http import ResilientHttpClient
from frontier_ingest.llm.orchestrator import LLMOrchestrator
from frontier_ingest.llm.providers import OpenAICompatibleProvider
from frontier_ingest.models import CanonicalRecord, EvidenceMethod, FieldEvidence, RecordType

PRICING_MODELS = {"FREE", "FREEMIUM", "PAID", "ENTERPRISE"}
ROLE_FAMILIES = {
    "Research",
    "Data",
    "Product",
    "Engineering",
    "Sales",
    "Marketing",
    "Operations",
    "Other",
}


@dataclass(slots=True)
class LLMEnrichmentStats:
    candidate_records: int = 0
    calls: int = 0
    applied_fields: int = 0
    rejected_fields: int = 0
    failures: int = 0
    provider_attempts: int = 0
    selected_fragments: int = 0


@dataclass(frozen=True, slots=True)
class EnrichmentTask:
    document: str
    schema: dict[str, Any]
    unresolved_fields: set[str]
    allowed_values: dict[str, set[str]]


def build_orchestrator(settings: Settings, http: ResilientHttpClient) -> LLMOrchestrator | None:
    if not settings.enable_llm_enrichment:
        return None
    providers = []
    configurations = (
        (
            "primary",
            settings.primary_llm_api_key,
            settings.primary_llm_base_url,
            settings.primary_llm_model,
        ),
        (
            "secondary",
            settings.secondary_llm_api_key,
            settings.secondary_llm_base_url,
            settings.secondary_llm_model,
        ),
        (
            "tertiary",
            settings.tertiary_llm_api_key,
            settings.tertiary_llm_base_url,
            settings.tertiary_llm_model,
        ),
    )
    for name, api_key, base_url, model in configurations:
        if api_key and base_url and model:
            providers.append(OpenAICompatibleProvider(name, http, base_url, api_key, model))
    if not providers:
        return None
    return LLMOrchestrator(
        providers,
        retries_per_provider=settings.llm_retries_per_provider,
    )


class EvidenceGatedLLMEnricher:
    """Apply LLM values only when an exact quote proves the field."""

    def __init__(self, orchestrator: LLMOrchestrator, max_calls: int) -> None:
        self.orchestrator = orchestrator
        self.max_calls = max(0, max_calls)
        self.stats = LLMEnrichmentStats()

    async def enrich(self, record: CanonicalRecord) -> CanonicalRecord:
        task = self._task(record)
        if task is None:
            return record
        self.stats.candidate_records += 1
        if self.stats.calls >= self.max_calls:
            return record
        self.stats.calls += 1
        try:
            result = await self.orchestrator.extract(
                task.document,
                task.schema,
                task.unresolved_fields,
            )
        except RuntimeError:
            self.stats.failures += 1
            return record

        envelope = result.data
        self.stats.provider_attempts += result.attempts
        self.stats.selected_fragments += result.selected_fragments
        values = envelope.get("values") if isinstance(envelope.get("values"), dict) else {}
        evidence = envelope.get("evidence") if isinstance(envelope.get("evidence"), dict) else {}
        for field in task.allowed_values:
            value = values.get(field)
            quote = evidence.get(field)
            if not isinstance(value, str) or not isinstance(quote, str):
                self.stats.rejected_fields += 1
                continue
            allowed = task.allowed_values.get(field)
            normalized = value.upper() if field == "pricingModel" else value
            if allowed and normalized not in allowed:
                self.stats.rejected_fields += 1
                continue
            quote = quote.strip()
            if not quote or quote not in task.document:
                self.stats.rejected_fields += 1
                continue
            record.content[field] = normalized
            record.evidence = [item for item in record.evidence if item.field != field]
            record.evidence.append(
                FieldEvidence(
                    field=field,
                    method=EvidenceMethod.LLM,
                    source_url=record.source.url,
                    source_pointer=f"llm:{result.provider}",
                    excerpt=quote[:500],
                    confidence=0.85,
                )
            )
            self.stats.applied_fields += 1
        return record

    @staticmethod
    def _task(record: CanonicalRecord) -> EnrichmentTask | None:
        if record.record_type is RecordType.PRODUCT and not record.content.get("pricingModel"):
            document = "\n\n".join(
                part
                for part in (
                    f"Product: {record.content.get('productName', '')}",
                    f"Tagline: {record.content.get('tagline', '')}",
                    f"Description: {record.content.get('description', '')}",
                )
                if part.split(":", 1)[-1].strip()
            )
            pricing_terms = {
                "free",
                "freemium",
                "paid",
                "pricing",
                "subscription",
                "enterprise",
                "plan",
            }
            if not any(term in document.casefold() for term in pricing_terms):
                return None
            return EnrichmentTask(
                document=document,
                schema={
                    "type": "object",
                    "properties": {
                        "pricingModel": {
                            "type": ["string", "null"],
                            "enum": ["FREE", "FREEMIUM", "PAID", "ENTERPRISE", None],
                        }
                    },
                },
                unresolved_fields={"pricingModel", "pricing", "free", "paid", "enterprise"},
                allowed_values={"pricingModel": PRICING_MODELS},
            )
        if record.record_type is RecordType.JOB and record.content.get("role_family") == "Other":
            document = (
                f"Job title: {record.content.get('title', '')}\n\n"
                f"Description: {record.content.get('full_text', '')}"
            )
            return EnrichmentTask(
                document=document,
                schema={
                    "type": "object",
                    "properties": {
                        "role_family": {
                            "type": "string",
                            "enum": sorted(ROLE_FAMILIES),
                        }
                    },
                },
                unresolved_fields={"role_family", "job", "role", "department"},
                allowed_values={"role_family": ROLE_FAMILIES},
            )
        return None
