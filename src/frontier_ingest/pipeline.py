from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from frontier_ingest.config import Settings
from frontier_ingest.core.http import ResilientHttpClient
from frontier_ingest.core.raw_store import ContentAddressedRawStore
from frontier_ingest.github import GitHubEnricher
from frontier_ingest.llm.enrichment import EvidenceGatedLLMEnricher, build_orchestrator
from frontier_ingest.models import CanonicalRecord, RecordType
from frontier_ingest.quality import QualityReport, validate_payloads
from frontier_ingest.resolution import EntityResolver
from frontier_ingest.sources.huggingface_papers import HuggingFacePapersAdapter
from frontier_ingest.sources.jobs import JobSignalsAdapter
from frontier_ingest.sources.news import NewsSignalsAdapter
from frontier_ingest.sources.yc import YCDirectoryAdapter
from frontier_ingest.storage import SQLiteStore, sqlite_path_from_url
from frontier_ingest.storage_postgres import PostgresStore


@dataclass(frozen=True, slots=True)
class CollectionSummary:
    collected: dict[str, int]
    database_path: str
    http_metrics: dict[str, int]
    llm_metrics: dict[str, int]
    source_errors: dict[str, str]


class IngestionPipeline:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        if settings.database_url.startswith(("postgresql://", "postgres://")):
            self.store = PostgresStore(settings.database_url)
            self.database_descriptor = settings.database_url.split("@")[-1]
        else:
            self.store = SQLiteStore(sqlite_path_from_url(settings.database_url))
            self.database_descriptor = str(self.store.path.resolve())
        self.raw_store = ContentAddressedRawStore(settings.raw_store_path)

    async def initialize(self) -> None:
        await self.store.initialize()

    def _http(self) -> ResilientHttpClient:
        return ResilientHttpClient(
            self.settings.user_agent,
            timeout_seconds=self.settings.request_timeout_seconds,
            default_concurrency=self.settings.default_host_concurrency,
            max_attempts=self.settings.max_attempts,
            host_policies={
                "export.arxiv.org": (1, 3.0),
                "www.ycombinator.com": (2, 0.5),
                "huggingface.co": (4, 0.1),
                "api.github.com": (2, 0.1),
                "remotive.com": (1, 1.0),
            },
        )

    async def collect(self, target: str, limit: int) -> CollectionSummary:
        await self.initialize()
        signal_type = {
            "jobs": RecordType.JOB,
            "news": RecordType.NEWS,
        }.get(target)
        if signal_type:
            await self.store.delete_expired_signals(
                signal_type,
                datetime.now(UTC) - timedelta(hours=24),
            )
        canonical_names = await self.store.canonical_startup_names()
        canonical_names.extend(self._seed_canonical_names())
        resolver = EntityResolver(list(dict.fromkeys(canonical_names)))
        async with self._http() as http:
            adapter = self._adapter(target, http)
            github = (
                GitHubEnricher(http, self.settings.github_token)
                if target == "papers" and self.settings.github_token
                else None
            )
            orchestrator = build_orchestrator(self.settings, http)
            llm = (
                EvidenceGatedLLMEnricher(
                    orchestrator,
                    max_calls=self.settings.llm_max_calls_per_run,
                )
                if orchestrator
                else None
            )
            records: list[CanonicalRecord] = []
            async for record in adapter.collect(limit):
                records.append(record)
                if len(records) >= max(1, self.settings.write_batch_size):
                    await self._process_batch(target, records, github, llm, resolver)
                    records = []
            if records:
                await self._process_batch(target, records, github, llm, resolver)
            metrics = {
                "requests": http.metrics.requests,
                "retries": http.metrics.retries,
                "failures": http.metrics.failures,
                "blocked": http.metrics.blocked,
                "bytes_received": http.metrics.bytes_received,
            }
            llm_metrics = (
                {key: int(value) for key, value in asdict(llm.stats).items()}
                if llm
                else {
                    "candidate_records": 0,
                    "calls": 0,
                    "applied_fields": 0,
                    "rejected_fields": 0,
                    "failures": 0,
                }
            )
            source_errors = dict(getattr(adapter, "source_errors", {}))
        return CollectionSummary(
            collected=await self.store.count_records(),
            database_path=self.database_descriptor,
            http_metrics=metrics,
            llm_metrics=llm_metrics,
            source_errors=source_errors,
        )

    async def _process_batch(
        self,
        target: str,
        records: list[CanonicalRecord],
        github: GitHubEnricher | None,
        llm: EvidenceGatedLLMEnricher | None,
        resolver: EntityResolver,
    ) -> None:
        if github:
            await github.enrich(records)
        if llm:
            for record in records:
                await llm.enrich(record)
        if target == "startups":
            for record in records:
                name = record.content.get("entityName")
                if name:
                    resolver.add_canonical(str(name))
        for record in records:
            record.validate_evidence_gate(
                optional_fields={"github_url", "github_stars"}
                if record.record_type is RecordType.RESEARCH_PAPER
                else set()
            )
        await self.store.upsert_records(records)
        await self._update_entity_mappings(records, resolver)

    def _seed_canonical_names(self) -> list[str]:
        try:
            payload = json.loads(self.settings.canonical_entities_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        if not isinstance(payload, list):
            return []
        return [str(item).strip() for item in payload if str(item).strip()]

    async def _update_entity_mappings(
        self,
        records: list[CanonicalRecord],
        resolver: EntityResolver,
    ) -> None:
        resolutions = []
        for record in records:
            raw_name = None
            if record.record_type is RecordType.STARTUP:
                raw_name = record.content.get("entityName")
            elif record.record_type is RecordType.PRODUCT:
                raw_name = record.content.get("startupName")
            elif record.record_type is RecordType.JOB:
                raw_name = record.content.get("company")
            if raw_name:
                resolutions.append(resolver.resolve(str(raw_name)))
        await self.store.upsert_entity_mappings(resolutions)

    def _adapter(self, target: str, http: ResilientHttpClient):
        if target == "papers":
            return HuggingFacePapersAdapter(
                http,
                self.raw_store,
                require_github=True,
                require_stars=not bool(self.settings.github_token),
            )
        if target == "startups":
            return YCDirectoryAdapter(http, self.raw_store, RecordType.STARTUP)
        if target == "products":
            return YCDirectoryAdapter(http, self.raw_store, RecordType.PRODUCT)
        if target == "jobs":
            return JobSignalsAdapter(http, self.raw_store)
        if target == "news":
            return NewsSignalsAdapter(http, self.raw_store)
        raise ValueError(f"unknown target: {target}")

    async def quality_reports(self) -> list[QualityReport]:
        await self.initialize()
        reports: list[QualityReport] = []
        for record_type in RecordType:
            payloads = await self.store.iter_payloads(record_type)
            reports.append(validate_payloads(record_type, payloads))
        return reports

    async def export_json(self, output_dir: str | Path) -> list[Path]:
        await self.initialize()
        destination = Path(output_dir)
        destination.mkdir(parents=True, exist_ok=True)
        paths: list[Path] = []
        for record_type in RecordType:
            payloads = await self.store.iter_payloads(record_type)
            path = destination / f"{record_type.value.lower()}.json"
            path.write_text(json.dumps(payloads, indent=2, ensure_ascii=False), encoding="utf-8")
            paths.append(path)
        reports = [report.to_dict() for report in await self.quality_reports()]
        quality_path = destination / "quality-report.json"
        quality_path.write_text(json.dumps(reports, indent=2), encoding="utf-8")
        paths.append(quality_path)
        mappings_path = destination / "entity_mapping_log.json"
        mappings_path.write_text(
            json.dumps(await self.store.entity_mappings(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        paths.append(mappings_path)
        return paths
