from __future__ import annotations

import asyncio
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
from frontier_ingest.resolution import EntityResolver, Resolution
from frontier_ingest.sources.huggingface_papers import HuggingFacePapersAdapter
from frontier_ingest.sources.huggingface_spaces import HuggingFaceSpacesAdapter
from frontier_ingest.sources.jobs import JobSignalsAdapter
from frontier_ingest.sources.news import NewsSignalsAdapter
from frontier_ingest.sources.yc import YCDirectoryAdapter
from frontier_ingest.storage import SQLiteStore, WorkItem, sqlite_path_from_url
from frontier_ingest.storage_postgres import PostgresStore


@dataclass(frozen=True, slots=True)
class CollectionSummary:
    collected: dict[str, int]
    database_path: str
    http_metrics: dict[str, int]
    llm_metrics: dict[str, int]
    source_errors: dict[str, str]
    source_yields: dict[str, int]


@dataclass(frozen=True, slots=True)
class QueueProductionSummary:
    queue_name: str
    discovered: int
    enqueued: int
    queue_counts: dict[str, int]
    http_metrics: dict[str, int]
    source_errors: dict[str, str]
    source_yields: dict[str, int]


@dataclass(frozen=True, slots=True)
class WorkerSummary:
    queue_name: str
    claimed: int
    completed: int
    retried: int
    dead_lettered: int
    queue_counts: dict[str, int]
    http_metrics: dict[str, int]
    llm_metrics: dict[str, int]


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
                    "provider_attempts": 0,
                    "selected_fragments": 0,
                }
            )
            source_errors = dict(getattr(adapter, "source_errors", {}))
            source_yields = {
                key: int(value) for key, value in getattr(adapter, "source_yields", {}).items()
            }
        summary = CollectionSummary(
            collected=await self.store.count_records(),
            database_path=self.database_descriptor,
            http_metrics=metrics,
            llm_metrics=llm_metrics,
            source_errors=source_errors,
            source_yields=source_yields,
        )
        await self.store.record_run_metrics(
            f"collect:{target}",
            {
                "recordCounts": summary.collected,
                "http": summary.http_metrics,
                "llm": summary.llm_metrics,
                "sourceErrors": summary.source_errors,
                "sourceYields": summary.source_yields,
            },
        )
        return summary

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
        resolutions = self._apply_canonical_names(records, resolver)
        for record in records:
            record.validate_evidence_gate(
                optional_fields={"github_url", "github_stars"}
                if record.record_type is RecordType.RESEARCH_PAPER
                else set()
            )
        await self.store.upsert_records(records)
        await self.store.upsert_entity_mappings(resolutions)

    def _seed_canonical_names(self) -> list[str]:
        try:
            payload = json.loads(self.settings.canonical_entities_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        if not isinstance(payload, list):
            return []
        return [str(item).strip() for item in payload if str(item).strip()]

    @staticmethod
    def _apply_canonical_names(
        records: list[CanonicalRecord],
        resolver: EntityResolver,
    ) -> list[Resolution]:
        """Write matched canonical names while retaining the source spelling."""
        resolutions = []
        fields = {
            RecordType.STARTUP: ("entityName", "rawEntityName"),
            RecordType.PRODUCT: ("startupName", "rawStartupName"),
            RecordType.JOB: ("company", "rawCompany"),
        }
        for record in records:
            pair = fields.get(record.record_type)
            if pair is None:
                continue
            canonical_field, raw_field = pair
            raw_name = str(
                record.content.get(raw_field) or record.content.get(canonical_field) or ""
            )
            if not raw_name:
                continue
            if not record.content.get(raw_field):
                record.content[raw_field] = raw_name
                original = next(
                    (item for item in record.evidence if item.field == canonical_field), None
                )
                if original:
                    record.evidence.append(
                        type(original)(
                            field=raw_field,
                            method=original.method,
                            source_url=original.source_url,
                            source_pointer=original.source_pointer,
                            excerpt=original.excerpt,
                            confidence=original.confidence,
                        )
                    )
            if record.record_type is RecordType.STARTUP:
                resolver.add_canonical(raw_name)
            resolution = resolver.resolve(raw_name)
            resolutions.append(resolution)
            if resolution.status == "MATCHED" and resolution.canonical_name:
                record.content[canonical_field] = resolution.canonical_name
        return resolutions

    async def produce(
        self,
        target: str,
        limit: int,
        queue_name: str,
    ) -> QueueProductionSummary:
        """Discover source records and enqueue them for independent workers."""
        await self.initialize()
        discovered = 0
        enqueued = 0
        async with self._http() as http:
            adapter = self._adapter(target, http)
            async for record in adapter.collect(limit):
                discovered += 1
                inserted = await self.store.enqueue(
                    queue_name,
                    f"{target}:{record.record_key}",
                    {"target": target, "record": record.to_dict()},
                )
                enqueued += int(inserted)
            http_metrics = {key: int(value) for key, value in asdict(http.metrics).items()}
            source_errors = dict(getattr(adapter, "source_errors", {}))
            source_yields = {
                key: int(value) for key, value in getattr(adapter, "source_yields", {}).items()
            }
        summary = QueueProductionSummary(
            queue_name,
            discovered,
            enqueued,
            await self.store.queue_counts(queue_name),
            http_metrics,
            source_errors,
            source_yields,
        )
        await self.store.record_run_metrics(
            f"produce:{target}",
            {
                "queueName": queue_name,
                "discovered": discovered,
                "enqueued": enqueued,
                "queueCounts": summary.queue_counts,
                "http": http_metrics,
                "sourceErrors": source_errors,
                "sourceYields": source_yields,
            },
        )
        return summary

    async def work(
        self,
        queue_name: str,
        batch_size: int = 100,
        max_items: int | None = None,
        idle_polls: int = 3,
        poll_interval: float = 1.0,
        max_attempts: int = 4,
    ) -> WorkerSummary:
        """Claim leased records, enrich/resolve/upsert them, and ack only on success."""
        await self.initialize()
        canonical_names = await self.store.canonical_startup_names()
        canonical_names.extend(self._seed_canonical_names())
        resolver = EntityResolver(list(dict.fromkeys(canonical_names)))
        claimed_total = completed = retried = dead = idle = 0
        async with self._http() as http:
            orchestrator = build_orchestrator(self.settings, http)
            llm = (
                EvidenceGatedLLMEnricher(orchestrator, self.settings.llm_max_calls_per_run)
                if orchestrator
                else None
            )
            while max_items is None or claimed_total < max_items:
                request_size = (
                    min(batch_size, max_items - claimed_total) if max_items else batch_size
                )
                items = await self.store.claim(queue_name, max(1, request_size))
                if not items:
                    idle += 1
                    if idle >= max(1, idle_polls):
                        break
                    await asyncio.sleep(max(0.0, poll_interval))
                    continue
                idle = 0
                claimed_total += len(items)
                by_target: dict[str, list[WorkItem]] = {}
                for item in items:
                    by_target.setdefault(str(item.payload.get("target") or ""), []).append(item)
                for target, target_items in by_target.items():
                    try:
                        records = [
                            CanonicalRecord.from_dict(dict(item.payload["record"]))
                            for item in target_items
                        ]
                        github = None
                        if target == "papers" and self.settings.github_token:
                            github = GitHubEnricher(http, self.settings.github_token)
                        await self._process_batch(target, records, github, llm, resolver)
                    except Exception as error:  # noqa: BLE001 - queue boundary must persist any failure
                        for item in target_items:
                            if item.attempts >= max_attempts:
                                await self.store.dead_letter(item, error)
                                dead += 1
                            else:
                                await self.store.retry(
                                    item,
                                    str(error),
                                    min(300, 2 ** max(0, item.attempts - 1)),
                                )
                                retried += 1
                    else:
                        for item in target_items:
                            await self.store.complete(item)
                            completed += 1
            http_metrics = {key: int(value) for key, value in asdict(http.metrics).items()}
            llm_metrics = (
                {key: int(value) for key, value in asdict(llm.stats).items()}
                if llm
                else self._empty_llm_metrics()
            )
        summary = WorkerSummary(
            queue_name,
            claimed_total,
            completed,
            retried,
            dead,
            await self.store.queue_counts(queue_name),
            http_metrics,
            llm_metrics,
        )
        await self.store.record_run_metrics(
            "worker",
            {
                "queueName": queue_name,
                "claimed": claimed_total,
                "completed": completed,
                "retried": retried,
                "deadLettered": dead,
                "queueCounts": summary.queue_counts,
                "http": http_metrics,
                "llm": llm_metrics,
            },
        )
        return summary

    async def reconcile_entities(self) -> int:
        """Second-pass canonicalization makes queue ordering irrelevant."""
        await self.initialize()
        startup_payloads = await self.store.iter_payloads(RecordType.STARTUP)
        canonical_names = [
            str(payload.get("content", {}).get("entityName") or "")
            for payload in startup_payloads
            if isinstance(payload.get("content"), dict)
        ]
        canonical_names.extend(self._seed_canonical_names())
        resolver = EntityResolver([name for name in canonical_names if name])
        updated = 0
        for record_type in (RecordType.STARTUP, RecordType.PRODUCT, RecordType.JOB):
            async for payloads in self.store.iter_payload_batches(
                record_type, self.settings.write_batch_size
            ):
                records = [CanonicalRecord.from_dict(payload) for payload in payloads]
                resolutions = self._apply_canonical_names(records, resolver)
                for record in records:
                    record.validate_evidence_gate()
                await self.store.upsert_records(records)
                await self.store.upsert_entity_mappings(resolutions)
                updated += len(records)
        return updated

    async def refresh_github(self, batch_size: int = 200) -> dict[str, object]:
        if not self.settings.github_token:
            raise RuntimeError("GITHUB_TOKEN is required for a direct GitHub refresh")
        await self.initialize()
        paper_records = (await self.store.count_records()).get(RecordType.RESEARCH_PAPER.value, 0)
        refreshed = 0
        async with self._http() as http:
            enricher = GitHubEnricher(http, self.settings.github_token)
            async for payloads in self.store.iter_payload_batches(
                RecordType.RESEARCH_PAPER, batch_size
            ):
                records = [CanonicalRecord.from_dict(item) for item in payloads]
                refreshed += await enricher.enrich(records)
                await self.store.upsert_records(records)
            metrics = {key: int(value) for key, value in asdict(http.metrics).items()}
        result: dict[str, object] = {
            "paperRecords": paper_records,
            "starMetricsRefreshed": refreshed,
            "unavailableRepositories": paper_records - refreshed,
            "http": metrics,
        }
        await self.store.record_run_metrics("refresh:github", result)
        return result

    @staticmethod
    def _empty_llm_metrics() -> dict[str, int]:
        return {
            "candidate_records": 0,
            "calls": 0,
            "applied_fields": 0,
            "rejected_fields": 0,
            "failures": 0,
            "provider_attempts": 0,
            "selected_fragments": 0,
        }

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
            return HuggingFaceSpacesAdapter(
                http,
                self.raw_store,
                token=self.settings.huggingface_token,
            )
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
            path = destination / f"{record_type.value.lower()}.json"
            await self._write_record_export(path, record_type)
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
        metrics_path = destination / "run_metrics.json"
        metrics_path.write_text(
            json.dumps(await self.store.run_metrics(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        paths.append(metrics_path)
        reports_by_type = {
            report.record_type: report.to_dict() for report in await self.quality_reports()
        }
        manifest_path = destination / "submission_manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schemaVersion": "1.1",
                    "generatedAt": datetime.now(UTC).isoformat(),
                    "recordCounts": await self.store.count_records(),
                    "quality": reports_by_type,
                    "liveGitHubRefreshConfigured": bool(self.settings.github_token),
                    "llmProvidersConfigured": sum(
                        bool(key)
                        for key in (
                            self.settings.primary_llm_api_key,
                            self.settings.secondary_llm_api_key,
                            self.settings.tertiary_llm_api_key,
                        )
                    ),
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        paths.append(manifest_path)
        return paths

    async def _write_record_export(self, path: Path, record_type: RecordType) -> None:
        await asyncio.to_thread(path.write_text, "[", "utf-8")
        first = True
        async for payloads in self.store.iter_payload_batches(record_type, 1_000):
            chunks = []
            for payload in payloads:
                prefix = "\n  " if first else ",\n  "
                chunks.append(prefix + json.dumps(payload, ensure_ascii=False))
                first = False
            await asyncio.to_thread(_append_text, path, "".join(chunks))
        await asyncio.to_thread(_append_text, path, "\n]\n")


def _append_text(path: Path, content: str) -> None:
    with path.open("a", encoding="utf-8") as file:
        file.write(content)
