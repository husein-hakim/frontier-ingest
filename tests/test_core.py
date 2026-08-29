from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from frontier_ingest.core.hashing import canonicalize_url, stable_key
from frontier_ingest.core.token_budget import select_fragments
from frontier_ingest.freshness import (
    FreshnessStatus,
    decide_first_seen_freshness,
    decide_freshness,
    parse_timestamp,
)
from frontier_ingest.llm.enrichment import EvidenceGatedLLMEnricher
from frontier_ingest.llm.orchestrator import (
    ExtractionProvider,
    LLMOrchestrator,
    PayloadTooLargeError,
    PermanentProviderError,
    RetryableProviderError,
)
from frontier_ingest.models import (
    CanonicalRecord,
    EvidenceMethod,
    FieldEvidence,
    RecordType,
    SourceRef,
)
from frontier_ingest.resolution import EntityResolver, normalize_entity_name


class HashingTests(unittest.TestCase):
    def test_canonical_url_removes_tracking_and_fragment(self) -> None:
        self.assertEqual(
            canonicalize_url("HTTPS://Example.COM/a/?utm_source=x&b=2#section"),
            "https://example.com/a?b=2",
        )

    def test_stable_key_is_repeatable(self) -> None:
        self.assertEqual(stable_key("arxiv", "1234"), stable_key("arxiv", "1234"))


class TokenBudgetTests(unittest.TestCase):
    def test_relevant_fragment_wins_under_budget(self) -> None:
        text = (
            "Navigation and cookies.\n\nPricing starts free with a paid pro plan.\n\nFooter links."
        )
        result = select_fragments(text, {"pricing", "free", "paid"}, token_budget=20)
        self.assertTrue(result)
        self.assertIn("Pricing", result[0].text)


class ResolutionTests(unittest.TestCase):
    def test_legal_suffix_and_spacing_resolve(self) -> None:
        resolver = EntityResolver(["OpenAI"])
        self.assertEqual(normalize_entity_name("OpenAI, Inc."), "openai")
        self.assertEqual(resolver.resolve("OpenAI, Inc.").canonical_name, "OpenAI")

    def test_resolver_can_abstain(self) -> None:
        resolver = EntityResolver(["OpenAI"])
        self.assertEqual(resolver.resolve("Completely Different Labs").status, "NEW_ENTITY")


class FreshnessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fetched_at = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)

    def test_relative_time_is_anchored_to_fetch(self) -> None:
        published, method, confidence = parse_timestamp("2 hours ago", self.fetched_at)
        decision = decide_freshness(published, self.fetched_at, method, confidence)
        self.assertEqual(decision.status, FreshnessStatus.INFERRED)
        self.assertTrue(decision.accepted)
        self.assertEqual(published, self.fetched_at - timedelta(hours=2))

    def test_missing_date_is_never_accepted(self) -> None:
        decision = decide_freshness(None, self.fetched_at, "missing", 0.0)
        self.assertEqual(decision.status, FreshnessStatus.UNCERTAIN)
        self.assertFalse(decision.accepted)

    def test_old_record_is_stale(self) -> None:
        decision = decide_freshness(
            self.fetched_at - timedelta(hours=25), self.fetched_at, "iso_8601", 1.0
        )
        self.assertEqual(decision.status, FreshnessStatus.STALE)

    def test_first_seen_requires_a_recent_completed_scan_and_ordering(self) -> None:
        accepted = decide_first_seen_freshness(
            fetched_at=self.fetched_at,
            previous_scan_at=self.fetched_at - timedelta(hours=1),
            seen_in_previous_scan=False,
            appeared_after_previous_head=True,
        )
        cold_start = decide_first_seen_freshness(
            fetched_at=self.fetched_at,
            previous_scan_at=None,
            seen_in_previous_scan=False,
            appeared_after_previous_head=True,
        )
        self.assertTrue(accepted.accepted)
        self.assertFalse(cold_start.accepted)


class _FakeProvider(ExtractionProvider):
    max_input_tokens = 1_000

    def __init__(self, name: str, outcomes: list[object]) -> None:
        self.name = name
        self.outcomes = outcomes
        self.input_lengths: list[int] = []

    async def extract(self, text: str, schema: dict[str, object]) -> dict[str, object]:
        self.input_lengths.append(len(text))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome  # type: ignore[return-value]


class OrchestratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_falls_back_after_provider_exhausts_retries(self) -> None:
        first = _FakeProvider(
            "first", [RetryableProviderError("429"), RetryableProviderError("429")]
        )
        second = _FakeProvider("second", [{"company": "OpenAI"}])
        orchestrator = LLMOrchestrator([first, second], retries_per_provider=1, base_delay=0)
        result = await orchestrator.extract("Company\n\nOpenAI", {}, {"company"})
        self.assertEqual(result.provider, "second")
        self.assertEqual(result.attempts, 3)

    async def test_permanent_failure_skips_retries(self) -> None:
        first = _FakeProvider("first", [PermanentProviderError("bad key")])
        second = _FakeProvider("second", [{"ok": True}])
        orchestrator = LLMOrchestrator([first, second], retries_per_provider=3, base_delay=0)
        result = await orchestrator.extract("text", {}, {"ok"})
        self.assertEqual(result.attempts, 2)

    async def test_413_shrinks_payload_before_retry(self) -> None:
        provider = _FakeProvider(
            "primary",
            [PayloadTooLargeError("413"), {"values": {}, "evidence": {}}],
        )
        orchestrator = LLMOrchestrator([provider], retries_per_provider=1, base_delay=0)
        await orchestrator.extract("pricing details " * 500, {}, {"pricing"})
        self.assertLess(provider.input_lengths[1], provider.input_lengths[0])

    async def test_evidence_enricher_accepts_only_exact_quote(self) -> None:
        provider = _FakeProvider(
            "primary",
            [
                {
                    "values": {"pricingModel": "FREE"},
                    "evidence": {"pricingModel": "free plan"},
                }
            ],
        )
        enricher = EvidenceGatedLLMEnricher(
            LLMOrchestrator([provider], retries_per_provider=0),
            max_calls=1,
        )
        now = datetime.now(UTC)
        record = CanonicalRecord(
            record_key="product-1",
            record_type=RecordType.PRODUCT,
            source=SourceRef("fixture", "https://example.com/product", now, "abc"),
            content={
                "productName": "Acme",
                "startupName": "Acme",
                "pricingModel": None,
                "description": "Start with a free plan, then upgrade when your team grows.",
            },
            evidence=[
                FieldEvidence(
                    "productName", EvidenceMethod.API, "https://example.com/product", "name"
                ),
                FieldEvidence(
                    "startupName", EvidenceMethod.API, "https://example.com/product", "name"
                ),
                FieldEvidence(
                    "description", EvidenceMethod.API, "https://example.com/product", "description"
                ),
            ],
        )
        await enricher.enrich(record)
        self.assertEqual(record.content["pricingModel"], "FREE")
        self.assertEqual(enricher.stats.applied_fields, 1)


if __name__ == "__main__":
    unittest.main()
