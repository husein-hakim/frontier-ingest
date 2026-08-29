from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from frontier_ingest.extraction import html_to_text, parse_absolute_timestamp
from frontier_ingest.github import GitHubEnricher
from frontier_ingest.models import (
    CanonicalRecord,
    EvidenceMethod,
    FieldEvidence,
    RecordType,
    SourceRef,
)
from frontier_ingest.quality import submission_gate, validate_payloads
from frontier_ingest.sources.huggingface_papers import HuggingFacePapersAdapter
from frontier_ingest.sources.yc import YCDirectoryAdapter
from frontier_ingest.storage import SQLiteStore


class ExtractionTests(unittest.TestCase):
    def test_html_to_text_removes_navigation_and_scripts(self) -> None:
        value = (
            "<nav>Menu</nav><article><h1>Title</h1><p>Useful body</p><script>x()</script></article>"
        )
        self.assertEqual(html_to_text(value), "Title\nUseful body")

    def test_epoch_is_utc(self) -> None:
        parsed = parse_absolute_timestamp(0)
        self.assertEqual(parsed, datetime(1970, 1, 1, tzinfo=UTC))


class SourceParsingTests(unittest.TestCase):
    def test_yc_embedded_payload_is_structured(self) -> None:
        payload = {
            "props": {
                "companies": [{"slug": "acme", "name": "Acme AI", "one_liner": "AI assistant"}]
            }
        }
        body = f'<div data-page="{json.dumps(payload).replace(chr(34), "&quot;")}"></div>'.encode()
        companies = YCDirectoryAdapter._extract_companies(body)
        self.assertEqual(companies[0]["slug"], "acme")

    def test_hugging_face_summary_github_link_is_normalized(self) -> None:
        url = HuggingFacePapersAdapter._github_url(
            {"summary": "Code: https://github.com/example/project.git."}
        )
        self.assertEqual(url, "https://github.com/example/project")

    def test_github_query_batches_aliases(self) -> None:
        record = _record()
        query, variables, aliases = GitHubEnricher._query([(("openai", "example"), [record])])
        self.assertIn("repo0: repository", query)
        self.assertEqual(variables["owner0"], "openai")
        self.assertEqual(aliases["repo0"], ("openai", "example"))


class StorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_record_upsert_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteStore(Path(directory) / "test.db")
            await store.initialize()
            await store.upsert_record(_record())
            await store.upsert_record(_record())
            self.assertEqual(await store.count_records(), {"STARTUP": 1})

    async def test_queue_deduplicates_and_leases(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteStore(Path(directory) / "test.db")
            await store.initialize()
            self.assertTrue(await store.enqueue("fetch", "url:1", {"url": "https://x"}))
            self.assertFalse(await store.enqueue("fetch", "url:1", {"url": "https://x"}))
            claimed = await store.claim("fetch", 10)
            self.assertEqual(len(claimed), 1)
            await store.complete(claimed[0])
            self.assertEqual(await store.claim("fetch", 10), [])

    async def test_bulk_upsert_and_canonical_name_projection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteStore(Path(directory) / "test.db")
            await store.initialize()
            first = _record()
            second = _record()
            second.record_key = "record-2"
            second.content = {"entityName": "Second AI"}
            await store.upsert_records([first, second])
            self.assertEqual(await store.count_records(), {"STARTUP": 2})
            self.assertEqual(
                sorted(await store.canonical_startup_names()),
                ["Acme AI", "Second AI"],
            )

    async def test_expired_signal_pruning_is_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteStore(Path(directory) / "test.db")
            await store.initialize()
            old = _record()
            old.record_type = RecordType.JOB
            old.content = {
                "company": "Acme AI",
                "title": "AI Engineer",
                "date": (datetime.now(UTC) - timedelta(hours=25)).isoformat(),
            }
            await store.upsert_record(old)
            deleted = await store.delete_expired_signals(
                RecordType.JOB, datetime.now(UTC) - timedelta(hours=24)
            )
            self.assertEqual(deleted, 1)
            self.assertEqual(await store.count_records(), {})


class QualityTests(unittest.TestCase):
    def test_missing_required_field_blocks_submission(self) -> None:
        payload = _record().to_dict()
        report = validate_payloads(RecordType.STARTUP, [payload])
        self.assertEqual(submission_gate([report], strict_counts=False), [])
        del payload["content"]["entityName"]
        report = validate_payloads(RecordType.STARTUP, [payload])
        self.assertTrue(submission_gate([report], strict_counts=False))


def _record() -> CanonicalRecord:
    now = datetime.now(UTC)
    return CanonicalRecord(
        record_key="record-1",
        record_type=RecordType.STARTUP,
        source=SourceRef("fixture", "https://example.com/acme", now, "abc"),
        content={"entityName": "Acme AI"},
        evidence=[
            FieldEvidence(
                "entityName",
                EvidenceMethod.API,
                "https://example.com/acme",
                "fixture.name",
            )
        ],
    )


if __name__ == "__main__":
    unittest.main()
