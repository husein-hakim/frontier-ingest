from __future__ import annotations

import asyncio
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from frontier_ingest.models import CanonicalRecord, RecordType
from frontier_ingest.resolution import Resolution

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS records (
    record_key TEXT PRIMARY KEY,
    record_type TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_name TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS records_type_idx ON records(record_type);
CREATE INDEX IF NOT EXISTS records_source_idx ON records(source_url);

CREATE TABLE IF NOT EXISTS work_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(queue_name, dedupe_key)
);
CREATE INDEX IF NOT EXISTS work_claim_idx
ON work_items(queue_name, status, available_at, lease_expires_at);

CREATE TABLE IF NOT EXISTS checkpoints (
    source_name TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    cursor_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(source_name, partition_key)
);

CREATE TABLE IF NOT EXISTS entity_mappings (
    raw_name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    canonical_name TEXT,
    method TEXT NOT NULL,
    confidence REAL NOT NULL,
    status TEXT NOT NULL,
    resolver_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(raw_name, resolver_version)
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage TEXT NOT NULL,
    record_key TEXT,
    source_url TEXT,
    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,
    payload_json TEXT,
    created_at TEXT NOT NULL
);
"""


@dataclass(frozen=True, slots=True)
class WorkItem:
    id: int
    queue_name: str
    dedupe_key: str
    payload: dict[str, object]
    attempts: int
    lease_owner: str


class SQLiteStore:
    """Local durable store with the same idempotency/lease semantics as production."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize)

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(SCHEMA)

    async def upsert_record(self, record: CanonicalRecord) -> None:
        await self.upsert_records([record])

    async def upsert_records(self, records: list[CanonicalRecord]) -> None:
        if records:
            await asyncio.to_thread(self._upsert_records, records)

    def _upsert_records(self, records: list[CanonicalRecord]) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO records(record_key, record_type, source_url, source_name,
                    content_hash, payload_json, collected_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_key) DO UPDATE SET
                    source_url=excluded.source_url,
                    source_name=excluded.source_name,
                    content_hash=excluded.content_hash,
                    payload_json=excluded.payload_json,
                    collected_at=excluded.collected_at,
                    updated_at=excluded.updated_at
                """,
                [
                    (
                        record.record_key,
                        record.record_type.value,
                        record.source.url,
                        record.source.name,
                        record.source.content_hash,
                        json.dumps(record.to_dict(), ensure_ascii=False),
                        record.collected_at.isoformat(),
                        now,
                    )
                    for record in records
                ],
            )

    async def enqueue(self, queue_name: str, dedupe_key: str, payload: dict[str, object]) -> bool:
        return await asyncio.to_thread(self._enqueue, queue_name, dedupe_key, payload)

    def _enqueue(self, queue_name: str, dedupe_key: str, payload: dict[str, object]) -> bool:
        now = datetime.now(UTC).isoformat()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO work_items(
                    queue_name, dedupe_key, payload_json, available_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (queue_name, dedupe_key, json.dumps(payload), now, now, now),
            )
            return cursor.rowcount == 1

    async def claim(self, queue_name: str, limit: int, lease_seconds: int = 120) -> list[WorkItem]:
        return await asyncio.to_thread(self._claim, queue_name, limit, lease_seconds)

    def _claim(self, queue_name: str, limit: int, lease_seconds: int) -> list[WorkItem]:
        now = datetime.now(UTC)
        owner = str(uuid.uuid4())
        expires = now + timedelta(seconds=lease_seconds)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT id FROM work_items
                WHERE queue_name=?
                  AND available_at<=?
                  AND (status='PENDING' OR (status='LEASED' AND lease_expires_at<?))
                ORDER BY id LIMIT ?
                """,
                (queue_name, now.isoformat(), now.isoformat(), max(1, limit)),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if not ids:
                return []
            placeholders = ",".join("?" for _ in ids)
            connection.execute(
                f"""UPDATE work_items SET status='LEASED', lease_owner=?, lease_expires_at=?,
                    attempts=attempts+1, updated_at=? WHERE id IN ({placeholders})""",
                (owner, expires.isoformat(), now.isoformat(), *ids),
            )
            claimed = connection.execute(
                f"SELECT * FROM work_items WHERE id IN ({placeholders}) ORDER BY id", ids
            ).fetchall()
        return [
            WorkItem(
                id=row["id"],
                queue_name=row["queue_name"],
                dedupe_key=row["dedupe_key"],
                payload=json.loads(row["payload_json"]),
                attempts=row["attempts"],
                lease_owner=owner,
            )
            for row in claimed
        ]

    async def complete(self, item: WorkItem) -> None:
        await asyncio.to_thread(self._finish, item, "DONE", None, None)

    async def retry(self, item: WorkItem, error: str, delay_seconds: int) -> None:
        available = datetime.now(UTC) + timedelta(seconds=max(0, delay_seconds))
        await asyncio.to_thread(self._finish, item, "PENDING", error, available)

    def _finish(
        self,
        item: WorkItem,
        status: str,
        error: str | None,
        available: datetime | None,
    ) -> None:
        now = datetime.now(UTC)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE work_items SET status=?, last_error=?, available_at=?, lease_owner=NULL,
                    lease_expires_at=NULL, updated_at=?
                WHERE id=? AND lease_owner=?
                """,
                (
                    status,
                    error,
                    (available or now).isoformat(),
                    now.isoformat(),
                    item.id,
                    item.lease_owner,
                ),
            )

    async def set_checkpoint(
        self, source_name: str, partition_key: str, cursor: dict[str, object]
    ) -> None:
        await asyncio.to_thread(self._set_checkpoint, source_name, partition_key, cursor)

    def _set_checkpoint(
        self, source_name: str, partition_key: str, cursor: dict[str, object]
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO checkpoints(source_name, partition_key, cursor_json, updated_at)
                VALUES (?, ?, ?, ?) ON CONFLICT(source_name, partition_key) DO UPDATE SET
                cursor_json=excluded.cursor_json, updated_at=excluded.updated_at""",
                (source_name, partition_key, json.dumps(cursor), now),
            )

    async def count_records(self) -> dict[str, int]:
        return await asyncio.to_thread(self._count_records)

    def _count_records(self) -> dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT record_type, COUNT(*) AS count FROM records GROUP BY record_type"
            ).fetchall()
        return {row["record_type"]: row["count"] for row in rows}

    async def delete_expired_signals(self, record_type: RecordType, cutoff: datetime) -> int:
        if record_type not in {RecordType.JOB, RecordType.NEWS}:
            return 0
        return await asyncio.to_thread(self._delete_expired_signals, record_type, cutoff)

    def _delete_expired_signals(self, record_type: RecordType, cutoff: datetime) -> int:
        date_field = "date" if record_type is RecordType.JOB else "published_at"
        with self._connect() as connection:
            cursor = connection.execute(
                f"""DELETE FROM records
                WHERE record_type=?
                  AND json_extract(payload_json, '$.content.{date_field}') < ?""",
                (record_type.value, cutoff.astimezone(UTC).isoformat()),
            )
            return cursor.rowcount

    async def canonical_startup_names(self) -> list[str]:
        return await asyncio.to_thread(self._canonical_startup_names)

    def _canonical_startup_names(self) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT json_extract(payload_json, '$.content.entityName') AS name
                FROM records WHERE record_type='STARTUP'"""
            ).fetchall()
        return [str(row["name"]) for row in rows if row["name"]]

    async def upsert_entity_mapping(
        self, resolution: Resolution, resolver_version: str = "1.0"
    ) -> None:
        await self.upsert_entity_mappings([resolution], resolver_version)

    async def upsert_entity_mappings(
        self, resolutions: list[Resolution], resolver_version: str = "1.0"
    ) -> None:
        if resolutions:
            await asyncio.to_thread(self._upsert_entity_mappings, resolutions, resolver_version)

    def _upsert_entity_mappings(self, resolutions: list[Resolution], resolver_version: str) -> None:
        from frontier_ingest.resolution import normalize_entity_name

        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO entity_mappings(raw_name, normalized_name, canonical_name,
                    method, confidence, status, resolver_version, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(raw_name, resolver_version) DO UPDATE SET
                    normalized_name=excluded.normalized_name,
                    canonical_name=excluded.canonical_name,
                    method=excluded.method,
                    confidence=excluded.confidence,
                    status=excluded.status,
                    created_at=excluded.created_at
                """,
                [
                    (
                        resolution.raw_name,
                        normalize_entity_name(resolution.raw_name),
                        resolution.canonical_name,
                        resolution.method,
                        resolution.confidence,
                        resolution.status,
                        resolver_version,
                        datetime.now(UTC).isoformat(),
                    )
                    for resolution in resolutions
                ],
            )

    async def entity_mappings(self) -> list[dict[str, object]]:
        return await asyncio.to_thread(self._entity_mappings)

    def _entity_mappings(self) -> list[dict[str, object]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM entity_mappings ORDER BY raw_name").fetchall()
        return [dict(row) for row in rows]

    async def iter_payloads(self, record_type: RecordType) -> list[dict[str, object]]:
        return await asyncio.to_thread(self._iter_payloads, record_type)

    def _iter_payloads(self, record_type: RecordType) -> list[dict[str, object]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT payload_json FROM records WHERE record_type=? ORDER BY record_key",
                (record_type.value,),
            ).fetchall()
        return [json.loads(row["payload_json"]) for row in rows]


def sqlite_path_from_url(database_url: str) -> Path:
    prefix = "sqlite:///"
    if not database_url.startswith(prefix):
        raise ValueError(
            "local runtime currently requires sqlite:///; production schema targets PostgreSQL"
        )
    return Path(database_url[len(prefix) :])
