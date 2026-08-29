from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from frontier_ingest.models import CanonicalRecord, RecordType
from frontier_ingest.resolution import Resolution, normalize_entity_name
from frontier_ingest.storage import WorkItem

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    record_key TEXT PRIMARY KEY,
    record_type TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_name TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS records_type_idx ON records(record_type);
CREATE INDEX IF NOT EXISTS records_source_idx ON records(source_url);

CREATE TABLE IF NOT EXISTS work_items (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(queue_name, dedupe_key)
);
CREATE INDEX IF NOT EXISTS work_claim_idx
ON work_items(queue_name, status, available_at, lease_expires_at);

CREATE TABLE IF NOT EXISTS checkpoints (
    source_name TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    cursor_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(source_name, partition_key)
);

CREATE TABLE IF NOT EXISTS entity_mappings (
    raw_name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    canonical_name TEXT,
    method TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL,
    resolver_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(raw_name, resolver_version)
);

CREATE TABLE IF NOT EXISTS dead_letters (
    id BIGSERIAL PRIMARY KEY,
    stage TEXT NOT NULL,
    record_key TEXT,
    source_url TEXT,
    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,
    payload_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class PostgresStore:
    def __init__(self, database_url: str, min_size: int = 1, max_size: int = 10) -> None:
        self.database_url = database_url
        self.min_size = min_size
        self.max_size = max_size
        self.pool: Any = None

    async def initialize(self) -> None:
        if self.pool is None:
            try:
                import asyncpg
            except ImportError as error:
                raise RuntimeError("install frontier-ingest[postgres] for PostgreSQL") from error
            self.pool = await asyncpg.create_pool(
                self.database_url, min_size=self.min_size, max_size=self.max_size
            )
        async with self.pool.acquire() as connection:
            await connection.execute(POSTGRES_SCHEMA)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def upsert_record(self, record: CanonicalRecord) -> None:
        await self.upsert_records([record])

    async def upsert_records(self, records: list[CanonicalRecord]) -> None:
        if not records:
            return
        rows = [
            (
                record.record_key,
                record.record_type.value,
                record.source.url,
                record.source.name,
                record.source.content_hash,
                json.dumps(record.to_dict(), ensure_ascii=False),
                record.collected_at,
            )
            for record in records
        ]
        async with self.pool.acquire() as connection, connection.transaction():
            await connection.executemany(
                """
                INSERT INTO records(record_key, record_type, source_url, source_name,
                    content_hash, payload_json, collected_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                ON CONFLICT(record_key) DO UPDATE SET
                    source_url=EXCLUDED.source_url,
                    source_name=EXCLUDED.source_name,
                    content_hash=EXCLUDED.content_hash,
                    payload_json=EXCLUDED.payload_json,
                    collected_at=EXCLUDED.collected_at,
                    updated_at=NOW()
                """,
                rows,
            )

    async def enqueue(self, queue_name: str, dedupe_key: str, payload: dict[str, object]) -> bool:
        async with self.pool.acquire() as connection:
            result = await connection.fetchval(
                """
                INSERT INTO work_items(queue_name, dedupe_key, payload_json)
                VALUES ($1, $2, $3::jsonb)
                ON CONFLICT(queue_name, dedupe_key) DO NOTHING
                RETURNING id
                """,
                queue_name,
                dedupe_key,
                json.dumps(payload),
            )
        return result is not None

    async def claim(self, queue_name: str, limit: int, lease_seconds: int = 120) -> list[WorkItem]:
        owner = str(uuid.uuid4())
        async with self.pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                WITH candidates AS (
                    SELECT id FROM work_items
                    WHERE queue_name=$1
                      AND available_at<=NOW()
                      AND (status='PENDING' OR
                           (status='LEASED' AND lease_expires_at<NOW()))
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT $2
                )
                UPDATE work_items AS work
                SET status='LEASED', lease_owner=$3,
                    lease_expires_at=NOW() + make_interval(secs => $4),
                    attempts=attempts+1, updated_at=NOW()
                FROM candidates
                WHERE work.id=candidates.id
                RETURNING work.*
                """,
                queue_name,
                max(1, limit),
                owner,
                lease_seconds,
            )
        return [
            WorkItem(
                id=row["id"],
                queue_name=row["queue_name"],
                dedupe_key=row["dedupe_key"],
                payload=dict(row["payload_json"]),
                attempts=row["attempts"],
                lease_owner=owner,
            )
            for row in rows
        ]

    async def complete(self, item: WorkItem) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """UPDATE work_items SET status='DONE', lease_owner=NULL,
                lease_expires_at=NULL, updated_at=NOW()
                WHERE id=$1 AND lease_owner=$2""",
                item.id,
                item.lease_owner,
            )

    async def retry(self, item: WorkItem, error: str, delay_seconds: int) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """UPDATE work_items SET status='PENDING', last_error=$1,
                available_at=NOW() + make_interval(secs => $2), lease_owner=NULL,
                lease_expires_at=NULL, updated_at=NOW()
                WHERE id=$3 AND lease_owner=$4""",
                error,
                max(0, delay_seconds),
                item.id,
                item.lease_owner,
            )

    async def set_checkpoint(
        self, source_name: str, partition_key: str, cursor: dict[str, object]
    ) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO checkpoints(source_name, partition_key, cursor_json)
                VALUES ($1, $2, $3::jsonb)
                ON CONFLICT(source_name, partition_key) DO UPDATE SET
                cursor_json=EXCLUDED.cursor_json, updated_at=NOW()
                """,
                source_name,
                partition_key,
                json.dumps(cursor),
            )

    async def count_records(self) -> dict[str, int]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT record_type, COUNT(*) AS count FROM records GROUP BY record_type"
            )
        return {row["record_type"]: row["count"] for row in rows}

    async def delete_expired_signals(self, record_type: RecordType, cutoff: datetime) -> int:
        if record_type not in {RecordType.JOB, RecordType.NEWS}:
            return 0
        date_field = "date" if record_type is RecordType.JOB else "published_at"
        async with self.pool.acquire() as connection:
            result = await connection.execute(
                f"""DELETE FROM records
                WHERE record_type=$1
                  AND (payload_json #>> '{{content,{date_field}}}')::timestamptz < $2""",
                record_type.value,
                cutoff,
            )
        return int(result.rsplit(" ", 1)[-1])

    async def canonical_startup_names(self) -> list[str]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """SELECT payload_json #>> '{content,entityName}' AS name
                FROM records WHERE record_type='STARTUP'"""
            )
        return [str(row["name"]) for row in rows if row["name"]]

    async def iter_payloads(self, record_type: RecordType) -> list[dict[str, object]]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT payload_json FROM records WHERE record_type=$1 ORDER BY record_key",
                record_type.value,
            )
        return [dict(row["payload_json"]) for row in rows]

    async def upsert_entity_mapping(
        self, resolution: Resolution, resolver_version: str = "1.0"
    ) -> None:
        await self.upsert_entity_mappings([resolution], resolver_version)

    async def upsert_entity_mappings(
        self, resolutions: list[Resolution], resolver_version: str = "1.0"
    ) -> None:
        if not resolutions:
            return
        rows = [
            (
                resolution.raw_name,
                normalize_entity_name(resolution.raw_name),
                resolution.canonical_name,
                resolution.method,
                resolution.confidence,
                resolution.status,
                resolver_version,
            )
            for resolution in resolutions
        ]
        async with self.pool.acquire() as connection, connection.transaction():
            await connection.executemany(
                """
                INSERT INTO entity_mappings(raw_name, normalized_name, canonical_name,
                    method, confidence, status, resolver_version)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT(raw_name, resolver_version) DO UPDATE SET
                    normalized_name=EXCLUDED.normalized_name,
                    canonical_name=EXCLUDED.canonical_name,
                    method=EXCLUDED.method,
                    confidence=EXCLUDED.confidence,
                    status=EXCLUDED.status,
                    created_at=NOW()
                """,
                rows,
            )

    async def entity_mappings(self) -> list[dict[str, object]]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch("SELECT * FROM entity_mappings ORDER BY raw_name")
        return [dict(row) for row in rows]
