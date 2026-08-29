from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterable
from pathlib import Path

from frontier_ingest.models import CanonicalRecord


async def write_jsonl(records: AsyncIterable[CanonicalRecord], path: str | Path) -> int:
    target = Path(path)
    await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
    count = 0
    buffer: list[str] = []
    first_write = True
    async for record in records:
        buffer.append(json.dumps(record.to_dict(), ensure_ascii=False) + "\n")
        count += 1
        if len(buffer) >= 500:
            await asyncio.to_thread(_write_lines, target, buffer, first_write)
            first_write = False
            buffer = []
    if buffer or first_write:
        await asyncio.to_thread(_write_lines, target, buffer, first_write)
    return count


def _write_lines(target: Path, lines: list[str], truncate: bool) -> None:
    with target.open("w" if truncate else "a", encoding="utf-8") as file:
        file.writelines(lines)
