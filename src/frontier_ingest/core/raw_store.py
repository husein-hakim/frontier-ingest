from __future__ import annotations

import asyncio
from pathlib import Path

from frontier_ingest.core.hashing import sha256_bytes


class ContentAddressedRawStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    async def put(self, content: bytes, suffix: str = ".bin") -> tuple[str, Path]:
        digest = sha256_bytes(content)
        target = self.root / digest[:2] / f"{digest}{suffix}"
        await asyncio.to_thread(self._write_once, target, content)
        return digest, target

    @staticmethod
    def _write_once(target: Path, content: bytes) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with target.open("xb") as file:
                file.write(content)
        except FileExistsError:
            pass
