from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from frontier_ingest.models import CanonicalRecord


class SourceAdapter(ABC):
    name: str

    @abstractmethod
    async def collect(self, limit: int) -> AsyncIterator[CanonicalRecord]:
        """Yield validated records without retaining the complete dataset in memory."""
        if False:
            yield  # pragma: no cover
