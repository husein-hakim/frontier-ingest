from __future__ import annotations

import asyncio
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from frontier_ingest.core.token_budget import select_fragments


class RetryableProviderError(RuntimeError):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class PayloadTooLargeError(RuntimeError):
    pass


class PermanentProviderError(RuntimeError):
    pass


class ExtractionProvider(ABC):
    name: str
    max_input_tokens: int

    @abstractmethod
    async def extract(self, text: str, schema: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    data: dict[str, Any]
    provider: str
    attempts: int
    selected_fragments: int


class LLMOrchestrator:
    """Try isolated providers without allowing retries to cause a traffic storm."""

    def __init__(
        self,
        providers: list[ExtractionProvider],
        retries_per_provider: int = 2,
        base_delay: float = 0.5,
    ) -> None:
        if not providers:
            raise ValueError("at least one provider is required")
        self.providers = providers
        self.retries_per_provider = max(0, retries_per_provider)
        self.base_delay = max(0.0, base_delay)

    async def extract(
        self,
        document: str,
        schema: dict[str, Any],
        unresolved_fields: set[str],
    ) -> ExtractionResult:
        attempts = 0
        failures: list[str] = []
        for provider in self.providers:
            budget = max(128, provider.max_input_tokens - 512)
            fragments = select_fragments(document, unresolved_fields, budget)
            payload = "\n\n".join(fragment.text for fragment in fragments)
            if not payload:
                payload = document[: budget * 3]
            for retry in range(self.retries_per_provider + 1):
                attempts += 1
                try:
                    data = await provider.extract(payload, schema)
                    if not isinstance(data, dict):
                        raise PermanentProviderError("provider returned a non-object result")
                    return ExtractionResult(data, provider.name, attempts, len(fragments))
                except PayloadTooLargeError:
                    payload = payload[: max(384, len(payload) // 2)]
                    failures.append(f"{provider.name}:413")
                except RetryableProviderError as error:
                    failures.append(f"{provider.name}:retryable")
                    if retry < self.retries_per_provider:
                        delay = error.retry_after
                        if delay is None:
                            ceiling = self.base_delay * (2**retry)
                            delay = random.uniform(0, ceiling)
                        await asyncio.sleep(delay)
                except PermanentProviderError as error:
                    failures.append(f"{provider.name}:permanent:{error}")
                    break
        raise RuntimeError("all LLM providers failed: " + ", ".join(failures))
