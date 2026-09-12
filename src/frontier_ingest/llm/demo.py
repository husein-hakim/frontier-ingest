from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from frontier_ingest.llm.orchestrator import (
    ExtractionProvider,
    LLMOrchestrator,
    PayloadTooLargeError,
    RetryableProviderError,
)


class ScriptedProvider(ExtractionProvider):
    """Deterministic provider used only to prove control-flow and metrics."""

    max_input_tokens = 1_000

    def __init__(self, name: str, outcomes: list[object]) -> None:
        self.name = name
        self.outcomes = outcomes
        self.input_lengths: list[int] = []

    async def extract(self, text: str, schema: dict[str, Any]) -> dict[str, Any]:
        self.input_lengths.append(len(text))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return dict(outcome)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class ResilienceDemoReport:
    simulation: bool
    payload_413: dict[str, object]
    rate_limit_fallback: dict[str, object]
    evidence_gate: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


async def run_resilience_demo() -> ResilienceDemoReport:
    schema = {"type": "object"}
    document = "Pricing details. Start with a free plan. " * 500

    shrinking = ScriptedProvider(
        "primary",
        [
            PayloadTooLargeError("413 payload too large"),
            {
                "values": {"pricingModel": "FREE"},
                "evidence": {"pricingModel": "free plan"},
            },
        ],
    )
    shrunk = await LLMOrchestrator([shrinking], retries_per_provider=1, base_delay=0).extract(
        document, schema, {"pricing", "free"}
    )

    limited = ScriptedProvider(
        "primary",
        [
            RetryableProviderError("429", retry_after=0),
            RetryableProviderError("429", retry_after=0),
        ],
    )
    fallback = ScriptedProvider(
        "secondary",
        [
            {
                "values": {"pricingModel": "FREE"},
                "evidence": {"pricingModel": "free plan"},
            }
        ],
    )
    recovered = await LLMOrchestrator(
        [limited, fallback], retries_per_provider=1, base_delay=0
    ).extract(document, schema, {"pricing", "free"})

    quote = str(recovered.data["evidence"]["pricingModel"])
    return ResilienceDemoReport(
        simulation=True,
        payload_413={
            "attempts": shrunk.attempts,
            "initialCharacters": shrinking.input_lengths[0],
            "retryCharacters": shrinking.input_lengths[1],
            "payloadShrank": shrinking.input_lengths[1] < shrinking.input_lengths[0],
            "resultProvider": shrunk.provider,
        },
        rate_limit_fallback={
            "attempts": recovered.attempts,
            "firstTierCalls": len(limited.input_lengths),
            "resultProvider": recovered.provider,
            "recovered": recovered.provider == "secondary",
        },
        evidence_gate={
            "quote": quote,
            "quoteFoundInInput": quote in document,
            "acceptedValue": recovered.data["values"]["pricingModel"],
        },
    )
