from __future__ import annotations

import json
from typing import Any

from frontier_ingest.core.http import FetchError, HttpStatusError, ResilientHttpClient
from frontier_ingest.llm.orchestrator import (
    ExtractionProvider,
    PayloadTooLargeError,
    PermanentProviderError,
    RetryableProviderError,
)

SYSTEM_PROMPT = """You extract structured data from supplied source fragments.
Return only a JSON object with two keys: values and evidence.
values must contain only fields explicitly supported by the fragments.
evidence must map each returned field to an exact short quote from the fragments.
Never infer a factual value that the source does not state. Use null when unsupported.
"""


class OpenAICompatibleProvider(ExtractionProvider):
    def __init__(
        self,
        name: str,
        http: ResilientHttpClient,
        base_url: str,
        api_key: str,
        model: str,
        max_input_tokens: int = 16_000,
    ) -> None:
        self.name = name
        self.http = http
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.max_input_tokens = max_input_tokens

    async def extract(self, text: str, schema: dict[str, Any]) -> dict[str, Any]:
        prompt = (
            "Target JSON schema:\n"
            + json.dumps(schema, ensure_ascii=False)
            + "\n\nSource fragments:\n"
            + text
        )
        try:
            result = await self.http.request(
                "POST",
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "temperature": 0,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                },
                detect_blocks=False,
            )
        except FetchError as error:
            if isinstance(error.__cause__, HttpStatusError) and error.__cause__.status == 413:
                raise PayloadTooLargeError(str(error)) from error
            retry_after = None
            if isinstance(error.__cause__, HttpStatusError):
                value = error.__cause__.headers.get("retry-after")
                try:
                    retry_after = float(value) if value else None
                except ValueError:
                    retry_after = None
            raise RetryableProviderError(str(error), retry_after=retry_after) from error
        payload = json.loads(result.body)
        try:
            content = payload["choices"][0]["message"]["content"]
            parsed = json.loads(content)
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as error:
            raise PermanentProviderError("invalid JSON response envelope") from error
        if not isinstance(parsed, dict) or not isinstance(parsed.get("values"), dict):
            raise PermanentProviderError("response lacks values object")
        if not isinstance(parsed.get("evidence"), dict):
            raise PermanentProviderError("response lacks evidence object")
        unsupported = {
            field
            for field, value in parsed["values"].items()
            if value not in (None, "", [], {}) and not parsed["evidence"].get(field)
        }
        if unsupported:
            raise PermanentProviderError(
                f"provider returned values without evidence: {sorted(unsupported)}"
            )
        return parsed
