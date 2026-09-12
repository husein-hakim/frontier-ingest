from __future__ import annotations

import asyncio
import email.utils
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Self
from urllib.parse import urlsplit

import aiohttp

RETRYABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504}
BLOCK_MARKERS = (
    "cf-chl-",
    "cloudflare ray id",
    "/cdn-cgi/challenge-platform",
    "g-recaptcha-response",
    "datadome",
    "verify you are human",
    "<title>just a moment...</title>",
)


class FetchError(RuntimeError):
    pass


class HttpStatusError(FetchError):
    def __init__(self, method: str, url: str, status: int, headers: dict[str, str]) -> None:
        super().__init__(f"{method} {url} returned {status}")
        self.status = status
        self.headers = headers


class BlockedSourceError(FetchError):
    pass


@dataclass(frozen=True, slots=True)
class FetchResult:
    url: str
    status: int
    body: bytes
    content_type: str
    fetched_at: datetime
    headers: dict[str, str]
    attempts: int
    elapsed_ms: int


@dataclass(slots=True)
class HttpMetrics:
    requests: int = 0
    retries: int = 0
    failures: int = 0
    blocked: int = 0
    bytes_received: int = 0


class HostController:
    """Per-host concurrency plus a polite minimum request interval."""

    def __init__(self, concurrency: int, min_interval_seconds: float = 0.0) -> None:
        self.semaphore = asyncio.Semaphore(max(1, concurrency))
        self.min_interval_seconds = max(0.0, min_interval_seconds)
        self._last_request = 0.0
        self._lock = asyncio.Lock()

    async def wait_turn(self) -> None:
        async with self._lock:
            remaining = self.min_interval_seconds - (time.monotonic() - self._last_request)
            if remaining > 0:
                await asyncio.sleep(remaining)
            self._last_request = time.monotonic()


class ResilientHttpClient:
    def __init__(
        self,
        user_agent: str,
        timeout_seconds: float = 45.0,
        default_concurrency: int = 4,
        max_attempts: int = 4,
        host_policies: dict[str, tuple[int, float]] | None = None,
    ) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.default_concurrency = default_concurrency
        self.max_attempts = max(1, max_attempts)
        self.host_policies = host_policies or {}
        self.metrics = HttpMetrics()
        self._session: aiohttp.ClientSession | None = None
        self._controllers: dict[str, HostController] = {}

    async def __aenter__(self) -> Self:
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"},
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._session:
            await self._session.close()
        self._session = None

    def _controller(self, url: str) -> HostController:
        host = (urlsplit(url).hostname or "unknown").lower()
        if host not in self._controllers:
            concurrency, interval = self.host_policies.get(host, (self.default_concurrency, 0.0))
            self._controllers[host] = HostController(concurrency, interval)
        return self._controllers[host]

    async def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        detect_blocks: bool = True,
    ) -> FetchResult:
        return await self.request(
            "GET", url, params=params, headers=headers, detect_blocks=detect_blocks
        )

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json: Any | None = None,
        detect_blocks: bool = True,
        max_attempts: int | None = None,
    ) -> FetchResult:
        if self._session is None:
            raise RuntimeError("ResilientHttpClient must be used as an async context manager")
        controller = self._controller(url)
        attempt_limit = max(1, max_attempts or self.max_attempts)
        started = time.monotonic()
        last_error: Exception | None = None
        async with controller.semaphore:
            for attempt in range(1, attempt_limit + 1):
                await controller.wait_turn()
                self.metrics.requests += 1
                try:
                    async with self._session.request(
                        method, url, params=params, headers=headers, json=json
                    ) as response:
                        body = await response.read()
                        self.metrics.bytes_received += len(body)
                        if response.status in RETRYABLE_STATUSES and attempt < attempt_limit:
                            self.metrics.retries += 1
                            await asyncio.sleep(self._retry_delay(response.headers, attempt))
                            continue
                        if response.status >= 400:
                            raise HttpStatusError(
                                method,
                                str(response.url),
                                response.status,
                                {key.lower(): value for key, value in response.headers.items()},
                            )
                        if detect_blocks and self._looks_blocked(body, response.headers):
                            self.metrics.blocked += 1
                            raise BlockedSourceError(f"block page detected at {response.url}")
                        return FetchResult(
                            url=str(response.url),
                            status=response.status,
                            body=body,
                            content_type=response.headers.get("Content-Type", ""),
                            fetched_at=datetime.now(UTC),
                            headers={key.lower(): value for key, value in response.headers.items()},
                            attempts=attempt,
                            elapsed_ms=int((time.monotonic() - started) * 1000),
                        )
                except BlockedSourceError:
                    raise
                except HttpStatusError as error:
                    last_error = error
                    if error.status not in RETRYABLE_STATUSES or attempt >= attempt_limit:
                        break
                    self.metrics.retries += 1
                    await asyncio.sleep(random.uniform(0, min(30.0, 0.5 * 2 ** (attempt - 1))))
                except (TimeoutError, aiohttp.ClientError, FetchError) as error:
                    last_error = error
                    if attempt >= attempt_limit:
                        break
                    self.metrics.retries += 1
                    await asyncio.sleep(random.uniform(0, min(30.0, 0.5 * 2 ** (attempt - 1))))
        self.metrics.failures += 1
        raise FetchError(f"request failed after {attempt_limit} attempts: {url}") from last_error

    @staticmethod
    def _retry_delay(headers: aiohttp.typedefs.LooseHeaders, attempt: int) -> float:
        retry_after = headers.get("Retry-After")
        if retry_after:
            try:
                return min(120.0, max(0.0, float(retry_after)))
            except ValueError:
                parsed = email.utils.parsedate_to_datetime(retry_after)
                if parsed:
                    return min(120.0, max(0.0, (parsed - datetime.now(UTC)).total_seconds()))
        return random.uniform(0, min(30.0, 0.5 * 2 ** (attempt - 1)))

    @staticmethod
    def _looks_blocked(body: bytes, headers: aiohttp.typedefs.LooseHeaders) -> bool:
        content_type = str(headers.get("Content-Type", "")).lower()
        if "text/html" not in content_type:
            return False
        sample = body[:200_000].decode("utf-8", errors="ignore").casefold()
        return any(marker in sample for marker in BLOCK_MARKERS)
