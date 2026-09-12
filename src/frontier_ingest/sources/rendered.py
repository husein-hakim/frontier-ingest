from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from frontier_ingest.core.http import BLOCK_MARKERS, BlockedSourceError
from frontier_ingest.core.raw_store import ContentAddressedRawStore


@dataclass(frozen=True, slots=True)
class RenderedDocument:
    url: str
    title: str
    content_hash: str
    raw_path: Path
    html_bytes: int


class AuthorizedRenderedFetcher:
    """Optional Playwright fetcher for explicitly allowlisted, permitted pages.

    It uses an ordinary browser context, never solves challenges, and stops when a
    CAPTCHA/Cloudflare/DataDome marker is present.
    """

    def __init__(
        self,
        raw_store: ContentAddressedRawStore,
        allowed_hosts: set[str],
        storage_state_path: Path | None = None,
    ) -> None:
        self.raw_store = raw_store
        self.allowed_hosts = {host.casefold() for host in allowed_hosts}
        self.storage_state_path = storage_state_path

    async def fetch(self, url: str) -> RenderedDocument:
        host = (urlsplit(url).hostname or "").casefold()
        if host not in self.allowed_hosts:
            raise PermissionError(f"rendered source host is not allowlisted: {host}")
        try:
            from playwright.async_api import async_playwright
        except ImportError as error:
            raise RuntimeError(
                "install frontier-ingest[browser] and Playwright Chromium"
            ) from error

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            options = {}
            if self.storage_state_path:
                options["storage_state"] = str(self.storage_state_path)
            context = await browser.new_context(**options)

            async def enforce_network_allowlist(route) -> None:
                request_host = (urlsplit(route.request.url).hostname or "").casefold()
                if request_host in self.allowed_hosts:
                    await route.continue_()
                else:
                    await route.abort("blockedbyclient")

            await context.route("**/*", enforce_network_allowlist)
            page = await context.new_page()
            response = await page.goto(url, wait_until="networkidle", timeout=45_000)
            if response is None or response.status >= 400:
                await browser.close()
                status = response.status if response else "no response"
                raise RuntimeError(f"rendered fetch failed: {status}")
            html = await page.content()
            title = await page.title()
            lower = html[:200_000].casefold()
            if any(marker in lower for marker in BLOCK_MARKERS):
                await browser.close()
                raise BlockedSourceError(f"challenge page detected at {url}")
            final_url = page.url
            final_host = (urlsplit(final_url).hostname or "").casefold()
            if final_host not in self.allowed_hosts:
                await browser.close()
                raise PermissionError(f"rendered source redirected outside allowlist: {final_host}")
            await browser.close()
        body = html.encode("utf-8")
        content_hash, raw_path = await self.raw_store.put(body, ".html")
        return RenderedDocument(final_url, title, content_hash, raw_path, len(body))
