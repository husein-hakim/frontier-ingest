from __future__ import annotations

from pathlib import Path

from aiohttp import web

from frontier_ingest.core.raw_store import ContentAddressedRawStore
from frontier_ingest.sources.rendered import AuthorizedRenderedFetcher

DEMO_HTML = b"""<!doctype html>
<html><head><title>Before JavaScript</title></head>
<body><main id="app">Loading</main>
<script>
document.title = "Rendered AI Product";
document.getElementById("app").textContent = "Client-rendered product evidence";
</script></body></html>"""


async def run_rendered_source_demo(raw_store_path: Path) -> dict[str, object]:
    """Prove the compliant renderer against a local JavaScript fixture."""

    async def page(_: web.Request) -> web.Response:
        return web.Response(body=DEMO_HTML, content_type="text/html")

    application = web.Application()
    application.router.add_get("/product", page)
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    sockets = site._server.sockets if site._server else []
    if not sockets:
        await runner.cleanup()
        raise RuntimeError("local rendered-source demo server did not start")
    port = int(sockets[0].getsockname()[1])
    try:
        document = await AuthorizedRenderedFetcher(
            ContentAddressedRawStore(raw_store_path),
            {"127.0.0.1"},
        ).fetch(f"http://127.0.0.1:{port}/product")
    finally:
        await runner.cleanup()
    return {
        "simulation": False,
        "source": "local authorized JavaScript fixture",
        "allowedHost": "127.0.0.1",
        "javascriptExecuted": document.title == "Rendered AI Product",
        "challengeBypassAttempted": False,
        "finalUrl": document.url,
        "title": document.title,
        "contentHash": document.content_hash,
        "rawEvidenceStored": document.raw_path.is_file(),
        "htmlBytes": document.html_bytes,
    }
