from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path
from urllib.parse import urlsplit

from frontier_ingest.config import Settings
from frontier_ingest.core.raw_store import ContentAddressedRawStore
from frontier_ingest.llm.demo import run_resilience_demo
from frontier_ingest.pipeline import IngestionPipeline
from frontier_ingest.quality import submission_gate
from frontier_ingest.render_demo import run_rendered_source_demo
from frontier_ingest.scale_benchmark import run_scale_benchmark
from frontier_ingest.sources.rendered import AuthorizedRenderedFetcher

TARGETS = ("startups", "products", "papers", "jobs", "news")


async def collect_command(target: str, limit: int) -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    summary = await pipeline.collect(target, limit)
    print(
        json.dumps(
            {
                "target": target,
                "database": summary.database_path,
                "record_counts": summary.collected,
                "http": summary.http_metrics,
                "llm": summary.llm_metrics,
                "source_errors": summary.source_errors,
                "source_yields": summary.source_yields,
            },
            indent=2,
        )
    )


async def collect_all_command(catalog_limit: int, signal_limit: int) -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    await pipeline.initialize()
    results = await asyncio.gather(
        *(
            pipeline.collect(
                target,
                catalog_limit if target in {"startups", "products", "papers"} else signal_limit,
            )
            for target in TARGETS
        )
    )
    summaries: dict[str, object] = {}
    for target, summary in zip(TARGETS, results, strict=True):
        summaries[target] = {
            "record_counts": summary.collected,
            "http": summary.http_metrics,
            "llm": summary.llm_metrics,
            "source_errors": summary.source_errors,
            "source_yields": summary.source_yields,
        }
    summaries["entity_reconciliation"] = {"records_updated": await pipeline.reconcile_entities()}
    print(json.dumps(summaries, indent=2))


async def produce_command(target: str, limit: int, queue_name: str) -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    summary = await pipeline.produce(target, limit, queue_name)
    print(json.dumps(asdict(summary), indent=2))


async def produce_all_command(catalog_limit: int, signal_limit: int, queue_name: str) -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    await pipeline.initialize()
    results = await asyncio.gather(
        *(
            pipeline.produce(
                target,
                catalog_limit if target in {"startups", "products", "papers"} else signal_limit,
                queue_name,
            )
            for target in TARGETS
        )
    )
    print(
        json.dumps(
            {target: asdict(result) for target, result in zip(TARGETS, results, strict=True)},
            indent=2,
        )
    )


async def work_command(
    queue_name: str,
    batch_size: int,
    max_items: int,
    idle_polls: int,
    poll_interval: float,
) -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    summary = await pipeline.work(
        queue_name,
        batch_size=batch_size,
        max_items=max_items or None,
        idle_polls=idle_polls,
        poll_interval=poll_interval,
    )
    print(json.dumps(asdict(summary), indent=2))


async def refresh_signals_command(limit: int) -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    await pipeline.initialize()
    jobs, news = await asyncio.gather(
        pipeline.collect("jobs", limit),
        pipeline.collect("news", limit),
    )
    print(json.dumps({"jobs": asdict(jobs), "news": asdict(news)}, indent=2))


async def refresh_github_command(batch_size: int) -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    print(json.dumps(await pipeline.refresh_github(batch_size), indent=2))


async def reconcile_entities_command() -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    print(json.dumps({"recordsUpdated": await pipeline.reconcile_entities()}, indent=2))


async def llm_demo_command(output: str) -> None:
    report = (await run_resilience_demo()).to_dict()
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2), encoding="utf-8")
    pipeline = IngestionPipeline(Settings.from_env())
    await pipeline.initialize()
    await pipeline.store.record_run_metrics("llm:resilience-demo", report)
    print(json.dumps({"output": str(target.resolve()), **report}, indent=2))


async def scale_benchmark_command(records: int, batch_size: int, output: str) -> None:
    report = await asyncio.to_thread(run_scale_benchmark, records, batch_size)
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    print(json.dumps({"output": str(target.resolve()), **report.to_dict()}, indent=2))


async def render_source_command(
    url: str, allowed_hosts: list[str], storage_state: str | None
) -> None:
    host = (urlsplit(url).hostname or "").casefold()
    if host not in {item.casefold() for item in allowed_hosts}:
        raise SystemExit("URL host must be explicitly included with --allow-host")
    settings = Settings.from_env()
    fetcher = AuthorizedRenderedFetcher(
        ContentAddressedRawStore(settings.raw_store_path),
        set(allowed_hosts),
        Path(storage_state) if storage_state else None,
    )
    print(json.dumps(asdict(await fetcher.fetch(url)), indent=2, default=str))


async def render_demo_command(output: str) -> None:
    settings = Settings.from_env()
    report = await run_rendered_source_demo(settings.raw_store_path)
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2), encoding="utf-8")
    pipeline = IngestionPipeline(settings)
    await pipeline.initialize()
    await pipeline.store.record_run_metrics("browser:rendered-source-demo", report)
    print(json.dumps({"output": str(target.resolve()), **report}, indent=2))


async def quality_command(strict: bool) -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    reports = await pipeline.quality_reports()
    failures = submission_gate(reports, strict_counts=strict)
    print(
        json.dumps(
            {
                "reports": [report.to_dict() for report in reports],
                "submission_ready": not failures,
                "failures": failures,
            },
            indent=2,
        )
    )
    if failures:
        raise SystemExit(1)


async def export_command(output: str) -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    paths = await pipeline.export_json(output)
    print("\n".join(str(path.resolve()) for path in paths))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="frontier-ingest",
        description="Evidence-gated AI ecosystem ingestion pipeline",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    collect = commands.add_parser("collect", help="Collect one data vertical")
    collect.add_argument("target", choices=TARGETS)
    collect.add_argument("--limit", type=int, default=10)

    collect_all = commands.add_parser("collect-all", help="Collect all required verticals")
    collect_all.add_argument("--catalog-limit", type=int, default=1_000)
    collect_all.add_argument("--signal-limit", type=int, default=500)

    produce = commands.add_parser("produce", help="Discover one vertical into the durable queue")
    produce.add_argument("target", choices=TARGETS)
    produce.add_argument("--limit", type=int, default=10)
    produce.add_argument("--queue-name", default="submission")

    produce_all = commands.add_parser("produce-all", help="Discover all verticals into the queue")
    produce_all.add_argument("--catalog-limit", type=int, default=1_000)
    produce_all.add_argument("--signal-limit", type=int, default=500)
    produce_all.add_argument("--queue-name", default="submission")

    work = commands.add_parser("work", help="Lease and process queued records")
    work.add_argument("--queue-name", default="submission")
    work.add_argument("--batch-size", type=int, default=100)
    work.add_argument(
        "--max-items", type=int, default=0, help="0 processes until the queue is idle"
    )
    work.add_argument("--idle-polls", type=int, default=5)
    work.add_argument("--poll-interval", type=float, default=2.0)

    refresh_signals = commands.add_parser(
        "refresh-signals", help="Recollect jobs and news and prune everything older than 24 hours"
    )
    refresh_signals.add_argument("--limit", type=int, default=500)

    refresh_github = commands.add_parser(
        "refresh-github", help="Refresh every paper's stars directly from GitHub"
    )
    refresh_github.add_argument("--batch-size", type=int, default=200)

    commands.add_parser(
        "reconcile-entities",
        help="Apply canonical names to stored records while retaining raw source names",
    )

    llm_demo = commands.add_parser(
        "llm-demo", help="Run a deterministic 413/429/fallback/evidence-gate demonstration"
    )
    llm_demo.add_argument("--output", default="outputs/submission/llm-resilience-demo.json")

    benchmark = commands.add_parser(
        "benchmark-scale", help="Run the bounded-memory 500k-record hot-path benchmark"
    )
    benchmark.add_argument("--records", type=int, default=500_000)
    benchmark.add_argument("--batch-size", type=int, default=200)
    benchmark.add_argument("--output", default="reports/scale-benchmark.json")

    render = commands.add_parser(
        "render-source", help="Fetch an explicitly allowlisted JavaScript-rendered source"
    )
    render.add_argument("url")
    render.add_argument("--allow-host", action="append", required=True)
    render.add_argument("--storage-state")

    render_demo = commands.add_parser(
        "render-demo", help="Prove JavaScript execution and allowlist enforcement locally"
    )
    render_demo.add_argument("--output", default="reports/rendered-source-demo.json")

    quality = commands.add_parser("quality", help="Run the submission quality gate")
    quality.add_argument(
        "--allow-small-sample",
        action="store_true",
        help="Validate quality without enforcing 1,000-row catalog minimums",
    )

    export = commands.add_parser("export", help="Export normalized JSON and reports")
    export.add_argument("--output", default="outputs/export")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "collect":
        asyncio.run(collect_command(args.target, args.limit))
    elif args.command == "collect-all":
        asyncio.run(collect_all_command(args.catalog_limit, args.signal_limit))
    elif args.command == "produce":
        asyncio.run(produce_command(args.target, args.limit, args.queue_name))
    elif args.command == "produce-all":
        asyncio.run(produce_all_command(args.catalog_limit, args.signal_limit, args.queue_name))
    elif args.command == "work":
        asyncio.run(
            work_command(
                args.queue_name,
                args.batch_size,
                args.max_items,
                args.idle_polls,
                args.poll_interval,
            )
        )
    elif args.command == "refresh-signals":
        asyncio.run(refresh_signals_command(args.limit))
    elif args.command == "refresh-github":
        asyncio.run(refresh_github_command(args.batch_size))
    elif args.command == "reconcile-entities":
        asyncio.run(reconcile_entities_command())
    elif args.command == "llm-demo":
        asyncio.run(llm_demo_command(args.output))
    elif args.command == "benchmark-scale":
        asyncio.run(scale_benchmark_command(args.records, args.batch_size, args.output))
    elif args.command == "render-source":
        asyncio.run(render_source_command(args.url, args.allow_host, args.storage_state))
    elif args.command == "render-demo":
        asyncio.run(render_demo_command(args.output))
    elif args.command == "quality":
        asyncio.run(quality_command(strict=not args.allow_small_sample))
    elif args.command == "export":
        asyncio.run(export_command(args.output))


if __name__ == "__main__":
    main()
