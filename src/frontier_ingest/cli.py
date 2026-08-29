from __future__ import annotations

import argparse
import asyncio
import json

from frontier_ingest.config import Settings
from frontier_ingest.pipeline import IngestionPipeline
from frontier_ingest.quality import submission_gate

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
            },
            indent=2,
        )
    )


async def collect_all_command(catalog_limit: int, signal_limit: int) -> None:
    pipeline = IngestionPipeline(Settings.from_env())
    summaries: dict[str, object] = {}
    for target in TARGETS:
        limit = catalog_limit if target in {"startups", "products", "papers"} else signal_limit
        summary = await pipeline.collect(target, limit)
        summaries[target] = {
            "record_counts": summary.collected,
            "http": summary.http_metrics,
            "llm": summary.llm_metrics,
            "source_errors": summary.source_errors,
        }
    print(json.dumps(summaries, indent=2))


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
    elif args.command == "quality":
        asyncio.run(quality_command(strict=not args.allow_small_sample))
    elif args.command == "export":
        asyncio.run(export_command(args.output))


if __name__ == "__main__":
    main()
