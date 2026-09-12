# Frontier Ingest

Frontier Ingest is a production-oriented pipeline for the GraphOne / FrontierAtlas AI Engineer assessment. It collects and normalizes AI startups, products, code-linked research papers, fresh jobs, and fresh news while preserving a legitimate source URL and field-level evidence for every populated value.

The design is called **Evidence-Gated Adaptive Ingestion**: parse structured sources first, preserve the raw response, and call an LLM only for a field that remains unresolved. A model value is accepted only when the response contains an exact quote found in the supplied source fragments. Unsupported fields remain null instead of being guessed.

## Submission status

The final 12 September 2026 dataset was produced by the durable pipeline, not by a fixture or hand-edited export. It contains 3,080 accepted records. The base catalog and signals passed through the queue; all 1,000 repository metrics and both time-sensitive lanes were refreshed immediately before export. The strict submission gate reports `submission_ready: true`.

Submission deliverables:

- [Public six-tab Google Sheet](https://docs.google.com/spreadsheets/d/159EFQ28ttxGrgvw5fMqtFggEC19GTCVi3nFKil0DFuo/edit)
- [Public source repository](https://github.com/husein-hakim/frontier-ingest)
- [Three-page production architecture](architecture.pdf)

| Dataset | Rows | Unique keys | Valid source URLs | Evidence complete |
| --- | ---: | ---: | ---: | ---: |
| Startups | 1,000 | 1,000 | 1,000 | 1,000 |
| Genuine AI products | 1,000 | 1,000 | 1,000 | 1,000 |
| Research papers with GitHub metrics | 1,000 | 1,000 | 1,000 | 1,000 |
| Jobs proven fresh within 24 hours | 69 | 69 | 69 | 69 |
| News proven fresh within 24 hours | 11 | 11 | 11 | 11 |

Fresh jobs and news naturally vary on each run. A record without sufficient freshness evidence is excluded. Product pricing remains null unless source text explicitly supports one of `FREE`, `FREEMIUM`, `PAID`, or `ENTERPRISE`; public visibility is not incorrectly treated as proof that a product is free.

The required design document is exactly three pages and covers the 500,000-record design, 413/429 behavior, distributed deduplication, source access policy, and PostgreSQL/pgvector/graph storage choices.

## Why this approach is different

Most scraping pipelines choose between brittle selectors and sending every page to an LLM. This pipeline uses a third path:

1. Store each raw response once under its SHA-256 content hash.
2. Extract APIs, RSS/Atom, JSON-LD, metadata, and page text deterministically.
3. Identify only missing semantic fields.
4. Rank small source fragments under a hard token budget.
5. Try up to three isolated LLM providers in order.
6. Require typed output plus an exact evidence quote before accepting a value.
7. Validate freshness, schema, entity resolution, source URLs, and uniqueness before export.

This makes the common path fast and token-free, while still supporting ambiguous HTML when a model is genuinely useful.

## Repository layout

```text
src/frontier_ingest/
  core/                 resilient HTTP, hashing, raw storage, token selection
  sources/              YC startups, Hugging Face products/papers, signals, rendering
  llm/                  provider adapters, fallback orchestration, evidence gate
  pipeline.py           bounded batch processing and source coordination
  storage.py            local SQLite reference implementation
  storage_postgres.py   production schema, leases, checkpoints and upserts
  resolution.py         normalized exact and blocked fuzzy entity resolution
  quality.py            submission gate and 24-hour validation
configs/                 50 canonical AI organization seeds
tests/                   deterministic, storage, freshness, LLM and source tests
reports/reference-run.json compact, non-secret execution evidence
reports/rendered-source-demo.json executable browser-adapter proof
architecture.pdf         required three-page production design
```

## Quick start

Python 3.11 or newer is required.

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev,postgres]"
cp .env.example .env
```

Run a small source smoke test:

```bash
.venv/bin/frontier-ingest collect startups --limit 10
.venv/bin/frontier-ingest collect papers --limit 10
.venv/bin/frontier-ingest quality --allow-small-sample
```

Run the complete acquisition through the same producer/worker boundary used in production:

```bash
.venv/bin/frontier-ingest produce-all --catalog-limit 1000 --signal-limit 500 --queue-name submission
.venv/bin/frontier-ingest work --queue-name submission --batch-size 200
.venv/bin/frontier-ingest reconcile-entities
.venv/bin/frontier-ingest refresh-github
.venv/bin/frontier-ingest refresh-signals --limit 500
.venv/bin/frontier-ingest quality
.venv/bin/frontier-ingest export --output outputs/submission
```

The default database is `sqlite:///data/frontier.db`. Commands are idempotent: stable record keys and database constraints make reruns update records instead of duplicating them.

## Data sources

| Vertical | Monitored sources | Method |
| --- | --- | --- |
| Startups | Y Combinator AI directory | Paginated embedded structured payload |
| Products | Hugging Face Spaces | Cursor-paginated public API; one hosted AI app per record |
| Research papers | Hugging Face daily papers, arXiv links, GitHub | Public API plus batched GitHub GraphQL refresh |
| Jobs | Arbeitnow, Remote OK, Jobicy, Himalayas, Remotive | Five public/official JSON APIs; full description retained |
| News | OpenAI, Google DeepMind, Hugging Face Blog, MIT News AI, TechCrunch AI | Five RSS/Atom feeds followed by full article extraction |

GitHub enrichment groups up to 40 repositories into one GraphQL request. Without `GITHUB_TOKEN`, the paper adapter requires the public Hugging Face star metric and records its collection timestamp. With a token, current stars are refreshed from GitHub.

The strict submission gate requires `github_stars_source=github_graphql` and a star timestamp no older than 24 hours. The final run refreshed 1,000/1,000 metrics in 25 GraphQL requests with no unavailable repositories. Cached Hugging Face metrics are usable for development only.

## Multi-tier LLM setup

Each tier uses an OpenAI-compatible chat-completions endpoint, so Gemini-compatible, Groq, DeepSeek, or another provider can be configured without changing code.

```dotenv
ENABLE_LLM_ENRICHMENT=true
PRIMARY_LLM_API_KEY=...
PRIMARY_LLM_BASE_URL=...
PRIMARY_LLM_MODEL=...
SECONDARY_LLM_API_KEY=...
SECONDARY_LLM_BASE_URL=...
SECONDARY_LLM_MODEL=...
TERTIARY_LLM_API_KEY=...
TERTIARY_LLM_BASE_URL=...
TERTIARY_LLM_MODEL=...
LLM_MAX_CALLS_PER_RUN=200
```

Provider 429 responses are retried with exponential backoff, jitter, and `Retry-After` support. A 413 immediately halves the selected payload before retrying. Provider failures are isolated: exhaustion moves to the next configured tier. The call budget prevents an unexpectedly ambiguous source from consuming the entire token allowance.

The selected sources expose most required facts in structured form, so the common path uses no model tokens. Only records whose text contains a relevant cue become LLM candidates. A deterministic demonstration makes the fallback executable without misrepresenting a simulator as a live model call:

```bash
.venv/bin/frontier-ingest llm-demo --output outputs/submission/llm-resilience-demo.json
```

The report proves payload shrinking after a simulated 413, retry/fallback after simulated 429s, and exact-quote evidence validation. When real provider keys are configured, the same orchestrator is used by workers and records live call metrics.

## Freshness and deduplication

Jobs and news are accepted only when the normalized UTC publication time is between five minutes in the future and 24 hours before collection. ISO timestamps, source-specific epoch fields, feed timestamps, and bounded relative times are supported.

For a source with no usable date, the first-seen fallback is conservative. It requires a completed scan less than 24 hours old, a previously unseen stable key, and ordering after the prior recorded head. Cold-start items are never accepted on first-seen evidence alone.

Across workers, PostgreSQL leases use `FOR UPDATE SKIP LOCKED`. Stable keys, unique constraints, checkpoints, and content hashes make at-least-once delivery safe. Failed work can be retried without re-downloading immutable raw evidence.

## Entity resolution

Resolution uses Unicode normalization, case folding, punctuation removal, and legal-suffix removal. Exact normalized matches are O(1). Unknown names use prefix/token/length blocking before similarity scoring, capped at 200 candidates, rather than comparing against the entire catalog. The resolver can return `MATCHED`, `NEEDS_REVIEW`, or `NEW_ENTITY`; it never forces a questionable merge.

The seed file contains 50 canonical AI organizations and is augmented by collected startup names. A second pass makes the result independent of queue arrival order. Matched canonical names are written into `entityName`, `startupName`, and `company`; the original source spelling remains in `rawEntityName`, `rawStartupName`, or `rawCompany`. Every decision is written to the Entity Mapping Log.

## Scaling to 500,000+

The reference process never retains the entire record set. It streams source pages and writes/enriches bounded batches (200 by default). Production uses:

- a durable discovery/work queue with source/page dedupe keys;
- stateless async workers scaled by queue lag and oldest-item age;
- content-addressed object storage for immutable raw bodies;
- PostgreSQL for ACID records, work leases, checkpoints, mappings, and quality events;
- pgvector only for candidate generation, never as the canonical identity decision;
- an optional rebuildable graph projection for multi-hop relationship queries.

Scaling adds workers, database capacity, and source partitions without changing extraction code. Per-domain limits remain authoritative; scale comes from more pages and sources, not from overwhelming one host.

The reproducible bounded-memory hot-path check processed 500,000 synthetic identifiers in 2,500 batches with 0.002 MiB peak traced allocation on the reference machine. It is deliberately labelled a microbenchmark—not an external-source SLA:

```bash
.venv/bin/frontier-ingest benchmark-scale --records 500000 --batch-size 200
```

## Source access and anti-bot policy

Access order is official API, RSS/Atom, sitemap, permitted HTTP, then JavaScript rendering when the site allows it. The client applies per-host concurrency, request spacing, retries, cache-friendly raw replay, and block-page detection.

Cloudflare/DataDome challenges pause the adapter and generate a source error. The project does not bypass CAPTCHA or access controls. For a protected high-value source, the production answer is an authorized API, licensed feed, or browser workflow permitted by the source, not stealth circumvention.

The optional browser adapter is executable and deliberately explicit:

```bash
.venv/bin/pip install -e ".[browser]"
.venv/bin/playwright install chromium
.venv/bin/frontier-ingest render-source https://allowed.example/page \
  --allow-host allowed.example --storage-state authorized-session.json
```

It only visits allowlisted network hosts, may use a user-authorized session state, stores rendered HTML by SHA-256, and stops when challenge markers appear. A local, non-simulated fixture proves that the browser actually executes JavaScript without contacting or bypassing a protected site:

```bash
.venv/bin/frontier-ingest render-demo --output reports/rendered-source-demo.json
```

## Verification

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check src tests
DATABASE_URL=sqlite:///data/submission.db .venv/bin/frontier-ingest quality
```

The strict quality command exits non-zero for missing catalog minimums, duplicate keys or source URLs, wrong schema casing, missing required/raw-name fields, invalid URLs, incomplete evidence, stale jobs/news, cloned company-as-product patterns, or GitHub metrics that are cached/stale.

## Container deployment

```bash
docker compose up --build --scale worker=4
```

The producer discovers all five verticals into PostgreSQL. Any number of workers safely claim expiring leases with `FOR UPDATE SKIP LOCKED`, enrich and canonicalize bounded batches, retry recoverable failures, dead-letter exhausted items, and idempotently upsert records. Raw evidence and PostgreSQL use named volumes; normalized exports are mounted under `outputs/`.

## Submission artifacts

- `architecture.pdf` — required three-page production design.
- `outputs/submission/*.json` — camelCase records, mapping log, quality report, run metrics, resilience demo, and manifest.
- `reports/reference-run.json` — compact tracked evidence from the clean queue run.
- `reports/scale-benchmark.json` — reproducible 500,000-record streaming hot-path result.
- `reports/rendered-source-demo.json` — real local Chromium execution, allowlist, and raw-evidence proof.
- `outputs/<run-id>/frontier-intelligence-data.xlsx` — verified six-tab workbook ready for native Google Sheets import.
- `docs/submission-checklist.md` — final authenticated refresh and publication steps.
- `docs/video-walkthrough.md` — seven-minute screen plan, word-for-word script, and reviewer Q&A.
- `docs/submission-readiness-and-video-guide.html` — self-contained readiness and recording guide.
