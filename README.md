# Frontier Ingest

Frontier Ingest is a production-oriented pipeline for the GraphOne / FrontierAtlas AI Engineer assessment. It collects and normalizes AI startups, products, code-linked research papers, fresh jobs, and fresh news while preserving a legitimate source URL and field-level evidence for every populated value.

The design is called **Evidence-Gated Adaptive Ingestion**: parse structured sources first, preserve the raw response, and call an LLM only for a field that remains unresolved. A model value is accepted only when the response contains an exact quote found in the supplied source fragments. Unsupported fields remain null instead of being guessed.

## Submission status

The clean reference run from 29 August 2026 passes the strict quality gate.

| Dataset | Rows | Unique keys | Valid source URLs | Evidence complete |
| --- | ---: | ---: | ---: | ---: |
| Startups | 1,000 | 1,000 | 1,000 | 1,000 |
| Products | 1,000 | 1,000 | 1,000 | 1,000 |
| Research papers with GitHub metrics | 1,000 | 1,000 | 1,000 | 1,000 |
| Jobs proven fresh within 24 hours | 65 | 65 | 65 | 65 |
| News proven fresh within 24 hours | 5 | 5 | 5 | 5 |

Fresh jobs and news naturally vary on each run. A record without sufficient freshness evidence is excluded. Product pricing also remains null when the source does not explicitly support one of `FREE`, `FREEMIUM`, `PAID`, or `ENTERPRISE`; this is deliberate protection against hallucination.

The required design document is [architecture.pdf](architecture.pdf). It is exactly three pages and covers the 500,000-record design, 413/429 behavior, distributed deduplication, source access policy, and PostgreSQL/pgvector/graph storage choices.

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
  sources/              YC, Hugging Face papers, five job APIs, five news feeds
  llm/                  provider adapters, fallback orchestration, evidence gate
  pipeline.py           bounded batch processing and source coordination
  storage.py            local SQLite reference implementation
  storage_postgres.py   production schema, leases, checkpoints and upserts
  resolution.py         normalized exact and blocked fuzzy entity resolution
  quality.py            submission gate and 24-hour validation
configs/                 50 canonical AI organization seeds
tests/                   deterministic, storage, freshness, LLM and source tests
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

Run the complete acquisition and export normalized JSON:

```bash
.venv/bin/frontier-ingest collect-all --catalog-limit 1000 --signal-limit 500
.venv/bin/frontier-ingest quality
.venv/bin/frontier-ingest export --output outputs/submission
```

The default database is `sqlite:///data/frontier.db`. Commands are idempotent: stable record keys and database constraints make reruns update records instead of duplicating them.

## Data sources

| Vertical | Monitored sources | Method |
| --- | --- | --- |
| Startups and products | Y Combinator AI directory | Paginated embedded structured payload |
| Research papers | Hugging Face daily papers, arXiv links, GitHub | Public API plus batched GitHub GraphQL refresh |
| Jobs | Arbeitnow, Remote OK, Jobicy, Himalayas, Remotive | Five public/official JSON APIs; full description retained |
| News | OpenAI, Google DeepMind, Hugging Face Blog, MIT News AI, TechCrunch AI | Five RSS/Atom feeds followed by full article extraction |

GitHub enrichment groups up to 40 repositories into one GraphQL request. Without `GITHUB_TOKEN`, the paper adapter requires the public Hugging Face star metric and records its collection timestamp. With a token, current stars are refreshed from GitHub.

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

The submitted data does not require an LLM credential because the selected sources expose the required facts in structured form. This is intentional: using a model when deterministic evidence exists would increase cost and hallucination risk.

## Freshness and deduplication

Jobs and news are accepted only when the normalized UTC publication time is between five minutes in the future and 24 hours before collection. ISO timestamps, source-specific epoch fields, feed timestamps, and bounded relative times are supported.

For a source with no usable date, the first-seen fallback is conservative. It requires a completed scan less than 24 hours old, a previously unseen stable key, and ordering after the prior recorded head. Cold-start items are never accepted on first-seen evidence alone.

Across workers, PostgreSQL leases use `FOR UPDATE SKIP LOCKED`. Stable keys, unique constraints, checkpoints, and content hashes make at-least-once delivery safe. Failed work can be retried without re-downloading immutable raw evidence.

## Entity resolution

Resolution uses Unicode normalization, case folding, punctuation removal, and legal-suffix removal. Exact normalized matches are O(1). Unknown names use prefix/token/length blocking before similarity scoring, capped at 200 candidates, rather than comparing against the entire catalog. The resolver can return `MATCHED`, `NEEDS_REVIEW`, or `NEW_ENTITY`; it never forces a questionable merge.

The seed file contains 50 canonical AI organizations and is augmented by collected startup names. Every decision is written to the Entity Mapping Log.

## Scaling to 500,000+

The reference process never retains the entire record set. It streams source pages and writes/enriches bounded batches (200 by default). Production uses:

- a durable discovery/work queue with source/page dedupe keys;
- stateless async workers scaled by queue lag and oldest-item age;
- content-addressed object storage for immutable raw bodies;
- PostgreSQL for ACID records, work leases, checkpoints, mappings, and quality events;
- pgvector only for candidate generation, never as the canonical identity decision;
- an optional rebuildable graph projection for multi-hop relationship queries.

Scaling adds workers, database capacity, and source partitions without changing extraction code. Per-domain limits remain authoritative; scale comes from more pages and sources, not from overwhelming one host.

## Source access and anti-bot policy

Access order is official API, RSS/Atom, sitemap, permitted HTTP, then JavaScript rendering when the site allows it. The client applies per-host concurrency, request spacing, retries, cache-friendly raw replay, and block-page detection.

Cloudflare/DataDome challenges pause the adapter and generate a source error. The project does not bypass CAPTCHA or access controls. For a protected high-value source, the production answer is an authorized API, licensed feed, or browser workflow permitted by the source, not stealth circumvention.

## Verification

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check src tests
DATABASE_URL=sqlite:///data/submission.db .venv/bin/frontier-ingest quality
```

The strict quality command exits non-zero for missing catalog minimums, duplicate keys, missing required fields, invalid URLs, incomplete evidence, or a job/news record outside the 24-hour window.

## Container deployment

```bash
docker compose up --build
```

The worker waits for PostgreSQL health, runs the five verticals, and exits after the bounded collection. Raw evidence and PostgreSQL data use named volumes; normalized exports are mounted under `outputs/`.
