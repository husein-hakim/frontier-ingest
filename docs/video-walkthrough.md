# Submission video walkthrough

Target length: **7 minutes**. The script is deliberately shorter than the available time so you can pause naturally while switching screens.

## Before recording

1. Record at 1920×1080 or higher, with browser and editor zoom around 110–125%.
2. Turn off notifications and close `.env`, account, token, email, and raw-data tabs.
3. Open these items in this order:
   - the public GitHub repository on the README;
   - the six-tab Google Sheet on **Startups**;
   - `architecture.pdf` in a PDF viewer;
   - VS Code with `pipeline.py`, `orchestrator.py`, `storage_postgres.py`, `quality.py`, and `rendered.py` already open;
   - a terminal at the repository root.
4. Keep the terminal large enough that the complete quality result fits on screen.
5. Do not refresh the public sources during the recording. Refresh them immediately before recording, then demonstrate the already-generated, deterministic reports.

## Screen plan

| Time | Show | What the interviewer should notice |
| --- | --- | --- |
| 0:00–0:35 | GitHub README: title and Submission status | Clear task coverage, public deliverables, more than 3,000 accepted records |
| 0:35–1:25 | Google Sheet: all six tabs | Genuine records, direct links, timestamps, evidence-oriented fields, native tables |
| 1:25–2:05 | `architecture.pdf`, page 1 | Two ingestion lanes and the evidence-gated pipeline |
| 2:05–2:55 | `pipeline.py`, lines 59–190 | Streaming batches, adapters, optional LLM, validation, upsert |
| 2:55–3:35 | `orchestrator.py`, lines 43–93 | Token selection, 413 shrinking, 429 backoff, provider fallback |
| 3:35–4:20 | `storage_postgres.py`, lines 13–81 and 152–205 | Durable schema, idempotency, leases, `SKIP LOCKED` |
| 4:20–4:55 | Google Sheet: Entity Mapping Log | Canonical names, raw names, matched/review/new outcomes |
| 4:55–5:30 | `architecture.pdf`, pages 2–3 | 500k scale, storage choices, failure recovery, compliance |
| 5:30–6:05 | `rendered.py`, lines 20–83; then rendered-source report | Authorized rendering, host allowlist, no CAPTCHA bypass |
| 6:05–6:45 | Terminal: tests, quality gate, scale and resilience reports | Executable proof instead of architecture-only claims |
| 6:45–7:00 | GitHub README: deliverables | Crisp close and links |

## Word-for-word script

### 0:00–0:35 — What I built

**Screen:** GitHub README, top section.

> Hi, I’m Husein. I built Frontier Ingest for the FrontierAtlas AI Engineer assessment. The task requires a large, source-backed AI intelligence dataset: one thousand startups, one thousand genuine AI products, one thousand research papers with current GitHub metrics, plus jobs and news proven to be from the last twenty-four hours. It also requires entity reconciliation and a production design that can scale to five hundred thousand records.
>
> The verified export contains more than 3,000 accepted records: exactly 1,000 startups, 1,000 products and 1,000 papers, plus the current jobs and news shown in the Sheet. Those two signal totals intentionally vary because stale records are removed rather than retained to hit an artificial number.

### 0:35–1:25 — Show the actual deliverable

**Screen:** Google Sheet. Briefly click through Startups, Products, Research Papers, Jobs, News, and Entity Mapping Log.

> This is the final native Google Sheet. Each tab is a real structured table, not a screenshot or a pasted sample.
>
> Every output record has a stable record key, collection timestamp, original source URL, source name, and content hash. Products are genuine product-level records from Hugging Face Spaces. I leave pricing blank when the source does not prove a pricing model, because public access is not evidence that a product is commercially free.
>
> The paper tab includes the repository URL, direct GitHub star count, the time that count was collected, and `github_graphql` as the metric source. Jobs and news contain the full text plus an explicit freshness method and confidence. The mapping log keeps every resolution decision auditable.

### 1:25–2:05 — The USP

**Screen:** `architecture.pdf`, page 1. Point to the left-to-right flow.

> The central idea is what I call Evidence-Gated Adaptive Ingestion.
>
> A normal scraper is brittle, while an LLM-on-every-page pipeline is expensive and can invent values. This system takes a third path. It stores each raw response once under its SHA-256 hash, extracts structured APIs, feeds, JSON-LD and metadata deterministically, and identifies only the fields that are still unresolved.
>
> An LLM is optional and is used as a constrained parser, not as a source of truth. A model-produced value is accepted only if it includes an exact quote found in the supplied source fragments. Otherwise the field stays null. That is the main reliability and token-efficiency advantage.

### 2:05–2:55 — The processing pipeline

**Screen:** `src/frontier_ingest/pipeline.py`, lines 59–190.

> The implementation is an asynchronous Python 3.11 package. Each source has a dedicated adapter, while the orchestration, record contract, evidence checks, raw store and persistence interfaces are shared.
>
> Collection is streamed into bounded batches, two hundred by default. A batch can be enriched, canonicalized, validated and upserted without holding the complete dataset in memory. The common structured path uses zero LLM calls. Only records with unresolved semantic fields become candidates, and every run records HTTP, source-yield and LLM metrics.
>
> There are two logical lanes: a catalog lane for the thousand-row datasets, and a freshness lane for jobs and news. Both end in the same canonical camelCase schema and the same fail-closed quality gate.

### 2:55–3:35 — Token and provider resilience

**Screen:** `src/frontier_ingest/llm/orchestrator.py`, lines 43–93.

> Before any model request, complete text fragments are ranked against only the unresolved fields and selected under a hard provider-safe token budget. The system never sends the whole page by default.
>
> A 413 response immediately halves the payload before retrying. A 429 or transient failure uses bounded exponential backoff with jitter and respects Retry-After. If one provider exhausts its isolated retry allowance, processing moves to the next configured provider. A global call budget prevents one difficult source from consuming the entire run.

### 3:35–4:20 — Concurrency, durability and deduplication

**Screen:** `src/frontier_ingest/storage_postgres.py`, first the schema, then lines 152–205.

> The local reference run uses SQLite for reproducibility, while production uses PostgreSQL. Records and work items have deterministic unique keys, so at-least-once delivery remains idempotent.
>
> Producers discover work and stateless workers lease it using `FOR UPDATE SKIP LOCKED`. A lease can expire and be reclaimed after a worker crash. Recoverable failures return to the queue with a delay, and exhausted items go to a dead-letter table with their error and evidence context.
>
> Raw responses live in content-addressed object storage, so parser upgrades can replay existing evidence without hitting the source again.

### 4:20–4:55 — Entity resolution without unsafe merges

**Screen:** Google Sheet, Entity Mapping Log. Show the Status and Confidence columns.

> Entity resolution is conservative. Unicode normalization, case folding, tokenization and legal-suffix removal handle exact aliases in constant time. Unknown names are blocked by prefix, token count and length before fuzzy scoring, with at most two hundred candidates.
>
> The resolver returns `MATCHED`, `NEEDS_REVIEW`, or `NEW_ENTITY`. It never forces an uncertain merge. Canonical names are written to the output, while the raw source spelling is preserved separately. A second reconciliation pass makes the result independent of queue arrival order.

### 4:55–5:30 — Why it scales to 500,000

**Screen:** `architecture.pdf`, pages 2 and 3.

> Scaling does not mean increasing concurrency against a single website. Per-domain limits remain fixed. Throughput comes from more independent source partitions, stateless workers, database capacity and object-storage throughput.
>
> The memory profile stays bounded because the pipeline streams pages and batches. GitHub enrichment groups up to forty repositories into one GraphQL request. Entity resolution avoids an all-pairs comparison. PostgreSQL remains the source of truth; pgvector is optional candidate generation, and a graph database is only a rebuildable projection for multi-hop analysis.
>
> The included hot-path benchmark processes five hundred thousand identifiers in 2,500 batches with roughly 592 thousand records per second and 0.002 MiB of traced peak allocation on the reference machine. I label it honestly as a transformation microbenchmark, not as an external-source SLA.

### 5:30–6:05 — JavaScript and anti-bot boundary

**Screen:** `src/frontier_ingest/sources/rendered.py`, then `reports/rendered-source-demo.json`.

> Source access follows a compliance-first order: official API, feed, sitemap, permitted HTTP, and only then authorized JavaScript rendering.
>
> The Playwright adapter requires an explicit hostname allowlist, blocks network requests to every other host, can use only a user-authorized session, stores the rendered HTML, and stops if it detects Cloudflare, DataDome or CAPTCHA markers. It does not attempt challenge bypass.
>
> This report is an executable local proof: JavaScript ran in a real Chromium session, raw evidence was stored, only the allowed host was contacted, and no challenge bypass was attempted.

### 6:05–6:45 — Run the proof

**Screen:** Terminal. Run these commands one at a time and pause after each result.

```bash
.venv/bin/python -m pytest -q
DATABASE_URL=sqlite:///data/final-submission.db .venv/bin/frontier-ingest quality
jq . reports/scale-benchmark.json
jq . outputs/submission/llm-resilience-demo.json
jq . reports/rendered-source-demo.json
```

Say:

> The repository includes deterministic tests for source parsing, freshness boundaries, evidence validation, queue semantics, canonical schema output and PostgreSQL JSON handling. All 29 tests pass.
>
> The final command is the submission gate. It independently checks the minimum catalog sizes, unique keys and URLs, valid source links, evidence completeness, genuine product identity, fresh signals, exact camelCase schema and direct current GitHub metrics. It currently reports `submission_ready: true` with no failures.
>
> The two small reports make the unusual branches reproducible: the LLM demo proves 413 payload shrinking, 429 provider fallback and exact-quote acceptance; the renderer demo proves authorized JavaScript execution and the access-control boundary.

### 6:45–7:00 — Close

**Screen:** Return to the README submission links.

> The result is not just a spreadsheet and not just an architecture diagram. It is a runnable ingestion system with evidence lineage, conservative abstention, bounded model usage, failure recovery, production queue semantics and a strict gate that prevents a stale or unsupported export from being submitted. The public repository, Google Sheet and three-page architecture document are linked here. Thank you.

## Optional answers if the reviewer asks

### Why not use an LLM for every page?

Structured sources are cheaper, faster and more reproducible. The model is reserved for unresolved semantic fields, and its output still has to pass the exact-quote evidence gate.

### Is the 500k benchmark a production SLA?

No. It proves that the transformation hot path is streaming and memory-bounded. External throughput remains governed by source limits, network latency and database capacity.

### Why PostgreSQL instead of only a graph database?

Ingestion needs ACID upserts, unique constraints, work leasing, checkpoints and JSONB flexibility. A graph projection is valuable for traversal but should be rebuildable from PostgreSQL rather than participating in every ingestion transaction.

### Why can pricing be null?

Null is an intentional, defensible abstention. A pricing category is written only when source text supports it; otherwise filling it would manufacture data.

### What happens if a worker dies?

The work lease expires, another worker safely reclaims the item, and the stable record key makes the repeated write idempotent.
