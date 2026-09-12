# Technical design

## Evidence-Gated Adaptive Ingestion

The system has a catalog lane for high-volume startups, products, and papers and a freshness lane for jobs and news. Both lanes share the same canonical record contract, content-addressed raw store, evidence model, quality gate, entity resolver, and idempotent persistence layer.

The governing invariant is: a populated canonical field must have field-level evidence pointing to a legitimate source URL. The LLM is not an evidence source; it is a constrained parser over supplied evidence. Its output is rejected unless it includes an exact quote present in those fragments.

## Processing stages

1. Source adapters discover pages or stable resource identifiers and producers enqueue records with deterministic dedupe keys.
2. The HTTP client applies per-host concurrency, spacing, timeouts, block detection, and bounded retries.
3. Raw bytes are written under their SHA-256 hash before extraction.
4. Deterministic extractors consume APIs, feeds, JSON-LD, metadata, and HTML.
5. The evidence gate identifies only missing or ambiguous semantic fields.
6. The token budgeter ranks complete fragments and enforces a provider-safe limit.
7. Up to three LLM providers are tried in order with isolated retry budgets.
8. Typed values must include exact source quotes; unsupported values are discarded.
9. Workers claim expiring leases, then freshness, schema, URL, uniqueness, and field-evidence validation run before persistence.
10. Entity resolution performs exact normalization, blocked fuzzy matching, or abstention and writes matched canonical names.
11. Bounded batches are idempotently upserted; a second reconciliation pass removes queue-order dependence.
12. JSON, a run manifest, metrics, and the six requested Sheet tabs are exported.

## 413 and 429 behavior

Each provider declares a maximum input size. The orchestrator reserves output space, ranks relevant paragraphs, and never sends the full document by default. A provider 413 is classified separately from other HTTP errors; the payload is halved immediately and retried. It is not repeated unchanged at the HTTP layer.

HTTP 429, transient 5xx, timeout, and connection failures use bounded exponential backoff with full jitter. Numeric `Retry-After` is propagated to the provider orchestrator. After the tier's retry allowance is exhausted, the request moves to the next provider. Concurrency and retry budgets are isolated per host so one degraded provider cannot create a system-wide retry storm.

## Freshness proof

Dates are normalized to timezone-aware UTC. An item is accepted only when its age is between negative five minutes and 24 hours. Structured API timestamps and feed timestamps receive full confidence. Relative timestamps are anchored to fetch time and accepted only above the configured confidence threshold.

For a source without a usable timestamp, first-seen evidence is accepted only after a completed prior scan inside the 24-hour window, when the stable key was absent and source ordering places the item after the recorded previous head. A cold start or broken ordering remains `UNCERTAIN` and is excluded.

Distributed nodes use stable record keys, unique database constraints, work-item dedupe keys, checkpoints, and expiring leases. This prevents duplicate output even under at-least-once delivery or worker death.

## Entity resolution

Names are normalized with NFKC Unicode normalization, case folding, alphanumeric tokenization, and legal-suffix removal. Exact normalized lookup is O(1). Fuzzy matching uses prefix, token-count, and length blocks, capped at 200 candidates, before `SequenceMatcher` scoring. Scores above the match threshold merge; the review band is logged as `NEEDS_REVIEW`; low scores become `NEW_ENTITY`.

The resolver starts with 50 seed organizations and augments the index with collected startup names. Products and job companies are resolved against that same index. A matched canonical value replaces the normalized output field while `rawEntityName`, `rawStartupName`, or `rawCompany` retains the source spelling. `NEEDS_REVIEW` values are never silently merged.

## Storage strategy

PostgreSQL is the primary database because the workload needs ACID upserts, unique constraints, JSONB schema flexibility, ordered checkpoints, and `FOR UPDATE SKIP LOCKED` work leasing. SQLite implements the same interfaces for reproducible local execution.

Raw responses belong in object storage under their content hash. Keeping raw evidence outside the canonical row allows parser replay, audit, and extractor-version comparison without another source request.

`pgvector` is an optional candidate-generation index for aliases or semantic retrieval. It must never make the final identity decision. A graph database such as Neo4j is an optional, rebuildable projection of canonical nodes and edges for deep traversal. PostgreSQL remains the source of truth so ingestion does not require a distributed transaction across databases.

## 500,000-record scale

The executable reference path streams records and writes 200-record batches. It does not collect the full dataset in memory. GitHub enrichment batches up to 40 repositories per GraphQL request, and entity resolution avoids all-pairs comparison.

The executable topology separates discovery from processing with `produce`, `produce-all`, and `work` commands. Stateless workers claim expiring leases and autoscale on queue lag. Source partitions and checkpoints make large directory scans resumable. Database writes use bounded transactions, raw bodies go to object storage, and dead letters preserve the error plus raw evidence.

Scale comes from adding workers, source partitions, database IOPS, and object-storage throughput. Per-domain policies remain fixed; the system does not gain throughput by violating a source's rate limit.

## Protected sources

The source order is official API, RSS/Atom, sitemap, permitted HTTP, and authorized JavaScript rendering. The optional Playwright adapter requires an explicit hostname allowlist, accepts only a user-authorized browser storage state, saves rendered HTML in the raw store, and refuses recognized Cloudflare, DataDome, or CAPTCHA pages. A protected high-value source requires authorized access or a licensed feed. CAPTCHA circumvention is outside the system's safety and compliance boundary.

## Observability and failure recovery

Every run persists requests, retries, failures, block detections, bytes, per-source yields/errors, queue states, LLM candidates/calls, accepted/rejected fields, reconciliation count, and dead-letter volume. The exported run manifest exposes configuration status without secrets. Production should additionally alert on queue lag, oldest work age, freshness rejection reason, token consumption, and entity-review rate.

Because raw bytes and stable keys survive failure, every stage can retry independently. Reprocessing after a parser upgrade reads content hashes rather than revisiting source sites.
