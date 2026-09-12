# Submission checklist

This checklist is intentionally fail-closed. Do not submit a workbook whose strict quality command is red.

## 1. Refresh dynamic evidence

Set a GitHub token with read-only public repository access, then refresh all 1,000 paper metrics directly:

```bash
export GITHUB_TOKEN=...
DATABASE_URL=sqlite:///data/final-submission.db .venv/bin/frontier-ingest refresh-github
```

Immediately before submitting, recollect the two 24-hour lanes:

```bash
DATABASE_URL=sqlite:///data/final-submission.db RAW_STORE_PATH=data/final-raw \
  .venv/bin/frontier-ingest refresh-signals --limit 500
```

## 2. Prove the system

```bash
DATABASE_URL=sqlite:///data/final-submission.db \
  .venv/bin/frontier-ingest llm-demo --output outputs/submission/llm-resilience-demo.json
.venv/bin/python -m pytest -q
.venv/bin/ruff check src tests
.venv/bin/frontier-ingest benchmark-scale --records 500000 --batch-size 200
DATABASE_URL=sqlite:///data/final-submission.db RAW_STORE_PATH=data/final-raw \
  .venv/bin/frontier-ingest render-demo --output reports/rendered-source-demo.json
DATABASE_URL=sqlite:///data/final-submission.db .venv/bin/frontier-ingest quality
```

The final command must print `"submission_ready": true`. It verifies exact camelCase envelope fields, 1,000 catalog records in each vertical, unique source URLs, evidence coverage, genuine product identity, direct/current GitHub metrics, and signal freshness.

## 3. Re-export artifacts

```bash
DATABASE_URL=sqlite:///data/final-submission.db \
  .venv/bin/frontier-ingest export --output outputs/submission
```

Rebuild the six-tab workbook with `tools/build_submission_workbook.mjs`, inspect its six rendered previews, and import the `.xlsx` as a native Google Sheet. Set link access to public view-only.

## 4. Publish source

Create a public GitHub repository without committing `.env`, raw bodies, databases, or temporary previews. Include the source tree, tests, configuration seeds, README, technical design, `architecture.pdf`, and the final non-secret reports.

Suggested final reviewer commands:

```bash
python -m pytest -q
frontier-ingest llm-demo
docker compose up --build --scale worker=4
```

## 5. Final links

Paste both public URLs into the application form and verify them in a private/incognito window:

- Public Google Sheet: https://docs.google.com/spreadsheets/d/159EFQ28ttxGrgvw5fMqtFggEC19GTCVi3nFKil0DFuo/edit
- Public GitHub repository: https://github.com/husein-hakim/frontier-ingest
