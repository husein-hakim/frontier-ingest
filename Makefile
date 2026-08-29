.PHONY: install test smoke collect export quality

install:
	python3 -m venv .venv
	.venv/bin/pip install -e ".[dev,postgres]"

test:
	.venv/bin/python -m pytest -q

smoke:
	.venv/bin/frontier-ingest collect startups --limit 3
	.venv/bin/frontier-ingest collect products --limit 3
	.venv/bin/frontier-ingest collect papers --limit 3
	.venv/bin/frontier-ingest collect jobs --limit 10
	.venv/bin/frontier-ingest collect news --limit 10
	.venv/bin/frontier-ingest quality --allow-small-sample

collect:
	.venv/bin/frontier-ingest collect-all --catalog-limit 1000 --signal-limit 500

export:
	.venv/bin/frontier-ingest export --output outputs/export

quality:
	.venv/bin/frontier-ingest quality
