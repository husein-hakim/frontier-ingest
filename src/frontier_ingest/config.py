from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str = "sqlite:///data/frontier.db"
    raw_store_path: Path = Path("data/raw")
    output_path: Path = Path("outputs")
    user_agent: str = "frontier-ingest/1.0 interview-assessment"
    github_token: str | None = None
    huggingface_token: str | None = None
    product_hunt_token: str | None = None
    canonical_entities_path: Path = Path("configs/canonical_entities.json")
    enable_llm_enrichment: bool = False
    llm_max_calls_per_run: int = 200
    llm_retries_per_provider: int = 2
    write_batch_size: int = 200
    primary_llm_api_key: str | None = None
    primary_llm_base_url: str | None = None
    primary_llm_model: str | None = None
    secondary_llm_api_key: str | None = None
    secondary_llm_base_url: str | None = None
    secondary_llm_model: str | None = None
    tertiary_llm_api_key: str | None = None
    tertiary_llm_base_url: str | None = None
    tertiary_llm_model: str | None = None
    request_timeout_seconds: float = 45.0
    default_host_concurrency: int = 4
    max_attempts: int = 4

    @classmethod
    def from_env(cls) -> Settings:
        defaults = cls()
        primary_key = os.getenv("PRIMARY_LLM_API_KEY") or None
        enable_llm = os.getenv(
            "ENABLE_LLM_ENRICHMENT", "true" if primary_key else "false"
        ).strip().casefold() in {"1", "true", "yes", "on"}
        return cls(
            database_url=os.getenv("DATABASE_URL", defaults.database_url),
            raw_store_path=Path(os.getenv("RAW_STORE_PATH", str(defaults.raw_store_path))),
            output_path=Path(os.getenv("OUTPUT_PATH", str(defaults.output_path))),
            user_agent=os.getenv("HTTP_USER_AGENT", defaults.user_agent),
            github_token=os.getenv("GITHUB_TOKEN") or None,
            huggingface_token=os.getenv("HUGGINGFACE_TOKEN") or None,
            product_hunt_token=os.getenv("PRODUCT_HUNT_TOKEN") or None,
            canonical_entities_path=Path(
                os.getenv("CANONICAL_ENTITIES_PATH", str(defaults.canonical_entities_path))
            ),
            enable_llm_enrichment=enable_llm,
            llm_max_calls_per_run=int(os.getenv("LLM_MAX_CALLS_PER_RUN", "200")),
            llm_retries_per_provider=int(os.getenv("LLM_RETRIES_PER_PROVIDER", "2")),
            write_batch_size=int(os.getenv("WRITE_BATCH_SIZE", "200")),
            primary_llm_api_key=primary_key,
            primary_llm_base_url=os.getenv("PRIMARY_LLM_BASE_URL") or None,
            primary_llm_model=os.getenv("PRIMARY_LLM_MODEL") or None,
            secondary_llm_api_key=os.getenv("SECONDARY_LLM_API_KEY") or None,
            secondary_llm_base_url=os.getenv("SECONDARY_LLM_BASE_URL") or None,
            secondary_llm_model=os.getenv("SECONDARY_LLM_MODEL") or None,
            tertiary_llm_api_key=os.getenv("TERTIARY_LLM_API_KEY") or None,
            tertiary_llm_base_url=os.getenv("TERTIARY_LLM_BASE_URL") or None,
            tertiary_llm_model=os.getenv("TERTIARY_LLM_MODEL") or None,
            request_timeout_seconds=float(os.getenv("REQUEST_TIMEOUT_SECONDS", "45")),
            default_host_concurrency=int(os.getenv("DEFAULT_HOST_CONCURRENCY", "4")),
            max_attempts=int(os.getenv("MAX_ATTEMPTS", "4")),
        )
