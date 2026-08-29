from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

from bs4 import BeautifulSoup


def html_to_text(value: str | bytes) -> str:
    soup = BeautifulSoup(value, "html.parser")
    for node in soup(["script", "style", "noscript", "svg", "nav", "footer", "form"]):
        node.decompose()
    container = soup.find("article") or soup.find("main") or soup.body or soup
    text = container.get_text("\n", strip=True)
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def extract_json_ld(html_body: str | bytes) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html_body, "html.parser")
    results: list[dict[str, Any]] = []
    for node in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = node.string or node.get_text()
        if not raw.strip():
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            graph = candidate.get("@graph")
            if isinstance(graph, list):
                results.extend(item for item in graph if isinstance(item, dict))
            else:
                results.append(candidate)
    return results


def parse_absolute_timestamp(value: object, assume_utc: bool = False) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, int | float):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (ValueError, OSError, OverflowError):
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(text)
        except (TypeError, ValueError, OverflowError):
            return None
    if parsed.tzinfo is None:
        if not assume_utc:
            return None
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def is_ai_relevant(title: str, description: str = "") -> bool:
    haystack = f"{title}\n{description[:5_000]}".casefold()
    phrases = (
        "artificial intelligence",
        "machine learning",
        "deep learning",
        "generative ai",
        "large language model",
        "llm",
        "computer vision",
        "nlp",
        "natural language processing",
        "applied scientist",
        "data scientist",
        "ml engineer",
        "ai engineer",
        "research scientist",
    )
    return any(phrase in haystack for phrase in phrases)


def classify_role_family(title: str) -> str:
    lowered = title.casefold()
    rules = (
        ("Research", ("research", "scientist")),
        ("Data", ("data scientist", "analytics", "data engineer")),
        ("Product", ("product manager", "product designer")),
        ("Engineering", ("engineer", "developer", "architect")),
        ("Sales", ("sales", "account executive", "business development")),
        ("Marketing", ("marketing", "growth")),
    )
    for family, markers in rules:
        if any(marker in lowered for marker in markers):
            return family
    return "Other"


def infer_remote(location: str, explicit: object | None = None) -> bool:
    if isinstance(explicit, bool):
        return explicit
    return "remote" in location.casefold()
