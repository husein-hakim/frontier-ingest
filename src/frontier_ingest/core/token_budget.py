from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RankedFragment:
    text: str
    score: float
    estimated_tokens: int


def estimate_tokens(text: str) -> int:
    # Conservative dependency-free estimate. Provider tokenizers replace this in production.
    return max(1, (len(text) + 2) // 3)


def select_fragments(text: str, keywords: set[str], token_budget: int) -> list[RankedFragment]:
    """Choose complete paragraphs with the highest task relevance."""
    if token_budget <= 0:
        return []
    lowered_keywords = {word.lower() for word in keywords}
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    ranked: list[RankedFragment] = []
    for position, paragraph in enumerate(paragraphs):
        words = set(re.findall(r"[a-z0-9_+-]+", paragraph.lower()))
        overlap = len(words & lowered_keywords)
        heading_bonus = 0.5 if len(paragraph) < 120 else 0.0
        early_bonus = 1.0 / (position + 1)
        ranked.append(
            RankedFragment(
                paragraph, overlap * 3 + heading_bonus + early_bonus, estimate_tokens(paragraph)
            )
        )
    selected: list[RankedFragment] = []
    used = 0
    for fragment in sorted(ranked, key=lambda item: item.score, reverse=True):
        if fragment.estimated_tokens > token_budget - used:
            continue
        selected.append(fragment)
        used += fragment.estimated_tokens
    return selected
