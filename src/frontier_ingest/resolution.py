from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher

LEGAL_SUFFIXES = {
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "ltd",
    "llc",
    "limited",
}


def normalize_entity_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    tokens = re.findall(r"[a-z0-9]+", normalized)
    while tokens and tokens[-1] in LEGAL_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


@dataclass(frozen=True, slots=True)
class Resolution:
    raw_name: str
    canonical_name: str | None
    method: str
    confidence: float
    status: str


class EntityResolver:
    def __init__(
        self,
        canonical_names: list[str],
        match_threshold: float = 0.92,
        review_threshold: float = 0.82,
        max_fuzzy_candidates: int = 200,
    ) -> None:
        self.canonical_names: list[str] = []
        self.match_threshold = match_threshold
        self.review_threshold = review_threshold
        self.max_fuzzy_candidates = max(10, max_fuzzy_candidates)
        self.index: dict[str, str] = {}
        self.blocks: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for name in canonical_names:
            self.add_canonical(name)

    @staticmethod
    def _block_keys(normalized: str) -> set[str]:
        compact = normalized.replace(" ", "")
        tokens = normalized.split()
        if not compact:
            return set()
        return {
            f"prefix:{compact[:4]}",
            f"initial:{compact[0]}:{len(compact) // 4}",
            f"tokens:{len(tokens)}:{compact[:2]}",
        }

    def add_canonical(self, name: str) -> None:
        normalized = normalize_entity_name(name)
        if not normalized or normalized in self.index:
            return
        self.canonical_names.append(name)
        self.index[normalized] = name
        for key in self._block_keys(normalized):
            self.blocks[key].append((normalized, name))

    def resolve(self, raw_name: str) -> Resolution:
        normalized = normalize_entity_name(raw_name)
        if normalized in self.index:
            return Resolution(raw_name, self.index[normalized], "normalized_exact", 1.0, "MATCHED")
        blocked: dict[str, str] = {}
        for block_key in self._block_keys(normalized):
            for candidate_key, canonical in self.blocks.get(block_key, []):
                blocked.setdefault(candidate_key, canonical)
                if len(blocked) >= self.max_fuzzy_candidates:
                    break
            if len(blocked) >= self.max_fuzzy_candidates:
                break
        candidates = [
            (SequenceMatcher(None, normalized, key).ratio(), canonical)
            for key, canonical in blocked.items()
        ]
        if not candidates:
            return Resolution(raw_name, None, "none", 0.0, "NEW_ENTITY")
        score, canonical = max(candidates)
        if score >= self.match_threshold:
            return Resolution(raw_name, canonical, "name_similarity", score, "MATCHED")
        if score >= self.review_threshold:
            return Resolution(raw_name, canonical, "name_similarity", score, "NEEDS_REVIEW")
        return Resolution(raw_name, None, "none", score, "NEW_ENTITY")
