from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum


class FreshnessStatus(StrEnum):
    VERIFIED = "VERIFIED"
    INFERRED = "INFERRED"
    UNCERTAIN = "UNCERTAIN"
    STALE = "STALE"


@dataclass(frozen=True, slots=True)
class FreshnessDecision:
    status: FreshnessStatus
    published_at: datetime | None
    method: str
    confidence: float
    accepted: bool


def parse_timestamp(value: str, fetched_at: datetime) -> tuple[datetime | None, str, float]:
    """Parse reliable ISO timestamps and bounded English relative timestamps.

    Ambiguous calendar dates are deliberately not guessed. Source-specific adapters
    can add explicitly configured formats and timezones.
    """
    text = value.strip()
    if not text:
        return None, "missing", 0.0
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return None, "iso_without_timezone", 0.0
        return parsed.astimezone(UTC), "iso_8601", 1.0
    except ValueError:
        pass

    match = re.fullmatch(r"(?:about\s+)?(\d+)\s+(minute|hour)s?\s+ago", text.casefold())
    if match:
        amount = int(match.group(1))
        unit = match.group(2)
        delta = timedelta(minutes=amount) if unit == "minute" else timedelta(hours=amount)
        return fetched_at.astimezone(UTC) - delta, f"relative_{unit}", 0.9
    return None, "unparsed", 0.0


def decide_freshness(
    published_at: datetime | None,
    fetched_at: datetime,
    method: str,
    confidence: float,
    window: timedelta = timedelta(hours=24),
) -> FreshnessDecision:
    if published_at is None:
        return FreshnessDecision(FreshnessStatus.UNCERTAIN, None, method, confidence, False)
    age = fetched_at.astimezone(UTC) - published_at.astimezone(UTC)
    if age < timedelta(minutes=-5):
        return FreshnessDecision(FreshnessStatus.UNCERTAIN, published_at, method, confidence, False)
    if age > window:
        return FreshnessDecision(FreshnessStatus.STALE, published_at, method, confidence, False)
    status = FreshnessStatus.VERIFIED if confidence >= 0.99 else FreshnessStatus.INFERRED
    return FreshnessDecision(status, published_at, method, confidence, confidence >= 0.85)


def decide_first_seen_freshness(
    *,
    fetched_at: datetime,
    previous_scan_at: datetime | None,
    seen_in_previous_scan: bool,
    appeared_after_previous_head: bool,
    window: timedelta = timedelta(hours=24),
) -> FreshnessDecision:
    """Conservative fallback for sources that publish no usable timestamp.

    A cold start is never enough. Acceptance requires a completed prior scan inside
    the freshness window, a new stable key, and source ordering that places the item
    after the previously recorded head. The fetch time is retained as an upper-bound
    observation time, not misrepresented as an exact publication timestamp.
    """
    fetched_at = fetched_at.astimezone(UTC)
    if previous_scan_at is None or seen_in_previous_scan or not appeared_after_previous_head:
        return FreshnessDecision(
            FreshnessStatus.UNCERTAIN,
            None,
            "first_seen_insufficient_evidence",
            0.0,
            False,
        )
    scan_age = fetched_at - previous_scan_at.astimezone(UTC)
    if scan_age < timedelta(0) or scan_age > window:
        return FreshnessDecision(
            FreshnessStatus.UNCERTAIN,
            None,
            "first_seen_scan_gap",
            0.0,
            False,
        )
    return FreshnessDecision(
        FreshnessStatus.INFERRED,
        fetched_at,
        "first_seen_after_previous_head",
        0.9,
        True,
    )
