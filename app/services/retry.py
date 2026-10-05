"""Pure retry/backoff helpers (no I/O, trivially unit-testable)."""
from __future__ import annotations

from datetime import datetime, timedelta


def backoff_delay_seconds(base_delay_seconds: float, attempt: int) -> float:
    """Exponential backoff for the given 1-based attempt number.

    After attempt #1 the wait is ``base_delay``; after attempt #2 it is
    ``base_delay * 2``; in general ``base_delay * 2 ** (attempt - 1)``.
    """
    if attempt < 1:
        raise ValueError("attempt must be 1-based and >= 1")
    return base_delay_seconds * (2 ** (attempt - 1))


def next_attempt_time(
    now: datetime, base_delay_seconds: float, attempt: int
) -> datetime:
    return now + timedelta(seconds=backoff_delay_seconds(base_delay_seconds, attempt))
