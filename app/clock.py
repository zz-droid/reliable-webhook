"""Injectable clock abstraction.

Time-dependent logic (lease expiry, retry scheduling) depends only on a
``Clock`` so tests can advance time deterministically instead of sleeping.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """Return current time as a timezone-aware UTC datetime."""


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)
