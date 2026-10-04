from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone


def utcnow() -> datetime:
    # All persisted timestamps are naive UTC so SQLite string comparisons stay consistent.
    return datetime.now(timezone.utc).replace(tzinfo=None)


class SystemClock:
    def now(self) -> datetime:
        return utcnow()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class FakeClock:
    def __init__(self, start: datetime | None = None):
        self._now = start or datetime(2026, 1, 1, 0, 0, 0)

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)
