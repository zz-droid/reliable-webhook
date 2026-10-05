"""Pure backoff calculation tests (requirement #14)."""
import pytest

from app.services.retry import backoff_delay_seconds, next_attempt_time
from datetime import datetime, timezone


def test_backoff_doubles_each_attempt():
    base = 5.0
    assert backoff_delay_seconds(base, 1) == 5.0
    assert backoff_delay_seconds(base, 2) == 10.0
    assert backoff_delay_seconds(base, 3) == 20.0
    assert backoff_delay_seconds(base, 4) == 40.0
    assert backoff_delay_seconds(base, 5) == 80.0


def test_backoff_base_delay_scaling():
    assert backoff_delay_seconds(2.0, 1) == 2.0
    assert backoff_delay_seconds(2.0, 3) == 8.0


def test_backoff_rejects_non_positive_attempt():
    with pytest.raises(ValueError):
        backoff_delay_seconds(5.0, 0)


def test_next_attempt_time_adds_delay():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    nxt = next_attempt_time(now, 5.0, 2)
    assert (nxt - now).total_seconds() == 10.0
