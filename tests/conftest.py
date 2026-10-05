"""Shared pytest fixtures: temp-file SQLite DB, controllable clock, fake sender."""
from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.container import build_context
from app.main import create_app
from app.worker.sender import SendResult


class FakeClock:
    """Deterministic clock; time only moves when tests advance it."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
        self._lock = threading.Lock()

    def now(self) -> datetime:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> datetime:
        with self._lock:
            self._now = self._now + timedelta(seconds=seconds)
            return self._now

    def set(self, value: datetime) -> None:
        with self._lock:
            self._now = value


class ScriptedSender:
    """Sender whose per-delivery results are scripted by the test.

    ``results`` maps delivery_id -> deque[SendResult|Exception]. It also
    records every attempted call.
    """

    def __init__(self, results: dict[str, deque] | None = None, default=None) -> None:
        self.results = results or {}
        self.default = default if default is not None else SendResult(ok=True, status_code=200)
        self.calls: list[dict] = []
        self._lock = threading.Lock()

    def send(self, url, *, event_id, event_type, delivery_id, payload) -> SendResult:
        with self._lock:
            self.calls.append(
                {
                    "url": url,
                    "event_id": event_id,
                    "event_type": event_type,
                    "delivery_id": delivery_id,
                    "payload": payload,
                }
            )
            queue = self.results.get(delivery_id)
            if queue:
                outcome = queue.popleft()
            else:
                outcome = self.default
        # Mirror HttpSender: transport errors become failed SendResults rather
        # than escaping the sender.
        if isinstance(outcome, Exception):
            return SendResult(
                ok=False,
                status_code=None,
                error=f"{type(outcome).__name__}: {outcome}",
            )
        return outcome


def make_settings(db_path: Path, **overrides) -> Settings:
    defaults = dict(
        database_url=f"sqlite:///{db_path.as_posix()}",
        lease_duration_seconds=30.0,
        base_delay_seconds=5.0,
        max_attempts=5,
        request_timeout_seconds=5.0,
        worker_idle_seconds=0.01,
        worker_batch_size=10,
        run_worker_in_api=False,
    )
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path / "test.db")


@pytest.fixture
def context(settings: Settings, clock: FakeClock):
    ctx = build_context(settings, clock)
    yield ctx
    ctx.engine.dispose()


@pytest.fixture
def session(context):
    s = context.session_factory()
    yield s
    s.close()


@pytest.fixture
def svc(session, clock, settings):
    """Bundle of all three services sharing one session."""
    from app.services.deliveries import DeliveryService
    from app.services.events import EventService
    from app.services.subscriptions import SubscriptionService

    class Services:
        subscriptions = SubscriptionService(session, clock)
        events = EventService(session, clock, settings.max_attempts)
        deliveries = DeliveryService(
            session,
            clock,
            settings.lease_duration_seconds,
            settings.base_delay_seconds,
        )

    return Services()


@pytest.fixture
def client(context):
    app = create_app(context=context, start_worker=False)
    with TestClient(app) as c:
        yield c


def make_worker(context, settings, sender, worker_id=None, clock=None):
    from app.worker.worker import Worker

    return Worker(
        context.session_factory,
        clock or context.clock,
        sender,
        settings,
        worker_id=worker_id,
        sleeper=lambda _s: None,
    )


@pytest.fixture
def worker_factory(context, settings):
    def _factory(sender, worker_id=None, clock=None):
        return make_worker(context, settings, sender, worker_id, clock)

    return _factory


def failure(error: str = "HTTP 500: boom", code: int = 500) -> SendResult:
    return SendResult(ok=False, status_code=code, error=error)


def success(code: int = 200) -> SendResult:
    return SendResult(ok=True, status_code=code)
