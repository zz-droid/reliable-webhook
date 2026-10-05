"""Idempotency: sequential reuse and true concurrent races (#4, #5)."""
from __future__ import annotations

import threading

from sqlalchemy.exc import IntegrityError

from app.models import DeliveryRow, Event
from app.services.events import EventService
from app.services.subscriptions import SubscriptionService


def test_duplicate_idempotency_key_returns_same_event(svc, session):
    svc.subscriptions.create("http://a", "evt")
    session.commit()

    first = svc.events.create("evt", {"v": 1}, idempotency_key="key-1")
    session.commit()
    second = svc.events.create("evt", {"v": 1}, idempotency_key="key-1")
    session.commit()

    assert first.created is True
    assert second.created is False
    assert second.event.id == first.event.id

    # No duplicate deliveries
    assert session.query(DeliveryRow).count() == 1
    assert session.query(Event).count() == 1


def test_events_without_key_are_always_distinct(svc, session):
    r1 = svc.events.create("evt", {})
    r2 = svc.events.create("evt", {})
    session.commit()
    assert r1.event.id != r2.event.id


def test_concurrent_same_idempotency_key_only_one_event(context, settings, clock):
    """Two threads in separate sessions race with the same key simultaneously.

    The DB unique constraint guarantees a single event and single set of
    deliveries; losers must observe the winner's event instead of crashing.
    """
    # Seed matching subscriptions first.
    setup = context.session_factory()
    SubscriptionService(setup, clock).create("http://a", "evt")
    SubscriptionService(setup, clock).create("http://b", "evt")
    setup.commit()
    setup.close()

    results: list[str] = []
    barrier = threading.Barrier(2)
    errors: list[Exception] = []

    def worker():
        session = context.session_factory()
        try:
            svc = EventService(session, clock, settings.max_attempts)
            barrier.wait()
            res = svc.create("evt", {"v": 1}, idempotency_key="race-key")
            session.commit()
            results.append(res.event.id)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            session.close()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], errors
    assert len(results) == 2
    assert results[0] == results[1]

    check = context.session_factory()
    assert check.query(Event).filter(Event.idempotency_key == "race-key").count() == 1
    assert check.query(DeliveryRow).count() == 2
    check.close()


def test_idempotency_constraint_exists_at_database_level(context, settings, clock):
    """A raw second insert with the same key must raise IntegrityError."""
    session = context.session_factory()
    svc = EventService(session, clock, settings.max_attempts)
    svc.create("evt", {}, idempotency_key="unique-x")
    session.commit()

    duplicate = Event(
        event_type="evt",
        payload={},
        idempotency_key="unique-x",
        created_at=clock.now(),
    )
    session.add(duplicate)
    try:
        with __import__("pytest").raises(IntegrityError):
            session.flush()
    finally:
        session.rollback()
        session.close()
