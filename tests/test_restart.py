"""Process-restart recovery semantics (#17).

Restart is simulated by disposing engine state and building brand new
sessions/workers against the same file-backed database, with the clock
preserved so lease timing is deterministic.
"""
from __future__ import annotations

from app.models import Delivery as DS
from app.services.deliveries import DeliveryService
from app.services.events import EventService
from app.services.subscriptions import SubscriptionService
from app.worker.worker import Worker
from tests.conftest import ScriptedSender, failure, success
from app.db import make_engine, make_session_factory


def _seed(context, clock, max_attempts=5):
    s = context.session_factory()
    SubscriptionService(s, clock).create("http://endpoint", "evt")
    res = EventService(s, clock, max_attempts).create("evt", {"v": 1})
    s.commit()
    delivery_id = res.deliveries[0].id
    s.close()
    return delivery_id


def _new_worker(settings, clock, sender, owner):
    # A fresh engine/session factory is the closest in-process analogue to a
    # restarted process sharing only the database file.
    engine = make_engine(settings.database_url)
    factory = make_session_factory(engine)
    worker = Worker(factory, clock, sender, settings, worker_id=owner, sleeper=lambda _: None)
    return worker, engine


def test_restart_keeps_succeeded(context, clock, settings):
    delivery_id = _seed(context, clock)
    w1, e1 = _new_worker(settings, clock, ScriptedSender(default=success()), "w1")
    w1.tick()
    e1.dispose()

    w2, e2 = _new_worker(settings, clock, ScriptedSender(default=success()), "w2")
    assert w2.tick() == 0  # succeeded is never redelivered
    e2.dispose()

    s = context.session_factory()
    from app.models import DeliveryRow

    assert s.get(DeliveryRow, delivery_id).status == DS.SUCCEEDED
    s.close()


def test_restart_keeps_permanently_failed(context, clock, settings):
    delivery_id = _seed(context, clock, max_attempts=1)
    w1, e1 = _new_worker(settings, clock, ScriptedSender(default=failure()), "w1")
    w1.tick()
    e1.dispose()

    clock.advance(60 * 60 * 24)
    w2, e2 = _new_worker(settings, clock, ScriptedSender(default=success()), "w2")
    assert w2.tick() == 0
    e2.dispose()

    from app.models import DeliveryRow

    s = context.session_factory()
    assert s.get(DeliveryRow, delivery_id).status == DS.FAILED
    s.close()


def test_restart_preserves_retry_schedule(context, clock, settings):
    """Waiting-for-retry delivery keeps its exact next_attempt_at after restart."""
    from app.models import DeliveryRow

    delivery_id = _seed(context, clock)
    w1, e1 = _new_worker(settings, clock, ScriptedSender(default=failure()), "w1")
    w1.tick()
    e1.dispose()

    s = context.session_factory()
    scheduled = s.get(DeliveryRow, delivery_id).next_attempt_at
    s.close()

    # Restart before the scheduled time: not claimable
    clock.advance(4)
    w2, e2 = _new_worker(settings, clock, ScriptedSender(default=success()), "w2")
    assert w2.tick() == 0
    e2.dispose()

    s = context.session_factory()
    assert s.get(DeliveryRow, delivery_id).next_attempt_at == scheduled
    s.close()

    # After the scheduled time passes it becomes claimable again
    clock.advance(2)
    w3, e3 = _new_worker(settings, clock, ScriptedSender(default=success()), "w3")
    assert w3.tick() == 1
    e3.dispose()


def test_restart_does_not_reset_running_with_valid_lease(context, clock, settings):
    """No blanket running->pending reset on startup; fresh lease stays protected."""
    from app.models import DeliveryRow

    delivery_id = _seed(context, clock)
    s = context.session_factory()
    DeliveryService(s, clock, settings.lease_duration_seconds, 5).claim_one("dead-worker")
    s.close()

    clock.advance(5)  # lease still valid (30s)
    w, e = _new_worker(settings, clock, ScriptedSender(default=success()), "new")
    assert w.tick() == 0
    e.dispose()

    s = context.session_factory()
    row = s.get(DeliveryRow, delivery_id)
    assert row.status == DS.RUNNING
    assert row.lease_owner == "dead-worker"
    s.close()


def test_restart_reclaims_expired_running_lease(context, clock, settings):
    """An expired lease left by a crashed worker is reclaimable after restart."""
    from app.models import DeliveryRow

    delivery_id = _seed(context, clock)
    s = context.session_factory()
    DeliveryService(
        s, clock, settings.lease_duration_seconds, 5
    ).claim_one("crashed")
    s.close()

    clock.advance(settings.lease_duration_seconds + 1)
    w, e = _new_worker(settings, clock, ScriptedSender(default=success()), "recovered")
    assert w.tick() == 1
    e.dispose()

    s = context.session_factory()
    row = s.get(DeliveryRow, delivery_id)
    assert row.status == DS.SUCCEEDED
    s.close()


def test_no_startup_running_reset_even_with_many(context, clock, settings, tmp_path):
    """Explicitly verify init_db() does not touch running rows."""
    from app.models import DeliveryRow

    delivery_id = _seed(context, clock)
    s = context.session_factory()
    DeliveryService(s, clock, 30, 5).claim_one("owner")
    s.close()

    # Re-run schema initialisation against the same DB (what startup does).
    engine = make_engine(settings.database_url)
    from app.db import init_db

    init_db(engine)  # create_all is a no-op on existing schema
    factory = make_session_factory(engine)
    check = factory()
    row = check.get(DeliveryRow, delivery_id)
    assert row.status == DS.RUNNING
    assert row.lease_owner == "owner"
    check.close()
    engine.dispose()
