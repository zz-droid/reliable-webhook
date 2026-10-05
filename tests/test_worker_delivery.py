"""Worker delivery behaviour: success/failure, retries, backoff, cap (#10-#16)."""
from __future__ import annotations

from collections import deque

import httpx
import pytest

from app.models import Delivery as DS
from app.models import DeliveryRow
from app.services.events import EventService
from app.services.subscriptions import SubscriptionService
from app.worker.sender import SendResult
from tests.conftest import ScriptedSender, failure, success


def _seed_event(context, clock, max_attempts=5, url="http://endpoint/hook"):
    session = context.session_factory()
    SubscriptionService(session, clock).create(url, "evt")
    result = EventService(session, clock, max_attempts).create("evt", {"hello": "world"})
    session.commit()
    event_id = result.event.id
    delivery_id = result.deliveries[0].id
    session.close()
    return event_id, delivery_id


def _get_row(context, delivery_id):
    s = context.session_factory()
    row = s.get(DeliveryRow, delivery_id)
    # detach simple copies
    info = {
        "status": row.status,
        "attempts": row.attempts,
        "last_error": row.last_error,
        "next_attempt_at": row.next_attempt_at,
        "lease_owner": row.lease_owner,
        "max_attempts": row.max_attempts,
    }
    s.close()
    return info


def test_http_2xx_marks_succeeded_and_includes_envelope(
    context, clock, settings, worker_factory
):
    """#10 + delivery request contains all required fields."""
    event_id, delivery_id = _seed_event(context, clock)
    sender = ScriptedSender()
    worker = worker_factory(sender, worker_id="w")

    processed = worker.tick()
    assert processed == 1
    row = _get_row(context, delivery_id)
    assert row["status"] == DS.SUCCEEDED
    assert row["attempts"] == 1
    assert row["lease_owner"] is None

    call = sender.calls[0]
    assert call["event_id"] == event_id
    assert call["event_type"] == "evt"
    assert call["delivery_id"] == delivery_id
    assert call["payload"] == {"hello": "world"}
    assert call["url"].startswith("http://endpoint")


@pytest.mark.parametrize("code", [200, 201, 204, 299])
def test_all_2xx_treated_success(context, clock, settings, worker_factory, code):
    _, delivery_id = _seed_event(context, clock)
    sender = ScriptedSender(default=SendResult(ok=True, status_code=code))
    worker_factory(sender).tick()
    assert _get_row(context, delivery_id)["status"] == DS.SUCCEEDED


@pytest.mark.parametrize("code", [301, 400, 404, 500, 503])
def test_non_2xx_enters_retry(context, clock, settings, worker_factory, code):
    """#11."""
    _, delivery_id = _seed_event(context, clock)
    sender = ScriptedSender(
        default=SendResult(ok=False, status_code=code, error=f"HTTP {code}")
    )
    worker_factory(sender).tick()
    row = _get_row(context, delivery_id)
    assert row["status"] == DS.PENDING
    assert row["attempts"] == 1
    assert "HTTP" in row["last_error"]
    assert row["lease_owner"] is None


def test_network_exception_does_not_crash_worker(context, clock, settings, tmp_path):
    """#12: connect errors/timeouts are recorded, worker keeps processing."""
    from tests.conftest import make_settings
    from app.worker.worker import Worker

    # Long lease so the failed delivery cannot be immediately reclaimed within
    # the same batch tick.
    long_lease = make_settings(
        tmp_path / "net.db", lease_duration_seconds=3600.0, base_delay_seconds=5.0
    )
    from app.db import make_engine, make_session_factory, init_db

    engine = make_engine(long_lease.database_url)
    init_db(engine)
    factory = make_session_factory(engine)
    # Re-seed subscriptions/events in this second database.
    from app.services.events import EventService
    from app.services.subscriptions import SubscriptionService

    s = factory()
    SubscriptionService(s, clock).create("http://broken/hook", "evt.fail")
    SubscriptionService(s, clock).create("http://good/hook", "evt.ok")
    r1 = EventService(s, clock, 5).create("evt.fail", {})
    r2 = EventService(s, clock, 5).create("evt.ok", {})
    s.commit()
    fail_id = r1.deliveries[0].id
    ok_id = r2.deliveries[0].id
    s.close()

    outcomes = {fail_id: deque([httpx.ConnectError("boom")])}
    sender = ScriptedSender(outcomes)
    worker = Worker(factory, clock, sender, long_lease, worker_id="w")

    # The exception must not escape; batch processes both deliveries once.
    processed = worker.tick()
    assert processed == 2
    check = factory()
    fr = check.get(DeliveryRow, fail_id)
    gr = check.get(DeliveryRow, ok_id)
    assert fr.status == DS.PENDING
    assert "ConnectError" in fr.last_error
    assert fr.attempts == 1
    assert gr.status == DS.SUCCEEDED
    check.close()
    engine.dispose()


def test_attempt_counter_increments(context, clock, settings, worker_factory):
    """#13."""
    _, delivery_id = _seed_event(context, clock)
    sender = ScriptedSender(
        default=SendResult(ok=False, status_code=500, error="HTTP 500")
    )
    worker = worker_factory(sender)

    worker.tick()
    assert _get_row(context, delivery_id)["attempts"] == 1
    # not due yet -> nothing claimed
    assert worker.tick() == 0

    clock.advance(5)  # base delay for attempt 1
    worker.tick()
    assert _get_row(context, delivery_id)["attempts"] == 2

    clock.advance(10)  # delay for attempt 2
    worker.tick()
    assert _get_row(context, delivery_id)["attempts"] == 3


def test_exponential_backoff_schedule(context, clock, settings, worker_factory):
    """#14: next_attempt_at follows base * 2**(attempt-1)."""
    start = clock.now()
    _, delivery_id = _seed_event(context, clock)
    sender = ScriptedSender(default=failure())
    worker = worker_factory(sender)

    worker.tick()
    row1 = _get_row(context, delivery_id)
    assert (row1["next_attempt_at"] - start).total_seconds() == 5.0

    clock.advance(5)
    worker.tick()
    row2 = _get_row(context, delivery_id)
    assert (row2["next_attempt_at"] - start).total_seconds() == 15.0  # 5 then 10

    clock.advance(10)
    worker.tick()
    row3 = _get_row(context, delivery_id)
    assert (row3["next_attempt_at"] - start).total_seconds() == 35.0  # +20


def test_delivery_not_claimable_before_next_attempt(context, clock, settings, worker_factory):
    _, delivery_id = _seed_event(context, clock)
    sender = ScriptedSender(default=failure())
    worker = worker_factory(sender)
    worker.tick()

    clock.advance(4.999)
    assert worker.tick() == 0
    assert _get_row(context, delivery_id)["status"] == DS.PENDING


def test_max_attempts_exhausted_permanently_failed(context, clock, settings, worker_factory):
    """#15."""
    _, delivery_id = _seed_event(context, clock, max_attempts=3)
    sender = ScriptedSender(default=failure())
    worker = worker_factory(sender)

    worker.tick()          # attempt 1 -> retry at +5
    clock.advance(5)
    worker.tick()          # attempt 2 -> retry at +15
    clock.advance(10)
    worker.tick()          # attempt 3 -> permanently failed

    row = _get_row(context, delivery_id)
    assert row["status"] == DS.FAILED
    assert row["attempts"] == 3
    assert row["lease_owner"] is None


def test_permanently_failed_never_claimed_again(context, clock, settings, worker_factory):
    """#16."""
    _, delivery_id = _seed_event(context, clock, max_attempts=1)
    sender = ScriptedSender(default=failure())
    worker = worker_factory(sender)
    worker.tick()
    assert _get_row(context, delivery_id)["status"] == DS.FAILED

    # Far in the future it must still be ignored.
    clock.advance(60 * 60 * 24 * 30)
    assert worker.tick() == 0
    assert _get_row(context, delivery_id)["status"] == DS.FAILED


def test_succeeded_never_claimed_again(context, clock, settings, worker_factory):
    _, delivery_id = _seed_event(context, clock)
    worker_factory(ScriptedSender(default=success())).tick()
    clock.advance(60 * 60 * 24)
    assert worker_factory(ScriptedSender(default=success())).tick() == 0


def test_event_with_no_matching_subscriptions_has_no_deliveries(context, clock):
    s = context.session_factory()
    SubscriptionService(s, clock).create("http://a", "other")
    res = EventService(s, clock, 5).create("evt", {})
    s.commit()
    assert res.deliveries == []
    s.close()
