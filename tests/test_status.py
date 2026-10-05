"""Event delivery status aggregation (#18)."""
from __future__ import annotations

from collections import deque

from app.models import Delivery as DS
from app.services.deliveries import DeliveryService
from app.services.events import EventService
from app.services.subscriptions import SubscriptionService
from app.worker.worker import Worker
from tests.conftest import ScriptedSender, failure, success


def _make_event_with_n_subs(context, clock, n, max_attempts=3):
    s = context.session_factory()
    ids = []
    for i in range(n):
        ids.append(SubscriptionService(s, clock).create(f"http://{i}", "evt").id)
    res = EventService(s, clock, max_attempts).create("evt", {})
    s.commit()
    s.close()
    return res.event.id, {d.subscription_id: d.id for d in res.deliveries}


def test_status_counts_match_real_db_state(context, clock, settings):
    event_id, by_sub = _make_event_with_n_subs(context, clock, 4, max_attempts=2)
    delivery_ids = list(by_sub.values())

    def status():
        s = context.session_factory()
        st = DeliveryService(
            s, clock, settings.lease_duration_seconds, settings.base_delay_seconds
        ).event_status(event_id)
        summary = (st.total, st.pending, st.running, st.succeeded, st.failed)
        details = [(d.id, d.status, d.attempts) for d in st.deliveries]
        s.close()
        return summary, details

    assert status()[0] == (4, 4, 0, 0, 0)

    # Two succeed on attempt 1; two fail twice and become permanently failed.
    outcomes = {
        delivery_ids[0]: deque([success()]),
        delivery_ids[1]: deque([success()]),
        delivery_ids[2]: deque([failure(), failure()]),
        delivery_ids[3]: deque([failure(), failure()]),
    }
    worker = Worker(
        context.session_factory,
        clock,
        ScriptedSender(outcomes),
        settings,
        worker_id="w",
        sleeper=lambda _: None,
    )
    worker.tick()  # attempt 1: 2 succeeded, 2 pending(retry, attempts=1)
    mid, _ = status()
    assert mid == (4, 2, 0, 2, 0)

    clock.advance(settings.base_delay_seconds)
    worker.tick()  # attempt 2: the 2 retrying ones exhaust their budget

    summary, details = status()
    assert summary == (4, 0, 0, 2, 2)
    by_id = {d_id: (stt, att) for d_id, stt, att in details}
    assert by_id[delivery_ids[0]] == (DS.SUCCEEDED, 1)
    assert by_id[delivery_ids[2]] == (DS.FAILED, 2)
    assert by_id[delivery_ids[3]] == (DS.FAILED, 2)


def test_status_unknown_event_returns_none(context, clock, settings):
    s = context.session_factory()
    assert (
        DeliveryService(
            s, clock, settings.lease_duration_seconds, settings.base_delay_seconds
        ).event_status("missing")
        is None
    )
    s.close()


def test_running_count_reflects_lease(context, clock, settings):
    event_id, by_sub = _make_event_with_n_subs(context, clock, 1)
    s = context.session_factory()
    DeliveryService(
        s, clock, settings.lease_duration_seconds, settings.base_delay_seconds
    ).claim_one("w")
    s.commit()
    s.close()

    s = context.session_factory()
    st = DeliveryService(
        s, clock, settings.lease_duration_seconds, settings.base_delay_seconds
    ).event_status(event_id)
    assert (st.total, st.pending, st.running) == (1, 0, 1)
    assert st.deliveries[0].lease_owner == "w"
    s.close()
