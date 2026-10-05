"""Lease / claim semantics and stale-worker fencing (#6, #7, #8, #9)."""
from __future__ import annotations

import threading

from app.models import Delivery as DS
from app.services.deliveries import DeliveryService
from app.services.events import EventService
from app.services.subscriptions import SubscriptionService


def _create_pending_delivery(context, clock, max_attempts=5):
    session = context.session_factory()
    SubscriptionService(session, clock).create("http://endpoint", "evt")
    result = EventService(session, clock, max_attempts).create("evt", {"v": 1})
    session.commit()
    delivery_id = result.deliveries[0].id
    session.close()
    return delivery_id


def test_claim_returns_due_delivery_and_marks_running(context, clock, settings):
    delivery_id = _create_pending_delivery(context, clock)
    session = context.session_factory()
    svc = DeliveryService(
        session, clock, settings.lease_duration_seconds, settings.base_delay_seconds
    )
    claimed = svc.claim_one("worker-A")
    assert claimed is not None
    assert claimed.id == delivery_id
    assert claimed.lease_owner == "worker-A"
    assert claimed.lease_generation == 1
    session.close()

    check = context.session_factory()
    row = check.get(__import__("app.models", fromlist=["DeliveryRow"]).DeliveryRow, delivery_id)
    assert row.status == DS.RUNNING
    assert row.lease_owner == "worker-A"
    assert row.lease_expires_at is not None
    check.close()


def test_valid_lease_blocks_other_workers(context, clock, settings):
    """#7: while a lease is valid no other worker can grab the delivery."""
    _create_pending_delivery(context, clock)

    s1 = context.session_factory()
    s2 = context.session_factory()
    svc1 = DeliveryService(s1, clock, 30, 5)
    svc2 = DeliveryService(s2, clock, 30, 5)

    first = svc1.claim_one("A")
    assert first is not None
    # Time advances but stays within lease
    clock.advance(10)
    assert svc2.claim_one("B") is None
    s1.close()
    s2.close()


def test_lease_expiry_allows_reclaim_with_new_generation(context, clock, settings):
    """#8: after lease expiry another worker may reclaim; generation bumps."""
    delivery_id = _create_pending_delivery(context, clock)

    s1 = context.session_factory()
    svc1 = DeliveryService(s1, clock, 30, 5)
    first = svc1.claim_one("A")
    gen1 = first.lease_generation
    s1.close()

    clock.advance(31)

    s2 = context.session_factory()
    svc2 = DeliveryService(s2, clock, 30, 5)
    second = svc2.claim_one("B")
    assert second is not None
    assert second.id == delivery_id
    assert second.lease_owner == "B"
    assert second.lease_generation == gen1 + 1
    s2.close()


def test_two_workers_racing_only_one_claims(context, clock, settings):
    """#6: concurrent claims of one due delivery: exactly one winner.

    Uses separate processes-style sessions on real threads and a shared file
    database to exercise the database-level exclusion.
    """
    delivery_id = _create_pending_delivery(context, clock)
    winners: list[str | None] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def run(owner: str) -> None:
        session = context.session_factory()
        svc = DeliveryService(session, clock, 30, 5)
        barrier.wait()
        claimed = svc.claim_one(owner)
        with lock:
            winners.append(claimed.id if claimed else None)
        session.close()

    threads = [threading.Thread(target=run, args=(name,)) for name in ("A", "B")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners_non_none = [w for w in winners if w is not None]
    assert winners_non_none == [delivery_id]
    assert len(winners) == 2


def test_stale_worker_cannot_overwrite_success(context, clock, settings):
    """#9: A's lease expires, B reclaims; A's late success write is rejected."""
    delivery_id = _create_pending_delivery(context, clock)

    s_a = context.session_factory()
    svc_a = DeliveryService(s_a, clock, 30, 5)
    claimed_a = svc_a.claim_one("A")
    gen_a = claimed_a.lease_generation

    # A runs long; lease expires and B takes over
    clock.advance(40)
    s_b = context.session_factory()
    svc_b = DeliveryService(s_b, clock, 30, 5)
    claimed_b = svc_b.claim_one("B")
    assert claimed_b is not None
    assert claimed_b.lease_generation > gen_a

    # A finally receives a 2xx and tries to mark success — must be rejected
    accepted = svc_a.mark_succeeded(delivery_id, "A", gen_a)
    assert accepted is False

    # B records its own success — accepted; row stays B's outcome
    assert svc_b.mark_succeeded(delivery_id, "B", claimed_b.lease_generation) is True
    s_a.close()
    s_b.close()

    check = context.session_factory()
    from app.models import DeliveryRow

    row = check.get(DeliveryRow, delivery_id)
    assert row.status == DS.SUCCEEDED
    assert row.lease_owner is None
    check.close()


def test_stale_worker_cannot_overwrite_failure(context, clock, settings):
    delivery_id = _create_pending_delivery(context, clock)
    s_a = context.session_factory()
    svc_a = DeliveryService(s_a, clock, 30, 5)
    claimed_a = svc_a.claim_one("A")

    clock.advance(40)
    s_b = context.session_factory()
    svc_b = DeliveryService(s_b, clock, 30, 5)
    claimed_b = svc_b.claim_one("B")

    assert (
        svc_a.record_failure(delivery_id, "A", claimed_a.lease_generation, "late")
        is False
    )
    assert (
        svc_b.record_failure(
            delivery_id, "B", claimed_b.lease_generation, "fresh"
        )
        is True
    )
    s_a.close()
    s_b.close()

    from app.models import DeliveryRow

    check = context.session_factory()
    row = check.get(DeliveryRow, delivery_id)
    assert row.last_error == "fresh"
    check.close()


def test_lease_within_expiry_even_after_worker_restart(context, clock, settings):
    """#17-ish: a new worker (process restart) still cannot grab a non-expired lease."""
    _create_pending_delivery(context, clock)
    s1 = context.session_factory()
    DeliveryService(s1, clock, 30, 5).claim_one("old-process")
    s1.close()

    # Simulate restart: brand new session, clock only slightly advanced
    clock.advance(5)
    s2 = context.session_factory()
    assert DeliveryService(s2, clock, 30, 5).claim_one("new-process") is None
    s2.close()
