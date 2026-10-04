import threading

from app import services


def _claim(session_factory, settings, clock, worker_id):
    with session_factory() as session:
        return services.claim_deliveries(
            session,
            worker_id=worker_id,
            now=clock.now(),
            lease_seconds=settings.lease_seconds,
            limit=10,
        )


def test_racing_workers_only_one_claims(session_factory, settings, clock, factory):
    factory.add_subscription()
    factory.add_event()

    results, errors = [], []
    barrier = threading.Barrier(2)

    def claim(worker_id):
        try:
            barrier.wait(timeout=10)
            results.append(_claim(session_factory, settings, clock, worker_id))
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=claim, args=(f"w{i}",)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert sum(len(r) for r in results) == 1


def test_valid_lease_blocks_other_workers(session_factory, settings, clock, factory):
    factory.add_subscription()
    factory.add_event()

    claimed = _claim(session_factory, settings, clock, "worker-a")
    assert len(claimed) == 1

    # Same instant: lease still valid, nothing left to claim.
    assert _claim(session_factory, settings, clock, "worker-b") == []

    delivery = factory.deliveries()[0]
    assert delivery.status == "running"
    assert delivery.lease_owner == "worker-a"
    assert delivery.lease_expires_at is not None


def test_expired_lease_can_be_reclaimed(session_factory, settings, clock, factory):
    factory.add_subscription()
    factory.add_event()

    claimed_a = _claim(session_factory, settings, clock, "worker-a")
    assert len(claimed_a) == 1

    clock.advance(settings.lease_seconds + 1)
    claimed_b = _claim(session_factory, settings, clock, "worker-b")
    assert len(claimed_b) == 1
    assert claimed_b[0][0] == claimed_a[0][0]  # same delivery
    assert claimed_b[0][1] != claimed_a[0][1]  # new fencing token

    delivery = factory.deliveries()[0]
    assert delivery.lease_owner == "worker-b"


def test_pending_delivery_not_claimable_before_next_attempt(session_factory, settings, clock, factory):
    factory.add_subscription()
    factory.add_event()
    delivery_id = factory.deliveries()[0].id

    with session_factory() as session:
        services.complete_delivery(
            session,
            delivery_id=delivery_id,
            lease_token=_claim(session_factory, settings, clock, "w")[0][1],
            success=False,
            error="boom",
            now=clock.now(),
            max_attempts=settings.max_attempts,
            base_delay_seconds=settings.base_delay_seconds,
        )

    # Backoff not yet elapsed.
    assert _claim(session_factory, settings, clock, "w") == []
    clock.advance(settings.base_delay_seconds)
    assert len(_claim(session_factory, settings, clock, "w")) == 1
