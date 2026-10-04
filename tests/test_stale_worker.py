from app import services


def test_stale_worker_cannot_overwrite_reclaimed_delivery(session_factory, settings, clock, factory):
    factory.add_subscription()
    factory.add_event()
    delivery_id = factory.deliveries()[0].id

    def claim(worker_id):
        with session_factory() as session:
            return services.claim_deliveries(
                session,
                worker_id=worker_id,
                now=clock.now(),
                lease_seconds=settings.lease_seconds,
                limit=10,
            )

    _, token_a = claim("worker-a")[0]

    # Worker A hangs past its lease; worker B reclaims the delivery.
    clock.advance(settings.lease_seconds + 1)
    _, token_b = claim("worker-b")[0]

    # A's late "success" must be rejected.
    with session_factory() as session:
        applied = services.complete_delivery(
            session,
            delivery_id=delivery_id,
            lease_token=token_a,
            success=True,
            error=None,
            now=clock.now(),
            max_attempts=settings.max_attempts,
            base_delay_seconds=settings.base_delay_seconds,
        )
    assert applied is False

    delivery = factory.get_delivery(delivery_id)
    assert delivery.status == "running"
    assert delivery.lease_owner == "worker-b"

    # The current owner can still complete normally.
    with session_factory() as session:
        applied = services.complete_delivery(
            session,
            delivery_id=delivery_id,
            lease_token=token_b,
            success=True,
            error=None,
            now=clock.now(),
            max_attempts=settings.max_attempts,
            base_delay_seconds=settings.base_delay_seconds,
        )
    assert applied is True
    assert factory.get_delivery(delivery_id).status == "succeeded"
