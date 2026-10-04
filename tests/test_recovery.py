from datetime import timedelta

from app import services
from app.db import make_engine, make_session_factory
from app.models import Base, Delivery, DeliveryStatus, Event, Subscription


def _seed_states(session_factory, now):
    with session_factory() as session:
        sub = Subscription(url="http://a.test/hook", event_type="x", enabled=True, created_at=now)
        event = Event(event_type="x", payload={}, idempotency_key=None, created_at=now)
        session.add_all([sub, event])
        session.flush()

        def delivery(status, **kwargs):
            d = Delivery(
                event_id=event.id,
                subscription_id=sub.id,
                status=status,
                attempt_count=0,
                created_at=now,
                updated_at=now,
                **kwargs,
            )
            session.add(d)
            session.flush()
            return d.id

        ids = {
            "succeeded": delivery(DeliveryStatus.SUCCEEDED),
            "failed": delivery(DeliveryStatus.FAILED, next_attempt_at=None),
            "waiting": delivery(
                DeliveryStatus.PENDING, next_attempt_at=now + timedelta(seconds=1000)
            ),
            "running_valid_lease": delivery(
                DeliveryStatus.RUNNING,
                lease_owner="old-worker",
                lease_token="t1",
                lease_expires_at=now + timedelta(seconds=1000),
            ),
            "running_expired_lease": delivery(
                DeliveryStatus.RUNNING,
                lease_owner="crashed-worker",
                lease_token="t2",
                lease_expires_at=now - timedelta(seconds=1),
            ),
        }
        session.commit()
        return ids


def test_restart_recovery(tmp_path, settings, clock):
    db_path = tmp_path / "restart.db"
    now = clock.now()

    engine1 = make_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine1)
    ids = _seed_states(make_session_factory(engine1), now)
    engine1.dispose()

    # "Restart": brand-new engine and sessions over the same database file.
    engine2 = make_engine(f"sqlite:///{db_path}")
    session_factory2 = make_session_factory(engine2)

    with session_factory2() as session:
        claimed = services.claim_deliveries(
            session,
            worker_id="new-worker",
            now=now,
            lease_seconds=settings.lease_seconds,
            limit=10,
        )
    assert [d_id for d_id, _ in claimed] == [ids["running_expired_lease"]]

    with session_factory2() as session:
        states = {d.id: d for d in session.query(Delivery).all()}

    assert states[ids["succeeded"]].status == DeliveryStatus.SUCCEEDED
    assert states[ids["failed"]].status == DeliveryStatus.FAILED
    # Waiting delivery keeps its original schedule untouched.
    waiting = states[ids["waiting"]]
    assert waiting.status == DeliveryStatus.PENDING
    assert waiting.next_attempt_at == now + timedelta(seconds=1000)
    # A live lease survives the restart; the delivery is not re-executed early.
    running = states[ids["running_valid_lease"]]
    assert running.status == DeliveryStatus.RUNNING
    assert running.lease_owner == "old-worker"

    engine2.dispose()
