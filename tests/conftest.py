from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from app import services
from app.clock import FakeClock
from app.config import Settings
from app.db import make_engine, make_session_factory
from app.main import create_app
from app.models import Base, Delivery
from app.worker import Worker


@pytest.fixture
def settings():
    return Settings(
        max_attempts=3,
        base_delay_seconds=10.0,
        lease_seconds=30.0,
        http_timeout_seconds=5.0,
        worker_batch_size=10,
    )


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def engine(tmp_path):
    eng = make_engine(f"sqlite:///{tmp_path}/test.db")
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def session_factory(engine):
    return make_session_factory(engine)


@pytest.fixture
def client(engine, settings, clock):
    return TestClient(create_app(settings=settings, engine=engine, clock=clock))


@pytest.fixture
def factory(session_factory, clock):
    class Factory:
        def add_subscription(self, event_type="x", url="http://endpoint.test/hook", enabled=True):
            with session_factory() as session:
                sub = services.create_subscription(
                    session, url=url, event_type=event_type, clock=clock
                )
                if not enabled:
                    services.set_subscription_enabled(session, sub.id, False)
                return sub.id

        def add_event(self, event_type="x", payload=None, idempotency_key=None):
            with session_factory() as session:
                event, _ = services.create_event(
                    session,
                    event_type=event_type,
                    payload=payload if payload is not None else {},
                    idempotency_key=idempotency_key,
                    clock=clock,
                )
                return event.id

        def deliveries(self, event_id=None):
            with session_factory() as session:
                query = select(Delivery).order_by(Delivery.created_at)
                if event_id is not None:
                    query = query.where(Delivery.event_id == event_id)
                return list(session.scalars(query).all())

        def get_delivery(self, delivery_id):
            with session_factory() as session:
                return session.get(Delivery, delivery_id)

    return Factory()


@pytest.fixture
def make_worker(session_factory, settings, clock):
    def _make(sender, worker_id=None):
        return Worker(
            session_factory,
            settings,
            sender=sender,
            clock=clock,
            worker_id=worker_id,
        )

    return _make
