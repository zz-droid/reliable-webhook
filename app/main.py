from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy.orm import Session

from . import services
from .clock import SystemClock
from .config import Settings
from .db import make_engine, make_session_factory
from .models import Base, DeliveryStatus
from .schemas import (
    EventCreate,
    EventOut,
    EventStatusOut,
    SubscriptionCreate,
    SubscriptionOut,
    SubscriptionPatch,
)


def create_app(settings: Settings | None = None, engine=None, clock=None) -> FastAPI:
    settings = settings or Settings.from_env()
    engine = engine or make_engine(settings.database_url)
    Base.metadata.create_all(engine)
    session_factory = make_session_factory(engine)
    clock = clock or SystemClock()

    app = FastAPI(title="Reliable Webhook Delivery Service")
    app.state.settings = settings
    app.state.session_factory = session_factory
    app.state.clock = clock

    def get_session():
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    @app.post("/subscriptions", status_code=201, response_model=SubscriptionOut)
    def create_subscription(body: SubscriptionCreate, session: Session = Depends(get_session)):
        return services.create_subscription(
            session, url=str(body.url), event_type=body.event_type, clock=clock
        )

    @app.get("/subscriptions", response_model=list[SubscriptionOut])
    def list_subscriptions(session: Session = Depends(get_session)):
        return services.list_subscriptions(session)

    @app.get("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
    def get_subscription(subscription_id: str, session: Session = Depends(get_session)):
        sub = services.get_subscription(session, subscription_id)
        if sub is None:
            raise HTTPException(status_code=404, detail="subscription not found")
        return sub

    @app.patch("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
    def patch_subscription(
        subscription_id: str, body: SubscriptionPatch, session: Session = Depends(get_session)
    ):
        sub = services.set_subscription_enabled(session, subscription_id, body.enabled)
        if sub is None:
            raise HTTPException(status_code=404, detail="subscription not found")
        return sub

    @app.post("/events", response_model=EventOut)
    def create_event(body: EventCreate, response: Response, session: Session = Depends(get_session)):
        event, created = services.create_event(
            session,
            event_type=body.event_type,
            payload=body.payload,
            idempotency_key=body.idempotency_key,
            clock=clock,
        )
        response.status_code = 201 if created else 200
        return event

    @app.get("/events/{event_id}/status", response_model=EventStatusOut)
    def event_status(event_id: str, session: Session = Depends(get_session)):
        result = services.get_event_status(session, event_id)
        if result is None:
            raise HTTPException(status_code=404, detail="event not found")
        event, deliveries = result
        counts = {status: 0 for status in (
            DeliveryStatus.PENDING, DeliveryStatus.RUNNING, DeliveryStatus.SUCCEEDED, DeliveryStatus.FAILED
        )}
        for delivery in deliveries:
            counts[delivery.status] += 1
        return EventStatusOut(
            event=EventOut.model_validate(event),
            total=len(deliveries),
            pending=counts[DeliveryStatus.PENDING],
            running=counts[DeliveryStatus.RUNNING],
            succeeded=counts[DeliveryStatus.SUCCEEDED],
            failed=counts[DeliveryStatus.FAILED],
            deliveries=deliveries,
        )

    return app


app = create_app()
