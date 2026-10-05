"""FastAPI dependency helpers."""
from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy.orm import Session

from fastapi import Depends, Request

from ..clock import Clock
from ..config import Settings
from ..services.deliveries import DeliveryService
from ..services.events import EventService
from ..services.subscriptions import SubscriptionService


def get_context(request: Request):
    return request.app.state.context


def get_session(context=Depends(get_context)) -> Iterator[Session]:
    session = context.session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_clock(context=Depends(get_context)) -> Clock:
    return context.clock


def get_settings(context=Depends(get_context)) -> Settings:
    return context.settings


def subscription_service(
    session: Session = Depends(get_session), clock: Clock = Depends(get_clock)
) -> SubscriptionService:
    return SubscriptionService(session, clock)


def event_service(
    session: Session = Depends(get_session),
    clock: Clock = Depends(get_clock),
    settings: Settings = Depends(get_settings),
) -> EventService:
    return EventService(session, clock, settings.max_attempts)


def delivery_service(
    session: Session = Depends(get_session),
    clock: Clock = Depends(get_clock),
    settings: Settings = Depends(get_settings),
) -> DeliveryService:
    return DeliveryService(
        session,
        clock,
        settings.lease_duration_seconds,
        settings.base_delay_seconds,
    )
