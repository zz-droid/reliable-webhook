from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from sqlalchemy import and_, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .models import Delivery, DeliveryStatus, Event, Subscription


def create_subscription(session: Session, *, url: str, event_type: str, clock) -> Subscription:
    sub = Subscription(url=url, event_type=event_type, enabled=True, created_at=clock.now())
    session.add(sub)
    session.commit()
    return sub


def get_subscription(session: Session, subscription_id: str) -> Subscription | None:
    return session.get(Subscription, subscription_id)


def list_subscriptions(session: Session) -> list[Subscription]:
    return list(session.scalars(select(Subscription).order_by(Subscription.created_at)).all())


def set_subscription_enabled(session: Session, subscription_id: str, enabled: bool) -> Subscription | None:
    sub = session.get(Subscription, subscription_id)
    if sub is None:
        return None
    sub.enabled = enabled
    session.commit()
    return sub


def create_event(
    session: Session,
    *,
    event_type: str,
    payload: dict,
    idempotency_key: str | None,
    clock,
) -> tuple[Event, bool]:
    """Returns (event, created). A duplicate idempotency key returns the original
    event with created=False. The unique constraint on idempotency_key is the
    race-safe guard; the pre-check is only a fast path."""
    now = clock.now()
    if idempotency_key is not None:
        existing = session.scalar(select(Event).where(Event.idempotency_key == idempotency_key))
        if existing is not None:
            return existing, False

    event = Event(event_type=event_type, payload=payload, idempotency_key=idempotency_key, created_at=now)
    session.add(event)
    session.flush()

    subscriptions = session.scalars(
        select(Subscription).where(
            Subscription.event_type == event_type,
            Subscription.enabled.is_(True),
        )
    ).all()
    for sub in subscriptions:
        session.add(
            Delivery(
                event_id=event.id,
                subscription_id=sub.id,
                status=DeliveryStatus.PENDING,
                attempt_count=0,
                next_attempt_at=now,
                created_at=now,
                updated_at=now,
            )
        )

    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        if idempotency_key is not None:
            existing = session.scalar(select(Event).where(Event.idempotency_key == idempotency_key))
            if existing is not None:
                return existing, False
        raise
    return event, True


def _claimable_condition(now: datetime):
    return or_(
        and_(
            Delivery.status == DeliveryStatus.PENDING,
            Delivery.next_attempt_at <= now,
        ),
        and_(
            Delivery.status == DeliveryStatus.RUNNING,
            Delivery.lease_expires_at <= now,
        ),
    )


def claim_deliveries(
    session: Session,
    *,
    worker_id: str,
    now: datetime,
    lease_seconds: float,
    limit: int,
) -> list[tuple[str, str]]:
    """Atomically claim up to `limit` executable deliveries. Returns
    (delivery_id, lease_token) pairs. Each claim is a single conditional UPDATE,
    so racing workers cannot both win the same row."""
    lease_expires_at = now + timedelta(seconds=lease_seconds)
    claimable = _claimable_condition(now)
    candidate_ids = list(
        session.scalars(
            select(Delivery.id).where(claimable).order_by(Delivery.created_at).limit(limit)
        ).all()
    )
    claimed: list[tuple[str, str]] = []
    for delivery_id in candidate_ids:
        token = uuid.uuid4().hex
        result = session.execute(
            update(Delivery)
            .where(Delivery.id == delivery_id, claimable)
            .values(
                status=DeliveryStatus.RUNNING,
                lease_owner=worker_id,
                lease_expires_at=lease_expires_at,
                lease_token=token,
                updated_at=now,
            )
        )
        if result.rowcount == 1:
            claimed.append((delivery_id, token))
    session.commit()
    return claimed


def complete_delivery(
    session: Session,
    *,
    delivery_id: str,
    lease_token: str,
    success: bool,
    error: str | None,
    now: datetime,
    max_attempts: int,
    base_delay_seconds: float,
) -> bool:
    """Record the outcome of a delivery attempt. The conditional UPDATE only
    matches if the lease token is still current, so a stale worker whose lease
    expired cannot overwrite the state of the worker that reclaimed the delivery.
    Returns True if the update was applied."""
    delivery = session.get(Delivery, delivery_id)
    if delivery is None:
        return False

    if success:
        values = dict(
            status=DeliveryStatus.SUCCEEDED,
            last_error=None,
        )
    else:
        attempt = delivery.attempt_count + 1
        if attempt >= max_attempts:
            values = dict(
                status=DeliveryStatus.FAILED,
                attempt_count=attempt,
                last_error=error,
                next_attempt_at=None,
            )
        else:
            delay = base_delay_seconds * (2 ** (attempt - 1))
            values = dict(
                status=DeliveryStatus.PENDING,
                attempt_count=attempt,
                last_error=error,
                next_attempt_at=now + timedelta(seconds=delay),
            )

    result = session.execute(
        update(Delivery)
        .where(
            Delivery.id == delivery_id,
            Delivery.lease_token == lease_token,
            Delivery.status == DeliveryStatus.RUNNING,
        )
        .values(
            **values,
            lease_owner=None,
            lease_token=None,
            lease_expires_at=None,
            updated_at=now,
        )
    )
    session.commit()
    return result.rowcount == 1


def get_event_status(session: Session, event_id: str) -> tuple[Event, list[Delivery]] | None:
    event = session.get(Event, event_id)
    if event is None:
        return None
    deliveries = list(
        session.scalars(
            select(Delivery).where(Delivery.event_id == event_id).order_by(Delivery.created_at)
        ).all()
    )
    return event, deliveries
