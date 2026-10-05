"""HTTP routes for subscriptions, events and delivery status."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status

from ..services.deliveries import DeliveryService
from ..services.events import EventService
from ..services.subscriptions import SubscriptionService
from .deps import delivery_service, event_service, subscription_service
from .schemas import (
    DeliveryOut,
    EventCreate,
    EventOut,
    EventStatusOut,
    SubscriptionCreate,
    SubscriptionOut,
    SubscriptionUpdate,
)

router = APIRouter()


# ------------------------------------------------------------- subscriptions
@router.post(
    "/subscriptions",
    response_model=SubscriptionOut,
    status_code=status.HTTP_201_CREATED,
)
def create_subscription(
    body: SubscriptionCreate,
    svc: SubscriptionService = Depends(subscription_service),
) -> SubscriptionOut:
    sub = svc.create(url=body.url, event_type=body.event_type, enabled=body.enabled)
    return SubscriptionOut.model_validate(sub)


@router.get("/subscriptions", response_model=list[SubscriptionOut])
def list_subscriptions(
    event_type: str | None = None,
    svc: SubscriptionService = Depends(subscription_service),
) -> list[SubscriptionOut]:
    return [SubscriptionOut.model_validate(s) for s in svc.list(event_type)]


@router.get("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
def get_subscription(
    subscription_id: str,
    svc: SubscriptionService = Depends(subscription_service),
) -> SubscriptionOut:
    sub = svc.get(subscription_id)
    if sub is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "subscription not found")
    return SubscriptionOut.model_validate(sub)


@router.patch("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
def update_subscription(
    subscription_id: str,
    body: SubscriptionUpdate,
    svc: SubscriptionService = Depends(subscription_service),
) -> SubscriptionOut:
    sub = svc.set_enabled(subscription_id, body.enabled)
    if sub is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "subscription not found")
    return SubscriptionOut.model_validate(sub)


# -------------------------------------------------------------------- events
@router.post("/events", response_model=EventOut, status_code=status.HTTP_201_CREATED)
def create_event(
    body: EventCreate,
    response: Response,
    svc: EventService = Depends(event_service),
) -> EventOut:
    result = svc.create(
        event_type=body.event_type,
        payload=body.payload,
        idempotency_key=body.idempotency_key,
    )
    if not result.created:
        response.status_code = status.HTTP_200_OK
    return EventOut.model_validate(result.event)


@router.get("/events/{event_id}/status", response_model=EventStatusOut)
def event_status(
    event_id: str,
    svc: DeliveryService = Depends(delivery_service),
) -> EventStatusOut:
    status_obj = svc.event_status(event_id)
    if status_obj is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "event not found")
    return EventStatusOut(
        event=EventOut.model_validate(status_obj.event),
        total=status_obj.total,
        pending=status_obj.pending,
        running=status_obj.running,
        succeeded=status_obj.succeeded,
        permanently_failed=status_obj.failed,
        deliveries=[
            DeliveryOut(
                id=d.id,
                subscription_id=d.subscription_id,
                status=d.status,
                attempts=d.attempts,
                max_attempts=d.max_attempts,
                next_attempt_at=d.next_attempt_at,
                last_error=d.last_error,
                lease_owner=d.lease_owner,
                lease_expires_at=d.lease_expires_at,
            )
            for d in status_obj.deliveries
        ],
    )
