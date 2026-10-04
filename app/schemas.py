from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field


class SubscriptionCreate(BaseModel):
    url: AnyHttpUrl
    event_type: str = Field(min_length=1, max_length=255)


class SubscriptionPatch(BaseModel):
    enabled: bool


class SubscriptionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    url: str
    event_type: str
    enabled: bool
    created_at: datetime


class EventCreate(BaseModel):
    event_type: str = Field(min_length=1, max_length=255)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, max_length=255)


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    event_type: str
    payload: dict[str, Any]
    idempotency_key: str | None
    created_at: datetime


class DeliveryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    event_id: str
    subscription_id: str
    status: str
    attempt_count: int
    next_attempt_at: datetime | None
    last_error: str | None
    lease_owner: str | None
    lease_expires_at: datetime | None


class EventStatusOut(BaseModel):
    event: EventOut
    total: int
    pending: int
    running: int
    succeeded: int
    failed: int
    deliveries: list[DeliveryOut]
