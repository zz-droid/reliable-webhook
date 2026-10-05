"""Pydantic request/response schemas for the HTTP API."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class SubscriptionCreate(BaseModel):
    url: str = Field(min_length=1, max_length=2000)
    event_type: str = Field(min_length=1, max_length=200)
    enabled: bool = True


class SubscriptionUpdate(BaseModel):
    enabled: bool


class SubscriptionOut(BaseModel):
    id: str
    url: str
    event_type: str
    enabled: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class EventCreate(BaseModel):
    event_type: str = Field(min_length=1, max_length=200)
    payload: Any = {}
    idempotency_key: str | None = Field(default=None, max_length=200)


class EventOut(BaseModel):
    id: str
    event_type: str
    payload: Any
    idempotency_key: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


class DeliveryOut(BaseModel):
    id: str
    subscription_id: str
    status: str
    attempts: int
    max_attempts: int
    next_attempt_at: datetime
    last_error: str | None
    lease_owner: str | None
    lease_expires_at: datetime | None

    model_config = {"from_attributes": True}


class EventStatusOut(BaseModel):
    event: EventOut
    total: int
    pending: int
    running: int
    succeeded: int
    permanently_failed: int
    deliveries: list[DeliveryOut]
