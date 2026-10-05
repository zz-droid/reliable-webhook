"""SQLAlchemy ORM models.

The Delivery table is the heart of the reliability guarantees:

* ``status`` is one of pending/running/succeeded/failed.
* A claimable pending row carries its earliest ``next_attempt_at``.
* While a worker owns a delivery it records ``lease_owner`` and
  ``lease_expires_at`` plus a monotonically increasing ``lease_generation``
  that is used as a fencing token.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def new_id() -> str:
    return uuid.uuid4().hex


class UTCDateTime(TypeDecorator):
    """DateTime that always stores naive UTC and returns aware UTC.

    SQLite has no native timestamp-with-timezone type; normalising to naive
    UTC on the way in avoids comparison surprises and re-attaches tzinfo on
    the way out.
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=timezone.utc)


class Base(DeclarativeBase):
    pass


class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)

    deliveries: Mapped[list["DeliveryRow"]] = relationship(
        back_populates="subscription"
    )


class Event(Base):
    __tablename__ = "events"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    event_type: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    payload: Mapped[Any] = mapped_column(JSON, nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(
        String(200), nullable=True, unique=True
    )
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)

    deliveries: Mapped[list["DeliveryRow"]] = relationship(
        back_populates="event", cascade="all, delete-orphan"
    )


class Delivery:
    """Status constants for deliveries.

    PENDING covers both "never attempted" and "waiting for retry"; the
    distinction is expressed by ``next_attempt_at``/``attempts``.
    RUNNING means a worker currently holds a valid (or possibly expired,
    not-yet-reclaimed) lease. SUCCEEDED / FAILED are terminal.
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"  # permanently failed


class DeliveryRow(Base):
    __tablename__ = "deliveries"
    __table_args__ = (
        # Defensive: one event must only produce one delivery per subscription.
        UniqueConstraint("event_id", "subscription_id", name="uq_delivery_event_sub"),
        Index(
            "ix_deliveries_claim",
            "status",
            "next_attempt_at",
        ),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    event_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("events.id"), nullable=False
    )
    subscription_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("subscriptions.id"), nullable=False
    )

    status: Mapped[str] = mapped_column(String(20), nullable=False, default=Delivery.PENDING)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    next_attempt_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    lease_owner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        UTCDateTime, nullable=True
    )
    # Fencing token: incremented on every (re-)claim. Completion/failure writes
    # are conditional on the generation still matching the claiming worker.
    lease_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)

    event: Mapped[Event] = relationship(back_populates="deliveries")
    subscription: Mapped[Subscription] = relationship(back_populates="deliveries")
