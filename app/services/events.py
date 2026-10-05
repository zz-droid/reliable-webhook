"""Event creation and fan-out to deliveries."""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..clock import Clock
from ..models import Delivery, DeliveryRow, Event, Subscription


class IdempotencyConflict(Exception):
    """An idempotency key was reused with a different event body."""


@dataclass
class CreatedEvent:
    event: Event
    created: bool  # False when the idempotency key already existed
    deliveries: list[DeliveryRow]


class EventService:
    def __init__(self, session: Session, clock: Clock, max_attempts: int) -> None:
        self.session = session
        self.clock = clock
        self.max_attempts = max_attempts

    def create(
        self,
        event_type: str,
        payload: dict | list | str | int | float | bool | None,
        idempotency_key: str | None = None,
    ) -> CreatedEvent:
        """Create an event and one delivery per matching enabled subscription.

        Idempotency is enforced by the UNIQUE constraint on
        ``events.idempotency_key`` rather than check-then-insert, so two
        concurrent requests with the same key cannot both insert.
        """
        now = self.clock.now()
        event = Event(
            event_type=event_type,
            payload=payload,
            idempotency_key=idempotency_key,
            created_at=now,
        )

        # SAVEPOINT isolates the unique-constraint violation so the outer
        # transaction (event + its deliveries) stays usable.
        savepoint = self.session.begin_nested()
        try:
            self.session.add(event)
            self.session.flush()
        except IntegrityError:
            savepoint.rollback()
            existing = self._lookup_key(idempotency_key)
            if existing is None:
                # The constraint violation was not an idempotency-key race.
                raise
            return CreatedEvent(event=existing, created=False, deliveries=[])

        deliveries = self._fan_out(event, now)
        self.session.flush()
        return CreatedEvent(event=event, created=True, deliveries=deliveries)

    def get(self, event_id: str) -> Event | None:
        return self.session.get(Event, id=event_id)

    def _lookup_key(self, key: str | None) -> Event | None:
        if not key:
            return None
        stmt = select(Event).where(Event.idempotency_key == key)
        return self.session.execute(stmt).scalar_one_or_none()

    def _fan_out(self, event: Event, now) -> list[DeliveryRow]:
        stmt = select(Subscription).where(
            Subscription.event_type == event.event_type,
            Subscription.enabled.is_(True),
        )
        subs = list(self.session.execute(stmt).scalars().all())
        rows: list[DeliveryRow] = []
        for sub in subs:
            rows.append(
                DeliveryRow(
                    event_id=event.id,
                    subscription_id=sub.id,
                    status=Delivery.PENDING,
                    attempts=0,
                    max_attempts=self.max_attempts,
                    # A fresh delivery is immediately claimable.
                    next_attempt_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
        self.session.add_all(rows)
        return rows
