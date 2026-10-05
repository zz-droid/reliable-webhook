"""Subscription business logic."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..clock import Clock
from ..models import Subscription


class SubscriptionService:
    def __init__(self, session: Session, clock: Clock) -> None:
        self.session = session
        self.clock = clock

    def create(self, url: str, event_type: str, enabled: bool = True) -> Subscription:
        sub = Subscription(
            url=url,
            event_type=event_type,
            enabled=enabled,
            created_at=self.clock.now(),
        )
        self.session.add(sub)
        self.session.flush()
        return sub

    def get(self, subscription_id: str) -> Subscription | None:
        return self.session.get(Subscription, subscription_id)

    def list(self, event_type: str | None = None) -> list[Subscription]:
        stmt = select(Subscription)
        if event_type is not None:
            stmt = stmt.where(Subscription.event_type == event_type)
        stmt = stmt.order_by(Subscription.created_at, Subscription.id)
        return list(self.session.execute(stmt).scalars().all())

    def set_enabled(self, subscription_id: str, enabled: bool) -> Subscription | None:
        sub = self.get(subscription_id)
        if sub is None:
            return None
        sub.enabled = enabled
        self.session.flush()
        return sub
