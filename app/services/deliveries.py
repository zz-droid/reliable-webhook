"""Delivery state machine: atomic claim/lease, fenced completion, queries.

All state transitions live in this module so the API layer and workers share
one implementation of the delivery state machine.
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import bindparam, case, select, text
from sqlalchemy.engine import Row
from sqlalchemy.orm import Session

from ..clock import Clock
from ..models import Delivery as DStatus
from ..models import DeliveryRow, Event, Subscription


@dataclass
class ClaimedDelivery:
    """Everything a worker needs to perform one HTTP attempt."""

    id: str
    event_id: str
    event_type: str
    payload: object
    subscription_id: str
    url: str
    attempts: int
    lease_owner: str
    lease_generation: int
    lease_expires_at: object


@dataclass
class EventStatus:
    event: Event
    total: int
    pending: int
    running: int
    succeeded: int
    failed: int
    deliveries: list[DeliveryRow]


class DeliveryService:
    def __init__(
        self,
        session: Session,
        clock: Clock,
        lease_duration_seconds: float,
        base_delay_seconds: float,
    ) -> None:
        self.session = session
        self.clock = clock
        self.lease_duration_seconds = lease_duration_seconds
        self.base_delay_seconds = base_delay_seconds

    # ------------------------------------------------------------------ claim
    def claim_one(self, owner: str) -> ClaimedDelivery | None:
        """Atomically claim one due delivery for ``owner``.

        A single ``UPDATE ... WHERE id IN (SELECT ...) RETURNING`` statement
        picks the highest-priority due row and marks it running with a fresh
        lease and fencing generation. The write lock taken by UPDATE makes
        this safe across multiple worker processes; only one worker can win
        each row.
        """
        from datetime import timedelta

        now = self.clock.now()
        lease_expires = now + timedelta(seconds=self.lease_duration_seconds)

        # A delivery is claimable when either:
        #  * it is pending and its retry schedule has come due, or
        #  * it is running but its lease has expired (worker crashed / stalled).
        is_due = (DeliveryRow.status == DStatus.PENDING) & (
            DeliveryRow.next_attempt_at <= now
        )
        is_orphaned = (DeliveryRow.status == DStatus.RUNNING) & (
            DeliveryRow.lease_expires_at < now
        )
        # Fresh pending work is preferred over reclaiming stale leases.
        priority = case((DeliveryRow.status == DStatus.RUNNING, 1), else_=0)

        candidate = (
            select(DeliveryRow.id)
            .where(is_due | is_orphaned)
            .order_by(priority, DeliveryRow.next_attempt_at, DeliveryRow.id)
            .limit(1)
            .scalar_subquery()
        )

        stmt = (
            DeliveryRow.__table__.update()
            .where(DeliveryRow.id == candidate)
            .values(
                status=DStatus.RUNNING,
                attempts=DeliveryRow.attempts + 1,
                lease_owner=owner,
                lease_expires_at=lease_expires,
                lease_generation=DeliveryRow.lease_generation + 1,
                updated_at=now,
            )
            .returning(
                DeliveryRow.id,
                DeliveryRow.event_id,
                DeliveryRow.subscription_id,
                DeliveryRow.attempts,
                DeliveryRow.lease_generation,
                DeliveryRow.lease_expires_at,
            )
        )
        row: Row | None = self.session.execute(stmt).first()
        if row is None:
            return None
        self.session.commit()

        delivery_id, event_id, sub_id, attempts, generation, expires_at = row
        # Fetch immutable delivery payload/subscription in a new txn.
        detail = self._fetch_detail(delivery_id)
        if detail is None:
            return None
        return ClaimedDelivery(
            id=delivery_id,
            event_id=event_id,
            event_type=detail.event_type,
            payload=detail.payload,
            subscription_id=sub_id,
            url=detail.url,
            attempts=attempts,
            lease_owner=owner,
            lease_generation=generation,
            lease_expires_at=expires_at,
        )

    # ------------------------------------------------------------ completion
    def mark_succeeded(
        self, delivery_id: str, owner: str, generation: int
    ) -> bool:
        """Transition a delivery to succeeded, only if the caller still owns
        the lease identified by ``generation``.

        Returns False when the lease has been lost (e.g. expired and reclaimed
        by another worker); the caller must treat that as "no-op".
        """
        now = self.clock.now()
        stmt = (
            DeliveryRow.__table__.update()
            .where(
                DeliveryRow.id == delivery_id,
                DeliveryRow.status == DStatus.RUNNING,
                DeliveryRow.lease_owner == owner,
                DeliveryRow.lease_generation == generation,
            )
            .values(
                status=DStatus.SUCCEEDED,
                lease_owner=None,
                lease_expires_at=None,
                last_error=None,
                updated_at=now,
            )
        )
        result = self.session.execute(stmt)
        self.session.commit()
        return result.rowcount == 1

    def record_failure(
        self,
        delivery_id: str,
        owner: str,
        generation: int,
        error: str,
    ) -> bool:
        """Record one failed attempt with fencing.

        If the attempt budget is exhausted the delivery becomes permanently
        failed; otherwise it goes back to pending with an exponentially
        scheduled ``next_attempt_at`` and releases its lease.

        Returns False if the caller no longer owns the lease.
        """
        now = self.clock.now()
        naive_now = now.replace(tzinfo=None) if now.tzinfo is not None else now
        now_literal = naive_now.strftime("%Y-%m-%d %H:%M:%S.%f")
        table = DeliveryRow.__table__
        fence = (
            (DeliveryRow.id == delivery_id)
            & (DeliveryRow.status == DStatus.RUNNING)
            & (DeliveryRow.lease_owner == owner)
            & (DeliveryRow.lease_generation == generation)
        )

        # 1) Terminal case: attempt budget already exhausted at claim time.
        terminal = (
            table.update()
            .where(fence & (DeliveryRow.attempts >= DeliveryRow.max_attempts))
            .values(
                status=DStatus.FAILED,
                lease_owner=None,
                lease_expires_at=None,
                last_error=error[:4000],
                updated_at=now,
            )
        )
        result = self.session.execute(terminal)
        if result.rowcount == 1:
            self.session.commit()
            return True

        # 2) Retry case: one fenced UPDATE that computes the exponential
        #    schedule inside SQLite (2**(attempts-1) via integer left shift),
        #    preserving sub-second precision through julianday.
        retry_sql = text(
            "UPDATE deliveries SET "
            " status = 'pending',"
            " lease_owner = NULL,"
            " lease_expires_at = NULL,"
            " last_error = :error,"
            " updated_at = :now,"
            " next_attempt_at = strftime("
            "   '%Y-%m-%d %H:%M:%f',"
            "   julianday(:now) + (:base * (1 << (attempts - 1))) / 86400.0"
            " ) "
            "WHERE id = :id AND status = 'running' "
            " AND lease_owner = :owner AND lease_generation = :gen"
        ).bindparams(
            bindparam("error", value=error[:4000]),
            bindparam("now", value=now_literal),
            bindparam("base", value=self.base_delay_seconds),
            bindparam("id", value=delivery_id),
            bindparam("owner", value=owner),
            bindparam("gen", value=generation),
        )
        result = self.session.execute(retry_sql)
        if result.rowcount != 1:
            self.session.rollback()
            return False
        self.session.commit()
        return True

    # --------------------------------------------------------------- queries
    def event_status(self, event_id: str) -> EventStatus | None:
        event = self.session.get(Event, event_id)
        if event is None:
            return None
        rows = list(
            self.session.execute(
                select(DeliveryRow)
                .where(DeliveryRow.event_id == event_id)
                .order_by(DeliveryRow.id)
            )
            .scalars()
            .all()
        )
        counts = {
            DStatus.PENDING: 0,
            DStatus.RUNNING: 0,
            DStatus.SUCCEEDED: 0,
            DStatus.FAILED: 0,
        }
        for r in rows:
            counts[r.status] += 1
        return EventStatus(
            event=event,
            total=len(rows),
            pending=counts[DStatus.PENDING],
            running=counts[DStatus.RUNNING],
            succeeded=counts[DStatus.SUCCEEDED],
            failed=counts[DStatus.FAILED],
            deliveries=rows,
        )

    # -------------------------------------------------------------- internals
    def _fetch_detail(self, delivery_id: str):
        stmt = (
            select(
                DeliveryRow.id,
                Event.id.label("eid"),
                Event.event_type,
                Event.payload,
                Subscription.url,
            )
            .join(Event, DeliveryRow.event_id == Event.id)
            .join(Subscription, DeliveryRow.subscription_id == Subscription.id)
            .where(DeliveryRow.id == delivery_id)
        )
        return self.session.execute(stmt).first()
