"""Background delivery worker.

Each worker independently claims due deliveries through the database-backed
lease mechanism, so any number of worker processes can run concurrently. A
worker never holds in-memory state about ownership; every decision is
re-read / conditionally written against the database.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable

from sqlalchemy.orm import sessionmaker, Session

from ..clock import Clock, SystemClock
from ..config import Settings
from ..services.deliveries import DeliveryService
from .sender import HttpSender, Sender

logger = logging.getLogger("webhook.worker")


class Worker:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        clock: Clock,
        sender: Sender,
        settings: Settings,
        worker_id: str | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.session_factory = session_factory
        self.clock = clock
        self.sender = sender
        self.settings = settings
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:12]}"
        self._sleeper = sleeper
        self._stop = threading.Event()

    # -------------------------------------------------------------- control
    def request_stop(self) -> None:
        self._stop.set()

    def run_forever(self) -> None:
        logger.info("worker %s started", self.worker_id)
        try:
            while not self._stop.is_set():
                processed = self.tick()
                if not processed:
                    self._interruptible_sleep(self.settings.worker_idle_seconds)
        finally:
            logger.info("worker %s stopped", self.worker_id)

    # ------------------------------------------------------------------ run
    def tick(self, max_claims: int | None = None) -> int:
        """Process up to ``max_claims`` (default: batch size) due deliveries.

        Returns the number of delivery attempts performed. Exceptions from a
        single endpoint are recorded on the delivery and never escape, so a
        broken endpoint cannot crash the worker or block other deliveries.
        """
        budget = max_claims if max_claims is not None else self.settings.worker_batch_size
        processed = 0
        for _ in range(budget):
            claimed = self._claim()
            if claimed is None:
                break
            processed += 1
            try:
                self._attempt(claimed)
            except Exception:  # defensive: sender/state errors must not kill the loop
                logger.exception(
                    "worker %s: unexpected error on delivery %s",
                    self.worker_id,
                    claimed.id,
                )
                with self._service() as svc:
                    svc.record_failure(
                        claimed.id,
                        claimed.lease_owner,
                        claimed.lease_generation,
                        error="WorkerError: unexpected internal error",
                    )
        return processed

    # ------------------------------------------------------------- internals
    def _service(self):
        from contextlib import contextmanager

        @contextmanager
        def cm():
            session = self.session_factory()
            try:
                yield DeliveryService(
                    session,
                    self.clock,
                    self.settings.lease_duration_seconds,
                    self.settings.base_delay_seconds,
                )
            finally:
                session.close()

        return cm()

    def _claim(self):
        with self._service() as svc:
            return svc.claim_one(self.worker_id)

    def _attempt(self, claimed) -> None:
        result = self.sender.send(
            claimed.url,
            event_id=claimed.event_id,
            event_type=claimed.event_type,
            delivery_id=claimed.id,
            payload=claimed.payload,
        )
        with self._service() as svc:
            if result.ok:
                accepted = svc.mark_succeeded(
                    claimed.id, claimed.lease_owner, claimed.lease_generation
                )
                if not accepted:
                    logger.warning(
                        "worker %s: stale success write rejected for delivery %s",
                        self.worker_id,
                        claimed.id,
                    )
            else:
                accepted = svc.record_failure(
                    claimed.id,
                    claimed.lease_owner,
                    claimed.lease_generation,
                    error=result.error or "unknown error",
                )
                if not accepted:
                    logger.warning(
                        "worker %s: stale failure write rejected for delivery %s",
                        self.worker_id,
                        claimed.id,
                    )

    def _interruptible_sleep(self, seconds: float) -> None:
        self._stop.wait(seconds)


def run_standalone_worker(settings: Settings) -> None:
    """Entry point for ``python -m app.worker_main``: API-less worker process."""
    from ..db import make_engine, make_session_factory, init_db

    engine = make_engine(settings.database_url)
    init_db(engine)
    factory = make_session_factory(engine)
    sender = HttpSender(settings.request_timeout_seconds)
    worker = Worker(factory, SystemClock(), sender, settings)
    try:
        worker.run_forever()
    except KeyboardInterrupt:
        worker.request_stop()
    finally:
        sender.close()
        engine.dispose()
