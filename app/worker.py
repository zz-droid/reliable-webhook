from __future__ import annotations

import logging
import threading
import uuid

import httpx

from . import services
from .clock import SystemClock
from .config import Settings
from .models import Delivery, Event, Subscription

logger = logging.getLogger(__name__)


class HttpSender:
    """Delivers one webhook POST and returns the HTTP status code."""

    def __init__(self, client: httpx.Client | None = None):
        self._client = client or httpx.Client()

    def __call__(self, url: str, body: dict, timeout: float) -> int:
        return self._client.post(url, json=body, timeout=timeout).status_code


class Worker:
    def __init__(self, session_factory, settings: Settings, sender=None, clock=None, worker_id: str | None = None):
        self._session_factory = session_factory
        self._settings = settings
        self._sender = sender or HttpSender()
        self._clock = clock or SystemClock()
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"

    def run_once(self) -> int:
        """Claim a batch of deliveries and attempt each one. Returns the number
        of deliveries attempted. A failing endpoint never stops the batch."""
        with self._session_factory() as session:
            claimed = services.claim_deliveries(
                session,
                worker_id=self.worker_id,
                now=self._clock.now(),
                lease_seconds=self._settings.lease_seconds,
                limit=self._settings.worker_batch_size,
            )
        attempted = 0
        for delivery_id, lease_token in claimed:
            try:
                self._deliver(delivery_id, lease_token)
                attempted += 1
            except Exception:
                # The lease will expire and another worker will retry; never let
                # one bad delivery kill the worker.
                logger.exception("worker %s: unexpected error on delivery %s", self.worker_id, delivery_id)
        return attempted

    def run_forever(self, stop_event: threading.Event | None = None) -> None:
        while stop_event is None or not stop_event.is_set():
            if self.run_once() == 0:
                self._clock.sleep(self._settings.worker_poll_interval_seconds)

    def _deliver(self, delivery_id: str, lease_token: str) -> None:
        with self._session_factory() as session:
            delivery = session.get(Delivery, delivery_id)
            event = session.get(Event, delivery.event_id)
            subscription = session.get(Subscription, delivery.subscription_id)
            url = subscription.url
            body = {
                "event_id": event.id,
                "event_type": event.event_type,
                "payload": event.payload,
                "delivery_id": delivery.id,
            }

        try:
            status_code = self._sender(url, body, self._settings.http_timeout_seconds)
        except Exception as exc:
            self._complete(delivery_id, lease_token, success=False, error=f"{type(exc).__name__}: {exc}")
            return

        if 200 <= status_code < 300:
            self._complete(delivery_id, lease_token, success=True, error=None)
        else:
            self._complete(delivery_id, lease_token, success=False, error=f"HTTP {status_code}")

    def _complete(self, delivery_id: str, lease_token: str, *, success: bool, error: str | None) -> None:
        with self._session_factory() as session:
            applied = services.complete_delivery(
                session,
                delivery_id=delivery_id,
                lease_token=lease_token,
                success=success,
                error=error,
                now=self._clock.now(),
                max_attempts=self._settings.max_attempts,
                base_delay_seconds=self._settings.base_delay_seconds,
            )
        if not applied:
            logger.warning(
                "worker %s: stale completion for delivery %s discarded (lease lost)",
                self.worker_id,
                delivery_id,
            )


def main() -> None:
    from .db import make_engine, make_session_factory
    from .models import Base

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    engine = make_engine(settings.database_url)
    Base.metadata.create_all(engine)
    worker = Worker(make_session_factory(engine), settings)
    logger.info("worker %s started", worker.worker_id)
    worker.run_forever()


if __name__ == "__main__":
    main()
