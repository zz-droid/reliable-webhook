import json

import httpx

from app.worker import HttpSender, Worker


def test_http_2xx_marks_delivery_succeeded(factory, make_worker):
    factory.add_subscription()
    factory.add_event()

    worker = make_worker(lambda url, body, timeout: 200)
    assert worker.run_once() == 1

    delivery = factory.deliveries()[0]
    assert delivery.status == "succeeded"
    assert delivery.attempt_count == 0
    assert delivery.last_error is None
    assert delivery.lease_owner is None
    assert worker.run_once() == 0  # nothing left to do


def test_non_2xx_schedules_retry(factory, make_worker, settings, clock):
    factory.add_subscription()
    factory.add_event()

    worker = make_worker(lambda url, body, timeout: 500)
    now = clock.now()
    assert worker.run_once() == 1

    delivery = factory.deliveries()[0]
    assert delivery.status == "pending"
    assert delivery.attempt_count == 1
    assert "500" in delivery.last_error
    assert (delivery.next_attempt_at - now).total_seconds() == settings.base_delay_seconds


def test_network_exception_does_not_crash_worker(factory, make_worker):
    factory.add_subscription(url="http://down.test/hook")
    factory.add_subscription(url="http://up.test/hook")
    factory.add_event()

    def sender(url, body, timeout):
        if "down.test" in url:
            raise ConnectionError("connection refused")
        return 200

    worker = make_worker(sender)
    assert worker.run_once() == 2  # both attempted, no crash

    deliveries = factory.deliveries()
    by_status = sorted(d.status for d in deliveries)
    assert by_status == ["pending", "succeeded"]
    failed = next(d for d in deliveries if d.status == "pending")
    assert "ConnectionError" in failed.last_error


def test_attempt_count_increments_on_each_failure(factory, make_worker, settings, clock):
    factory.add_subscription()
    factory.add_event()

    worker = make_worker(lambda url, body, timeout: 503)
    for expected_attempts in (1, 2):
        assert worker.run_once() == 1
        delivery = factory.deliveries()[0]
        assert delivery.attempt_count == expected_attempts
        clock.advance(settings.base_delay_seconds * 10)


def test_exponential_backoff(factory, session_factory, settings, clock):
    settings.max_attempts = 5
    factory.add_subscription()
    factory.add_event()

    worker = Worker(
        session_factory,
        settings,
        sender=lambda url, body, timeout: 500,
        clock=clock,
    )
    base = settings.base_delay_seconds
    for attempt, expected_delay in enumerate((base, 2 * base, 4 * base, 8 * base), start=1):
        failed_at = clock.now()
        assert worker.run_once() == 1
        delivery = factory.deliveries()[0]
        assert delivery.status == "pending"
        assert delivery.attempt_count == attempt
        assert (delivery.next_attempt_at - failed_at).total_seconds() == expected_delay
        clock.advance(expected_delay)


def test_max_attempts_exhausted_marks_permanently_failed(factory, make_worker, settings, clock):
    assert settings.max_attempts == 3
    factory.add_subscription()
    factory.add_event()

    worker = make_worker(lambda url, body, timeout: 500)
    for _ in range(settings.max_attempts):
        assert worker.run_once() == 1
        clock.advance(settings.base_delay_seconds * 10)

    delivery = factory.deliveries()[0]
    assert delivery.status == "failed"
    assert delivery.attempt_count == settings.max_attempts
    assert delivery.next_attempt_at is None


def test_permanently_failed_delivery_is_never_claimed(factory, make_worker, settings, clock):
    factory.add_subscription()
    factory.add_event()

    worker = make_worker(lambda url, body, timeout: 500)
    for _ in range(settings.max_attempts):
        worker.run_once()
        clock.advance(settings.base_delay_seconds * 10)

    assert factory.deliveries()[0].status == "failed"
    clock.advance(10_000_000)
    assert worker.run_once() == 0


def test_delivery_request_body_and_real_http_path(factory, session_factory, settings, clock):
    factory.add_subscription(url="http://receiver.test/webhook")
    event_id = factory.add_event(payload={"hello": "world"})

    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(204)

    sender = HttpSender(httpx.Client(transport=httpx.MockTransport(handler)))
    worker = Worker(session_factory, settings, sender=sender, clock=clock)
    assert worker.run_once() == 1

    assert seen["url"] == "http://receiver.test/webhook"
    body = seen["body"]
    delivery = factory.deliveries()[0]
    assert body["event_id"] == event_id
    assert body["event_type"] == "x"
    assert body["payload"] == {"hello": "world"}
    assert body["delivery_id"] == delivery.id
    assert delivery.status == "succeeded"
