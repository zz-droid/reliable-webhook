"""End-to-end tests through the FastAPI HTTP API."""
from __future__ import annotations

from app.worker.worker import Worker
from tests.conftest import ScriptedSender, failure, success


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_subscription_lifecycle_api(client):
    r = client.post(
        "/api/v1/subscriptions",
        json={"url": "http://example/hook", "event_type": "order.created"},
    )
    assert r.status_code == 201, r.text
    sub = r.json()
    assert sub["enabled"] is True
    sid = sub["id"]

    assert client.get(f"/api/v1/subscriptions/{sid}").status_code == 200
    listed = client.get("/api/v1/subscriptions", params={"event_type": "order.created"})
    assert len(listed.json()) == 1

    r = client.patch(f"/api/v1/subscriptions/{sid}", json={"enabled": False})
    assert r.status_code == 200
    assert r.json()["enabled"] is False

    assert client.get("/api/v1/subscriptions/nope").status_code == 404


def test_create_event_fans_out_and_status_api(client, context, settings, clock):
    client.post("/api/v1/subscriptions", json={"url": "http://a", "event_type": "e"})
    client.post("/api/v1/subscriptions", json={"url": "http://b", "event_type": "e"})
    client.post("/api/v1/subscriptions", json={"url": "http://c", "event_type": "other"})

    r = client.post("/api/v1/events", json={"event_type": "e", "payload": {"k": 1}})
    assert r.status_code == 201
    event = r.json()
    assert event["event_type"] == "e"

    status = client.get(f"/api/v1/events/{event['id']}/status").json()
    assert status["total"] == 2
    assert status["pending"] == 2
    assert len(status["deliveries"]) == 2
    for d in status["deliveries"]:
        assert d["status"] == "pending"
        assert d["attempts"] == 0


def test_idempotent_event_creation_over_http(client):
    body = {"event_type": "e", "payload": {"x": 1}, "idempotency_key": "abc"}
    r1 = client.post("/api/v1/events", json=body)
    r2 = client.post("/api/v1/events", json=body)
    assert r1.status_code == 201
    assert r2.status_code == 200  # replay, not a new create
    assert r1.json()["id"] == r2.json()["id"]


def test_end_to_end_success_via_worker(client, context, settings):
    client.post("/api/v1/subscriptions", json={"url": "http://dest/hook", "event_type": "e"})
    event = client.post("/api/v1/events", json={"event_type": "e", "payload": {"z": 2}}).json()

    sender = ScriptedSender(default=success())
    worker = Worker(
        context.session_factory,
        context.clock,
        sender,
        settings,
        worker_id="w",
        sleeper=lambda _: None,
    )
    assert worker.tick() == 1

    status = client.get(f"/api/v1/events/{event['id']}/status").json()
    assert status["succeeded"] == 1
    assert status["pending"] == 0
    assert sender.calls[0]["url"] == "http://dest/hook"


def test_end_to_end_failure_then_permanent_fail(client, context, settings, clock):
    client.post("/api/v1/subscriptions", json={"url": "http://dest", "event_type": "e"})
    event = client.post("/api/v1/events", json={"event_type": "e", "payload": {}}).json()

    sender = ScriptedSender(default=failure())
    worker = Worker(
        context.session_factory,
        context.clock,
        sender,
        settings,
        worker_id="w",
        sleeper=lambda _: None,
    )
    for attempt in range(1, settings.max_attempts + 1):
        worker.tick()
        clock.advance(settings.base_delay_seconds * (2 ** (attempt - 1)))

    status = client.get(f"/api/v1/events/{event['id']}/status").json()
    assert status["permanently_failed"] == 1
    assert status["succeeded"] == 0
    assert status["deliveries"][0]["attempts"] == settings.max_attempts
