from datetime import timedelta

from sqlalchemy import select

from app.models import Delivery, DeliveryStatus, Subscription


def test_event_status_stats(client, session_factory, settings, clock, make_worker):
    client.post("/subscriptions", json={"url": "http://ok.test/hook", "event_type": "x"})
    client.post("/subscriptions", json={"url": "http://fail.test/hook", "event_type": "x"})
    client.post("/subscriptions", json={"url": "http://stuck.test/hook", "event_type": "x"})
    event_id = client.post(
        "/events", json={"event_type": "x", "payload": {"n": 1}, "idempotency_key": "k1"}
    ).json()["id"]

    # The delivery for stuck.test stays leased by a worker that never comes back yet.
    with session_factory() as session:
        delivery = session.scalars(
            select(Delivery)
            .join(Subscription, Delivery.subscription_id == Subscription.id)
            .where(Subscription.url.contains("stuck.test"))
        ).one()
        delivery.status = DeliveryStatus.RUNNING
        delivery.lease_owner = "stuck-worker"
        delivery.lease_token = "stuck-token"
        delivery.lease_expires_at = clock.now() + timedelta(seconds=settings.lease_seconds)
        session.commit()

    def sender(url, body, timeout):
        return 200 if "ok.test" in url else 500

    assert make_worker(sender).run_once() == 2

    resp = client.get(f"/events/{event_id}/status")
    assert resp.status_code == 200
    status = resp.json()

    assert status["event"]["id"] == event_id
    assert status["event"]["event_type"] == "x"
    assert status["event"]["payload"] == {"n": 1}
    assert status["total"] == 3
    assert status["succeeded"] == 1
    assert status["pending"] == 1
    assert status["running"] == 1
    assert status["failed"] == 0

    deliveries = status["deliveries"]
    assert len(deliveries) == 3
    by_status = {d["status"]: d for d in deliveries}
    assert by_status["succeeded"]["attempt_count"] == 0
    assert by_status["running"]["attempt_count"] == 0
    assert by_status["pending"]["attempt_count"] == 1
    assert by_status["pending"]["last_error"] == "HTTP 500"
    assert by_status["running"]["lease_owner"] == "stuck-worker"


def test_event_status_not_found(client):
    assert client.get("/events/missing/status").status_code == 404
