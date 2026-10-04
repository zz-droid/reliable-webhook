import threading

from sqlalchemy import func, select

from app import services
from app.models import Delivery, Event


def test_event_fans_out_to_matching_subscriptions(client):
    client.post("/subscriptions", json={"url": "http://a.test/hook", "event_type": "x"})
    client.post("/subscriptions", json={"url": "http://b.test/hook", "event_type": "x"})
    client.post("/subscriptions", json={"url": "http://c.test/hook", "event_type": "other"})

    resp = client.post("/events", json={"event_type": "x", "payload": {"n": 1}})
    assert resp.status_code == 201
    status = client.get(f"/events/{resp.json()['id']}/status").json()

    assert status["total"] == 2
    assert len({d["subscription_id"] for d in status["deliveries"]}) == 2
    assert all(d["status"] == "pending" for d in status["deliveries"])


def test_deliveries_are_independent(client, session_factory, settings, clock, make_worker):
    client.post("/subscriptions", json={"url": "http://ok.test/hook", "event_type": "x"})
    client.post("/subscriptions", json={"url": "http://fail.test/hook", "event_type": "x"})
    event_id = client.post("/events", json={"event_type": "x", "payload": {}}).json()["id"]

    def sender(url, body, timeout):
        return 200 if "ok.test" in url else 500

    make_worker(sender).run_once()

    status = client.get(f"/events/{event_id}/status").json()
    by_status = {d["status"] for d in status["deliveries"]}
    assert status["succeeded"] == 1
    assert status["pending"] == 1  # failed attempt scheduled for retry
    assert by_status == {"succeeded", "pending"}


def test_idempotency_key_replay_returns_original(client):
    client.post("/subscriptions", json={"url": "http://a.test/hook", "event_type": "x"})

    first = client.post(
        "/events", json={"event_type": "x", "payload": {"v": 1}, "idempotency_key": "key-1"}
    )
    assert first.status_code == 201
    second = client.post(
        "/events", json={"event_type": "x", "payload": {"v": 1}, "idempotency_key": "key-1"}
    )
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]

    status = client.get(f"/events/{first.json()['id']}/status").json()
    assert status["total"] == 1  # no duplicate deliveries


def test_events_without_key_are_not_deduplicated(client):
    a = client.post("/events", json={"event_type": "x", "payload": {}})
    b = client.post("/events", json={"event_type": "x", "payload": {}})
    assert a.json()["id"] != b.json()["id"]


def test_concurrent_same_idempotency_key(session_factory, clock):
    with session_factory() as session:
        services.create_subscription(session, url="http://a.test/hook", event_type="x", clock=clock)

    event_ids, errors = [], []
    barrier = threading.Barrier(6)

    def create():
        with session_factory() as session:
            try:
                barrier.wait(timeout=10)
                event, _ = services.create_event(
                    session,
                    event_type="x",
                    payload={},
                    idempotency_key="dup-key",
                    clock=clock,
                )
                event_ids.append(event.id)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

    threads = [threading.Thread(target=create) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(set(event_ids)) == 1
    with session_factory() as session:
        assert session.scalar(select(func.count(Event.id))) == 1
        assert session.scalar(select(func.count(Delivery.id))) == 1
