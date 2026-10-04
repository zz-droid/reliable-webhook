def test_create_and_get_subscription(client):
    resp = client.post(
        "/subscriptions",
        json={"url": "http://example.test/hook", "event_type": "user.created"},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["url"] == "http://example.test/hook"
    assert body["event_type"] == "user.created"
    assert body["enabled"] is True
    assert body["id"]
    assert body["created_at"]

    got = client.get(f"/subscriptions/{body['id']}")
    assert got.status_code == 200
    assert got.json()["id"] == body["id"]

    listed = client.get("/subscriptions")
    assert listed.status_code == 200
    assert [s["id"] for s in listed.json()] == [body["id"]]


def test_get_missing_subscription_returns_404(client):
    assert client.get("/subscriptions/nope").status_code == 404


def test_disabled_subscription_gets_no_new_deliveries(client):
    sub_id = client.post(
        "/subscriptions", json={"url": "http://example.test/hook", "event_type": "x"}
    ).json()["id"]

    first = client.post("/events", json={"event_type": "x", "payload": {}}).json()
    assert client.get(f"/events/{first['id']}/status").json()["total"] == 1

    patch = client.patch(f"/subscriptions/{sub_id}", json={"enabled": False})
    assert patch.status_code == 200
    assert patch.json()["enabled"] is False

    second = client.post("/events", json={"event_type": "x", "payload": {}}).json()
    assert client.get(f"/events/{second['id']}/status").json()["total"] == 0

    # Historical deliveries survive the disable, and re-enabling resumes fan-out.
    assert client.get(f"/events/{first['id']}/status").json()["total"] == 1
    client.patch(f"/subscriptions/{sub_id}", json={"enabled": True})
    third = client.post("/events", json={"event_type": "x", "payload": {}}).json()
    assert client.get(f"/events/{third['id']}/status").json()["total"] == 1
