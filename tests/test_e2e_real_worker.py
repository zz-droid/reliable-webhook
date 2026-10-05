"""True end-to-end test: API + in-process background worker + local receiver.

This spins up the FastAPI app with its worker thread enabled, posts an event
through the HTTP API, and waits (polling, no fixed sleep) for the worker to
deliver it to a local HTTP receiver.
"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.container import build_context
from app.main import create_app


class E2EHandler(BaseHTTPRequestHandler):
    received: list[dict] = []
    failures_remaining = 0
    request_count = 0
    lock = threading.Lock()

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length).decode())
        with self.lock:
            type(self).request_count += 1
            if type(self).failures_remaining > 0:
                type(self).failures_remaining -= 1
                code = 500
            else:
                code = 200
                type(self).received.append(body)
        self.send_response(code)
        self.end_headers()
        self.wfile.write(b"ok" if code < 400 else b"boom")

    def log_message(self, *args):
        pass


@pytest.fixture
def receiver():
    E2EHandler.received = []
    E2EHandler.failures_remaining = 0
    E2EHandler.request_count = 0
    server = HTTPServer(("127.0.0.1", 0), E2EHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}/webhook"
    server.shutdown()
    server.server_close()


def _wait_for(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_event_is_automatically_delivered(receiver, tmp_path):
    settings = Settings(
        database_url=f"sqlite:///{(tmp_path / 'e2e.db').as_posix()}",
        lease_duration_seconds=30,
        base_delay_seconds=1,
        max_attempts=3,
        worker_idle_seconds=0.02,
        run_worker_in_api=False,
    )
    ctx = build_context(settings)
    app = create_app(context=ctx, start_worker=True)

    with TestClient(app) as client:
        client.post(
            "/api/v1/subscriptions",
            json={"url": receiver, "event_type": "order.paid"},
        )
        event = client.post(
            "/api/v1/events",
            json={"event_type": "order.paid", "payload": {"order": 77}},
        ).json()

        assert _wait_for(lambda: len(E2EHandler.received) == 1)
        delivered = E2EHandler.received[0]
        assert delivered["event_id"] == event["id"]
        assert delivered["event_type"] == "order.paid"
        assert delivered["payload"] == {"order": 77}
        assert delivered["delivery_id"]

        # Status converges to succeeded via real persisted state.
        def succeeded():
            st = client.get(f"/api/v1/events/{event['id']}/status").json()
            return st["succeeded"] == 1 and st["total"] == 1

        assert _wait_for(succeeded)

    ctx.engine.dispose()


def test_event_retries_then_succeeds(receiver, tmp_path):
    settings = Settings(
        database_url=f"sqlite:///{(tmp_path / 'e2e2.db').as_posix()}",
        lease_duration_seconds=30,
        base_delay_seconds=0.05,
        max_attempts=5,
        worker_idle_seconds=0.02,
        run_worker_in_api=False,
    )
    ctx = build_context(settings)
    app = create_app(context=ctx, start_worker=True)

    with TestClient(app) as client:
        client.post(
            "/api/v1/subscriptions",
            json={"url": receiver, "event_type": "t"},
        )
        event = client.post(
            "/api/v1/events", json={"event_type": "t", "payload": {}}
        ).json()

        # Script the receiver: first two POSTs return 500, then 200.
        E2EHandler.failures_remaining = 2

        def eventually_succeeded():
            st = client.get(f"/api/v1/events/{event['id']}/status").json()
            return st["succeeded"] == 1

        assert _wait_for(eventually_succeeded, timeout=8)
        status = client.get(f"/api/v1/events/{event['id']}/status").json()
        assert status["deliveries"][0]["attempts"] == 3
        assert E2EHandler.request_count == 3

    ctx.engine.dispose()
