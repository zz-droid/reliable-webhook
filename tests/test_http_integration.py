"""Integration test exercising the real HttpSender against a local server.

No public network and no real sleeps: a tiny HTTP server runs in a daemon
thread on localhost, and its failure mode is scripted from the test.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from app.worker.sender import HttpSender


class _Handler(BaseHTTPRequestHandler):
    received: list[dict] = []
    status_to_return = 200
    lock = threading.Lock()

    def do_POST(self):  # noqa: N802 - http.server API
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        body = json.loads(raw.decode())
        with self.lock:
            self.received.append(
                {
                    "path": self.path,
                    "headers": {
                        "X-Webhook-Event-Id": self.headers.get("X-Webhook-Event-Id"),
                        "X-Webhook-Event-Type": self.headers.get(
                            "X-Webhook-Event-Type"
                        ),
                        "X-Webhook-Delivery-Id": self.headers.get(
                            "X-Webhook-Delivery-Id"
                        ),
                        "Content-Type": self.headers.get("Content-Type"),
                    },
                    "body": body,
                }
            )
        code = type(self).status_to_return
        self.send_response(code)
        self.end_headers()
        self.wfile.write(b"ok" if code < 400 else b"boom")

    def log_message(self, *args):  # silence test server logs
        pass


@pytest.fixture
def receiver():
    _Handler.received = []
    _Handler.status_to_return = 200
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server, f"http://127.0.0.1:{port}/hook"
    server.shutdown()
    server.server_close()


def test_sender_posts_full_envelope_on_success(receiver):
    _server, url = receiver
    sender = HttpSender(timeout_seconds=5)
    try:
        result = sender.send(
            url,
            event_id="evt-1",
            event_type="order.created",
            delivery_id="del-1",
            payload={"amount": 42},
        )
    finally:
        sender.close()

    assert result.ok is True
    assert result.status_code == 200
    assert len(_Handler.received) == 1
    rec = _Handler.received[0]
    assert rec["path"] == "/hook"
    assert rec["body"] == {
        "delivery_id": "del-1",
        "event_id": "evt-1",
        "event_type": "order.created",
        "payload": {"amount": 42},
    }
    assert rec["headers"]["X-Webhook-Event-Id"] == "evt-1"
    assert rec["headers"]["X-Webhook-Delivery-Id"] == "del-1"
    assert rec["headers"]["Content-Type"] == "application/json"


@pytest.mark.parametrize("code", [404, 500, 503])
def test_sender_reports_non_2xx(receiver, code):
    _server, url = receiver
    _Handler.status_to_return = code
    sender = HttpSender(timeout_seconds=5)
    try:
        result = sender.send(
            url, event_id="e", event_type="t", delivery_id="d", payload={}
        )
    finally:
        sender.close()
    assert result.ok is False
    assert result.status_code == code
    assert "boom" in result.error


def test_sender_connection_error_is_caught():
    # Nothing listens on this port (ephemeral range, closed socket).
    import socket

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    sender = HttpSender(timeout_seconds=2)
    try:
        result = sender.send(
            f"http://127.0.0.1:{port}/",
            event_id="e",
            event_type="t",
            delivery_id="d",
            payload={},
        )
    finally:
        sender.close()
    assert result.ok is False
    assert result.status_code is None
    assert isinstance(result.error, str) and result.error
