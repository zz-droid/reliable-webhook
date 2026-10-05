"""HTTP delivery transport.

The transport is isolated behind a small interface so tests can substitute a
fake sender without any network I/O.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import httpx


@dataclass
class SendResult:
    ok: bool
    status_code: int | None = None
    error: str | None = None


class Sender(Protocol):
    def send(
        self,
        url: str,
        *,
        event_id: str,
        event_type: str,
        delivery_id: str,
        payload: Any,
    ) -> SendResult:
        ...


class HttpSender:
    def __init__(self, timeout_seconds: float = 10.0) -> None:
        # trust_env=False keeps ambient HTTP(S)_PROXY settings from silently
        # rerouting webhook deliveries; endpoints are called directly.
        self._client = httpx.Client(timeout=timeout_seconds, trust_env=False)

    def send(
        self,
        url: str,
        *,
        event_id: str,
        event_type: str,
        delivery_id: str,
        payload: Any,
    ) -> SendResult:
        body = {
            "delivery_id": delivery_id,
            "event_id": event_id,
            "event_type": event_type,
            "payload": payload,
        }
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Event-Id": event_id,
            "X-Webhook-Event-Type": event_type,
            "X-Webhook-Delivery-Id": delivery_id,
        }
        try:
            response = self._client.post(url, json=body, headers=headers)
        except httpx.HTTPError as exc:
            # Covers connection errors, timeouts, DNS failures, etc.
            return SendResult(ok=False, status_code=None, error=f"{type(exc).__name__}: {exc}")
        ok = 200 <= response.status_code < 300
        error = None
        if not ok:
            error = f"HTTP {response.status_code}: {response.text[:500]}"
        return SendResult(ok=ok, status_code=response.status_code, error=error)

    def close(self) -> None:
        self._client.close()
