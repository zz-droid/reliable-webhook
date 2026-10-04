from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    database_url: str = "sqlite:///./webhook.db"
    max_attempts: int = 5
    base_delay_seconds: float = 1.0
    lease_seconds: float = 30.0
    http_timeout_seconds: float = 10.0
    worker_batch_size: int = 10
    worker_poll_interval_seconds: float = 0.5

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.getenv("WEBHOOK_DATABASE_URL", cls.database_url),
            max_attempts=int(os.getenv("WEBHOOK_MAX_ATTEMPTS", cls.max_attempts)),
            base_delay_seconds=float(os.getenv("WEBHOOK_BASE_DELAY_SECONDS", cls.base_delay_seconds)),
            lease_seconds=float(os.getenv("WEBHOOK_LEASE_SECONDS", cls.lease_seconds)),
            http_timeout_seconds=float(os.getenv("WEBHOOK_HTTP_TIMEOUT_SECONDS", cls.http_timeout_seconds)),
            worker_batch_size=int(os.getenv("WEBHOOK_WORKER_BATCH_SIZE", cls.worker_batch_size)),
            worker_poll_interval_seconds=float(
                os.getenv("WEBHOOK_WORKER_POLL_INTERVAL_SECONDS", cls.worker_poll_interval_seconds)
            ),
        )
