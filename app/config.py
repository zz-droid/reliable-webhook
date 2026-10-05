"""Application configuration.

All settings can be overridden through environment variables (prefix
``WEBHOOK_``) so the same code can run with different databases, retry
policies and worker settings without code changes.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.environ.get(f"WEBHOOK_{name}", default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(f"WEBHOOK_{name}")
    return int(raw) if raw is not None and raw != "" else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(f"WEBHOOK_{name}")
    return float(raw) if raw is not None and raw != "" else default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(f"WEBHOOK_{name}")
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    database_url: str = "sqlite:///./webhook.db"
    # Lease granted to a worker for one delivery attempt.
    lease_duration_seconds: float = 30.0
    # Exponential backoff: base_delay * 2 ** (attempt - 1).
    base_delay_seconds: float = 5.0
    max_attempts: int = 5
    # HTTP delivery timeout.
    request_timeout_seconds: float = 10.0
    # Worker loop behaviour.
    worker_idle_seconds: float = 0.5
    worker_batch_size: int = 10
    run_worker_in_api: bool = True

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=_env("DATABASE_URL", cls.database_url),
            lease_duration_seconds=_env_float(
                "LEASE_DURATION_SECONDS", cls.lease_duration_seconds
            ),
            base_delay_seconds=_env_float(
                "BASE_DELAY_SECONDS", cls.base_delay_seconds
            ),
            max_attempts=_env_int("MAX_ATTEMPTS", cls.max_attempts),
            request_timeout_seconds=_env_float(
                "REQUEST_TIMEOUT_SECONDS", cls.request_timeout_seconds
            ),
            worker_idle_seconds=_env_float(
                "WORKER_IDLE_SECONDS", cls.worker_idle_seconds
            ),
            worker_batch_size=_env_int("WORKER_BATCH_SIZE", cls.worker_batch_size),
            run_worker_in_api=_env_bool("RUN_WORKER_IN_API", cls.run_worker_in_api),
        )
