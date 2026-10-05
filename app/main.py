"""FastAPI application factory and process entry point."""
from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api.routes import router
from .clock import Clock
from .config import Settings
from .container import AppContext, build_context

logger = logging.getLogger("webhook")


def create_app(
    settings: Settings | None = None,
    clock: Clock | None = None,
    context: AppContext | None = None,
    start_worker: bool | None = None,
) -> FastAPI:
    ctx = context or build_context(settings, clock)
    should_start = (
        ctx.settings.run_worker_in_api if start_worker is None else start_worker
    )

    worker_state: dict = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if should_start:
            # Imported lazily so tests / pure-API deployments pay no HTTP cost.
            from .worker.sender import HttpSender
            from .worker.worker import Worker

            sender = HttpSender(ctx.settings.request_timeout_seconds)
            worker = Worker(
                ctx.session_factory,
                ctx.clock,
                sender,
                ctx.settings,
            )
            thread = threading.Thread(
                target=worker.run_forever, name="webhook-worker", daemon=True
            )
            worker_state.update(worker=worker, sender=sender, thread=thread)
            thread.start()
        try:
            yield
        finally:
            if should_start:
                worker = worker_state["worker"]
                thread = worker_state["thread"]
                worker.request_stop()
                thread.join(timeout=5)
                worker_state["sender"].close()

    app = FastAPI(
        title="Reliable Webhook Delivery Service",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.context = ctx
    app.state.worker_state = worker_state
    app.include_router(router, prefix="/api/v1")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def main() -> None:  # pragma: no cover - manual launch helper
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    port = int(os.environ.get("WEBHOOK_PORT", "8000"))
    uvicorn.run(create_app, host="0.0.0.0", port=port, factory=True)


if __name__ == "__main__":  # pragma: no cover
    main()
