"""Run a standalone worker process: ``python -m app.worker_main``."""
from __future__ import annotations

import logging

from .config import Settings
from .worker.worker import run_standalone_worker


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    run_standalone_worker(Settings.from_env())


if __name__ == "__main__":
    main()
