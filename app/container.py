"""Application container: wires config, DB, clock and services together."""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .clock import Clock, SystemClock
from .config import Settings
from .db import init_db, make_engine, make_session_factory


@dataclass
class AppContext:
    settings: Settings
    engine: Engine
    session_factory: sessionmaker[Session]
    clock: Clock


def build_context(settings: Settings | None = None, clock: Clock | None = None) -> AppContext:
    settings = settings or Settings.from_env()
    engine = make_engine(settings.database_url)
    init_db(engine)
    return AppContext(
        settings=settings,
        engine=engine,
        session_factory=make_session_factory(engine),
        clock=clock or SystemClock(),
    )
