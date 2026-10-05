"""Database engine, session factory and schema initialisation."""
from __future__ import annotations

from collections.abc import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .models import Base


def _configure_sqlite(dbapi_connection, _connection_record) -> None:
    """Apply pragmas that make SQLite safe for concurrent worker processes.

    - WAL allows a reader and multiple writers from different processes
      without blocking each other on reads.
    - busy_wait makes writers wait for the write lock instead of failing
      immediately with "database is locked".
    - foreign_keys enforces ON DELETE constraints.
    """
    import sqlite3

    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=10000")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
    if isinstance(dbapi_connection, sqlite3.Connection):
        dbapi_connection.isolation_level = None  # autocommit at DBAPI level; SQLAlchemy manages txns


def make_engine(database_url: str) -> Engine:
    connect_args = {}
    if database_url.startswith("sqlite"):
        # Threads may share the connection (FastAPI + worker threads).
        connect_args["check_same_thread"] = False
    engine = create_engine(
        database_url,
        connect_args=connect_args,
        pool_pre_ping=True,
        future=True,
    )
    if database_url.startswith("sqlite"):
        event.listens_for(engine, "connect")(_configure_sqlite)
    return engine


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(engine)


def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """Context manager yielding a session with commit/rollback handling."""
    from contextlib import contextmanager

    @contextmanager
    def _scope() -> Iterator[Session]:
        session = session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    return _scope()
