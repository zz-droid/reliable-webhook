"""Cross-process claim exclusion (#6/#7 at the OS-process level).

Spawns two real OS processes that race to claim the same delivery on a shared
file-backed SQLite database. Exactly one must win.
"""
from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

CLAIMER = textwrap.dedent(
    """
    import sys, os
    sys.path.insert(0, %r)
    from app.config import Settings
    from app.db import make_engine, make_session_factory
    from app.clock import SystemClock
    from app.services.deliveries import DeliveryService

    db_url, project_root = sys.argv[1], sys.argv[2]
    s = Settings(database_url=db_url, lease_duration_seconds=30)
    eng = make_engine(s.database_url)
    f = make_session_factory(eng)
    sess = f()
    svc = DeliveryService(sess, SystemClock(), 30, 5)
    claimed = svc.claim_one("pid-" + str(os.getpid()))
    print(claimed.id if claimed else "NONE", flush=True)
    sess.close()
    eng.dispose()
    """
)


def test_two_os_processes_claim_exactly_one(tmp_path):
    project_root = str(Path(__file__).resolve().parents[1])
    db_path = (tmp_path / "proc.db").resolve()
    db_url = f"sqlite:///{db_path.as_posix()}"

    # Seed schema + one due delivery.
    sys.path.insert(0, project_root)
    try:
        from app.clock import SystemClock
        from app.db import init_db, make_engine, make_session_factory
        from app.services.events import EventService
        from app.services.subscriptions import SubscriptionService

        engine = make_engine(db_url)
        init_db(engine)
        factory = make_session_factory(engine)
        sess = factory()
        SubscriptionService(sess, SystemClock()).create("http://endpoint", "evt")
        EventService(sess, SystemClock(), 5).create("evt", {})
        sess.commit()
        sess.close()
        engine.dispose()
    finally:
        if project_root in sys.path:
            sys.path.remove(project_root)

    script = tmp_path / "claimer.py"
    script.write_text(CLAIMER % project_root)

    procs = [
        subprocess.Popen(
            [sys.executable, str(script), db_url, project_root],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    results = []
    errors = []
    for p in procs:
        out, err = p.communicate(timeout=30)
        results.append(out.strip())
        if p.returncode != 0:
            errors.append(err)

    assert errors == [], errors
    winners = [r for r in results if r and r != "NONE"]
    losers = [r for r in results if r == "NONE"]
    assert len(winners) == 1
    assert len(losers) == 1
