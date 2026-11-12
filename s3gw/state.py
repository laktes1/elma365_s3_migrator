"""State of the change journal at a glance, read-only (PIPELINE.md: monitoring, C2.2, C4.2).

    python -m s3gw.state                       # JOURNAL_DB from the environment, as the units have it
    python -m s3gw.state --wait-drained 600    # until nothing is in flight or waiting for the handler

Prints JSON. Exit code 0; with --wait-drained 1 if the journal did not drain in time; 2 if it cannot be opened.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from . import journal

POLL_SECONDS = 2.0
REPORT_EVERY = 30.0
# Longer than any request takes to get its result written: such an intent lost it (disk full, journal error)
# and its key waits for a gateway restart (PIPELINE.md, "Намерение осталось без результата").
STUCK_SECONDS = 60.0


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def queue(conn: sqlite3.Connection) -> Dict[str, Any]:
    """What C2.2 waits for; by the partial indexes, cheap to repeat."""
    backlog, oldest = journal.backlog(conn)
    row = conn.execute("SELECT count(*) AS n, min(created_at) AS oldest FROM intents WHERE result IS NULL").fetchone()
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=STUCK_SECONDS)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    stuck = conn.execute("SELECT count(*) FROM intents WHERE result IS NULL AND created_at < ?", (cutoff,)).fetchone()[0]
    return {"drained": backlog == 0 and row["n"] == 0, "backlog": backlog, "backlog_oldest": oldest,
            "open_intents": row["n"], "open_intents_oldest": row["oldest"], "stuck_intents": stuck}


def state(conn: sqlite3.Connection) -> Dict[str, Any]:
    epoch = conn.execute("SELECT epoch, started_at, host, pid FROM gateway_epochs ORDER BY epoch DESC LIMIT 1").fetchone()
    # No index on 'error' (keys leave it only by hand): one scan of key_state, as the query it replaces.
    errors = [dict(r) for r in conn.execute("SELECT key, attempts, last_error FROM key_state WHERE state = 'error'")]
    return {
        "epoch": dict(epoch) if epoch else None,
        **queue(conn),
        "error_keys": len(errors),
        "error_sample": errors[:20],
        "open_mpu": conn.execute("SELECT count(*) FROM mpu WHERE closed_at IS NULL").fetchone()[0],
        "recent_errors": [dict(r) for r in conn.execute(
            "SELECT at, component, kind, key, substr(detail, 1, 300) AS detail FROM errors ORDER BY id DESC LIMIT 5")],
        "counters": {r["name"]: r["value"] for r in conn.execute("SELECT name, value FROM counters")},
    }


def wait_drained(conn: sqlite3.Connection, timeout: float) -> bool:
    end = time.monotonic() + timeout
    next_report = time.monotonic() + REPORT_EVERY
    hinted = False
    while True:
        q = queue(conn)
        if q["drained"]:
            return True
        if q["stuck_intents"] and not hinted:
            hinted = True
            print(f"{q['stuck_intents']} intents have no result for over {STUCK_SECONDS:.0f} s: their keys wait for a "
                  "gateway restart (PIPELINE.md, «Намерение осталось без результата»)", file=sys.stderr, flush=True)
        if time.monotonic() >= end:
            return False
        if time.monotonic() >= next_report:
            next_report = time.monotonic() + REPORT_EVERY
            print(f"waiting: backlog {q['backlog']}, open intents {q['open_intents']}", file=sys.stderr, flush=True)
        time.sleep(POLL_SECONDS)


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description="State of the gateway journal: epoch, backlog, open intents, keys in error")
    p.add_argument("--journal", default=os.environ.get("JOURNAL_DB", "journal.db"))
    p.add_argument("--wait-drained", type=float, metavar="SECONDS",
                   help="wait until no key waits for the handler and no intent is open, then print the state")
    a = p.parse_args(argv)
    try:
        conn = connect(a.journal)
        try:
            drained = wait_drained(conn, a.wait_drained) if a.wait_drained is not None else True
            print(json.dumps({"journal": a.journal, **state(conn)}, ensure_ascii=False, indent=2))
        finally:
            conn.close()
    except sqlite3.OperationalError as exc:  # no journal yet: the gateway creates it on its first start
        print(f"s3gw.state: {a.journal}: {exc} (the gateway has not created the journal yet?)", file=sys.stderr)
        return 2
    return 0 if drained else 1


if __name__ == "__main__":
    sys.exit(main())
