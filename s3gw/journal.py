"""Intent journal and per-key state (docs/gateway-design.md, sections 3, 4, 6, 7).

All writers share one SQLite file in WAL mode. The gateway connection uses
synchronous=FULL; handler and bulk connections may use NORMAL.
"""
from __future__ import annotations

import os
import re
import socket
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS gateway_epochs (
  epoch       INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at  TEXT NOT NULL,
  host        TEXT NOT NULL,
  pid         INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  epoch       INTEGER NOT NULL,
  key         TEXT NOT NULL,
  op          TEXT NOT NULL,
  request_id  TEXT NOT NULL,
  created_at  TEXT NOT NULL,
  result      TEXT,
  http_status INTEGER,
  s3_code     TEXT,
  finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_intents_open ON intents (result) WHERE result IS NULL;
CREATE INDEX IF NOT EXISTS idx_intents_key ON intents (key);
CREATE INDEX IF NOT EXISTS idx_intents_request ON intents (request_id);

CREATE TABLE IF NOT EXISTS key_state (
  key          TEXT PRIMARY KEY,
  change_seq   INTEGER NOT NULL DEFAULT 0,
  synced_seq   INTEGER NOT NULL DEFAULT -1,
  in_flight    INTEGER NOT NULL DEFAULT 0,
  state        TEXT NOT NULL,
  lease_owner  TEXT,
  lease_until  REAL,
  not_before   REAL NOT NULL DEFAULT 0,
  attempts     INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT,
  src_size     INTEGER,
  src_etag     TEXT,
  updated_at   TEXT NOT NULL
);
-- Keys out of sync are always in one of WORK_STATES; queries over key_state name
-- them explicitly so SQLite uses this index instead of scanning 12M rows.
CREATE INDEX IF NOT EXISTS idx_key_state_work ON key_state (state, not_before)
  WHERE state IN ('dirty', 'uncertain', 'syncing', 'bulk');

CREATE TABLE IF NOT EXISTS mpu (
  upload_id   TEXT PRIMARY KEY,
  key         TEXT NOT NULL,
  epoch       INTEGER NOT NULL,
  created_at  TEXT NOT NULL,
  closed_at   TEXT,
  outcome     TEXT
);

CREATE TABLE IF NOT EXISTS errors (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  at          TEXT NOT NULL,
  component   TEXT NOT NULL,
  key         TEXT,
  kind        TEXT NOT NULL,
  detail      TEXT
);

CREATE TABLE IF NOT EXISTS counters (
  name  TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);
"""

# Results of a gateway operation.
SUCCESS = "success"
ERROR = "error"
UNCERTAIN = "uncertain"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


_FRACTION = re.compile(r"\.(\d+)")


def utc_iso(ts: str) -> Optional[str]:
    """RFC 3339 time ('...T12:00:00Z', Go's '...T12:00:00.123456789+05:00') in now_iso() format, or None.

    Stored times compare as strings, so a bound must have the same format: '...:00Z' sorts after '...:00.5Z'.
    """
    ts = _FRACTION.sub(lambda m: "." + (m.group(1) + "000000")[:6], ts.strip().replace("Z", "+00:00"), count=1)
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def connect(path: Path, synchronous: str = "FULL", timeout: float = 30.0) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=timeout, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute(f"PRAGMA synchronous = {synchronous}")
    conn.execute(f"PRAGMA busy_timeout = {int(timeout * 1000)}")
    conn.executescript(SCHEMA)
    return conn


class _Tx:
    """BEGIN IMMEDIATE ... COMMIT/ROLLBACK for autocommit connections."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        if exc_type is None:
            self.conn.execute("COMMIT")
        else:
            self.conn.execute("ROLLBACK")


def tx(conn: sqlite3.Connection) -> _Tx:
    return _Tx(conn)


def counter_inc(conn: sqlite3.Connection, name: str, delta: int = 1) -> None:
    conn.execute(
        "INSERT INTO counters (name, value) VALUES (?, ?) "
        "ON CONFLICT(name) DO UPDATE SET value = value + excluded.value",
        (name, delta),
    )


def record_error(conn: sqlite3.Connection, component: str, kind: str, key: Optional[str], detail: str) -> None:
    conn.execute(
        "INSERT INTO errors (at, component, key, kind, detail) VALUES (?, ?, ?, ?, ?)",
        (now_iso(), component, key, kind, detail[:2000]),
    )


# ---------------------------------------------------------------------------
# Gateway side
# ---------------------------------------------------------------------------

def start_epoch(conn: sqlite3.Connection, settle_seconds: float, now: Optional[float] = None) -> int:
    """Register a new gateway epoch and close intents left open by earlier ones.

    Section 7: open intents of previous epochs become 'uncertain'; their keys
    get in_flight-1, change_seq+1 and are not picked before settle_seconds.
    """
    t = time.time() if now is None else now
    with tx(conn):
        cur = conn.execute(
            "INSERT INTO gateway_epochs (started_at, host, pid) VALUES (?, ?, ?)",
            (now_iso(), socket.gethostname(), os.getpid()),
        )
        epoch = int(cur.lastrowid)
        open_rows = conn.execute(
            "SELECT id, key FROM intents WHERE result IS NULL AND epoch < ?", (epoch,)
        ).fetchall()
        for row in open_rows:
            conn.execute(
                "UPDATE intents SET result = ?, finished_at = ? WHERE id = ?",
                (UNCERTAIN, now_iso(), row["id"]),
            )
            conn.execute(
                """UPDATE key_state
                   SET in_flight = MAX(in_flight - 1, 0), change_seq = change_seq + 1,
                       state = 'uncertain', not_before = MAX(not_before, ?), updated_at = ?
                   WHERE key = ?""",
                (t + settle_seconds, now_iso(), row["key"]),
            )
        conn.execute(
            "UPDATE mpu SET outcome = 'uncertain' WHERE closed_at IS NULL AND outcome IS NULL AND epoch < ?",
            (epoch,),
        )
        if open_rows:
            counter_inc(conn, "intents_recovered_uncertain", len(open_rows))
    return epoch


def write_intents(
    conn: sqlite3.Connection, epoch: int, request_id: str, op: str, keys: Sequence[str]
) -> List[int]:
    """Step 1: durable intent for every key, in one transaction. Returns intent ids."""
    ts = now_iso()
    ids: List[int] = []
    with tx(conn):
        for key in keys:
            cur = conn.execute(
                "INSERT INTO intents (epoch, key, op, request_id, created_at) VALUES (?, ?, ?, ?, ?)",
                (epoch, key, op, request_id, ts),
            )
            ids.append(int(cur.lastrowid))
            conn.execute(
                """INSERT INTO key_state (key, change_seq, in_flight, state, updated_at)
                   VALUES (?, 1, 1, 'dirty', ?)
                   ON CONFLICT(key) DO UPDATE SET
                     change_seq = change_seq + 1,
                     in_flight = in_flight + 1,
                     attempts = CASE WHEN state = 'error' THEN 0 ELSE attempts END,
                     state = CASE WHEN state IN ('syncing', 'bulk') THEN state ELSE 'dirty' END,
                     updated_at = excluded.updated_at""",
                (key, ts),
            )
        counter_inc(conn, "intents", len(keys))
    return ids


def write_results(
    conn: sqlite3.Connection,
    results: Iterable[Tuple[int, str, str, Optional[int], Optional[str]]],
) -> None:
    """Step 3: (intent_id, key, result, http_status, s3_code) for each intent."""
    ts = now_iso()
    with tx(conn):
        for intent_id, key, result, http_status, s3_code in results:
            cur = conn.execute(
                """UPDATE intents SET result = ?, http_status = ?, s3_code = ?, finished_at = ?
                   WHERE id = ? AND result IS NULL""",
                (result, http_status, s3_code, ts, intent_id),
            )
            if cur.rowcount == 0:
                continue  # already closed by recovery
            conn.execute(
                """UPDATE key_state
                   SET in_flight = MAX(in_flight - 1, 0), change_seq = change_seq + 1,
                       state = CASE WHEN state IN ('syncing', 'bulk') THEN state ELSE 'dirty' END,
                       updated_at = ?
                   WHERE key = ?""",
                (ts, key),
            )
            counter_inc(conn, f"result_{result}")


def mpu_open(conn: sqlite3.Connection, epoch: int, upload_id: str, key: str) -> None:
    with tx(conn):
        conn.execute(
            "INSERT OR IGNORE INTO mpu (upload_id, key, epoch, created_at) VALUES (?, ?, ?, ?)",
            (upload_id, key, epoch, now_iso()),
        )


def mpu_close(conn: sqlite3.Connection, upload_id: str, outcome: str) -> None:
    with tx(conn):
        conn.execute(
            "UPDATE mpu SET closed_at = ?, outcome = ? WHERE upload_id = ? AND closed_at IS NULL",
            (now_iso(), outcome, upload_id),
        )


# ---------------------------------------------------------------------------
# Handler / bulk side: leases
# ---------------------------------------------------------------------------

@dataclass
class Lease:
    key: str
    owner: str
    seq: int
    until: float


def claim_dirty(
    conn: sqlite3.Connection, owner: str, lease_seconds: float, limit: int = 1, now: Optional[float] = None
) -> List[Lease]:
    """Section 5, step 1: lease dirty keys with no operation in flight."""
    t = time.time() if now is None else now
    until = t + lease_seconds
    leases: List[Lease] = []
    with tx(conn):
        # Runs under the write lock: a full scan here would hold the gateway's intents past its timeout.
        rows = conn.execute(
            """SELECT key, change_seq FROM key_state
               WHERE state IN ('dirty', 'uncertain', 'syncing', 'bulk') AND in_flight = 0 AND not_before <= ?
                 AND (
                   (state IN ('dirty', 'uncertain') AND (lease_owner IS NULL OR lease_until < ?))
                   OR (state IN ('syncing', 'bulk') AND lease_until < ?)
                 )
               ORDER BY not_before LIMIT ?""",
            (t, t, t, limit),
        ).fetchall()
        for row in rows:
            conn.execute(
                """UPDATE key_state SET state = 'syncing', lease_owner = ?, lease_until = ?, updated_at = ?
                   WHERE key = ?""",
                (owner, until, now_iso(), row["key"]),
            )
            leases.append(Lease(row["key"], owner, int(row["change_seq"]), until))
    return leases


def finish_sync(
    conn: sqlite3.Connection,
    lease: Lease,
    outcome: str,
    src_size: Optional[int] = None,
    src_etag: Optional[str] = None,
) -> bool:
    """Section 5, step 4. outcome: 'synced' | 'deleted'. Returns True if the key is now clean."""
    with tx(conn):
        row = conn.execute(
            "SELECT change_seq, in_flight, lease_owner FROM key_state WHERE key = ?", (lease.key,)
        ).fetchone()
        if row is None or row["lease_owner"] != lease.owner:
            counter_inc(conn, "sync_lease_lost")
            return False
        clean = int(row["change_seq"]) == lease.seq and int(row["in_flight"]) == 0
        if clean:
            conn.execute(
                """UPDATE key_state SET synced_seq = ?, state = ?, lease_owner = NULL, lease_until = NULL,
                       attempts = 0, last_error = NULL, src_size = ?, src_etag = ?, updated_at = ?
                   WHERE key = ?""",
                (lease.seq, outcome, src_size, src_etag, now_iso(), lease.key),
            )
            counter_inc(conn, f"sync_{outcome}")
        else:
            conn.execute(
                """UPDATE key_state SET state = 'dirty', lease_owner = NULL, lease_until = NULL, updated_at = ?
                   WHERE key = ?""",
                (now_iso(), lease.key),
            )
            counter_inc(conn, "sync_changed_during")
        return clean


def fail_sync(
    conn: sqlite3.Connection,
    lease: Lease,
    error: str,
    backoff_seconds: float,
    max_attempts: int,
    now: Optional[float] = None,
) -> str:
    """Release a lease after a failed attempt. Returns the new state ('dirty' or 'error')."""
    t = time.time() if now is None else now
    with tx(conn):
        row = conn.execute("SELECT attempts, lease_owner FROM key_state WHERE key = ?", (lease.key,)).fetchone()
        if row is None or row["lease_owner"] != lease.owner:
            return "lost"
        attempts = int(row["attempts"]) + 1
        state = "error" if attempts >= max_attempts else "dirty"
        delay = min(backoff_seconds * (2 ** (attempts - 1)), 600.0)
        conn.execute(
            """UPDATE key_state SET state = ?, attempts = ?, last_error = ?, not_before = ?,
                   lease_owner = NULL, lease_until = NULL, updated_at = ?
               WHERE key = ?""",
            (state, attempts, error[:2000], t + delay, now_iso(), lease.key),
        )
        counter_inc(conn, f"sync_fail_{state}")
        return state


def bulk_claim(
    conn: sqlite3.Connection, key: str, owner: str, lease_seconds: float, now: Optional[float] = None
) -> Optional[Lease]:
    """Section 6, step 1: take a key only if the journal has never seen it."""
    t = time.time() if now is None else now
    with tx(conn):
        cur = conn.execute(
            """INSERT INTO key_state (key, change_seq, in_flight, state, lease_owner, lease_until, updated_at)
               VALUES (?, 0, 0, 'bulk', ?, ?, ?) ON CONFLICT(key) DO NOTHING""",
            (key, owner, t + lease_seconds, now_iso()),
        )
        if cur.rowcount == 0:
            return None
    return Lease(key, owner, 0, t + lease_seconds)


def bulk_finish(conn: sqlite3.Connection, lease: Lease, source_missing: bool) -> str:
    """Section 6, step 3. Returns 'synced', 'dirty', 'forgotten' or 'lost'."""
    with tx(conn):
        row = conn.execute(
            "SELECT change_seq, in_flight, lease_owner FROM key_state WHERE key = ?", (lease.key,)
        ).fetchone()
        if row is None or row["lease_owner"] != lease.owner:
            return "lost"
        clean = int(row["change_seq"]) == 0 and int(row["in_flight"]) == 0
        if clean and source_missing:
            conn.execute("DELETE FROM key_state WHERE key = ?", (lease.key,))
            return "forgotten"
        if clean:
            conn.execute(
                """UPDATE key_state SET state = 'synced', synced_seq = 0, lease_owner = NULL, lease_until = NULL,
                       updated_at = ? WHERE key = ?""",
                (now_iso(), lease.key),
            )
            return "synced"
        conn.execute(
            """UPDATE key_state SET state = 'dirty', lease_owner = NULL, lease_until = NULL, updated_at = ?
               WHERE key = ?""",
            (now_iso(), lease.key),
        )
        return "dirty"


def lease_remaining(lease: Lease, now: Optional[float] = None) -> float:
    return lease.until - (time.time() if now is None else now)


def mark_dirty(conn: sqlite3.Connection, keys: Iterable[str]) -> int:
    """Keys changed in MinIO past the gateway (e.g. while the redirect was rolled back):
    the handler re-reads them, bulk copy no longer owns them. Same update as write_results."""
    ts = now_iso()
    n = 0
    with tx(conn):
        for key in keys:
            conn.execute(
                """INSERT INTO key_state (key, change_seq, state, updated_at) VALUES (?, 1, 'dirty', ?)
                   ON CONFLICT(key) DO UPDATE SET
                     change_seq = change_seq + 1,
                     state = CASE WHEN state IN ('syncing', 'bulk') THEN state ELSE 'dirty' END,
                     updated_at = excluded.updated_at""",
                (key, ts),
            )
            n += 1
        counter_inc(conn, "marked_dirty", n)
    return n


def backlog(conn: sqlite3.Connection) -> Tuple[int, Optional[str]]:
    """Number of keys not yet in sync and the oldest updated_at among them.

    A key leaves 'synced'/'deleted' with every change, so these states are exactly
    the keys not in sync (minus 'error'); the partial index keeps this off the
    12M rows the bulk transfer leaves in key_state.
    """
    row = conn.execute(
        """SELECT count(*) AS n, min(updated_at) AS oldest FROM key_state
           WHERE state IN ('dirty', 'uncertain', 'syncing', 'bulk')
             AND NOT (synced_seq = change_seq AND in_flight = 0)"""
    ).fetchone()
    return int(row["n"]), row["oldest"]


def bulk_release(conn: sqlite3.Connection, lease: Lease) -> str:
    """Bulk failed on a key and will retry it later: drop its claim.

    An untouched key goes back to 'never seen' so the next bulk attempt can
    claim it again; a key the gateway changed meanwhile goes to the handler.
    """
    with tx(conn):
        row = conn.execute(
            "SELECT change_seq, in_flight, lease_owner FROM key_state WHERE key = ?", (lease.key,)
        ).fetchone()
        if row is None or row["lease_owner"] != lease.owner:
            return "lost"
        if int(row["change_seq"]) == 0 and int(row["in_flight"]) == 0:
            conn.execute("DELETE FROM key_state WHERE key = ?", (lease.key,))
            return "released"
        conn.execute(
            """UPDATE key_state SET state = 'dirty', lease_owner = NULL, lease_until = NULL, updated_at = ?
               WHERE key = ?""",
            (now_iso(), lease.key),
        )
        return "dirty"


def live_epoch(conn: sqlite3.Connection) -> Optional[int]:
    row = conn.execute("SELECT max(epoch) FROM gateway_epochs").fetchone()
    return None if row is None or row[0] is None else int(row[0])


def purge_intents(conn: sqlite3.Connection, older_than_days: float, batch: int = 10000) -> int:
    """Delete closed intents older than the retention whose key is in sync.

    Open intents and intents of keys still waiting for the handler are kept:
    they are the evidence the handler and recovery rely on.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=older_than_days)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    total = 0
    while True:
        with tx(conn):
            cur = conn.execute(
                """DELETE FROM intents WHERE id IN (
                     SELECT i.id FROM intents i JOIN key_state s ON s.key = i.key
                     WHERE i.result IS NOT NULL AND i.finished_at < ?
                       AND s.synced_seq = s.change_seq AND s.in_flight = 0
                     LIMIT ?)""",
                (cutoff, batch),
            )
            n = cur.rowcount
        total += n
        if n < batch:
            return total
