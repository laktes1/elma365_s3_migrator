"""Bulk transfer: list the source once with checkpoints, copy keys the journal has not seen.

docs/bulk-transfer-design.md. Runs only while the gateway journal is live.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import signal
import sqlite3
import sys
import threading
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence, Tuple

from . import journal
from .copykey import SOURCE_MISSING, CopyConfig, LeaseExpired, min_lease_seconds, sync_key, write_guard

BULK_SCHEMA = """
CREATE TABLE IF NOT EXISTS ranges (
  id           INTEGER PRIMARY KEY,
  lo           TEXT NOT NULL,
  hi           TEXT,
  status       TEXT NOT NULL,
  start_after  TEXT,
  listed_count INTEGER NOT NULL DEFAULT 0,
  listed_bytes INTEGER NOT NULL DEFAULT 0,
  pages        INTEGER NOT NULL DEFAULT 0,
  attempts     INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT,
  started_at   TEXT,
  finished_at  TEXT,
  updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
  key           TEXT PRIMARY KEY,
  range_id      INTEGER NOT NULL,
  size          INTEGER NOT NULL,
  etag          TEXT,
  last_modified TEXT,
  state         TEXT NOT NULL,
  owner         TEXT,
  claim_until   REAL,
  attempts      INTEGER NOT NULL DEFAULT 0,
  not_before    REAL NOT NULL DEFAULT 0,
  last_error    TEXT,
  verify        TEXT,
  updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_work ON jobs (state, range_id) WHERE state IN ('pending', 'retry');
CREATE INDEX IF NOT EXISTS idx_jobs_claimed ON jobs (state, claim_until) WHERE state = 'claimed';

CREATE TABLE IF NOT EXISTS control (
  name  TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Jobs by state, kept by triggers (_ensure_job_counts): status and the end-of-work check
-- read this instead of scanning 12M rows of jobs.
CREATE TABLE IF NOT EXISTS job_counts (
  state TEXT PRIMARY KEY,
  n     INTEGER NOT NULL DEFAULT 0,
  bytes INTEGER NOT NULL DEFAULT 0
);
"""

_JOB_COUNT_TRIGGERS = (
    """CREATE TRIGGER jobs_count_insert AFTER INSERT ON jobs BEGIN
         INSERT INTO job_counts (state, n, bytes) VALUES (NEW.state, 1, NEW.size)
           ON CONFLICT(state) DO UPDATE SET n = n + 1, bytes = bytes + excluded.bytes;
       END""",
    """CREATE TRIGGER jobs_count_update AFTER UPDATE OF state ON jobs WHEN OLD.state IS NOT NEW.state BEGIN
         UPDATE job_counts SET n = n - 1, bytes = bytes - OLD.size WHERE state = OLD.state;
         INSERT INTO job_counts (state, n, bytes) VALUES (NEW.state, 1, NEW.size)
           ON CONFLICT(state) DO UPDATE SET n = n + 1, bytes = bytes + excluded.bytes;
       END""",
    """CREATE TRIGGER jobs_count_delete AFTER DELETE ON jobs BEGIN
         UPDATE job_counts SET n = n - 1, bytes = bytes - OLD.size WHERE state = OLD.state;
       END""",
)
WORK_STATES = ("pending", "retry", "claimed")

MAX_KEY_BYTES = 1024
REFRESH_EVERY = 5.0  # seconds between limit recalculations (schedule, pause, journal lag)
HEX = "0123456789abcdef"


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.executescript(BULK_SCHEMA)
    _ensure_job_counts(conn)
    return conn


def _ensure_job_counts(conn: sqlite3.Connection) -> None:
    """Triggers that keep job_counts; a bulk.db from before them is counted once, under the write lock."""
    exists = "SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = 'jobs_count_update'"
    if conn.execute(exists).fetchone():
        return
    with journal.tx(conn):
        if conn.execute(exists).fetchone():  # another connection got here first
            return
        for ddl in _JOB_COUNT_TRIGGERS:
            conn.execute(ddl)
        conn.execute("DELETE FROM job_counts")
        conn.execute("INSERT INTO job_counts (state, n, bytes) SELECT state, count(*), coalesce(sum(size), 0) FROM jobs GROUP BY state")


# ---------------------------------------------------------------------------
# Ranges
# ---------------------------------------------------------------------------

def default_bounds() -> List[str]:
    """Boundaries '01', '02', ..., 'ff' -> 256 ranges ['', '01') ... ['ff', inf)."""
    return [a + b for a in HEX for b in HEX][1:]


def ranges_from_bounds(bounds: Sequence[str]) -> List[Tuple[str, Optional[str]]]:
    bs = sorted(set(b for b in bounds if b), key=lambda s: s.encode())
    los = [""] + bs
    his: List[Optional[str]] = list(bs) + [None]
    return list(zip(los, his))


def init_ranges(conn: sqlite3.Connection, bounds: Optional[Sequence[str]] = None) -> int:
    if conn.execute("SELECT count(*) FROM ranges").fetchone()[0]:
        return 0
    rows = ranges_from_bounds(bounds if bounds is not None else default_bounds())
    with journal.tx(conn):
        for i, (lo, hi) in enumerate(rows, start=1):
            conn.execute(
                "INSERT INTO ranges (id, lo, hi, status, updated_at) VALUES (?, ?, ?, 'pending', ?)",
                (i, lo, hi, journal.now_iso()),
            )
    return len(rows)


def pred(lo: str) -> str:
    """Largest possible S3 key (<= 1024 UTF-8 bytes) that sorts before lo.

    Every valid key k with pred(lo) < k < lo would have to be longer than
    1024 bytes, so ListObjectsV2(StartAfter=pred(lo)) starts exactly at lo.
    """
    if not lo:
        raise ValueError("pred('') is undefined")
    last = lo[-1]
    if ord(last) == 0:
        return lo[:-1]
    cp = ord(last) - 1
    if 0xD800 <= cp <= 0xDFFF:  # surrogates are not valid in UTF-8
        cp = 0xD7FF
    base = lo[:-1] + chr(cp)
    used = len(base.encode("utf-8"))
    fill = []
    for ch, width in (("\U0010FFFF", 4), ("￿", 3), ("߿", 2), ("\x7F", 1)):
        n = (MAX_KEY_BYTES - used) // width
        fill.append(ch * n)
        used += n * width
    return base + "".join(fill)


def common_prefix(lo: str, hi: Optional[str]) -> str:
    if not lo or hi is None:
        return ""
    n = 0
    for a, b in zip(lo, hi):
        if a != b:
            break
        n += 1
    return lo[:n]


def _key_bytes(k: str) -> bytes:
    return k.encode("utf-8")


def list_range(
    conn: sqlite3.Connection,
    src: Any,
    bucket: str,
    range_id: int,
    page_size: int,
    stop: threading.Event,
    max_attempts: int = 10,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """List one range from its checkpoint to the end. Returns the final status."""
    row = conn.execute("SELECT * FROM ranges WHERE id = ?", (range_id,)).fetchone()
    lo, hi = row["lo"], row["hi"]
    start_after: Optional[str] = row["start_after"] or (pred(lo) if lo else None)
    prefix = common_prefix(lo, hi)
    hi_b = _key_bytes(hi) if hi is not None else None
    lo_b = _key_bytes(lo)
    with journal.tx(conn):
        conn.execute(
            "UPDATE ranges SET status = 'listing', started_at = coalesce(started_at, ?), updated_at = ? WHERE id = ?",
            (journal.now_iso(), journal.now_iso(), range_id),
        )
    attempts = 0
    token: Optional[str] = None
    while not stop.is_set():
        kwargs: Dict[str, Any] = {"Bucket": bucket, "MaxKeys": page_size}
        if prefix:
            kwargs["Prefix"] = prefix
        # MinIO continues a listing from its token; a StartAfter-only page walks the flat bucket again.
        if token:
            kwargs["ContinuationToken"] = token
        elif start_after:
            kwargs["StartAfter"] = start_after
        try:
            page = src.list_objects_v2(**kwargs)
            attempts = 0
        except Exception as exc:
            token = None  # e.g. MinIO restarted: go on from the checkpoint
            attempts += 1
            err = f"{type(exc).__name__}: {exc}"[:2000]
            if attempts >= max_attempts:
                with journal.tx(conn):
                    conn.execute(
                        "UPDATE ranges SET status = 'error', attempts = attempts + 1, last_error = ?, updated_at = ? WHERE id = ?",
                        (err, journal.now_iso(), range_id),
                    )
                return "error"
            sleep(min(2 ** attempts, 120))
            continue

        contents = page.get("Contents", []) or []
        rows: List[Tuple[Any, ...]] = []
        done = not page.get("IsTruncated")
        last_key: Optional[str] = None
        for obj in contents:
            key = obj["Key"]
            kb = _key_bytes(key)
            if hi_b is not None and kb >= hi_b:
                done = True
                break
            last_key = key
            if kb < lo_b:
                continue
            lm = obj.get("LastModified")
            rows.append(
                (
                    key,
                    range_id,
                    int(obj.get("Size", 0)),
                    str(obj.get("ETag", "")).strip('"'),
                    lm.isoformat() if isinstance(lm, datetime) else (str(lm) if lm else None),
                    journal.now_iso(),
                )
            )
        with journal.tx(conn):
            conn.executemany(
                """INSERT OR IGNORE INTO jobs (key, range_id, size, etag, last_modified, state, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'pending', ?)""",
                rows,
            )
            conn.execute(
                """UPDATE ranges SET start_after = coalesce(?, start_after), listed_count = listed_count + ?,
                       listed_bytes = listed_bytes + ?, pages = pages + 1, updated_at = ?
                   WHERE id = ?""",
                (last_key, len(rows), sum(r[2] for r in rows), journal.now_iso(), range_id),
            )
            if done:
                conn.execute(
                    "UPDATE ranges SET status = 'listed', finished_at = ?, last_error = NULL WHERE id = ?",
                    (journal.now_iso(), range_id),
                )
        if done:
            return "listed"
        token = page.get("NextContinuationToken")
        if last_key is None:
            # Truncated page with nothing in range: continue after the page's last key.
            if not contents:
                return "listed"
            start_after = contents[-1]["Key"]
        else:
            start_after = last_key
    with journal.tx(conn):
        conn.execute("UPDATE ranges SET status = 'pending', updated_at = ? WHERE id = ?", (journal.now_iso(), range_id))
    return "stopped"


def next_range(conn: sqlite3.Connection) -> Optional[int]:
    with journal.tx(conn):
        row = conn.execute("SELECT id FROM ranges WHERE status = 'pending' ORDER BY id LIMIT 1").fetchone()
        if row is None:
            return None
        conn.execute("UPDATE ranges SET status = 'listing', updated_at = ? WHERE id = ?", (journal.now_iso(), row["id"]))
        return int(row["id"])


# ---------------------------------------------------------------------------
# Load control
# ---------------------------------------------------------------------------

@dataclass
class Window:
    start_min: int
    end_min: int
    workers: int
    mbps: float


def parse_schedule(spec: str) -> List[Window]:
    """'08:00-20:00 workers=4 mbps=200; 20:00-08:00 workers=32 mbps=800'."""
    out: List[Window] = []
    for part in [p.strip() for p in spec.split(";") if p.strip()]:
        fields = part.split()
        a, b = fields[0].split("-")
        kv = dict(f.split("=", 1) for f in fields[1:])
        to_min = lambda s: int(s.split(":")[0]) * 60 + int(s.split(":")[1])  # noqa: E731
        out.append(Window(to_min(a), to_min(b), int(kv.get("workers", "1")), float(kv.get("mbps", "0"))))
    return out


def schedule_limits(windows: Sequence[Window], minute_of_day: int, default_workers: int) -> Tuple[int, float]:
    for w in windows:
        inside = (w.start_min <= minute_of_day < w.end_min) if w.start_min < w.end_min else (
            minute_of_day >= w.start_min or minute_of_day < w.end_min
        )
        if inside:
            return w.workers, w.mbps
    return default_workers, 0.0


@dataclass
class Limits:
    workers: int
    mbps: float
    reason: str


class Controller:
    """Recomputes the allowed parallelism from schedule, manual control and journal lag."""

    def __init__(
        self,
        bulk_path: Path,
        journal_path: Path,
        schedule: Sequence[Window],
        tz: str,
        default_workers: int,
        backlog_max: int,
    ) -> None:
        self.bulk = connect(bulk_path)
        self.jconn = journal.connect(journal_path, synchronous="NORMAL")
        self.schedule = list(schedule)
        self.tz = tz
        self.default_workers = default_workers
        self.backlog_max = backlog_max
        self.limits = Limits(default_workers, 0.0, "start")
        self._lock = threading.Lock()
        self._tokens = 0.0
        self._last = time.monotonic()

    def _now_minute(self) -> int:
        try:
            from zoneinfo import ZoneInfo

            now = datetime.now(ZoneInfo(self.tz))
        except Exception:
            now = datetime.now()
        return now.hour * 60 + now.minute

    def refresh(self) -> Limits:
        ctl = {r["name"]: r["value"] for r in self.bulk.execute("SELECT name, value FROM control")}
        workers, mbps = schedule_limits(self.schedule, self._now_minute(), self.default_workers)
        reason = "schedule"
        if "workers_max" in ctl and int(ctl["workers_max"]) < workers:
            workers, reason = int(ctl["workers_max"]), "manual"
        if "mbps_max" in ctl and (mbps == 0 or float(ctl["mbps_max"]) < mbps):
            mbps = float(ctl["mbps_max"])
        if ctl.get("paused") == "1":
            workers, reason = 0, "paused"
        if self.backlog_max and journal.backlog(self.jconn)[0] > self.backlog_max:
            workers, reason = 0, "journal_lag"
        self.limits = Limits(workers, mbps, reason)
        return self.limits

    def take_bytes(self, n: int, stop: Optional[threading.Event] = None) -> None:
        """Token bucket over all workers; mbps = 0 means unlimited. A set `stop` ends the wait."""
        rate = self.limits.mbps * 1024 * 1024 / 8
        if rate <= 0:
            return
        with self._lock:
            now = time.monotonic()
            self._tokens = min(self._tokens + (now - self._last) * rate, rate)
            self._last = now
            self._tokens -= n
            wait = -self._tokens / rate if self._tokens < 0 else 0.0
        if wait > 0:
            if stop is not None:
                stop.wait(min(wait, 60))
            else:
                time.sleep(min(wait, 60))


# ---------------------------------------------------------------------------
# Copy workers
# ---------------------------------------------------------------------------

@dataclass
class BulkConfig:
    bulk_path: Path
    journal_path: Path
    copy: CopyConfig
    workers: int = 8
    list_workers: int = 4
    page_size: int = 1000
    lease_seconds: float = 900.0
    lease_guard_seconds: float = 30.0
    claim_batch: int = 50
    max_attempts: int = 10
    schedule: Sequence[Window] = field(default_factory=list)
    tz: str = "Europe/Moscow"
    backlog_max: int = 0
    report_every: float = 30.0
    progress_path: Optional[Path] = None
    status_path: Optional[Path] = None  # current state, rewritten every STATUS_EVERY seconds
    require_gateway: bool = True


def claim_jobs(conn: sqlite3.Connection, owner: str, n: int, claim_seconds: float, now: Optional[float] = None) -> List[sqlite3.Row]:
    t = time.time() if now is None else now
    with journal.tx(conn):
        # Not ORDER BY key: it is not in idx_jobs_work, so every pending job would be sorted under the
        # write lock. Within a range the index already returns jobs in rowid order, which is listing order.
        rows = conn.execute(
            """SELECT key, size, etag, attempts FROM jobs
               WHERE state IN ('pending', 'retry') AND not_before <= ?
               ORDER BY range_id LIMIT ?""",
            (t, n),
        ).fetchall()
        for r in rows:
            conn.execute(
                "UPDATE jobs SET state = 'claimed', owner = ?, claim_until = ?, updated_at = ? WHERE key = ?",
                (owner, t + claim_seconds, journal.now_iso(), r["key"]),
            )
    return rows


def release_stale_claims(conn: sqlite3.Connection, now: Optional[float] = None) -> int:
    t = time.time() if now is None else now
    with journal.tx(conn):
        cur = conn.execute(
            "UPDATE jobs SET state = 'retry', owner = NULL, updated_at = ? WHERE state = 'claimed' AND claim_until < ?",
            (journal.now_iso(), t),
        )
    return cur.rowcount


def release_own_claims(conn: sqlite3.Connection, owner: str) -> int:
    """Hand a worker's unfinished claims back, so the other workers do not wait for them forever."""
    with journal.tx(conn):
        cur = conn.execute(
            "UPDATE jobs SET state = 'retry', owner = NULL, claim_until = NULL, updated_at = ? "
            "WHERE state = 'claimed' AND owner = ?",
            (journal.now_iso(), owner),
        )
    return cur.rowcount


def _set_job(conn: sqlite3.Connection, key: str, state: str, error: Optional[str] = None,
             verify: Optional[str] = None, not_before: float = 0.0, bump: bool = False) -> None:
    with journal.tx(conn):
        conn.execute(
            f"""UPDATE jobs SET state = ?, owner = NULL, claim_until = NULL, last_error = ?, verify = coalesce(?, verify),
                   not_before = ?, attempts = attempts + {1 if bump else 0}, updated_at = ? WHERE key = ?""",
            (state, error, verify, not_before, journal.now_iso(), key),
        )


def copy_one(
    bconn: sqlite3.Connection,
    jconn: sqlite3.Connection,
    job: sqlite3.Row,
    owner: str,
    src: Any,
    dst: Any,
    cfg: BulkConfig,
    clock: Callable[[], float] = time.time,
) -> str:
    key = job["key"]
    lease = journal.bulk_claim(jconn, key, owner, cfg.lease_seconds, now=clock())
    if lease is None:
        _set_job(bconn, key, "skipped_journal")
        return "skipped_journal"

    def check_lease(nbytes: int) -> None:
        if journal.lease_remaining(lease, clock()) < write_guard(cfg.lease_guard_seconds, nbytes):
            raise LeaseExpired(key)

    try:
        outcome = sync_key(src, dst, key, cfg.copy, check_lease=check_lease, delete_missing=False)
    except Exception as exc:
        journal.bulk_release(jconn, lease)
        attempts = int(job["attempts"]) + 1
        err = f"{type(exc).__name__}: {exc}"[:2000]
        if attempts >= cfg.max_attempts:
            _set_job(bconn, key, "error", err, bump=True)
            return "error"
        _set_job(bconn, key, "retry", err, not_before=clock() + min(2 ** attempts, 600), bump=True)
        return "retry"
    missing = outcome.status == SOURCE_MISSING
    journal.bulk_finish(jconn, lease, source_missing=missing)
    state = "source_missing" if missing else "done"
    _set_job(bconn, key, state, verify=outcome.verify)
    return state


# ---------------------------------------------------------------------------
# Run statistics (bulk-status.json)
# ---------------------------------------------------------------------------

STATUS_EVERY = 10.0  # seconds between rewrites of the status file


class _TimedBody:
    """GetObject body whose read time counts as source time."""

    def __init__(self, body: Any, add: Callable[[float], None]) -> None:
        self._body, self._add = body, add

    def read(self, *args: Any) -> bytes:
        t = time.monotonic()
        try:
            return self._body.read(*args)
        finally:
            self._add(time.monotonic() - t)

    def close(self) -> None:
        close = getattr(self._body, "close", None)
        if close:
            close()


class _Timed:
    """S3 client proxy: time spent in its calls (and in reading GetObject bodies) goes to `add`."""

    def __init__(self, client: Any, add: Callable[[float], None]) -> None:
        self._client, self._add = client, add

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._client, name)
        if not callable(attr):
            return attr

        def call(*args: Any, **kwargs: Any) -> Any:
            t = time.monotonic()
            try:
                resp = attr(*args, **kwargs)
            finally:
                self._add(time.monotonic() - t)
            if name == "get_object" and isinstance(resp, dict) and "Body" in resp:
                resp = {**resp, "Body": _TimedBody(resp["Body"], self._add)}
            return resp

        return call


class RunStats:
    """Cumulative counters of this run and samples of them, for rates over windows."""

    FIELDS = ("finished", "copied", "copied_bytes", "skipped", "missing", "retries", "errors",
              "src_seconds", "dst_seconds", "throttle_seconds", "list_seconds", "listed", "pages")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.c: Dict[str, float] = dict.fromkeys(self.FIELDS, 0.0)
        self.samples: Deque[Tuple[float, Dict[str, float]]] = deque()
        self.last_error: Optional[Dict[str, str]] = None
        self.started_at = journal.now_iso()
        self.t0 = time.monotonic()

    def add(self, **kw: float) -> None:
        with self._lock:
            for k, v in kw.items():
                self.c[k] += v

    def job(self, state: str, size: int) -> None:
        self.add(finished=state in ("done", "skipped_journal", "source_missing"), copied=state == "done",
                 copied_bytes=size if state == "done" else 0, skipped=state == "skipped_journal",
                 missing=state == "source_missing", retries=state == "retry", errors=state == "error")

    def sample(self, listed: int, pages: int, now: Optional[float] = None) -> None:
        """listed/pages are the totals in bulk.db: their deltas are this run's listing speed."""
        now = time.monotonic() if now is None else now
        with self._lock:
            self.c["listed"], self.c["pages"] = float(listed), float(pages)
            self.samples.append((now, dict(self.c)))
        while len(self.samples) > 2 and now - self.samples[1][0] > 3600:
            self.samples.popleft()

    def window(self, seconds: float) -> Tuple[float, Dict[str, float]]:
        """(length, counter deltas) over the last `seconds`, or since the start if the run is younger."""
        now, cur = self.samples[-1]
        base_t, base = next(((t, c) for t, c in self.samples if now - t <= seconds), self.samples[-1])
        return now - base_t, {k: cur[k] - base[k] for k in cur}


def _rate(span: float, d: Dict[str, float]) -> Dict[str, Any]:
    if span <= 0:
        return {"objects_per_min": None, "copied_per_min": None, "mb_per_s": None}
    return {"objects_per_min": round(d["finished"] * 60 / span), "copied_per_min": round(d["copied"] * 60 / span),
            "mb_per_s": round(d["copied_bytes"] / span / 2**20, 2)}


def _shares(span: float, d: Dict[str, float], workers: int) -> Optional[Dict[str, float]]:
    """Where the copiers' time went: reading the source, writing the destination, waiting for the mbps limit."""
    if span < 5 or workers <= 0:
        return None
    total = span * workers
    out = {"source_read": d["src_seconds"] / total, "destination_write": d["dst_seconds"] / total,
           "throttle_wait": d["throttle_seconds"] / total}
    out["idle"] = max(0.0, 1.0 - sum(out.values()))
    return {k: round(v, 2) for k, v in out.items()}


def bottleneck(limits: "Limits", shares: Optional[Dict[str, float]], remaining: int) -> str:
    """A hint for tuning (PIPELINE.md, B3), from the last 5 minutes."""
    if limits.reason in ("paused", "journal_lag"):
        return limits.reason
    if limits.workers <= 0:
        return "no_workers"
    if not remaining:
        return "done"
    if shares is None:
        return "starting"
    if shares["throttle_wait"] >= 0.4:
        return "mbps_limit"
    if shares["idle"] >= 0.5:
        return "idle"
    if shares["source_read"] >= 1.5 * shares["destination_write"]:
        return "source"
    if shares["destination_write"] >= 1.5 * shares["source_read"]:
        return "destination"
    return "balanced"


def write_json_atomic(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    os.replace(tmp, path)


def _duration(seconds: float) -> str:
    seconds = int(seconds)
    return f"{seconds // 86400}d {seconds % 86400 // 3600}h {seconds % 3600 // 60}m" if seconds >= 86400 \
        else f"{seconds // 3600}h {seconds % 3600 // 60}m"


class Runner:
    def __init__(self, cfg: BulkConfig, make_clients: Callable[[], tuple]) -> None:
        self.cfg = cfg
        self.make_clients = make_clients
        self.stop = threading.Event()
        self.controller = Controller(cfg.bulk_path, cfg.journal_path, cfg.schedule, cfg.tz, cfg.workers, cfg.backlog_max)
        self.listing_done = threading.Event()
        self.failed: List[str] = []
        self.stats = RunStats()
        self.mode = "run"

    def _guard(self, fn: Callable[[int], None], idx: int) -> None:
        """A worker that dies on an unexpected error stops the run: its claims would never be released."""
        try:
            fn(idx)
        except Exception:
            traceback.print_exc()
            self.failed.append(f"{fn.__name__}-{idx}")
            self.stop.set()

    def preflight(self, do_copy: bool = True) -> None:
        jconn = journal.connect(self.cfg.journal_path, synchronous="NORMAL")
        try:
            if self.cfg.require_gateway and journal.live_epoch(jconn) is None:
                raise SystemExit("gateway journal has no epoch: start the gateway before the bulk transfer")
        finally:
            jconn.close()
        src, dst = self.make_clients()
        src.head_bucket(Bucket=self.cfg.copy.src_bucket)
        if do_copy:
            dst.head_bucket(Bucket=self.cfg.copy.dst_bucket)
        bconn = connect(self.cfg.bulk_path)
        init_ranges(bconn)
        bconn.execute("UPDATE ranges SET status = 'pending' WHERE status = 'listing'")
        release_stale_claims(bconn, now=float("inf"))
        bconn.close()

    def _lister(self, idx: int) -> None:
        conn = connect(self.cfg.bulk_path)
        src, _ = self.make_clients()
        src = _Timed(src, lambda s: self.stats.add(list_seconds=s))
        try:
            while not self.stop.is_set():
                if idx >= self.controller.limits.workers:  # schedule and pause, checked between ranges
                    if not conn.execute("SELECT 1 FROM ranges WHERE status = 'pending' LIMIT 1").fetchone():
                        return  # nothing left for a lister: do not hold the run open
                    self.stop.wait(1.0)
                    continue
                rid = next_range(conn)
                if rid is None:
                    return
                list_range(conn, src, self.cfg.copy.src_bucket, rid, self.cfg.page_size, self.stop, self.cfg.max_attempts)
        finally:
            conn.close()

    def _copier(self, idx: int) -> None:
        owner = f"bulk-{os.getpid()}-{idx}"
        bconn = connect(self.cfg.bulk_path)
        jconn = journal.connect(self.cfg.journal_path, synchronous="NORMAL")
        src, dst = self.make_clients()
        src = _Timed(src, lambda s: self.stats.add(src_seconds=s))
        dst = _Timed(dst, lambda s: self.stats.add(dst_seconds=s))
        try:
            while not self.stop.is_set():
                if idx >= self.controller.limits.workers:
                    if self.listing_done.is_set() and _remaining(bconn) == 0:
                        return  # nothing left: do not hold the run open
                    self.stop.wait(1.0)
                    continue
                jobs = claim_jobs(bconn, owner, self.cfg.claim_batch, self.cfg.lease_seconds)
                if not jobs:
                    if self.listing_done.is_set() and _remaining(bconn) == 0:
                        return
                    self.stop.wait(1.0)
                    continue
                for job in jobs:
                    if self.stop.is_set() or idx >= self.controller.limits.workers:
                        _set_job(bconn, job["key"], "retry")
                        continue
                    t = time.monotonic()
                    self.controller.take_bytes(int(job["size"]), self.stop)
                    self.stats.add(throttle_seconds=time.monotonic() - t)
                    if self.stop.is_set():
                        _set_job(bconn, job["key"], "retry")
                        continue
                    state = copy_one(bconn, jconn, job, owner, src, dst, self.cfg)
                    self.stats.job(state, int(job["size"]))
                    if state in ("retry", "error"):
                        err = bconn.execute("SELECT last_error FROM jobs WHERE key = ?", (job["key"],)).fetchone()
                        self.stats.last_error = {"time": journal.now_iso(), "key": job["key"], "state": state,
                                                 "error": (err[0] if err else "")[:300]}
        finally:
            try:
                release_own_claims(bconn, owner)
            finally:
                bconn.close()
            jconn.close()

    def run(self, do_list: bool = True, do_copy: bool = True) -> Dict[str, Any]:
        self.mode = "run" if do_list and do_copy else ("list" if do_list else "copy")
        self.preflight(do_copy)
        if not do_copy:
            self.controller.backlog_max = 0  # listing alone does not add to the handler's backlog
        self.controller.refresh()
        listers = [threading.Thread(target=self._guard, args=(self._lister, i), daemon=True) for i in range(self.cfg.list_workers if do_list else 0)]
        copiers = [threading.Thread(target=self._guard, args=(self._copier, i), daemon=True) for i in range(self.cfg.workers if do_copy else 0)]
        for t in listers + copiers:
            t.start()
        last_report = last_refresh = last_status = 0.0
        while any(t.is_alive() for t in listers + copiers):
            if not any(t.is_alive() for t in listers):
                self.listing_done.set()
            if time.monotonic() - last_refresh >= REFRESH_EVERY:
                last_refresh = time.monotonic()
                self.controller.refresh()
            if time.monotonic() - last_report >= self.cfg.report_every:
                last_report = last_status = time.monotonic()
                self.report()
            elif time.monotonic() - last_status >= STATUS_EVERY:
                last_status = time.monotonic()
                self.snapshot()
            for t in listers + copiers:
                t.join(timeout=0.5)
                if t.is_alive():
                    break
        return self.report()

    def snapshot(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """(status() of bulk.db, the full picture written to the status file)."""
        conn = connect(self.cfg.bulk_path)
        try:
            st = status(conn)
            counts = job_counts(conn)
            pages = int(conn.execute("SELECT coalesce(sum(pages), 0) FROM ranges").fetchone()[0])
        finally:
            conn.close()
        stats = self.stats
        stats.sample(st["listed_objects"], pages)
        backlog, oldest = journal.backlog(self.controller.jconn)
        lim = self.controller.limits
        span5, d5 = stats.window(300)
        span60, d60 = stats.window(3600)
        uptime = time.monotonic() - stats.t0
        remaining = sum(counts.get(s, (0, 0))[0] for s in WORK_STATES)
        remaining_bytes = sum(counts.get(s, (0, 0))[1] for s in WORK_STATES)
        total = sum(n for n, _b in counts.values())
        listing_complete = not any(st["ranges"].get(s) for s in ("pending", "listing"))
        shares = _shares(span5, d5, lim.workers)

        # ETA by the last hour (since the start, if younger): the slower of objects and bytes
        eta: Dict[str, Any] = {"hours": None, "at": None, "basis": "last_1h" if span60 >= 3000 else "since_start"}
        if remaining and span60 > 0:
            secs = []
            if d60["finished"] > 0:
                secs.append(remaining / (d60["finished"] / span60))
            if remaining_bytes and d60["copied_bytes"] > 0:
                secs.append(remaining_bytes / (d60["copied_bytes"] / span60))
            if secs:
                eta["hours"] = round(max(secs) / 3600, 1)
                eta["at"] = (datetime.now(timezone.utc) + timedelta(seconds=max(secs))).strftime("%Y-%m-%dT%H:%M:%SZ")
        if not listing_complete:
            eta["note"] = "listing not complete: the remaining work still grows"

        per_object = None
        if d5["copied"] > 0:
            mb = d5["copied_bytes"] / 2**20
            per_object = {"avg_kb": round(d5["copied_bytes"] / 1024 / d5["copied"], 1),
                          "source_ms": round(d5["src_seconds"] * 1000 / d5["copied"], 1),
                          "destination_ms": round(d5["dst_seconds"] * 1000 / d5["copied"], 1),
                          "source_ms_per_mb": round(d5["src_seconds"] * 1000 / mb) if mb else None,
                          "destination_ms_per_mb": round(d5["dst_seconds"] * 1000 / mb) if mb else None}
        snap = {
            "time": journal.now_iso(),
            "started_at": stats.started_at,
            "running_for": _duration(uptime),
            "mode": self.mode,
            "progress": {"jobs_total": total, "finished": total - remaining, "remaining": remaining,
                         "pct": round(100 * (total - remaining) / total, 2) if total else None,
                         "listing_complete": listing_complete},
            "jobs": {state: n for state, (n, _b) in sorted(counts.items())},
            "bytes": {"listed": st["listed_bytes"], "copied": counts.get("done", (0, 0))[1], "remaining": remaining_bytes},
            "rate": {"last_5m": _rate(span5, d5), "last_1h": _rate(span60, d60), "since_start": _rate(uptime, stats.c)},
            "eta": eta,
            "limits": {"workers": lim.workers, "mbps": lim.mbps, "reason": lim.reason},
            "copiers_last_5m": shares,
            "per_copied_object_last_5m": per_object,
            "bottleneck": bottleneck(lim, shares, remaining),
            "listing": {"ranges": st["ranges"], "listed_objects": st["listed_objects"],
                        "objects_per_min_last_5m": round(d5["listed"] * 60 / span5) if span5 > 0 else None,
                        "page_ms_last_5m": round(d5["list_seconds"] * 1000 / d5["pages"]) if d5["pages"] else None},
            "journal": {"backlog": backlog, "backlog_max": self.controller.backlog_max, "oldest_dirty": oldest},
            "errors": {"retries_last_5m": int(d5["retries"]), "jobs_in_error": counts.get("error", (0, 0))[0],
                       "ranges_in_error": st["ranges"].get("error", 0), "last": stats.last_error},
        }
        if self.cfg.status_path:
            write_json_atomic(self.cfg.status_path, snap)
        return st, snap

    def report(self) -> Dict[str, Any]:
        """One line of history (stdout and the progress file); also refreshes the status file."""
        rep, snap = self.snapshot()
        rep["journal_backlog"] = snap["journal"]["backlog"]
        rep["journal_oldest_dirty"] = snap["journal"]["oldest_dirty"]
        rep["limit_workers"] = snap["limits"]["workers"]
        rep["limit_mbps"] = snap["limits"]["mbps"]  # 0: unlimited
        rep["limit_reason"] = snap["limits"]["reason"]
        rep["objects_per_min_5m"] = snap["rate"]["last_5m"]["objects_per_min"]
        rep["mb_per_s_5m"] = snap["rate"]["last_5m"]["mb_per_s"]
        rep["eta_hours"] = snap["eta"]["hours"]
        rep["bottleneck"] = snap["bottleneck"]
        line = json.dumps(rep, ensure_ascii=False)
        print(line, flush=True)
        if self.cfg.progress_path:
            with open(self.cfg.progress_path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        return rep


def _remaining(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT coalesce(sum(n), 0) FROM job_counts WHERE state IN (?, ?, ?)", WORK_STATES).fetchone()[0])


def job_counts(conn: sqlite3.Connection) -> Dict[str, Tuple[int, int]]:
    """state -> (jobs, bytes)."""
    return {r["state"]: (int(r["n"]), int(r["bytes"])) for r in conn.execute("SELECT state, n, bytes FROM job_counts WHERE n != 0")}


def status(conn: sqlite3.Connection) -> Dict[str, Any]:
    ranges = {r["status"]: r["n"] for r in conn.execute("SELECT status, count(*) AS n FROM ranges GROUP BY status")}
    jobs = {state: n for state, (n, _b) in job_counts(conn).items()}
    listed = conn.execute("SELECT coalesce(sum(listed_count), 0), coalesce(sum(listed_bytes), 0) FROM ranges").fetchone()
    return {
        "time": journal.now_iso(),
        "ranges": ranges,
        "jobs": jobs,
        "listed_objects": int(listed[0]),
        "listed_bytes": int(listed[1]),
    }


def export_errors(conn: sqlite3.Connection, out: Path, fmt: str = "jsonl") -> int:
    rows = list(conn.execute(
        "SELECT 'job' AS kind, key, state, attempts, last_error FROM jobs WHERE state IN ('error', 'retry') "
        "UNION ALL SELECT 'range', lo, status, attempts, last_error FROM ranges WHERE status = 'error'"
    ))
    with open(out, "w", encoding="utf-8", newline="") as fh:
        if fmt == "csv":
            w = csv.writer(fh)
            w.writerow(["kind", "key", "state", "attempts", "last_error"])
            for r in rows:
                w.writerow(list(r))
        else:
            for r in rows:
                fh.write(json.dumps(dict(r), ensure_ascii=False) + "\n")
    return len(rows)


def acquire_lock(bulk_path: Path) -> Any:
    """Exclusive lock for run/list/copy on one bulk.db; the OS drops it when the process dies.

    preflight() resets 'listing' ranges and all claims, which is right only
    when no other bulk process works on the same database.
    """
    fh = open(str(bulk_path) + ".lock", "a+")
    try:
        try:
            import fcntl

            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except ImportError:  # Windows dev machines
            import msvcrt

            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        fh.close()
        raise SystemExit(f"another bulk process is working on {bulk_path} (lock {bulk_path}.lock)")
    return fh


def set_control(conn: sqlite3.Connection, name: str, value: str) -> None:
    with journal.tx(conn):
        conn.execute(
            "INSERT INTO control (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value",
            (name, value),
        )


def main(argv: Optional[List[str]] = None) -> int:
    from .handler import clients_from_env

    p = argparse.ArgumentParser(description="Bulk transfer of keys not seen by the gateway journal")
    p.add_argument("command", choices=["run", "list", "copy", "status", "pause", "resume", "set", "retry-errors", "export-errors"])
    p.add_argument("args", nargs="*")
    p.add_argument("--bulk-db", default=os.environ.get("BULK_DB", "bulk.db"))
    p.add_argument("--journal", default=os.environ.get("JOURNAL_DB", "journal.db"))
    p.add_argument("--src-bucket", default=os.environ.get("SRC_BUCKET", "s3elma365"))
    p.add_argument("--dst-bucket", default=os.environ.get("DST_BUCKET", "s3elma365"))
    p.add_argument("--workers", type=int, default=int(os.environ.get("BULK_WORKERS", "8")))
    p.add_argument("--list-workers", type=int, default=int(os.environ.get("LIST_WORKERS", "4")))
    p.add_argument("--page-size", type=int, default=int(os.environ.get("LIST_PAGE_SIZE", "1000")))
    p.add_argument("--schedule", default=os.environ.get("SCHEDULE", ""))
    p.add_argument("--tz", default=os.environ.get("SCHEDULE_TZ", "Europe/Moscow"))
    p.add_argument("--backlog-max", type=int, default=int(os.environ.get("JOURNAL_BACKLOG_MAX", "0")))
    # same journal lease as the handler's: a key held by a killed bulk process goes to the handler after it
    p.add_argument("--lease-seconds", type=float, default=float(os.environ.get("GW_LEASE_SECONDS", "900")))
    p.add_argument("--max-attempts", type=int, default=int(os.environ.get("BULK_MAX_ATTEMPTS", "10")),
                   help="attempts of a job or a range before 'error' (backoff 2, 4, 8 ... s, up to 10 min)")
    p.add_argument("--progress", default=os.environ.get("PROGRESS_FILE", ""))
    p.add_argument("--status-file", default=os.environ.get("BULK_STATUS_FILE", ""),
                   help="current state as JSON, rewritten every 10 s (default: bulk-status.json next to --bulk-db)")
    p.add_argument("--no-tags", action="store_true", help="do not copy object tags (same as COPY_TAGS=0)")
    p.add_argument("--out", default="bulk-errors.jsonl")
    a = p.parse_args(argv)

    bconn = connect(Path(a.bulk_db))
    if a.command == "status":
        print(json.dumps(status(bconn), ensure_ascii=False, indent=2))
        return 0
    if a.command == "pause":
        set_control(bconn, "paused", "1")
        return 0
    if a.command == "resume":
        set_control(bconn, "paused", "0")
        return 0
    if a.command == "set":
        if len(a.args) != 2:
            p.error("set needs NAME VALUE, e.g. set workers_max 4")
        set_control(bconn, a.args[0], a.args[1])
        return 0
    if a.command == "retry-errors":
        with journal.tx(bconn):
            n = bconn.execute("UPDATE jobs SET state = 'retry', attempts = 0, not_before = 0 WHERE state = 'error'").rowcount
            bconn.execute("UPDATE ranges SET status = 'pending', attempts = 0 WHERE status = 'error'")
        print(n)
        return 0
    if a.command == "export-errors":
        print(export_errors(bconn, Path(a.out), "csv" if a.out.endswith(".csv") else "jsonl"))
        return 0
    bconn.close()

    cfg = BulkConfig(
        bulk_path=Path(a.bulk_db),
        journal_path=Path(a.journal),
        copy=CopyConfig(src_bucket=a.src_bucket, dst_bucket=a.dst_bucket, copy_tags=not a.no_tags and os.environ.get("COPY_TAGS", "1") != "0"),
        workers=a.workers,
        list_workers=a.list_workers,
        page_size=a.page_size,
        schedule=parse_schedule(a.schedule) if a.schedule else [],
        tz=a.tz,
        backlog_max=a.backlog_max,
        lease_seconds=a.lease_seconds,
        max_attempts=a.max_attempts,
        progress_path=Path(a.progress) if a.progress else None,
        status_path=Path(a.status_file) if a.status_file else Path(a.bulk_db).with_name("bulk-status.json"),
    )
    if cfg.schedule:
        try:
            from zoneinfo import ZoneInfo

            ZoneInfo(cfg.tz)
        except Exception:  # without it the windows would silently follow the server's local time
            p.error(f"SCHEDULE_TZ {cfg.tz!r} is not a known time zone (install tzdata)")
    need = min_lease_seconds(cfg.lease_guard_seconds, cfg.copy)
    if a.command in ("run", "copy") and cfg.lease_seconds <= need:
        p.error(f"--lease-seconds (GW_LEASE_SECONDS) must be above {need:.0f} s: a write starts only with that much lease left")
    lock = acquire_lock(Path(a.bulk_db))
    runner = Runner(cfg, clients_from_env())
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, lambda *_: runner.stop.set())
        signal.signal(signal.SIGINT, lambda *_: runner.stop.set())
    do_list, do_copy = a.command in ("run", "list"), a.command in ("run", "copy")
    try:
        rep = runner.run(do_list=do_list, do_copy=do_copy)
    finally:
        lock.close()
    if runner.failed:
        print(f"workers failed: {runner.failed}", file=sys.stderr)
        return 3  # systemd restarts on it and the next run resumes from checkpoints
    return exit_code(rep, do_list, do_copy, stopped=runner.stop.is_set())


def exit_code(rep: Dict[str, Any], do_list: bool, do_copy: bool, stopped: bool) -> int:
    """2: errors to retry; 3: work left without a stop request (a worker died, see the log); else 0."""
    if rep["jobs"].get("error") or rep["ranges"].get("error"):
        return 2
    left = sum(rep["ranges"].get(s, 0) for s in ("pending", "listing")) if do_list else 0
    if do_copy:
        left += sum(rep["jobs"].get(s, 0) for s in ("pending", "retry", "claimed"))
    return 3 if left and not stopped else 0


if __name__ == "__main__":
    sys.exit(main())
