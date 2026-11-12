#!/usr/bin/env python3
"""SQLite at the migration's scale: journal.db and bulk.db with 12 million keys (task 13).

    python scripts/bench_journal_scale.py [--keys 12000000] [--intents 1000000]     # needs Docker; ~10 minutes

Runs in a container of the stand image (Linux, the stands' Python and SQLite) on a Docker volume, with the s3gw
code of the working tree. Synthetic databases: key_state with --keys keys, all copied (state 'synced'); intents of
--days days of ELMA365 writes; bulk.db with a job per key. The functions measured are those of s3gw, not copies:
journal.tx is wrapped to record how long each write transaction waited for the lock and held it.
  J  journal.db: the gateway's intent and result, claim_dirty and backlog at 1 thousand, 300 thousand and 1 million
     keys waiting for the handler, s3gw.state, mark_dirty of 10 thousand keys, bulk_claim/bulk_finish of new keys,
     the runbook's batched reset of keys in error, purge_intents, VACUUM INTO (the A5 backup)
  M  everything at once at 300 thousand dirty keys: the gateway at 20 requests/s, 4 handler and 8 bulk workers, the
     operator's s3gw.state and the first purge of the weeks of intents of stage A; 60 s, then to the purge's end and
     30 s more. The norm is the gateway's wait for the journal, apart during the purge and without it
  T  tracecheck on an hour of trace (--trace-lines changes), and what holding every intent in memory would take
  B  bulk.db: claim_jobs at the start, the middle and the end of the copy, status, retry-errors
  C  C1 reconcile: the --resume preamble on 257 partitions, the diff of one partition
Norm: a write transaction holds the lock well under GW_JOURNAL_TIMEOUT (2 s); the gateway's intent waits under it;
the backup fits in an hour and in the free space next to the journal.
Report: .runtime/bench-journal-scale/report.json
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sqlite3
import statistics
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / ".runtime" / "bench-journal-scale"
CONTAINER, VOLUME = "bench-journal-scale", "bench-journal-scale-data"
TIMEOUT = 2.0  # GW_JOURNAL_TIMEOUT

# ---------------------------------------------------------------------------
# inside the container
# ---------------------------------------------------------------------------

LOCK: Dict[str, List[float]] = {}  # label -> lock hold times, s
WAIT: Dict[str, List[float]] = {}  # label -> waits for the lock, s
_label = threading.local()


def label(name: str) -> None:
    _label.name = name


def install_timed_tx() -> None:
    """journal.tx records the wait for BEGIN IMMEDIATE and the time from it to COMMIT (the write lock is held)."""
    from s3gw import journal

    class TimedTx:
        def __init__(self, conn: sqlite3.Connection) -> None:
            self.conn = conn

        def __enter__(self) -> sqlite3.Connection:
            t = time.perf_counter()
            self.conn.execute("BEGIN IMMEDIATE")
            self.t1 = time.perf_counter()
            WAIT.setdefault(getattr(_label, "name", "?"), []).append(self.t1 - t)
            return self.conn

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
            self.conn.execute("COMMIT" if exc_type is None else "ROLLBACK")
            LOCK.setdefault(getattr(_label, "name", "?"), []).append(time.perf_counter() - self.t1)

    journal.tx = TimedTx  # type: ignore[assignment]


def ms(xs: List[float]) -> Dict[str, Any]:
    if not xs:
        return {"n": 0}
    s = sorted(xs)
    return {"n": len(s), "p50_ms": round(1000 * statistics.median(s), 2), "p99_ms": round(1000 * s[min(len(s) - 1, int(len(s) * 0.99))], 2),
            "max_ms": round(1000 * s[-1], 2)}


def held(name: str) -> Dict[str, Any]:
    return {"lock_held": ms(LOCK.get(name, [])), "lock_wait": ms(WAIT.get(name, []))}


def timed(fn: Callable[[], Any]) -> float:
    t = time.perf_counter()
    fn()
    return time.perf_counter() - t


def key_of(i: int, n: int) -> str:
    """The i-th of n UUID keys, evenly over the hex space and sorted as i grows (bulk lists in key order)."""
    return str(uuid.UUID(int=i * (2 ** 128 // n) + 0x5EED))


def iso(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def drop_caches() -> bool:
    os.sync()
    try:
        Path("/proc/sys/vm/drop_caches").write_text("3")
        return True
    except OSError:
        return False


def fsync_rate(d: Path) -> Dict[str, Any]:
    """What the gateway's synchronous=FULL commits stand on: one 4 KiB write + fsync."""
    f = d / "fsync.test"
    fd = os.open(f, os.O_WRONLY | os.O_CREAT, 0o600)
    t = time.perf_counter()
    for i in range(500):
        os.pwrite(fd, b"x" * 4096, i * 4096)
        os.fsync(fd)
    dt = time.perf_counter() - t
    os.close(fd)
    f.unlink()
    return {"fsyncs_per_s": round(500 / dt), "ms_per_fsync": round(1000 * dt / 500, 2)}


def build_journal(path: Path, n: int, n_intents: int, days: float, rng: random.Random) -> Dict[str, Any]:
    """key_state: n keys, all synced (bulk copied them); intents: n_intents writes over `days` days, results written."""
    from s3gw import journal

    t0 = time.time()
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = -1000000")
    conn.executescript(journal.SCHEMA)
    now = time.time()
    upd = iso(now - days * 86400)
    batch = 200_000
    for lo in range(0, n, batch):
        conn.execute("BEGIN")
        conn.executemany("INSERT INTO key_state (key, change_seq, synced_seq, state, src_size, src_etag, updated_at) "
                         "VALUES (?, 0, 0, 'synced', ?, ?, ?)",
                         ((key_of(i, n), 1000 + i % 90000, f"{i:032x}", upd) for i in range(lo, min(n, lo + batch))))
        conn.execute("COMMIT")
    conn.execute("INSERT INTO gateway_epochs (started_at, host, pid) VALUES (?, 'minio2', 1)", (upd,))
    # writes of ELMA365 at an even rate; a key written k times has change_seq = synced_seq = 2k (intent + result)
    step = days * 86400 / n_intents
    writes: Dict[int, int] = {}
    rows = []
    for j in range(n_intents):
        i = rng.randrange(n)
        writes[i] = writes.get(i, 0) + 1
        t = now - days * 86400 + j * step
        rows.append((1, key_of(i, n), "put" if j % 11 else "delete", uuid.uuid4().hex, iso(t), "success", 200, iso(t + 0.05)))
        if len(rows) == batch or j == n_intents - 1:
            conn.execute("BEGIN")
            conn.executemany("INSERT INTO intents (epoch, key, op, request_id, created_at, result, http_status, finished_at) "
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
            conn.execute("COMMIT")
            rows = []
    conn.execute("BEGIN")
    conn.executemany("UPDATE key_state SET change_seq = ?, synced_seq = ? WHERE key = ?",
                     ((2 * k, 2 * k, key_of(i, n)) for i, k in writes.items()))
    conn.execute("INSERT INTO counters (name, value) VALUES ('intents', ?)", (n_intents,))
    conn.execute("COMMIT")
    conn.close()
    journal.connect(path).close()  # WAL, as the gateway opens it
    return {"seconds": round(time.time() - t0), "bytes": path.stat().st_size, "keys_written": len(writes)}


def set_dirty(conn: sqlite3.Connection, n: int, target: int, rng: random.Random) -> int:
    """Bring the number of keys waiting for the handler to `target` (as ELMA365's writes of stage A leave them)."""
    have = conn.execute("SELECT count(*) FROM key_state WHERE state IN ('dirty', 'uncertain', 'syncing', 'bulk')").fetchone()[0]
    if have > target:  # the handler synced the rest
        conn.execute("UPDATE key_state SET state = 'synced', synced_seq = change_seq, lease_owner = NULL WHERE rowid IN "
                     "(SELECT rowid FROM key_state WHERE state = 'dirty' LIMIT ?)", (have - target,))
        return target
    keys = {key_of(rng.randrange(n), n) for _ in range(max(0, target - have) * 11 // 10)}
    conn.execute("BEGIN")
    added = 0
    for k in keys:
        if have + added >= target:
            break
        added += conn.execute("UPDATE key_state SET state = 'dirty', change_seq = change_seq + 2, updated_at = ? "
                              "WHERE key = ? AND state = 'synced'", (iso(time.time()), k)).rowcount
    conn.execute("COMMIT")
    return have + added


def bench_journal(d: Path, a: argparse.Namespace, rep: Dict[str, Any]) -> None:
    from s3gw import journal, state as gwstate

    rng = random.Random(7)
    n = a.keys
    path = d / "journal.db"
    rep["build_journal"] = build_journal(path, n, a.intents, a.days, rng)
    print(f"journal built: {rep['build_journal']}", flush=True)

    gw = journal.connect(path, synchronous="FULL", timeout=TIMEOUT)
    hd = journal.connect(path, synchronous="NORMAL")
    epoch = journal.start_epoch(gw, 900)
    rep["sqlite_version"] = sqlite3.sqlite_version

    # J1 the gateway: intent, then result (one key), cold cache first
    rep["cache_dropped"] = drop_caches()
    label("gateway")
    lat = []
    for j in range(300):
        k = key_of(rng.randrange(n), n)
        t = time.perf_counter()
        ids = journal.write_intents(gw, epoch, uuid.uuid4().hex, "put", [k])
        lat.append(time.perf_counter() - t)
        journal.write_results(gw, [(ids[0], k, journal.SUCCESS, 200, None)])
    rep["J1_gateway_intent"] = {"intent_ms": ms(lat), **held("gateway")}

    # J2/J3 claim_dirty and backlog at three sizes of the handler's queue
    rep["J2_claim_dirty"] = {}
    for target in a.dirty:
        have = set_dirty(hd, n, target, rng)
        drop_caches()
        lbl = f"claim@{target}"
        label(lbl)
        cold = timed(lambda: journal.claim_dirty(hd, "h-0", 900, limit=1))
        for _ in range(50):
            for lease in journal.claim_dirty(hd, "h-0", 900, limit=1):
                journal.finish_sync(hd, lease, "synced")
        b_t = []
        for _ in range(5):
            b_t.append(timed(lambda: journal.backlog(hd)))
        rep["J2_claim_dirty"][str(have)] = {"cold_first_claim_ms": round(1000 * cold, 1), **held(lbl),
                                            "backlog_ms": ms(b_t), "backlog": journal.backlog(hd)[0]}
        print(f"claim at {have} dirty: {rep['J2_claim_dirty'][str(have)]}", flush=True)
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as ro:
            rep["J2_claim_dirty"][str(have)]["plan"] = [r[3] for r in ro.execute(
                "EXPLAIN QUERY PLAN SELECT key, change_seq FROM key_state WHERE state IN ('dirty', 'uncertain', 'syncing', 'bulk') "
                "AND in_flight = 0 AND not_before <= 1 AND ((state IN ('dirty', 'uncertain') AND (lease_owner IS NULL OR "
                "lease_until < 1)) OR (state IN ('syncing', 'bulk') AND lease_until < 1)) ORDER BY not_before LIMIT 1")]

    # J4 s3gw.state: the operator's look, with 100 keys in error (the full scan for 'error')
    hd.execute("BEGIN")
    hd.executemany("UPDATE key_state SET state = 'error', attempts = 20, last_error = 'boom' WHERE key = ?",
                   ((key_of(rng.randrange(n), n),) for _ in range(100)))
    hd.execute("COMMIT")
    drop_caches()
    ro = gwstate.connect(str(path))
    t_cold = timed(lambda: gwstate.state(ro))
    t_warm = timed(lambda: gwstate.state(ro))
    t_queue = timed(lambda: gwstate.queue(ro))
    ro.close()
    rep["J4_s3gw_state"] = {"cold_ms": round(1000 * t_cold), "warm_ms": round(1000 * t_warm), "queue_only_ms": round(1000 * t_queue, 1)}

    # J5 mark_dirty of 10k keys in one transaction (tracecheck/verifykeys --mark-dirty)
    label("mark_dirty_10k")
    journal.mark_dirty(hd, sorted({key_of(rng.randrange(n), n) for _ in range(10000)}))
    rep["J5_mark_dirty_10k"] = held("mark_dirty_10k")

    # J6 bulk_claim + bulk_finish of keys the journal never saw (keys between the existing ones)
    label("bulk_claim")
    for j in range(300):
        k = key_of(rng.randrange(n), n) + "-new"
        lease = journal.bulk_claim(hd, k, "b-0", 900)
        if lease:
            journal.bulk_finish(hd, lease, source_missing=False)
    rep["J6_bulk_claim_finish"] = held("bulk_claim")

    # J7 the runbook's reset of keys in error, one batch of 50 000 rowids
    label("error_reset_batch")
    mx = hd.execute("SELECT max(rowid) FROM key_state").fetchone()[0]
    times = []
    for s in range(0, mx, mx // 10):
        times.append(timed(lambda: hd.execute(
            "UPDATE key_state SET state = 'dirty', attempts = 0, not_before = 0, lease_owner = NULL "
            "WHERE state = 'error' AND rowid BETWEEN ? AND ?", (s, s + 49999))))
    rep["J7_error_reset_batch"] = ms(times)

    gw.close()
    hd.close()


def bench_mix(d: Path, a: argparse.Namespace, rep: Dict[str, Any]) -> None:
    """Everything at once on the 12M journal; the gateway's wait for its intent is what ELMA365 feels."""
    from s3gw import journal, state as gwstate

    path = d / "journal.db"
    n = a.keys
    hd = journal.connect(path, synchronous="NORMAL")
    have = set_dirty(hd, n, 300_000, random.Random(11))
    hd.close()
    stop = threading.Event()
    gw_lat: List[float] = []
    gw_at: List[float] = []  # when each of gw_lat started
    res_lat: List[float] = []
    purge_span: List[float] = []
    over: List[float] = []
    counts: Dict[str, int] = {}
    errors: List[str] = []

    def guard(fn: Callable[[], None]) -> Callable[[], None]:
        def run() -> None:
            try:
                fn()
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{threading.current_thread().name}: {type(exc).__name__}: {exc}")
        return run

    def gateway() -> None:
        label("mix_gateway")
        rng = random.Random(1)
        conn = journal.connect(path, synchronous="FULL", timeout=TIMEOUT)
        epoch = journal.live_epoch(conn) or 1
        nxt = time.monotonic()
        while not stop.is_set():
            k = key_of(rng.randrange(n), n) if rng.random() < 0.6 else str(uuid.uuid4())
            t = time.perf_counter()
            try:
                ids = journal.write_intents(conn, epoch, uuid.uuid4().hex, "put", [k])
            except sqlite3.OperationalError:
                over.append(time.perf_counter() - t)
                continue
            gw_lat.append(time.perf_counter() - t)
            gw_at.append(t)
            time.sleep(0.01)  # the request in MinIO
            t = time.perf_counter()
            try:
                journal.write_results(conn, [(ids[0], k, journal.SUCCESS, 200, None)])
            except sqlite3.OperationalError:  # the gateway logs "journal write failed", the intent stays open
                counts["gateway_results_failed"] = counts.get("gateway_results_failed", 0) + 1
            res_lat.append(time.perf_counter() - t)
            counts["gateway"] = counts.get("gateway", 0) + 1
            nxt += 1 / 20
            time.sleep(max(0.0, nxt - time.monotonic()))
        conn.close()

    def handler(i: int) -> None:
        label("mix_handler")
        conn = journal.connect(path, synchronous="NORMAL")
        while not stop.is_set():
            leases = journal.claim_dirty(conn, f"h-{i}", 900, limit=1)
            if not leases:
                stop.wait(0.5)
                continue
            time.sleep(0.02)  # GET from MinIO, PUT to the new S3
            journal.finish_sync(conn, leases[0], "synced")
            counts["handler"] = counts.get("handler", 0) + 1
        conn.close()

    def bulk(i: int) -> None:
        label("mix_bulk")
        conn = journal.connect(path, synchronous="NORMAL")
        rng = random.Random(100 + i)
        while not stop.is_set():
            lease = journal.bulk_claim(conn, key_of(rng.randrange(n), n) + f"-b{i}", f"b-{i}", 900)
            time.sleep(0.02)
            if lease:
                journal.bulk_finish(conn, lease, source_missing=False)
            counts["bulk"] = counts.get("bulk", 0) + 1
        conn.close()

    def purge() -> None:
        label("mix_purge")
        conn = journal.connect(path, synchronous="NORMAL")
        stop.wait(10)
        t = time.perf_counter()
        counts["purged"] = journal.purge_intents(conn, 14)
        purge_span.extend([t, time.perf_counter()])
        counts["purge_seconds"] = round(time.perf_counter() - t, 1)
        conn.close()

    def operator() -> None:
        stop.wait(20)
        ro = gwstate.connect(str(path))
        t = time.perf_counter()
        gwstate.state(ro)
        counts["state_seconds"] = round(time.perf_counter() - t, 1)
        ro.close()

    threads = [threading.Thread(target=guard(gateway), name="gateway")]
    threads += [threading.Thread(target=guard(lambda i=i: handler(i)), name=f"handler-{i}") for i in range(4)]
    threads += [threading.Thread(target=guard(lambda i=i: bulk(i)), name=f"bulk-{i}") for i in range(8)]
    threads += [threading.Thread(target=guard(purge), name="purge"), threading.Thread(target=guard(operator), name="operator")]
    t_mix = time.monotonic()
    for t in threads:
        t.start()
    time.sleep(a.mix_seconds)
    threads[-2].join(1800)  # the purge of weeks of intents runs to its end under the load
    time.sleep(30)  # and the steady state after it
    stop.set()
    for t in threads:
        t.join(300)
    rep["M_mix"] = {"dirty_at_start": have, "seconds": round(time.monotonic() - t_mix), "done": counts, "errors": errors[:5],
                    "gateway_intent_ms": ms(gw_lat), "gateway_intent_over_timeout": len(over), "gateway_result_ms": ms(res_lat),
                    "gateway_intent_ms_during_purge": ms([x for x, at in zip(gw_lat, gw_at) if purge_span and purge_span[0] <= at <= purge_span[1]]),
                    "gateway_intent_ms_without_purge": ms([x for x, at in zip(gw_lat, gw_at) if not purge_span or not purge_span[0] <= at <= purge_span[1]]),
                    "gateway_waits_over_1s": sum(1 for x in gw_lat if x > 1.0),
                    "lock_held": {k: held(k)["lock_held"] for k in ("mix_gateway", "mix_handler", "mix_bulk", "mix_purge")}}
    print(f"mix: {json.dumps(rep['M_mix'])}", flush=True)

    # purge again: nothing old is left, the scan alone
    conn = journal.connect(path, synchronous="NORMAL")
    label("purge_empty")
    t = timed(lambda: journal.purge_intents(conn, 14))
    rep["J8_purge_nothing_left"] = {"seconds": round(t, 2), **held("purge_empty")}
    rep["intents_left"] = conn.execute("SELECT count(*) FROM intents").fetchone()[0]
    conn.close()


def bench_backup(d: Path, rep: Dict[str, Any]) -> None:
    """A5: the hourly copy, the way s3gw-journal-backup.sh takes it."""
    path = d / "journal.db"
    wal = Path(str(path) + "-wal")
    out = d / "backup" / "journal-copy.db"
    out.parent.mkdir(exist_ok=True)
    src = path.stat().st_size + (wal.stat().st_size if wal.exists() else 0)
    drop_caches()
    t = time.perf_counter()
    subprocess.run(["sqlite3", str(path), f"VACUUM INTO '{out}.tmp'"], check=True)
    dt = time.perf_counter() - t
    os.replace(f"{out}.tmp", out)
    du = shutil.disk_usage(d)
    rep["J9_backup_vacuum_into"] = {"seconds": round(dt, 1), "journal_bytes": src, "copy_bytes": out.stat().st_size,
                                    "free_bytes_on_volume": du.free,
                                    "space_needed_by_runbook_A2": "journal + KEEP=2 copies + one .tmp"}
    out.unlink()


def bench_tracecheck(d: Path, a: argparse.Namespace, rep: Dict[str, Any]) -> None:
    """A6: an hour of trace against the whole journal; the last `trace_lines` intents are the hour's changes."""
    path = d / "journal.db"
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    rows = conn.execute("SELECT key, request_id FROM intents ORDER BY id DESC LIMIT ?", (a.trace_lines,)).fetchall()
    n_intents = conn.execute("SELECT count(*) FROM intents").fetchone()[0]
    conn.close()
    tpl = json.loads(TRACE_RECORD)
    trace = d / "trace.jsonl"
    with open(trace, "w", encoding="utf-8") as fh:
        for key, rid in rows:
            tpl["path"] = tpl["request"]["path"] = f"/s3elma365/{key}"
            tpl["request"]["headers"]["X-S3gw-Request-Id"] = rid
            fh.write(json.dumps(tpl) + "\n")
    # VmHWM, not ru_maxrss: that one keeps the parent's resident set from before the exec
    hwm = "print([x.split()[1] for x in open('/proc/self/status') if x.startswith('VmHWM')][0], code, file=sys.stderr)"
    t = time.perf_counter()
    r = subprocess.run([sys.executable, "-c",
                        "import runpy, sys; sys.argv = sys.argv[1:]; code = None\n"
                        "try:\n    runpy.run_module('s3gw.tracecheck', run_name='__main__')\n"
                        "except SystemExit as e:\n    code = e.code\n" + hwm,
                        "tracecheck", str(trace), "--journal", str(path)], capture_output=True, text=True, cwd="/opt/code")
    dt = time.perf_counter() - t
    out = json.loads(r.stdout)
    peak_kb, code = r.stderr.strip().splitlines()[-1].split()
    # what loading every intent of the journal into memory (tracecheck before 2026-10-25) takes
    whole = subprocess.run([sys.executable, "-c",
                            "import sqlite3, sys; out = {}; code = 0\n"
                            "c = sqlite3.connect(f'file:{sys.argv[1]}?mode=ro', uri=True)\n"
                            "for rid, key in c.execute('SELECT request_id, key FROM intents'):\n"
                            "    out.setdefault(rid, set()).add(key)\n" + hwm, str(path)], capture_output=True, text=True)
    rep["T_tracecheck"] = {"seconds": round(dt, 1), "peak_mb": round(int(peak_kb) / 1024), "exit": int(code),
                           "whole_journal_in_memory_mb": round(int(whole.stderr.split()[0]) / 1024),
                           "trace_lines": len(rows), "trace_bytes": trace.stat().st_size, "intents_in_journal": n_intents,
                           **{k: out[k] for k in ("changes", "bypass_total", "missed_total")}}
    trace.unlink()


def bench_bulk(d: Path, a: argparse.Namespace, rep: Dict[str, Any]) -> None:
    from s3gw import bulk

    n = a.keys
    path = d / "bulk.db"
    t0 = time.time()
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = -1000000")
    conn.executescript(bulk.BULK_SCHEMA)
    now = iso(time.time())
    batch = 200_000
    for lo in range(0, n, batch):
        conn.execute("BEGIN")
        conn.executemany("INSERT INTO jobs (key, range_id, size, etag, last_modified, state, updated_at) "
                         "VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                         ((k, int(k[:2], 16) + 1, 1000 + i % 90000, f"{i:032x}", now, now)
                          for i in range(lo, min(n, lo + batch)) for k in (key_of(i, n),)))
        conn.execute("COMMIT")
    conn.close()
    bconn = bulk.connect(path)  # WAL, triggers and job_counts as the transfer has them
    bulk.init_ranges(bconn)
    bconn.execute("UPDATE ranges SET status = 'listed'")
    rep["build_bulk"] = {"seconds": round(time.time() - t0), "bytes": path.stat().st_size}
    print(f"bulk built: {rep['build_bulk']}", flush=True)

    def stage(done_below: int) -> None:
        """Jobs of ranges below `done_below` copied; 2000 jobs in retry waiting for their backoff."""
        bconn.execute("DROP TRIGGER IF EXISTS jobs_count_update")
        bconn.execute("BEGIN")
        bconn.execute("UPDATE jobs SET state = 'done' WHERE range_id < ? AND state != 'done'", (done_below,))
        bconn.execute("UPDATE jobs SET state = 'retry', not_before = ? WHERE rowid IN "
                      "(SELECT rowid FROM jobs WHERE state = 'pending' LIMIT 2000)", (time.time() + 3600,))
        bconn.execute("COMMIT")
        bconn.execute("DROP TRIGGER IF EXISTS jobs_count_insert")
        bconn.execute("DROP TRIGGER IF EXISTS jobs_count_delete")
        bulk._ensure_job_counts(bconn)  # recount, as for a bulk.db from before the triggers

    rep["B_claim_jobs"] = {}
    for name, below in (("start", 1), ("middle", 129), ("end", 256)):
        stage(below)
        drop_caches()
        lbl = f"claim_jobs@{name}"
        label(lbl)
        cold = timed(lambda: bulk.claim_jobs(bconn, "c-0", 50, 900))
        for _ in range(30):
            for r in bulk.claim_jobs(bconn, "c-0", 50, 900):
                bulk._set_job(bconn, r["key"], "done")
        plan = [r[3] for r in bconn.execute(
            "EXPLAIN QUERY PLAN SELECT key, size, etag, attempts FROM jobs WHERE state IN ('pending', 'retry') "
            "AND not_before <= 1 ORDER BY range_id LIMIT 50")]
        rep["B_claim_jobs"][name] = {"cold_first_claim_ms": round(1000 * cold, 1), **held(lbl), "plan": plan,
                                     "remaining": bulk._remaining(bconn)}
        print(f"claim_jobs {name}: {rep['B_claim_jobs'][name]}", flush=True)
    rep["B_status_ms"] = round(1000 * timed(lambda: bulk.status(bconn)), 1)
    t = timed(lambda: bconn.execute("UPDATE jobs SET state = 'retry', attempts = 0, not_before = 0 WHERE state = 'error'"))
    rep["B_retry_errors_ms"] = round(1000 * t)
    bconn.close()


def bench_reconcile(d: Path, rep: Dict[str, Any]) -> None:
    """C1 at 12M: what --resume does before listing, and the per-partition diff (~47 thousand keys a side)."""
    sys.path.insert(0, "/opt/code/migrator")
    from migrator import reconcile_buckets as rb

    path = d / "state.db"
    conn = rb.db_connect(path)
    prefixes = rb.build_prefixes(2)
    conn.executemany("INSERT INTO partition_progress (prefix, status) VALUES (?, ?)",
                     [(p, "done" if i < 200 else "running") for i, p in enumerate(prefixes)])
    rng = random.Random(5)
    conn.executemany("INSERT OR IGNORE INTO reconcile_queue (key, partition, action, src_size) VALUES (?, ?, 'missing', 1)",
                     [(k, k[:2]) for k in (str(uuid.uuid4()) for _ in range(100_000))])
    conn.executemany("INSERT OR IGNORE INTO dst_only (key, partition, dst_size, seen_at) VALUES (?, ?, 1, '')",
                     [(k, k[:2]) for k in (str(uuid.uuid4()) for _ in range(100_000))])
    conn.commit()
    undone = "SELECT prefix FROM partition_progress WHERE status != 'done'"
    t = time.perf_counter()
    conn.execute(f"DELETE FROM reconcile_queue WHERE partition IN ({undone})")
    conn.execute(f"DELETE FROM dst_only WHERE partition IN ({undone})")
    done = {r["prefix"] for r in conn.execute("SELECT prefix FROM partition_progress WHERE status = 'done'")}
    conn.commit()
    t_resume = time.perf_counter() - t
    part = "7f"
    rb.prepare_partition_tables(conn, part)
    keys = sorted(f"{part}{uuid.uuid4().hex[2:]}" for _ in range(47_000))
    conn.executemany(f"INSERT INTO src_partition_{part} VALUES (?, ?, '', ?)", [(k, 100, "e" * 32) for k in keys])
    conn.executemany(f"INSERT INTO dst_partition_{part} VALUES (?, ?, '', ?)",
                     [(k, 100 if rng.random() > 0.001 else 101, "e" * 32) for k in keys if rng.random() > 0.001])
    conn.commit()
    t_diff = timed(lambda: rb.compute_partition_diff(conn, part))
    conn.close()
    rep["C_reconcile"] = {"resume_preamble_ms": round(1000 * t_resume, 1), "partitions_done": len(done),
                          "partition_diff_ms": round(1000 * t_diff), "keys_per_side": len(keys)}


def inside(a: argparse.Namespace) -> int:
    d = Path(a.dir)
    for f in d.glob("*"):
        if f.is_file():
            f.unlink()
    sys.path.insert(0, "/opt/code")
    install_timed_tx()
    rep: Dict[str, Any] = {"keys": a.keys, "intents": a.intents, "days": a.days, "fsync": fsync_rate(d),
                           "python": sys.version.split()[0]}
    print(f"fsync: {rep['fsync']}", flush=True)
    steps = [("journal", lambda: bench_journal(d, a, rep)), ("tracecheck", lambda: bench_tracecheck(d, a, rep)),
             ("mix", lambda: bench_mix(d, a, rep)), ("backup", lambda: bench_backup(d, rep)),
             ("bulk", lambda: bench_bulk(d, a, rep)), ("reconcile", lambda: bench_reconcile(d, rep))]
    for name, fn in steps:
        t = time.time()
        fn()
        rep.setdefault("step_seconds", {})[name] = round(time.time() - t)
        print(f"--- {name} done in {rep['step_seconds'][name]} s", flush=True)
    rep["db_bytes"] = {f.name: f.stat().st_size for f in d.glob("*.db*")}
    print("REPORT " + json.dumps(rep, ensure_ascii=False), flush=True)
    return 0


TRACE_RECORD = json.dumps({
    "type": "S3", "host": "10.10.0.12:9000", "api": "s3.PutObject", "time": "2026-11-20T10:00:00.000000000Z", "duration": 16442218,
    "path": "", "request": {"time": "2026-11-20T10:00:00.000000000Z", "proto": "HTTP/1.1", "method": "PUT", "path": "", "headers": {
        "Authorization": "AWS4-HMAC-SHA256 Credential=elmauser/20261120/ru-central-1/s3/aws4_request, "
                         "SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=" + "0" * 64,
        "Content-Length": "6629", "Host": "10.10.0.12:9000", "User-Agent": "MinIO (linux; amd64) minio-go/v7.0.70",
        "X-Amz-Content-Sha256": "UNSIGNED-PAYLOAD", "X-Amz-Date": "20261120T100000Z", "X-Forwarded-For": "10.233.1.7",
        "X-Real-Ip": "10.233.1.7", "X-S3gw-Request-Id": ""}, "body": "<BLOB>"},
    "response": {"time": "2026-11-20T10:00:00.016000000Z", "headers": {"Content-Length": "0", "ETag": "\"26947b742c957e1eba46d7e1f320a20b\"",
                                                                       "Server": "MinIO"}, "body": "<BLOB>", "statusCode": 200},
    "callStats": {"rx": 6804, "tx": 0, "duration": 16442218, "timeToFirstByte": 16408617}})

# ---------------------------------------------------------------------------
# on the host
# ---------------------------------------------------------------------------


def outside(a: argparse.Namespace) -> int:
    import stand_pipeline as sp

    stand = sp.Stand(0)
    stand.ensure_images()
    OUT.mkdir(parents=True, exist_ok=True)
    sp.run("docker", "rm", "-f", CONTAINER, check=False)
    sp.run("docker", "volume", "create", VOLUME)
    # privileged: drop_caches before the cold measurements
    sp.run("docker", "run", "-d", "--name", CONTAINER, "--privileged", "-v", f"{VOLUME}:/data", stand.host_img, "sleep", "infinity")
    try:
        rev = sp.run("git", "-C", str(REPO), "stash", "create").stdout.strip() or "HEAD"
        tar = subprocess.run(["git", "-C", str(REPO), "archive", "--format=tar", rev], capture_output=True, check=True).stdout
        sp.run("docker", "exec", CONTAINER, "mkdir", "-p", "/opt/code")
        subprocess.run(["docker", "exec", "-i", CONTAINER, "tar", "-x", "-C", "/opt/code"], input=tar, check=True)
        sp.run("docker", "cp", str(Path(__file__).resolve()), f"{CONTAINER}:/opt/bench.py")
        args = ["--inside", "--dir", "/data", "--keys", str(a.keys), "--intents", str(a.intents), "--days", str(a.days),
                "--dirty", ",".join(map(str, a.dirty)), "--trace-lines", str(a.trace_lines), "--mix-seconds", str(a.mix_seconds)]
        proc = subprocess.Popen(["docker", "exec", "-w", "/opt/code", CONTAINER, "/opt/venv/bin/python", "/opt/bench.py", *args],
                                stdout=subprocess.PIPE, text=True, encoding="utf-8")
        report: Dict[str, Any] = {}
        assert proc.stdout is not None
        for line in proc.stdout:
            if line.startswith("REPORT "):
                report = json.loads(line[7:])
            else:
                print(line, end="", flush=True)
        rc = proc.wait()
        report["code_rev"] = sp.run("git", "-C", str(REPO), "rev-parse", "--short", rev).stdout.strip()
        (OUT / "report.json").write_text(json.dumps(report, indent=1, ensure_ascii=False), encoding="utf-8")
        print(f"report {OUT / 'report.json'}")
        return rc
    finally:
        if not a.keep:
            sp.run("docker", "rm", "-f", CONTAINER, check=False)
            sp.run("docker", "volume", "rm", "-f", VOLUME, check=False)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--keys", type=int, default=12_000_000)
    p.add_argument("--intents", type=int, default=1_000_000, help="ELMA365 writes kept in the journal (~8.8 thousand a day)")
    p.add_argument("--days", type=float, default=60, help="the intents span this many days")
    p.add_argument("--dirty", type=lambda s: [int(x) for x in s.split(",")], default=[1000, 300_000, 1_000_000])
    p.add_argument("--trace-lines", type=int, default=10_000, help="changes in the hour of trace (real: ~400)")
    p.add_argument("--mix-seconds", type=int, default=60)
    p.add_argument("--keep", action="store_true", help="leave the container and the volume")
    p.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--dir", default="/data", help=argparse.SUPPRESS)
    a = p.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    return inside(a) if a.inside else outside(a)


if __name__ == "__main__":
    sys.exit(main())
