from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from s3gw import bulk, journal
from s3gw.copykey import CopyConfig


class FakeListing:
    """ListObjectsV2 over a sorted key set with S3 semantics (byte order, StartAfter, Prefix)."""

    def __init__(self, keys: List[str], fail_on_call: Optional[int] = None) -> None:
        self.keys = sorted(set(keys), key=lambda k: k.encode())
        self.calls: List[Dict[str, Any]] = []
        self.fail_on_call = fail_on_call

    def list_objects_v2(self, Bucket: str, MaxKeys: int, Prefix: str = "", StartAfter: str = "",
                        ContinuationToken: str = "") -> Dict[str, Any]:
        self.calls.append({"Prefix": Prefix, "StartAfter": StartAfter, "ContinuationToken": ContinuationToken})
        if self.fail_on_call is not None and len(self.calls) == self.fail_on_call:
            raise RuntimeError("listing failed")
        after = ContinuationToken[len("tok:"):] if ContinuationToken else StartAfter  # S3 ignores StartAfter then
        sel = [k for k in self.keys if k.startswith(Prefix) and (not after or k.encode() > after.encode())]
        page = sel[:MaxKeys]
        out: Dict[str, Any] = {
            "Contents": [{"Key": k, "Size": len(k), "ETag": '"e"'} for k in page],
            "IsTruncated": len(sel) > MaxKeys,
        }
        if out["IsTruncated"]:
            out["NextContinuationToken"] = "tok:" + page[-1]
        return out


KEYS = (
    [f"{a}{b}-{i}" for a in "0f9a" for b in "07c" for i in range(3)]
    + ["AB12-upper", "Zz", "-dash", "папка/файл", "ff", "f", "0", "", "\U0010ffff"]
)


def _list_all(tmp_path: Path, src: FakeListing, page_size: int = 2) -> List[str]:
    conn = bulk.connect(tmp_path / "bulk.db")
    bulk.init_ranges(conn)
    stop = threading.Event()
    while True:
        rid = bulk.next_range(conn)
        if rid is None:
            break
        assert bulk.list_range(conn, src, "b", rid, page_size, stop, sleep=lambda _s: None) == "listed"
    return [r[0] for r in conn.execute("SELECT key FROM jobs")]


def test_every_key_listed_exactly_once(tmp_path: Path) -> None:
    keys = [k for k in KEYS if k]
    src = FakeListing(keys)
    listed = _list_all(tmp_path, src)
    assert sorted(listed) == sorted(keys)


def test_range_listing_does_not_rescan_previous_ranges(tmp_path: Path) -> None:
    keys = [f"{a}{b}x{i:04d}" for a in "0123456789abcdef" for b in "0123456789abcdef" for i in range(2)]
    src = FakeListing(keys)
    _list_all(tmp_path, src, page_size=1000)
    # 256 ranges, one page each: StartAfter = pred(lo) jumps straight to the range.
    assert len(src.calls) == 256


def test_pred_sorts_just_before_lo() -> None:
    for lo in ["01", "a0", "ff", "0", "п", "a\x00"]:
        p = bulk.pred(lo)
        assert p.encode() < lo.encode()
        assert len(p.encode()) <= bulk.MAX_KEY_BYTES
        # Any key that starts with lo's prefix and sorts between p and lo would exceed 1024 bytes.
        assert len(p.encode()) >= bulk.MAX_KEY_BYTES - 1 or lo.endswith("\x00")


def test_listing_resumes_from_checkpoint(tmp_path: Path) -> None:
    keys = [f"0{i:03d}" for i in range(10)]  # all in range ['', '01')
    conn = bulk.connect(tmp_path / "bulk.db")
    bulk.init_ranges(conn)
    stop = threading.Event()
    src = FakeListing(keys, fail_on_call=3)
    rid = bulk.next_range(conn)
    assert bulk.list_range(conn, src, "b", rid, 3, stop, max_attempts=1, sleep=lambda _s: None) == "error"
    row = conn.execute("SELECT status, start_after, listed_count FROM ranges WHERE id = ?", (rid,)).fetchone()
    assert (row["status"], row["start_after"], row["listed_count"]) == ("error", "0005", 6)
    src2 = FakeListing(keys)
    assert bulk.list_range(conn, src2, "b", rid, 3, stop) == "listed"
    assert src2.calls[0]["StartAfter"] == "0005"
    assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 10


def test_listing_continues_with_the_token_and_restarts_from_the_checkpoint(tmp_path: Path) -> None:
    # MinIO answers a StartAfter-only page by walking the flat bucket again: within a run follow its token.
    keys = [f"0{i:03d}" for i in range(10)]
    conn = bulk.connect(tmp_path / "bulk.db")
    bulk.init_ranges(conn)
    src = FakeListing(keys, fail_on_call=3)
    rid = bulk.next_range(conn)
    assert bulk.list_range(conn, src, "b", rid, 3, threading.Event(), max_attempts=2, sleep=lambda _s: None) == "listed"
    assert [c["ContinuationToken"] for c in src.calls] == ["", "tok:0002", "tok:0005", "", "tok:0008"]
    assert src.calls[3]["StartAfter"] == "0005"  # after the failed page: the checkpoint, not the old token
    assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 10


def test_claim_does_not_sort_every_pending_job(tmp_path: Path) -> None:
    # Sorting all pending jobs under the write lock took ~2 s per claim at 12M jobs.
    conn = bulk.connect(tmp_path / "bulk.db")
    conn.executemany("INSERT INTO jobs (key, range_id, size, state, updated_at) VALUES (?, ?, 1, 'pending', 'x')",
                     ((f"{r:02x}{i:05d}", r + 1) for r in range(256) for i in range(80)))
    steps = []
    conn.set_progress_handler(lambda: steps.append(1), 1000)
    rows = bulk.claim_jobs(conn, "o", 50, 60)
    conn.set_progress_handler(None, 0)
    assert len(rows) == 50 and {r["key"][:2] for r in rows} == {"00"}  # ranges in order
    assert len(steps) < 50, len(steps)  # a full sort of the 20k jobs is ~10x more


@pytest.mark.parametrize(
    "minute,expected",
    [(9 * 60, (4, 200.0)), (21 * 60, (32, 800.0)), (3 * 60, (32, 800.0))],
)
def test_schedule(minute: int, expected: tuple) -> None:
    w = bulk.parse_schedule("08:00-20:00 workers=4 mbps=200; 20:00-08:00 workers=32 mbps=800")
    assert bulk.schedule_limits(w, minute, 1) == expected


class _Body:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self, n: int = -1) -> bytes:
        out, self.data = (self.data, b"") if n < 0 else (self.data[:n], self.data[n:])
        return out


class MemS3:
    def __init__(self, objects: Optional[Dict[str, bytes]] = None) -> None:
        self.objects = dict(objects or {})

    def get_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        if Key not in self.objects:
            from s3gw.copykey import ClientError

            exc = ClientError.__new__(ClientError)
            Exception.__init__(exc, "NoSuchKey")
            exc.response = {"Error": {"Code": "NoSuchKey"}}
            raise exc
        import hashlib

        data = self.objects[Key]
        return {"Body": _Body(data), "ContentLength": len(data), "ETag": '"%s"' % hashlib.md5(data).hexdigest()}

    def put_object(self, Bucket: str, Key: str, Body: bytes, **_kw: Any) -> None:
        self.objects[Key] = Body

    def head_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        return {"ContentLength": len(self.objects[Key])}


def _cfg(tmp_path: Path) -> bulk.BulkConfig:
    return bulk.BulkConfig(
        bulk_path=tmp_path / "bulk.db",
        journal_path=tmp_path / "journal.db",
        copy=CopyConfig(src_bucket="b", dst_bucket="b", copy_tags=False),
        lease_seconds=60,
        lease_guard_seconds=1,
    )


def _job(conn: Any, key: str, size: int = 1) -> Any:
    conn.execute(
        "INSERT INTO jobs (key, range_id, size, state, updated_at) VALUES (?, 1, ?, 'pending', 'x')", (key, size)
    )
    return conn.execute("SELECT * FROM jobs WHERE key = ?", (key,)).fetchone()


def test_copy_one_states(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    bconn = bulk.connect(cfg.bulk_path)
    jconn = journal.connect(cfg.journal_path, synchronous="NORMAL")
    src, dst = MemS3({"a": b"1", "c": b"3"}), MemS3()
    epoch = journal.start_epoch(jconn, 0)
    ids = journal.write_intents(jconn, epoch, "r", "put", ["c"])  # gateway already owns "c"
    journal.write_results(jconn, [(ids[0], "c", journal.SUCCESS, 200, None)])

    assert bulk.copy_one(bconn, jconn, _job(bconn, "a"), "bulk-1", src, dst, cfg) == "done"
    assert bulk.copy_one(bconn, jconn, _job(bconn, "b"), "bulk-1", src, dst, cfg) == "source_missing"
    assert bulk.copy_one(bconn, jconn, _job(bconn, "c"), "bulk-1", src, dst, cfg) == "skipped_journal"
    assert dst.objects == {"a": b"1"}
    assert jconn.execute("SELECT state FROM key_state WHERE key = 'a'").fetchone()[0] == "synced"
    assert jconn.execute("SELECT count(*) FROM key_state WHERE key = 'b'").fetchone()[0] == 0


def test_copy_failure_releases_key_for_retry(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    bconn = bulk.connect(cfg.bulk_path)
    jconn = journal.connect(cfg.journal_path, synchronous="NORMAL")

    class Broken(MemS3):
        def put_object(self, **_kw: Any) -> None:
            raise RuntimeError("dst down")

    src = MemS3({"a": b"1"})
    assert bulk.copy_one(bconn, jconn, _job(bconn, "a"), "bulk-1", src, Broken(), cfg) == "retry"
    assert jconn.execute("SELECT count(*) FROM key_state").fetchone()[0] == 0
    job = bconn.execute("SELECT state, attempts FROM jobs WHERE key = 'a'").fetchone()
    assert (job["state"], job["attempts"]) == ("retry", 1)


class ListOnlySource(FakeListing):
    def head_bucket(self, Bucket: str) -> None:
        pass


def _list_only_runner(tmp_path: Path, keys: List[str]) -> bulk.Runner:
    jconn = journal.connect(tmp_path / "journal.db", synchronous="NORMAL")
    epoch = journal.start_epoch(jconn, 0)
    journal.write_intents(jconn, epoch, "r", "put", ["dirty"])  # handler is not running: backlog grows
    cfg = _cfg(tmp_path)
    cfg.backlog_max, cfg.report_every, cfg.list_workers = 1, 0.1, 2
    src = ListOnlySource(keys)
    return bulk.Runner(cfg, lambda: (src, None))  # no destination configured yet


def test_list_only_runs_without_destination_and_ignores_backlog(tmp_path: Path) -> None:
    keys = [f"{a}{b}k" for a in "0f" for b in "0a"]
    rep = _list_only_runner(tmp_path, keys).run(do_list=True, do_copy=False)
    assert rep["listed_objects"] == len(keys)
    assert rep["ranges"] == {"listed": 256}


def test_listing_waits_while_paused(tmp_path: Path) -> None:
    runner = _list_only_runner(tmp_path, ["0a", "ff"])
    bulk.set_control(bulk.connect(tmp_path / "bulk.db"), "paused", "1")
    t = threading.Thread(target=runner.run, kwargs={"do_list": True, "do_copy": False})
    t.start()
    time.sleep(1.0)
    assert bulk.connect(tmp_path / "bulk.db").execute("SELECT count(*) FROM jobs").fetchone()[0] == 0
    bulk.set_control(bulk.connect(tmp_path / "bulk.db"), "paused", "0")
    t.join(10)
    assert not t.is_alive()
    assert bulk.connect(tmp_path / "bulk.db").execute("SELECT count(*) FROM jobs").fetchone()[0] == 2


def test_dead_worker_stops_the_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # An unexpected error in a worker (here: the bulk.db write) must end the run as failed, not hang it.
    runner = _list_only_runner(tmp_path, ["0a"])

    def broken(*a: Any, **kw: Any) -> str:
        raise RuntimeError("disk I/O error")

    monkeypatch.setattr(bulk, "list_range", broken)
    t = threading.Thread(target=runner.run, kwargs={"do_list": True, "do_copy": False})
    t.start()
    t.join(10)
    assert not t.is_alive()
    assert runner.failed and runner.stop.is_set()
def test_dead_worker_claims_go_back_to_retry(tmp_path: Path) -> None:
    conn = bulk.connect(tmp_path / "bulk.db")
    for k in ("a", "b", "c"):
        _job(conn, k)
    assert len(bulk.claim_jobs(conn, "bulk-1-0", 2, 900)) == 2
    bulk.claim_jobs(conn, "bulk-1-1", 1, 900)
    assert bulk.release_own_claims(conn, "bulk-1-0") == 2
    states = dict(conn.execute("SELECT key, state FROM jobs").fetchall())
    assert states == {"a": "retry", "b": "retry", "c": "claimed"}


@pytest.mark.parametrize(
    "ranges, jobs, do_list, do_copy, stopped, code",
    [
        ({"listed": 256}, {"done": 5}, True, True, False, 0),
        ({"listed": 255, "error": 1}, {"done": 5}, True, True, False, 2),
        ({"listed": 256}, {"done": 5, "error": 1}, True, True, False, 2),
        ({"listed": 255, "listing": 1}, {"done": 5}, True, True, False, 3),  # a lister died
        ({"listed": 256}, {"done": 5, "claimed": 2}, True, True, False, 3),  # a copier died
        ({"listed": 255, "pending": 1}, {"pending": 9}, True, True, True, 0),  # stopped by SIGTERM
        ({"listed": 256}, {"pending": 9}, True, False, False, 0),  # list only: jobs wait for copy
        ({"pending": 10}, {"done": 9}, False, True, False, 0),  # copy only: listing is elsewhere
    ],
)
def test_exit_code(ranges: dict, jobs: dict, do_list: bool, do_copy: bool, stopped: bool, code: int) -> None:
    assert bulk.exit_code({"ranges": ranges, "jobs": jobs}, do_list, do_copy, stopped) == code


def test_second_bulk_process_on_same_db_is_refused(tmp_path: Path) -> None:
    first = bulk.acquire_lock(tmp_path / "bulk.db")
    try:
        with pytest.raises(SystemExit, match="another bulk process"):
            bulk.acquire_lock(tmp_path / "bulk.db")
    finally:
        first.close()
    bulk.acquire_lock(tmp_path / "bulk.db").close()  # free again once the holder is gone


def test_throttle_wait_ends_on_stop(tmp_path: Path) -> None:
    ctl = bulk.Controller(tmp_path / "bulk.db", tmp_path / "journal.db", [], "UTC", 1, 0)
    ctl.limits = bulk.Limits(1, 8.0, "test")  # 1 MiB/s
    stop = threading.Event()
    stop.set()
    t = time.monotonic()
    ctl.take_bytes(100 * 1024 * 1024, stop)  # would wait 60 s
    assert time.monotonic() - t < 5


class TaggedS3(MemS3):
    def __init__(self, objects: Optional[Dict[str, bytes]] = None, tags: Optional[Dict[str, list]] = None) -> None:
        super().__init__(objects)
        self.tags = dict(tags or {})
        self.tag_calls: List[str] = []
        self.tag_reads: List[str] = []

    def get_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        resp = super().get_object(Bucket, Key)
        if self.tags.get(Key):
            resp["TagCount"] = len(self.tags[Key])  # MinIO sends x-amz-tagging-count only for tagged objects
        return resp

    def get_object_tagging(self, Bucket: str, Key: str) -> Dict[str, Any]:
        self.tag_reads.append(Key)
        return {"TagSet": self.tags.get(Key, [])}

    def put_object_tagging(self, Bucket: str, Key: str, Tagging: Dict[str, Any]) -> None:
        self.tag_calls.append(f"put:{Key}")
        self.tags[Key] = Tagging["TagSet"]

    def delete_object_tagging(self, Bucket: str, Key: str) -> None:
        self.tag_calls.append(f"delete:{Key}")


def test_tags_copied_without_request_for_untagged_objects() -> None:
    from s3gw.copykey import sync_key

    src = TaggedS3({"a": b"1", "b": b"2"}, tags={"a": [{"Key": "k", "Value": "v"}]})
    dst = TaggedS3()
    cfg = CopyConfig(src_bucket="b", dst_bucket="b", copy_tags=True)
    sync_key(src, dst, "a", cfg)
    sync_key(src, dst, "b", cfg)
    assert dst.tag_calls == ["put:a"]
    assert dst.tags["a"] == [{"Key": "k", "Value": "v"}]
    assert src.tag_reads == ["a"]  # no GetObjectTagging to the HDD source for the untagged object


def test_large_write_needs_more_lease_left(tmp_path: Path) -> None:
    from s3gw.copykey import MIN_WRITE_BPS, write_guard

    assert write_guard(30, 1024) == 30
    assert write_guard(30, 16 * 1024 * 1024) == 16 * 1024 * 1024 / MIN_WRITE_BPS == 64
    cfg = _cfg(tmp_path)
    cfg.lease_seconds, cfg.lease_guard_seconds = 40, 1  # enough for small writes, not for 16 MiB
    bconn = bulk.connect(cfg.bulk_path)
    jconn = journal.connect(cfg.journal_path, synchronous="NORMAL")
    src, dst = MemS3({"big": b"x" * (16 * 1024 * 1024), "small": b"1"}), MemS3()
    assert bulk.copy_one(bconn, jconn, _job(bconn, "small"), "bulk-1", src, dst, cfg) == "done"
    assert bulk.copy_one(bconn, jconn, _job(bconn, "big"), "bulk-1", src, dst, cfg) == "retry"
    assert "big" not in dst.objects
    assert "LeaseExpired" in bconn.execute("SELECT last_error FROM jobs WHERE key = 'big'").fetchone()[0]


def test_short_lease_is_refused_for_copy_not_for_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(SystemExit):
        bulk.main(["copy", "--bulk-db", str(tmp_path / "bulk.db"), "--lease-seconds", "20"])
    assert not (tmp_path / "bulk.db.lock").exists()  # refused before taking the lock
    started: List[str] = []
    monkeypatch.setattr(bulk.Runner, "run", lambda self, **kw: started.append("run") or {"jobs": {}, "ranges": {}})
    monkeypatch.setattr("s3gw.handler.clients_from_env", lambda: (lambda: (None, None)))
    assert bulk.main(["list", "--bulk-db", str(tmp_path / "bulk.db"), "--journal", str(tmp_path / "j.db"),
                      "--lease-seconds", "20"]) == 0
    assert started == ["run"]


def test_schedule_with_unknown_time_zone_is_refused(tmp_path: Path) -> None:
    # Without a known zone the windows would silently follow the server's local time (UTC on most servers).
    with pytest.raises(SystemExit):
        bulk.main(["copy", "--bulk-db", str(tmp_path / "bulk.db"), "--schedule", "08:00-20:00 workers=1",
                   "--tz", "Europe/Moskow"])


def test_listing_gives_up_after_max_attempts(tmp_path: Path) -> None:
    class Failing:
        calls = 0

        def list_objects_v2(self, **kw: Any) -> dict:
            Failing.calls += 1
            raise RuntimeError("AccessDenied")

    conn = bulk.connect(tmp_path / "bulk.db")
    bulk.init_ranges(conn, ["5"])
    st = bulk.list_range(conn, Failing(), "b", 1, 10, threading.Event(), max_attempts=3, sleep=lambda s: None)
    assert st == "error" and Failing.calls == 3
    assert conn.execute("SELECT status, last_error FROM ranges WHERE id = 1").fetchone()[1].startswith("RuntimeError")


def _scan_counts(conn: Any) -> Dict[str, Any]:
    return {r[0]: (r[1], r[2]) for r in conn.execute("SELECT state, count(*), sum(size) FROM jobs GROUP BY state")}


def test_job_counts_follow_every_change_and_an_old_db_is_counted_once(tmp_path: Path) -> None:
    conn = bulk.connect(tmp_path / "bulk.db")
    for k in "abcde":
        _job(conn, k, size=10)
    conn.execute("INSERT OR IGNORE INTO jobs (key, range_id, size, state, updated_at) VALUES ('a', 1, 10, 'pending', 'x')")
    bulk.claim_jobs(conn, "o", 3, 60)
    bulk._set_job(conn, "a", "done")
    bulk._set_job(conn, "b", "retry")
    bulk.release_own_claims(conn, "o")
    assert bulk.job_counts(conn) == _scan_counts(conn) == {"done": (1, 10), "retry": (2, 20), "pending": (2, 20)}
    assert bulk._remaining(conn) == 4
    # a bulk.db from before the triggers is counted on the first connect
    for t in ("jobs_count_insert", "jobs_count_update", "jobs_count_delete"):
        conn.execute(f"DROP TRIGGER {t}")
    conn.execute("DELETE FROM job_counts")
    _job(conn, "f", 5)
    conn.close()
    conn = bulk.connect(tmp_path / "bulk.db")
    assert bulk.job_counts(conn) == _scan_counts(conn)


def test_status_and_remaining_do_not_scan_jobs(tmp_path: Path) -> None:
    conn = bulk.connect(tmp_path / "bulk.db")
    for q in ("SELECT coalesce(sum(n), 0) FROM job_counts WHERE state IN ('pending', 'retry', 'claimed')",
              "SELECT state, n, bytes FROM job_counts WHERE n != 0"):
        plan = " ".join(r[-1] for r in conn.execute("EXPLAIN QUERY PLAN " + q))
        assert "jobs" not in plan.replace("job_counts", ""), plan


def test_timed_client_counts_calls_and_body_reads() -> None:
    spent: List[float] = []

    class SlowBody:
        def read(self, n: int = -1) -> bytes:
            time.sleep(0.03)
            return b"x"

    class Slow:
        meta = "kept"

        def get_object(self, **_kw: Any) -> Dict[str, Any]:
            time.sleep(0.02)
            return {"Body": SlowBody(), "ContentLength": 1}

    c = bulk._Timed(Slow(), spent.append)
    resp = c.get_object(Bucket="b", Key="k")
    assert resp["Body"].read() == b"x" and resp["ContentLength"] == 1 and c.meta == "kept"
    assert len(spent) == 2 and sum(spent) >= 0.045


def test_bottleneck_hint() -> None:
    def shares(src: float, dst: float, thr: float) -> Dict[str, float]:
        return {"source_read": src, "destination_write": dst, "throttle_wait": thr, "idle": max(0.0, 1 - src - dst - thr)}

    L = bulk.Limits
    assert bulk.bottleneck(L(4, 0, "journal_lag"), shares(0.5, 0.5, 0), 10) == "journal_lag"
    assert bulk.bottleneck(L(4, 0, "schedule"), shares(0.1, 0.1, 0.6), 10) == "mbps_limit"
    assert bulk.bottleneck(L(4, 0, "schedule"), shares(0.6, 0.2, 0.1), 10) == "source"
    assert bulk.bottleneck(L(4, 0, "schedule"), shares(0.2, 0.6, 0.1), 10) == "destination"
    assert bulk.bottleneck(L(4, 0, "schedule"), shares(0.4, 0.4, 0.1), 10) == "balanced"
    assert bulk.bottleneck(L(4, 0, "schedule"), shares(0.1, 0.1, 0.1), 10) == "idle"
    assert bulk.bottleneck(L(4, 0, "schedule"), None, 0) == "done"


class _FullSource(MemS3):
    def __init__(self, objects: Dict[str, bytes]) -> None:
        super().__init__(objects)
        self.lister = FakeListing(list(objects))

    def list_objects_v2(self, **kw: Any) -> Dict[str, Any]:
        page = self.lister.list_objects_v2(**kw)
        for o in page["Contents"]:
            o["Size"] = len(self.objects[o["Key"]])
        return page

    def head_bucket(self, Bucket: str) -> None:
        pass


class _Dst(MemS3):
    def head_bucket(self, Bucket: str) -> None:
        pass


def test_run_writes_the_status_file_and_history(tmp_path: Path) -> None:
    import json

    journal.start_epoch(journal.connect(tmp_path / "journal.db", synchronous="NORMAL"), 0)
    objs = {f"{a}{b}-{i}": b"x" * (100 + i) for a in "0f" for b in "07" for i in range(5)}
    src, dst = _FullSource(objs), _Dst()
    cfg = _cfg(tmp_path)
    cfg.workers, cfg.report_every = 2, 0.2
    cfg.status_path, cfg.progress_path = tmp_path / "bulk-status.json", tmp_path / "bulk-progress.jsonl"
    bulk.Runner(cfg, lambda: (src, dst)).run()
    assert dst.objects == objs
    st = json.loads(cfg.status_path.read_text(encoding="utf-8"))
    assert st["progress"] == {"jobs_total": 20, "finished": 20, "remaining": 0, "pct": 100.0, "listing_complete": True}
    assert st["jobs"] == {"done": 20} and st["bytes"]["copied"] == sum(len(v) for v in objs.values())
    assert st["rate"]["since_start"]["copied_per_min"] > 0 and st["bottleneck"] == "done"
    assert st["listing"]["listed_objects"] == 20 and st["errors"]["jobs_in_error"] == 0
    lines = [json.loads(line) for line in cfg.progress_path.read_text(encoding="utf-8").splitlines()]
    assert lines and all({"objects_per_min_5m", "mb_per_s_5m", "eta_hours", "bottleneck", "limit_workers"} <= set(r) for r in lines)
    assert not list(tmp_path.glob("*.tmp"))  # written through a temp file and a rename


def test_run_ends_when_fewer_workers_are_allowed_than_threads(tmp_path: Path) -> None:
    # SCHEDULE workers=1 or `set workers_max 1` with LIST_WORKERS=4, BULK_WORKERS=3: the idle threads
    # must not keep `bulk run` (and the systemd unit) alive after the work is done.
    journal.start_epoch(journal.connect(tmp_path / "journal.db", synchronous="NORMAL"), 0)
    objs = {f"{a}-{i}": b"y" * 10 for a in "05af" for i in range(3)}
    src, dst = _FullSource(objs), _Dst()
    cfg = _cfg(tmp_path)
    cfg.workers, cfg.list_workers, cfg.report_every = 3, 4, 0.2
    bulk.set_control(bulk.connect(cfg.bulk_path), "workers_max", "1")
    t = threading.Thread(target=lambda: bulk.Runner(cfg, lambda: (src, dst)).run(), daemon=True)
    t.start()
    t.join(30)
    assert not t.is_alive() and dst.objects == objs
