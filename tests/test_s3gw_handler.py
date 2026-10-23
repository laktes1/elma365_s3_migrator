from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any, List

import pytest

from s3gw import handler, journal
from s3gw.copykey import CopyConfig


def test_worker_survives_journal_error(tmp_path: Path, monkeypatch: Any) -> None:
    cfg = handler.HandlerConfig(
        journal_path=tmp_path / "journal.db", copy=CopyConfig("s", "d"), backoff_seconds=0.01, idle_sleep=0.01
    )
    stop = threading.Event()
    calls: List[int] = []

    def flaky_claim(*_a: Any, **_kw: Any) -> list:
        calls.append(1)
        if len(calls) == 1:
            raise sqlite3.OperationalError("database is locked")
        stop.set()
        return []

    monkeypatch.setattr(journal, "claim_dirty", flaky_claim)
    handler.worker_loop("handler-1-1", cfg, lambda: (None, None), stop)
    assert len(calls) == 2


def test_dead_worker_fails_the_process(tmp_path: Path) -> None:
    # A worker that dies (here: the journal cannot be opened) must not leave the rest
    # running short-handed: the process fails and systemd restarts it.
    cfg = handler.HandlerConfig(journal_path=tmp_path, copy=CopyConfig("s", "d"), workers=2)  # a directory, not a db
    with pytest.raises(RuntimeError, match="handler workers failed"):
        handler.run(cfg, lambda: (None, None), once=True)


def test_key_that_never_fits_in_the_lease_ends_in_error(tmp_path: Path) -> None:
    # Before: LeaseExpired released nothing and counted no attempt, so such a key was re-read forever.
    from tests.test_s3gw_bulk import MemS3

    cfg = handler.HandlerConfig(journal_path=tmp_path / "journal.db", copy=CopyConfig("s", "d", copy_tags=False),
                                lease_seconds=40, lease_guard_seconds=1, backoff_seconds=0, max_attempts=2)
    conn = journal.connect(cfg.journal_path, synchronous="NORMAL")
    journal.mark_dirty(conn, ["big"])
    src, dst = MemS3({"big": b"x" * (16 * 1024 * 1024)}), MemS3()  # one 16 MiB write needs 64 s of lease
    results = [handler.process_one(conn, journal.claim_dirty(conn, "h", cfg.lease_seconds)[0], src, dst, cfg)
               for _ in range(2)]
    assert results == ["retry", "error"]
    row = conn.execute("SELECT state, attempts, last_error FROM key_state WHERE key = 'big'").fetchone()
    assert (row["state"], row["attempts"]) == ("error", 2) and "LeaseExpired" in row["last_error"]
    assert "big" not in dst.objects


def test_short_lease_is_refused_at_start(monkeypatch: Any) -> None:
    # A lease below the write guard of one 16 MiB part (64 s) never lets a write start:
    # every key would cycle through LeaseExpired without an error.
    monkeypatch.setenv("DST_ENDPOINT", "http://new-s3")
    with pytest.raises(SystemExit):
        handler.main(["--lease-seconds", "60"])
