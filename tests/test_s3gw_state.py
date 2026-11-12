"""s3gw.state: the journal at a glance and the wait of C2.2 / C4.2."""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from s3gw import journal as j
from s3gw import state


def _journal(tmp_path: Path):  # type: ignore[no-untyped-def]
    conn = j.connect(tmp_path / "journal.db", synchronous="NORMAL")
    epoch = j.start_epoch(conn, 0)
    ids = j.write_intents(conn, epoch, "r1", "put", ["synced"])
    j.write_results(conn, [(ids[0], "synced", j.SUCCESS, 200, None)])
    j.finish_sync(conn, j.claim_dirty(conn, "h", 60)[0], "synced")
    return conn, epoch


def test_state_reports_queue_errors_and_epoch(tmp_path: Path, capsys) -> None:
    conn, epoch = _journal(tmp_path)
    j.write_intents(conn, epoch, "r2", "put", ["in-flight"])
    ids = j.write_intents(conn, epoch, "r3", "put", ["broken"])
    j.write_results(conn, [(ids[0], "broken", j.SUCCESS, 200, None)])
    j.fail_sync(conn, j.claim_dirty(conn, "h", 60)[0], "AccessDenied", backoff_seconds=1, max_attempts=1)
    j.mpu_open(conn, epoch, "u1", "big")
    j.record_error(conn, "handler", "max_attempts", "broken", "AccessDenied")
    assert state.main(["--journal", str(tmp_path / "journal.db")]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["epoch"]["epoch"] == epoch
    assert (out["drained"], out["backlog"], out["open_intents"], out["stuck_intents"]) == (False, 1, 1, 0)
    assert out["error_keys"] == 1 and out["error_sample"][0]["key"] == "broken"
    assert out["open_mpu"] == 1 and out["recent_errors"][0]["kind"] == "max_attempts"
    assert out["counters"]["sync_synced"] == 1


def test_wait_drained_returns_once_the_handler_is_done(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(state, "POLL_SECONDS", 0.05)
    conn, epoch = _journal(tmp_path)
    ids = j.write_intents(conn, epoch, "r2", "put", ["k"])

    def gateway_and_handler() -> None:
        time.sleep(0.3)
        c = j.connect(tmp_path / "journal.db", synchronous="NORMAL")
        j.write_results(c, [(ids[0], "k", j.SUCCESS, 200, None)])
        j.finish_sync(c, j.claim_dirty(c, "h", 60)[0], "synced")
        c.close()

    t = threading.Thread(target=gateway_and_handler)
    t.start()
    assert state.main(["--journal", str(tmp_path / "journal.db"), "--wait-drained", "10"]) == 0
    t.join()
    assert json.loads(capsys.readouterr().out)["drained"] is True


def test_missing_journal_is_a_message_not_a_traceback(tmp_path: Path, capsys) -> None:
    assert state.main(["--journal", str(tmp_path / "journal.db")]) == 2
    assert "has not created the journal yet" in capsys.readouterr().err


def test_wait_drained_times_out_and_names_lost_results(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(state, "POLL_SECONDS", 0.05)
    conn, epoch = _journal(tmp_path)
    j.write_intents(conn, epoch, "r2", "put", ["k"])  # its result was never written (disk full)
    conn.execute("UPDATE intents SET created_at = '2026-01-01T00:00:00.000000Z' WHERE key = 'k'")
    assert state.main(["--journal", str(tmp_path / "journal.db"), "--wait-drained", "0.2"]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out)["stuck_intents"] == 1
    assert "gateway restart" in captured.err
