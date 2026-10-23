"""Lease and counter rules of the intent journal (docs/gateway-design.md, section 6 scenarios)."""
from __future__ import annotations

from pathlib import Path

from s3gw import journal as j


def _conn(tmp_path: Path):
    return j.connect(tmp_path / "journal.db", synchronous="NORMAL")


def _state(conn, key: str):
    return conn.execute("SELECT * FROM key_state WHERE key = ?", (key,)).fetchone()


def _put(conn, epoch: int, key: str, result: str = j.SUCCESS) -> None:
    ids = j.write_intents(conn, epoch, "r", "put", [key])
    j.write_results(conn, [(ids[0], key, result, 200, None)])


def test_intent_marks_key_dirty_and_in_flight(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, settle_seconds=0)
    ids = j.write_intents(conn, epoch, "r1", "put", ["k"])
    row = _state(conn, "k")
    assert (row["change_seq"], row["in_flight"], row["state"]) == (1, 1, "dirty")
    assert j.claim_dirty(conn, "h", 60) == []  # in flight: not claimable (scenario 7)
    j.write_results(conn, [(ids[0], "k", j.SUCCESS, 200, None)])
    row = _state(conn, "k")
    assert (row["change_seq"], row["in_flight"]) == (2, 0)
    assert [l.key for l in j.claim_dirty(conn, "h", 60)] == ["k"]


def test_handler_sync_clean(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    _put(conn, epoch, "k")
    lease = j.claim_dirty(conn, "h", 60)[0]
    assert j.finish_sync(conn, lease, "synced", 4, "e")
    row = _state(conn, "k")
    assert row["state"] == "synced" and row["synced_seq"] == row["change_seq"]
    assert j.backlog(conn)[0] == 0


def test_change_during_sync_keeps_key_dirty(tmp_path: Path) -> None:
    # Scenario 6.
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    _put(conn, epoch, "k")
    lease = j.claim_dirty(conn, "h", 60)[0]
    _put(conn, epoch, "k")
    assert _state(conn, "k")["state"] == "syncing"  # lease kept while handler works
    assert not j.finish_sync(conn, lease, "synced")
    assert _state(conn, "k")["state"] == "dirty"
    assert [l.key for l in j.claim_dirty(conn, "h", 60)] == ["k"]


def test_bulk_skips_key_known_to_journal(tmp_path: Path) -> None:
    # Scenarios 2 and 5.
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    _put(conn, epoch, "k")
    assert j.bulk_claim(conn, "k", "bulk-1", 60) is None


def test_bulk_clean_copy_becomes_synced(tmp_path: Path) -> None:
    # Scenario 1.
    conn = _conn(tmp_path)
    lease = j.bulk_claim(conn, "k", "bulk-1", 60)
    assert lease is not None
    assert j.claim_dirty(conn, "h", 60) == []  # handler does not take a bulk-leased key
    assert j.bulk_finish(conn, lease, source_missing=False) == "synced"
    assert j.backlog(conn)[0] == 0


def test_put_during_bulk_copy_hands_key_to_handler(tmp_path: Path) -> None:
    # Scenarios 3 and 4.
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    lease = j.bulk_claim(conn, "k", "bulk-1", 60)
    _put(conn, epoch, "k")
    assert _state(conn, "k")["state"] == "bulk"
    assert j.claim_dirty(conn, "h", 60) == []  # bulk lease still active
    assert j.bulk_finish(conn, lease, source_missing=False) == "dirty"
    assert [l.key for l in j.claim_dirty(conn, "h", 60)] == ["k"]


def test_bulk_source_missing_forgets_untouched_key(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    lease = j.bulk_claim(conn, "k", "bulk-1", 60)
    assert j.bulk_finish(conn, lease, source_missing=True) == "forgotten"
    assert _state(conn, "k") is None


def test_expired_bulk_lease_goes_to_handler(tmp_path: Path) -> None:
    # Scenario 9.
    conn = _conn(tmp_path)
    j.bulk_claim(conn, "k", "bulk-1", 10, now=1000.0)
    assert j.claim_dirty(conn, "h", 60, now=1005.0) == []
    leases = j.claim_dirty(conn, "h", 60, now=1011.0)
    assert [l.key for l in leases] == ["k"]


def test_expired_handler_lease_is_retaken_and_old_owner_cannot_commit(tmp_path: Path) -> None:
    # Scenario 10.
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    _put(conn, epoch, "k")
    first = j.claim_dirty(conn, "h1", 10, now=1e10)[0]
    second = j.claim_dirty(conn, "h2", 10, now=1e10 + 11)[0]
    assert not j.finish_sync(conn, first, "synced")
    assert j.finish_sync(conn, second, "synced")


def test_recovery_marks_open_intents_uncertain(tmp_path: Path) -> None:
    # Scenario 11.
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    j.write_intents(conn, epoch, "r", "put", ["k"])  # gateway dies before the result
    j.start_epoch(conn, settle_seconds=100, now=5000.0)
    row = _state(conn, "k")
    assert (row["in_flight"], row["state"], row["change_seq"]) == (0, "uncertain", 2)
    assert conn.execute("SELECT result FROM intents").fetchone()[0] == j.UNCERTAIN
    assert j.claim_dirty(conn, "h", 60, now=5050.0) == []  # settling
    assert [l.key for l in j.claim_dirty(conn, "h", 60, now=5101.0)] == ["k"]


def test_late_result_after_recovery_is_ignored(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    ids = j.write_intents(conn, epoch, "r", "put", ["k"])
    j.start_epoch(conn, 0)
    j.write_results(conn, [(ids[0], "k", j.SUCCESS, 200, None)])
    assert _state(conn, "k")["in_flight"] == 0


def test_fail_sync_backoff_and_error(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    _put(conn, epoch, "k")
    lease = j.claim_dirty(conn, "h", 60, now=1e10)[0]
    assert j.fail_sync(conn, lease, "boom", backoff_seconds=5, max_attempts=2, now=1e10) == "dirty"
    assert j.claim_dirty(conn, "h", 60, now=1e10 + 4) == []
    lease = j.claim_dirty(conn, "h", 60, now=1e10 + 6)[0]
    assert j.fail_sync(conn, lease, "boom", backoff_seconds=5, max_attempts=2, now=1e10 + 6) == "error"
    assert j.claim_dirty(conn, "h", 60, now=1e12) == []  # errors need an operator
    assert j.backlog(conn)[0] == 0


def test_batch_intents_are_one_transaction(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    ids = j.write_intents(conn, epoch, "r", "delete_batch", ["a", "b", "c"])
    assert len(ids) == 3
    assert conn.execute("SELECT count(*) FROM key_state WHERE in_flight = 1").fetchone()[0] == 3


def test_purge_keeps_open_and_unsynced_intents(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    _put(conn, epoch, "synced")
    lease = j.claim_dirty(conn, "h", 60)[0]
    j.finish_sync(conn, lease, "synced")
    _put(conn, epoch, "dirty")
    j.write_intents(conn, epoch, "r", "put", ["open"])
    assert j.purge_intents(conn, older_than_days=-1) == 1
    left = {r[0] for r in conn.execute("SELECT key FROM intents")}
    assert left == {"dirty", "open"}


def test_mark_dirty_hands_synced_and_bulk_keys_to_handler(tmp_path: Path) -> None:
    # Keys changed past the gateway during a rollback of the redirect (PIPELINE.md, A4).
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    _put(conn, epoch, "synced")
    j.finish_sync(conn, j.claim_dirty(conn, "h", 60)[0], "synced")
    lease = j.bulk_claim(conn, "copying", "bulk-1", 60)
    assert j.mark_dirty(conn, ["synced", "copying", "new"]) == 3
    assert j.bulk_finish(conn, lease, source_missing=False) == "dirty"
    assert j.bulk_claim(conn, "new", "bulk-1", 60) is None  # bulk no longer owns it
    assert sorted(l.key for l in j.claim_dirty(conn, "h", 60, limit=10)) == ["copying", "new", "synced"]


def test_work_queries_use_the_partial_index(tmp_path: Path) -> None:
    # claim_dirty runs under the write lock: a scan of 12M key_state rows there holds
    # the gateway's intent writes past their 2 s timeout (ELMA365 gets 503).
    import inspect
    import re

    conn = _conn(tmp_path)
    for fn in (j.claim_dirty, j.backlog):
        sql = re.search(r'"""(SELECT .*?)"""', inspect.getsource(fn), re.S).group(1)
        plan = " ".join(r[-1] for r in conn.execute("EXPLAIN QUERY PLAN " + sql, (0,) * sql.count("?")))
        assert "idx_key_state_work" in plan, (fn.__name__, plan)


def test_backlog_counts_unsynced_keys_only(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    _put(conn, 1, "a")
    _put(conn, 1, "b")
    (lease,) = j.claim_dirty(conn, "h", 60)
    assert j.finish_sync(conn, lease, "synced")      # one of them is in sync, the other still dirty
    j.bulk_claim(conn, "c", "bulk-1", 60)            # being copied by bulk
    assert j.backlog(conn)[0] == 2
def test_new_change_gives_error_key_fresh_attempts(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    _put(conn, epoch, "k")
    lease = j.claim_dirty(conn, "h", 60, now=1e10)[0]
    assert j.fail_sync(conn, lease, "boom", backoff_seconds=5, max_attempts=1, now=1e10) == "error"
    _put(conn, epoch, "k")  # the client writes the key again
    assert (_state(conn, "k")["state"], _state(conn, "k")["attempts"]) == ("dirty", 0)
    lease = j.claim_dirty(conn, "h", 60, now=1e11)[0]
    assert j.fail_sync(conn, lease, "boom", backoff_seconds=5, max_attempts=2, now=1e11) == "dirty"


def test_backlog_by_state_matches_seq_definition(tmp_path: Path) -> None:
    conn = _conn(tmp_path)
    epoch = j.start_epoch(conn, 0)
    _put(conn, epoch, "dirty")
    _put(conn, epoch, "synced")
    j.finish_sync(conn, j.claim_dirty(conn, "h", 60, now=1e10)[0], "synced")  # one of the two
    j.write_intents(conn, epoch, "r", "put", ["in-flight"])
    j.bulk_claim(conn, "bulk-held", "b", 60)
    lease = j.bulk_claim(conn, "bulk-done", "b", 60)
    j.bulk_finish(conn, lease, source_missing=False)
    _put(conn, epoch, "err")
    lease = [x for x in j.claim_dirty(conn, "h", 60, limit=10, now=1e12) if x.key == "err"][0]
    assert j.fail_sync(conn, lease, "boom", 0, 1, now=1e12) == "error"
    by_seq = conn.execute(
        "SELECT count(*) FROM key_state WHERE NOT (synced_seq = change_seq AND in_flight = 0) AND state != 'error'"
    ).fetchone()[0]
    assert j.backlog(conn)[0] == by_seq == 3  # the unsynced one of dirty/synced, in-flight, bulk-held
