from __future__ import annotations

import json
import urllib.parse
import sqlite3
from pathlib import Path
from typing import Optional

from s3gw import journal, tracecheck


def _ev(func: str, method: str, path: str, rid: Optional[str] = None, status: int = 200, headers: bool = True) -> str:
    # mc 2025-08 `admin trace --json --verbose` against MinIO 2025-09 (captured on a stand); without
    # --verbose there is no "request". MinIO reports X-Forwarded-For as client: same for every request.
    rec = {"type": "S3", "api": func, "path": urllib.parse.quote(path), "client": "10.0.0.5",
           "response": {"statusCode": status}}
    if headers:
        rec["request"] = {"method": method, "path": urllib.parse.quote(path),
                          "headers": {"Host": "elma.example.com", **({"X-S3gw-Request-Id": rid} if rid else {})}}
    else:
        rec["statusCode"] = status
    return json.dumps(rec)


def _ev_old(func: str, method: str, path: str, rid: str) -> str:
    # Older mc: request/response under "http", call named funcName, header values as lists.
    return json.dumps({"funcName": func, "path": path, "http": {
        "request": {"method": method, "headers": {"X-S3gw-Request-Id": [rid]}}, "response": {"statusCode": 200}}})


def test_check_finds_bypass_and_missed_keys() -> None:
    intents = {"r1": {"a/файл.pdf"}, "r2": {"a b"}, "r4": {"x", "y"}, "r5": {"other-key"}}
    lines = [
        _ev("s3.PutObject", "PUT", "/s3elma365/a/файл.pdf", "r1"),
        _ev("s3.PutObject", "PUT", "/s3elma365/a b", "r2"),            # trace path is encoded, journal key is not
        _ev("s3.DeleteObject", "DELETE", "/s3elma365/gone", "r3"),      # through the gateway, no intent: missed
        _ev("s3.PutObject", "PUT", "/s3elma365/k", "r5"),               # intent of that request is for another key
        _ev("s3.PutObjectPart", "PUT", "/s3elma365/big", "r9"),         # part: not a key change
        _ev("s3.DeleteMultipleObjects", "POST", "/s3elma365/", "r4"),   # batch: keys in the body
        _ev("s3.PutObject", "PUT", "/s3elma365/direct"),                # no mark: bypass
        _ev("s3.GetObject", "GET", "/s3elma365/x"),                     # read
        _ev("s3.PutObject", "PUT", "/s3elma365/denied", status=403),    # rejected
        _ev("s3.PutObject", "PUT", "/other/x"),                         # other bucket
        "not json",
    ]
    rep = tracecheck.check(lines, intents, "s3elma365")
    assert rep["changes"] == 7 and rep["without_headers"] == 0
    assert [e["key"] for e in rep["missed"]] == ["gone", "k"]
    assert [e["key"] for e in rep["bypass"]] == ["direct"]


def test_trace_without_headers_cannot_be_verified(tmp_path: Path) -> None:
    conn = journal.connect(tmp_path / "journal.db")
    conn.close()
    trace = tmp_path / "trace.jsonl"
    trace.write_text(_ev("s3.PutObject", "PUT", "/s3elma365/a", headers=False) + "\n", encoding="utf-8")
    assert tracecheck.main([str(trace), "--journal", str(tmp_path / "journal.db")]) == 2


def test_empty_or_partly_headerless_trace_is_not_clean(tmp_path: Path) -> None:
    # A rollback trace whose mc died (empty) or lost --verbose for some records must not read as "no bypass".
    conn = journal.connect(tmp_path / "journal.db")
    epoch = journal.start_epoch(conn, 0)
    journal.write_intents(conn, epoch, "rid-1", "put", ["a"])
    conn.close()
    trace = tmp_path / "trace.jsonl"
    trace.write_text("", encoding="utf-8")
    assert tracecheck.main([str(trace), "--journal", str(tmp_path / "journal.db")]) == 2
    trace.write_text(_ev("s3.PutObject", "PUT", "/s3elma365/a", "rid-1") + "\n"
                     + _ev("s3.PutObject", "PUT", "/s3elma365/b", headers=False) + "\n", encoding="utf-8")
    assert tracecheck.main([str(trace), "--journal", str(tmp_path / "journal.db")]) == 2


def test_main_reads_request_ids_from_journal(tmp_path: Path) -> None:
    conn = journal.connect(tmp_path / "journal.db")
    epoch = journal.start_epoch(conn, 0)
    journal.write_intents(conn, epoch, "rid-1", "put", ["a"])
    conn.close()
    trace = tmp_path / "trace.jsonl"
    trace.write_text(_ev("s3.PutObject", "PUT", "/s3elma365/a", "rid-1") + "\n", encoding="utf-8")
    assert tracecheck.main([str(trace), "--journal", str(tmp_path / "journal.db")]) == 0
    assert sqlite3.connect(str(tmp_path / "journal.db")).execute("SELECT count(*) FROM intents").fetchone()[0] == 1


def test_older_trace_shape_is_understood() -> None:
    rep = tracecheck.check([_ev_old("s3.PutObject", "PUT", "/s3elma365/a b", "r1")], {"r1": {"a b"}}, "s3elma365")
    assert rep["changes"] == 1 and not rep["bypass"] and not rep["missed"]


def test_trace_without_verbose_counts_changes_but_skips_reads() -> None:
    lines = [_ev("s3.PutObject", "PUT", "/s3elma365/a", headers=False),
             _ev("s3.GetObject", "GET", "/s3elma365/a", headers=False)]
    rep = tracecheck.check(lines, {}, "s3elma365")
    assert (rep["changes"], rep["without_headers"]) == (1, 1)


def test_mark_dirty_writes_bypass_keys_to_journal(tmp_path: Path, capsys) -> None:
    db = tmp_path / "journal.db"
    journal.connect(db).close()
    trace = tmp_path / "t.jsonl"
    trace.write_text("\n".join([
        _ev("s3.PutObject", "PUT", "/s3elma365/a b"),
        _ev("s3.PutObjectPart", "PUT", "/s3elma365/big"),
    ]), encoding="utf-8")
    assert tracecheck.main([str(trace), "--journal", str(db), "--mark-dirty"]) == 1
    assert json.loads(capsys.readouterr().out)["marked_dirty"] == 1
    conn = journal.connect(db)
    assert [r["key"] for r in conn.execute("SELECT key FROM key_state WHERE state = 'dirty'")] == ["a b"]


def test_utc_iso_matches_journal_format() -> None:
    assert journal.utc_iso("2026-10-22T12:00:00.123456789+05:00") == "2026-10-22T07:00:00.123456Z"
    assert journal.utc_iso("2026-11-20T18:00:00Z") == "2026-11-20T18:00:00.000000Z"
    assert journal.utc_iso("2026-11-20 18:00") is None  # no zone: refused, not guessed
