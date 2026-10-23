from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterator

import pytest

from migrator import reconcile_buckets as mrb

HEX = "0123456789abcdef"


class _S3:
    """list_objects_v2 as S3 does it: keys in UTF-8 byte order, Prefix, StartAfter, pages. Counts requests."""

    def __init__(self, objects: dict[str, tuple[int, str]]) -> None:
        self.objects = objects  # key -> (size, etag)
        self.requests = 0

    def get_paginator(self, _name: str) -> "_S3":
        return self

    def paginate(
        self, Bucket: str, PaginationConfig: dict[str, int], Prefix: str = "", StartAfter: str = ""
    ) -> Iterator[dict[str, Any]]:
        keys = sorted((k for k in self.objects if k.startswith(Prefix) and k.encode() > StartAfter.encode()), key=str.encode)
        size = PaginationConfig["PageSize"]
        for i in range(0, max(len(keys), 1), size):
            self.requests += 1
            yield {
                "Contents": [
                    {"Key": k, "Size": self.objects[k][0], "ETag": f'"{self.objects[k][1]}"', "LastModified": "2026-06-01T00:00:00Z"}
                    for k in keys[i : i + size]
                ]
            }


def _insert(conn: sqlite3.Connection, side: str, rows: list[tuple[str, int, str]]) -> None:
    conn.executemany(
        f"INSERT INTO {side}_partition_00(key, size, last_modified, etag) VALUES(?, ?, '2026-06-01T00:00:00Z', ?)", rows
    )


def test_partition_diff_queues_missing_and_size_mismatch_but_reports_dst_only(
    tmp_path: Path,
) -> None:
    conn = mrb.db_connect(tmp_path / "state.db")
    try:
        mrb.prepare_partition_tables(conn, "00")
        _insert(conn, "src", [("missing", 10, "m"), ("same", 20, "s"), ("changed", 30, "c")])
        _insert(conn, "dst", [("same", 20, "s"), ("changed", 31, "c"), ("extra", 99, "x")])

        summary = mrb.compute_partition_diff(conn, "00")

        assert summary.missing_count == 1
        assert summary.size_mismatch_count == 1
        assert summary.dst_only_count == 1
        rows = conn.execute(
            "SELECT key, action, src_size, dst_size "
            "FROM reconcile_queue ORDER BY key"
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            ("changed", "size_mismatch", 30, 31),
            ("missing", "missing", 10, None),
        ]
        # dst_only keys are kept for a re-check (not queued: nothing copies or deletes them)
        assert [tuple(r) for r in conn.execute("SELECT key, partition, dst_size FROM dst_only")] == [("extra", "00", 99)]
        mrb.prepare_partition_tables(conn, "00")  # the partition is diffed again: no stale rows
        mrb.compute_partition_diff(conn, "00")
        assert conn.execute("SELECT count(*) FROM dst_only").fetchone()[0] == 0
    finally:
        conn.close()


def test_verify_queues_same_size_with_another_single_part_etag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Same size, other content: caught by the ETag when it is an MD5 on both sides (no '-', as in s3gw/verifykeys.py).
    src = _S3({"00/same": (5, "aaa"), "00/changed": (5, "bbb"), "00/multipart": (5, "ccc-2")})
    dst = _S3({"00/same": (5, "aaa"), "00/changed": (5, "ddd"), "00/multipart": (5, "eee")})
    monkeypatch.setattr(mrb, "get_thread_s3_client", lambda side, _creds, _timeout: src if side == "src" else dst)
    db_path = tmp_path / "state.db"
    args = mrb.build_parser().parse_args(
        ["--src-alias", "a", "--src-bucket", "s", "--dst-alias", "b", "--dst-bucket", "d", "--state-db", str(db_path),
         "--prefix-len", "2", "--only-prefixes", "00", "--list-workers", "1"]
    )

    assert mrb.run_verify(args, object(), object(), ("http://src", "ak", "sk"), ("http://dst", "ak", "sk")) == 2

    conn = sqlite3.connect(str(db_path))
    assert conn.execute("SELECT key, action, src_size, dst_size FROM reconcile_queue").fetchall() == [
        ("00/changed", "etag_mismatch", 5, 5)
    ]
    conn.close()
    report = json.loads((tmp_path / "reconcile_verify_report.json").read_text(encoding="utf-8"))
    assert (report["size_mismatch_count"], report["overall_ok"]) == (1, False)


@pytest.mark.parametrize(
    "prefix_len, gaps, outside_hex",
    [
        (1, 3, {"!x", "AB12", "zz", "ü", "\U0001f600"}),
        (2, 33, {"!x", "0", "0/x", "0:x", "0zz", "3g", "5Z", "9z", "AB12", "a", "a/b", "fz", "zz", "ü", "\U0001f600"}),
    ],
)
def test_catch_all_lists_only_the_gaps_between_hex_partitions(prefix_len: int, gaps: int, outside_hex: set[str]) -> None:
    uuid_like = [f"{i:02x}{j}/{i * 8 + j:08x}" for i in range(256) for j in range(8)]
    odd = ["!x", "0", "0/x", "0:x", "0zz", "3g", "5Z", "9z", "AB12", "a", "a/b", "fz", "zz", "ü", "\U0001f600"]
    bucket = _S3({k: (1, "e") for k in uuid_like + odd})

    def listed(partition: str) -> list[str]:
        return [o.key for page in mrb.stream_list_objects(bucket, "b", partition, prefix_len, None, page_size=10) for o in page]

    catch_all = listed(mrb.CATCH_ALL_PREFIX)
    # One request per gap between the hex partitions, not the whole bucket again (2,063 keys = 207 pages).
    assert bucket.requests == gaps
    assert set(catch_all) == outside_hex
    partitions = [a + b for a in HEX for b in HEX] if prefix_len == 2 else list(HEX)
    every = catch_all + [k for p in partitions for k in listed(p)]
    assert sorted(every) == sorted(bucket.objects)  # each key in exactly one partition


def test_stream_list_objects_uses_configured_page_size() -> None:
    class _Paginator:
        def __init__(self) -> None:
            self.kwargs: dict[str, Any] = {}

        def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
            self.kwargs = kwargs
            return [{"Contents": []}]

    class _Client:
        def __init__(self) -> None:
            self.paginator = _Paginator()

        def get_paginator(self, _name: str) -> _Paginator:
            return self.paginator

    client = _Client()
    objects = list(
        mrb.stream_list_objects(
            client=client,
            bucket="bucket",
            partition="00",
            prefix_len=2,
            watermark=None,
            page_size=100,
            log_every_pages=0,
            progress_label="src",
        )
    )

    assert objects == []
    assert client.paginator.kwargs["PaginationConfig"]["PageSize"] == 100
    assert client.paginator.kwargs["Prefix"] == "00"


def test_scan_partitions_marks_listing_side_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_stream_insert(
        _db_path: Path,
        _client: object,
        _bucket: str,
        _partition: str,
        _prefix_len: int,
        _watermark: object,
        _page_size: int,
        _log_every_pages: int,
        _label: str,
        side: str,
    ) -> int:
        if side == "src":
            raise RuntimeError("access denied")
        return 0

    monkeypatch.setattr(mrb, "_stream_list_and_insert", fake_stream_insert)
    db_path = tmp_path / "state.db"

    scanned, queued = mrb.scan_partitions(
        db_path=db_path,
        prefixes=["00"],
        src_bucket="src-bucket",
        dst_bucket="dst-bucket",
        src_client=object(),
        dst_client=object(),
        prefix_len=2,
        watermark=None,
        progress_file=None,
        list_workers=2,
    )

    conn = mrb.db_connect(db_path)
    try:
        row = conn.execute(
            "SELECT status, last_error FROM partition_progress WHERE prefix='00'"
        ).fetchone()
    finally:
        conn.close()
    assert (scanned, queued) == (0, 0)
    assert row["status"] == "error"
    assert row["last_error"] == "src listing failed: access denied"


def test_scan_partitions_records_errors_with_single_list_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_stream_insert(
        _db_path: Path,
        _client: object,
        _bucket: str,
        _partition: str,
        _prefix_len: int,
        _watermark: object,
        _page_size: int,
        _log_every_pages: int,
        _label: str,
        side: str,
    ) -> int:
        if side == "src":
            raise RuntimeError("read timeout")
        return 0

    monkeypatch.setattr(mrb, "_stream_list_and_insert", fake_stream_insert)
    db_path = tmp_path / "state.db"

    scanned, queued = mrb.scan_partitions(
        db_path=db_path,
        prefixes=["00"],
        src_bucket="src-bucket",
        dst_bucket="dst-bucket",
        src_client=object(),
        dst_client=object(),
        prefix_len=2,
        watermark=None,
        progress_file=None,
        list_workers=1,
    )

    conn = mrb.db_connect(db_path)
    try:
        row = conn.execute(
            "SELECT status, last_error FROM partition_progress WHERE prefix='00'"
        ).fetchone()
    finally:
        conn.close()
    assert (scanned, queued) == (0, 0)
    assert row["status"] == "error"
    assert row["last_error"] == "src listing failed: read timeout"


def test_scan_partitions_uses_side_specific_list_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = {"src": 0, "dst": 0}
    max_active = {"src": 0, "dst": 0}
    calls: list[tuple[str, str, str, int]] = []
    lock = threading.Lock()

    def fake_get_thread_s3_client(side: str, _creds: object, timeout: int) -> str:
        return f"{side}-timeout-{timeout}"

    def fake_stream_insert(
        _db_path: Path,
        client: str,
        _bucket: str,
        partition: str,
        _prefix_len: int,
        _watermark: object,
        page_size: int,
        _log_every_pages: int,
        _label: str,
        side: str,
    ) -> int:
        with lock:
            active[side] += 1
            max_active[side] = max(max_active[side], active[side])
            calls.append((side, partition, client, page_size))
        time.sleep(0.02)
        with lock:
            active[side] -= 1
        return 0

    monkeypatch.setattr(mrb, "get_thread_s3_client", fake_get_thread_s3_client)
    monkeypatch.setattr(mrb, "_stream_list_and_insert", fake_stream_insert)
    db_path = tmp_path / "state.db"

    scanned, queued = mrb.scan_partitions(
        db_path=db_path,
        prefixes=["00", "01"],
        src_bucket="src-bucket",
        dst_bucket="dst-bucket",
        src_client=object(),
        dst_client=object(),
        prefix_len=2,
        watermark=None,
        progress_file=None,
        list_workers=2,
        src_list_workers=1,
        dst_list_workers=2,
        src_list_page_size=100,
        dst_list_page_size=1000,
        src_s3_read_timeout=900,
        dst_s3_read_timeout=300,
        src_creds=("http://src", "ak", "sk"),
        dst_creds=("http://dst", "ak", "sk"),
    )

    assert (scanned, queued) == (2, 0)
    assert max_active["src"] == 1
    assert max_active["dst"] == 2
    assert ("src", "00", "src-timeout-900", 100) in calls
    assert ("src", "01", "src-timeout-900", 100) in calls
    assert ("dst", "00", "dst-timeout-300", 1000) in calls
    assert ("dst", "01", "dst-timeout-300", 1000) in calls


def test_filter_prefixes_supports_start_stop_and_only_lists() -> None:
    prefixes = ["00", "01", "02", "03", "04", mrb.CATCH_ALL_PREFIX]

    assert mrb.filter_prefixes_for_run(prefixes, start_prefix="02") == [
        "02",
        "03",
        "04",
        mrb.CATCH_ALL_PREFIX,
    ]
    assert mrb.filter_prefixes_for_run(prefixes, stop_prefix="02") == ["00", "01", "02"]
    assert mrb.filter_prefixes_for_run(prefixes, only_prefixes="03,01") == ["01", "03"]
    assert mrb.filter_prefixes_for_run(
        prefixes,
        start_prefix="02",
        stop_prefix="04",
        only_prefixes="01,02,04",
    ) == ["02", "04"]


def test_catch_all_keeps_uppercase_hex_keys() -> None:
    # Prefix listing is case-sensitive: "AB..." is not returned by Prefix="ab",
    # so the catch-all partition must not treat it as covered.
    assert mrb._is_covered_by_hex_prefix("ab12", 2)
    assert not mrb._is_covered_by_hex_prefix("AB12", 2)
    assert not mrb._is_covered_by_hex_prefix("Ab12", 2)
    assert not mrb._is_covered_by_hex_prefix("zz12", 2)


def test_scan_marks_partition_done_and_drops_its_listings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    listed: list[tuple[str, str]] = []

    def fake_stream_insert(_db_path: Path, _client: str, _bucket: str, partition: str, *args: object) -> int:
        listed.append((args[-1], partition))  # type: ignore[arg-type]
        return 0

    monkeypatch.setattr(mrb, "get_thread_s3_client", lambda side, _creds, timeout: side)
    monkeypatch.setattr(mrb, "_stream_list_and_insert", fake_stream_insert)
    db_path = tmp_path / "state.db"

    mrb.scan_partitions(
        db_path=db_path, prefixes=["00"], src_bucket="s", dst_bucket="d", src_client=object(),
        dst_client=object(), prefix_len=2, watermark=None, progress_file=None,
        list_workers=1, src_list_workers=1, dst_list_workers=1, src_list_page_size=100,
        dst_list_page_size=100, src_s3_read_timeout=1, dst_s3_read_timeout=1,
        src_creds=("http://src", "ak", "sk"), dst_creds=("http://dst", "ak", "sk"),
    )

    assert sorted(listed) == [("dst", "00"), ("src", "00")]
    conn = sqlite3.connect(str(db_path))
    assert conn.execute("SELECT status FROM partition_progress WHERE prefix='00'").fetchone()[0] == "done"
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert not {t for t in tables if t.startswith(("src_partition_", "dst_partition_"))}


def test_watermark_filters_source_listing_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    old = "2026-06-01T00:00:00Z"
    fresh = "2026-06-02T00:00:00Z"
    # The fresh key was copied to dst after the watermark: it must not be reported missing.
    listings = {
        "src": [("00/old", old), ("00/fresh", fresh)],
        "dst": [("00/old", old), ("00/fresh", fresh)],
    }

    class _Paginator:
        def __init__(self, side: str) -> None:
            self.side = side

        def paginate(self, **_kwargs: Any) -> list[dict[str, Any]]:
            return [{"Contents": [{"Key": k, "Size": 1, "LastModified": lm} for k, lm in listings[self.side]]}]

    class _Client:
        def __init__(self, side: str) -> None:
            self.side = side

        def get_paginator(self, _name: str) -> _Paginator:
            return _Paginator(self.side)

    monkeypatch.setattr(mrb, "get_thread_s3_client", lambda side, _creds, _timeout: _Client(side))
    db_path = tmp_path / "state.db"
    mrb.scan_partitions(
        db_path=db_path, prefixes=["00"], src_bucket="s", dst_bucket="d", src_client=object(),
        dst_client=object(), prefix_len=2, watermark=mrb.parse_utc("2026-06-01T12:00:00Z"),
        progress_file=None, src_creds=("http://src", "ak", "sk"), dst_creds=("http://dst", "ak", "sk"),
    )

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT src_count, dst_count, missing_count, dst_only_count FROM partition_progress WHERE prefix='00'"
    ).fetchone()
    conn.close()
    # src skips the fresh key; dst keeps it (dst-only is only reported, nothing is copied).
    assert row == (1, 2, 0, 1)
