#!/usr/bin/env python3
"""Stage 5: bucket-level reconcile, phase verify (PIPELINE.md, C1).

The script compares source and destination buckets by S3 listing data only:
key, size and ETag. Source keys that are missing in destination, have a
different size or a different single-part ETag go to reconcile_queue,
destination-only keys to dst_only. Nothing is copied or deleted: the keys are
re-checked one by one (s3gw.verifykeys --keys-file).
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import sqlite3
import sys
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from migrator import common
except ImportError:  # python3 migrator/reconcile_buckets.py
    import common  # type: ignore[no-redef]
from s3gw.bulk import common_prefix, pred  # noqa: E402  common puts the repo root on sys.path


SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS partition_progress (
  prefix                TEXT PRIMARY KEY,
  status                TEXT NOT NULL DEFAULT 'pending',
  started_at            TEXT,
  finished_at           TEXT,
  src_count             INTEGER NOT NULL DEFAULT 0,
  src_bytes             INTEGER NOT NULL DEFAULT 0,
  dst_count             INTEGER NOT NULL DEFAULT 0,
  dst_bytes             INTEGER NOT NULL DEFAULT 0,
  missing_count         INTEGER NOT NULL DEFAULT 0,
  missing_bytes         INTEGER NOT NULL DEFAULT 0,
  size_mismatch_count   INTEGER NOT NULL DEFAULT 0,
  size_mismatch_bytes   INTEGER NOT NULL DEFAULT 0,
  dst_only_count        INTEGER NOT NULL DEFAULT 0,
  dst_only_bytes        INTEGER NOT NULL DEFAULT 0,
  db_size_bytes         INTEGER NOT NULL DEFAULT 0,
  last_error            TEXT
);

-- Source keys to re-check (PIPELINE.md, C1): action is missing, size_mismatch or etag_mismatch.
CREATE TABLE IF NOT EXISTS reconcile_queue (
  key        TEXT PRIMARY KEY,
  partition  TEXT,
  action     TEXT NOT NULL,
  src_size   INTEGER NOT NULL,
  dst_size   INTEGER,
  queued_at  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_reconcile_queue_partition
  ON reconcile_queue(partition);

-- Keys in dst but not in src (newer than the watermark in src, or really extra): a report only,
-- nothing copies or deletes them. Kept so the keys can be re-checked (PIPELINE.md, C1).
CREATE TABLE IF NOT EXISTS dst_only (
  key        TEXT PRIMARY KEY,
  partition  TEXT,
  dst_size   INTEGER NOT NULL,
  seen_at    TEXT NOT NULL
);
"""

CATCH_ALL_PREFIX = "__catch_all__"
HEX = "0123456789abcdef"
Credentials = common.Credentials
load_mc_alias_credentials = common.load_mc_alias_credentials
_MAX_POOL_CONNECTIONS = 64

# Same content rule as s3gw/verifykeys.py: an ETag with '-' is a multipart one, not the MD5 of the object.
_MISMATCH = "t2.size != t1.size OR (instr(t1.etag, '-') = 0 AND instr(t2.etag, '-') = 0 AND t2.etag != t1.etag)"

_log_lock = threading.Lock()
_ERROR_LOG_FILE: Optional[Path] = None


@dataclass(frozen=True)
class ListedObject:
    key: str
    size: int
    last_modified: str
    etag: str


@dataclass(frozen=True)
class DiffSummary:
    partition: str
    src_count: int
    src_bytes: int
    dst_count: int
    dst_bytes: int
    missing_count: int
    missing_bytes: int
    size_mismatch_count: int
    size_mismatch_bytes: int
    dst_only_count: int
    dst_only_bytes: int


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def set_error_log_file(path_value: str) -> None:
    global _ERROR_LOG_FILE
    path_value = (path_value or "").strip()
    _ERROR_LOG_FILE = Path(path_value).resolve() if path_value else None
    if _ERROR_LOG_FILE is not None:
        _ERROR_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)


def _append_error_log(line: str) -> None:
    if _ERROR_LOG_FILE is None:
        return
    try:
        with _ERROR_LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


def log(message: str, is_error: bool = False) -> None:
    line = f"[{now_utc()}] {message}"
    with _log_lock:
        print(line, flush=True)
    if is_error:
        _append_error_log(line)


def log_error(message: str) -> None:
    log(message, is_error=True)


def get_thread_s3_client(name: str, creds: Credentials, read_timeout: int) -> Any:
    return common.get_thread_s3_client(name, creds, read_timeout, _MAX_POOL_CONNECTIONS)


def validate_bucket_access(client: Any, bucket: str) -> None:
    client.head_bucket(Bucket=bucket)


def db_connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=120)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA busy_timeout = 120000")
    conn.commit()
    return conn


def prepare_partition_tables(conn: sqlite3.Connection, suffix: str = "") -> None:
    t = "src_partition" if not suffix else f"src_partition_{suffix}"
    d = "dst_partition" if not suffix else f"dst_partition_{suffix}"
    conn.execute(f"DROP TABLE IF EXISTS {t}")
    conn.execute(f"DROP TABLE IF EXISTS {d}")
    conn.execute(
        f"CREATE TABLE {t}(key TEXT PRIMARY KEY, size INTEGER NOT NULL, last_modified TEXT NOT NULL, etag TEXT NOT NULL)"
    )
    conn.execute(
        f"CREATE TABLE {d}(key TEXT PRIMARY KEY, size INTEGER NOT NULL, last_modified TEXT NOT NULL, etag TEXT NOT NULL)"
    )
    conn.commit()


def compute_partition_diff(conn: sqlite3.Connection, partition: str) -> DiffSummary:
    s = f"src_partition_{partition}"
    d = f"dst_partition_{partition}"
    src_count, src_bytes = conn.execute(
        f"SELECT COUNT(*), COALESCE(SUM(size), 0) FROM {s}"
    ).fetchone()
    dst_count, dst_bytes = conn.execute(
        f"SELECT COUNT(*), COALESCE(SUM(size), 0) FROM {d}"
    ).fetchone()
    missing_count, missing_bytes = conn.execute(
        f"""
        SELECT COUNT(*), COALESCE(SUM(t1.size), 0)
        FROM {s} t1
        LEFT JOIN {d} t2 ON t2.key = t1.key
        WHERE t2.key IS NULL
        """
    ).fetchone()
    mismatch_count, mismatch_bytes = conn.execute(
        f"""
        SELECT COUNT(*), COALESCE(SUM(t1.size), 0)
        FROM {s} t1
        JOIN {d} t2 ON t2.key = t1.key
        WHERE {_MISMATCH}
        """
    ).fetchone()
    dst_only_count, dst_only_bytes = conn.execute(
        f"""
        SELECT COUNT(*), COALESCE(SUM(t2.size), 0)
        FROM {d} t2
        LEFT JOIN {s} t1 ON t1.key = t2.key
        WHERE t1.key IS NULL
        """
    ).fetchone()

    ts = now_utc()
    conn.execute(
        f"""
        INSERT OR REPLACE INTO reconcile_queue(key, partition, action, src_size, dst_size, queued_at)
        SELECT t1.key, ?, 'missing', t1.size, NULL, ?
        FROM {s} t1
        LEFT JOIN {d} t2 ON t2.key = t1.key
        WHERE t2.key IS NULL
        """,
        (partition, ts),
    )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO reconcile_queue(key, partition, action, src_size, dst_size, queued_at)
        SELECT t1.key, ?, CASE WHEN t2.size != t1.size THEN 'size_mismatch' ELSE 'etag_mismatch' END, t1.size, t2.size, ?
        FROM {s} t1
        JOIN {d} t2 ON t2.key = t1.key
        WHERE {_MISMATCH}
        """,
        (partition, ts),
    )
    conn.execute("DELETE FROM dst_only WHERE partition = ?", (partition,))
    conn.execute(
        f"""
        INSERT OR REPLACE INTO dst_only(key, partition, dst_size, seen_at)
        SELECT t2.key, ?, t2.size, ?
        FROM {d} t2
        LEFT JOIN {s} t1 ON t1.key = t2.key
        WHERE t1.key IS NULL
        """,
        (partition, ts),
    )
    conn.commit()

    return DiffSummary(
        partition=partition,
        src_count=int(src_count),
        src_bytes=int(src_bytes),
        dst_count=int(dst_count),
        dst_bytes=int(dst_bytes),
        missing_count=int(missing_count),
        missing_bytes=int(missing_bytes),
        size_mismatch_count=int(mismatch_count),
        size_mismatch_bytes=int(mismatch_bytes),
        dst_only_count=int(dst_only_count),
        dst_only_bytes=int(dst_only_bytes),
    )


def build_prefixes(prefix_len: int) -> List[str]:
    if prefix_len == 1:
        return list(HEX) + [CATCH_ALL_PREFIX]
    if prefix_len == 2:
        return [a + b for a in HEX for b in HEX] + [CATCH_ALL_PREFIX]
    raise ValueError("prefix_len must be 1 or 2")


def parse_prefix_list(value: str) -> List[str]:
    return [item.strip().lower() for item in value.split(",") if item.strip()]


def filter_prefixes_for_run(
    prefixes: Sequence[str],
    start_prefix: str = "",
    stop_prefix: str = "",
    only_prefixes: str = "",
) -> List[str]:
    start = start_prefix.strip().lower()
    stop = stop_prefix.strip().lower()
    only = set(parse_prefix_list(only_prefixes))
    result: List[str] = []
    for prefix in prefixes:
        p = prefix.lower()
        if only and p not in only:
            continue
        if p == CATCH_ALL_PREFIX:
            if stop and stop != CATCH_ALL_PREFIX:
                continue
            result.append(prefix)
            continue
        if start and p < start:
            continue
        if stop and p > stop:
            continue
        result.append(prefix)
    return result


def catch_all_ranges(prefix_len: int) -> List[Tuple[str, Optional[str]]]:
    """[lo, hi) key ranges between the hex partitions; hi None is the end of the bucket.

    Partition p holds the keys [p, p with its last character + 1) in S3 (UTF-8 byte) order,
    so for PREFIX_LEN=2 the gaps are ['', '00'), ['0:', '0a'), ['0g', '10'), ... ['fg', end).
    """
    ranges: List[Tuple[str, Optional[str]]] = []
    lo = ""
    for p in build_prefixes(prefix_len)[:-1]:
        if p != lo:
            ranges.append((lo, p))
        lo = p[:-1] + chr(ord(p[-1]) + 1)
    ranges.append((lo, None))
    return ranges


def _list_requests(partition: str, prefix_len: int) -> List[Tuple[Dict[str, str], Optional[bytes]]]:
    """list_objects_v2 arguments of a partition, each with the key (UTF-8) at which its listing stops."""
    if partition != CATCH_ALL_PREFIX:
        return [({"Prefix": partition}, None)]
    requests: List[Tuple[Dict[str, str], Optional[bytes]]] = []
    for lo, hi in catch_all_ranges(prefix_len):
        kwargs = {"StartAfter": pred(lo)} if lo else {}
        prefix = common_prefix(lo, hi)
        if prefix:
            kwargs["Prefix"] = prefix
        requests.append((kwargs, hi.encode("utf-8") if hi is not None else None))
    return requests


def _side_watermark(side: str, watermark: Optional[datetime]) -> Optional[datetime]:
    # Only the source is cut at the watermark. Filtering dst too would report objects
    # copied after the watermark (e.g. by the handler) as missing.
    return watermark if side == "src" else None


def _is_covered_by_hex_prefix(key: str, prefix_len: int) -> bool:
    if len(key) < prefix_len:
        return False
    pfx = key[:prefix_len]  # case-sensitive: Prefix listing is case-sensitive
    return len(pfx) == prefix_len and all(ch in HEX for ch in pfx)


def stream_list_objects(
    client: Any,
    bucket: str,
    partition: str,
    prefix_len: int,
    watermark: Optional[datetime],
    page_size: int = 1000,
    log_every_pages: int = 0,
    progress_label: str = "",
) -> Iterable[List[ListedObject]]:
    paginator = client.get_paginator("list_objects_v2")
    pages = 0
    total_yielded = 0
    for list_kwargs, stop_at in _list_requests(partition, prefix_len):
        for page in paginator.paginate(Bucket=bucket, PaginationConfig={"PageSize": page_size}, **list_kwargs):
            pages += 1
            batch: List[ListedObject] = []
            past_range = False
            for obj in page.get("Contents", []):
                key = str(obj["Key"])
                if stop_at is not None and key.encode("utf-8") >= stop_at:
                    past_range = True
                    break
                if partition == CATCH_ALL_PREFIX and _is_covered_by_hex_prefix(key, prefix_len):
                    continue
                lm = obj.get("LastModified")
                if isinstance(lm, datetime):
                    lm_dt = lm.astimezone(timezone.utc)
                else:
                    lm_dt = parse_utc(str(lm)) if lm else datetime.fromtimestamp(0, timezone.utc)
                if watermark is not None and lm_dt > watermark:
                    continue
                batch.append(
                    ListedObject(
                        key=key,
                        size=int(obj.get("Size", 0)),
                        last_modified=format_utc(lm_dt),
                        etag=str(obj.get("ETag", "")).strip('"'),
                    )
                )
            if batch:
                total_yielded += len(batch)
                yield batch
            if log_every_pages > 0 and pages % log_every_pages == 0:
                label = f"{progress_label} " if progress_label else ""
                log(f"[list] {label}partition={partition} pages={pages:,} objects={total_yielded:,}")
            if past_range:
                break


def _stream_list_and_insert(
    db_path: Path,
    client: Any,
    bucket: str,
    partition: str,
    prefix_len: int,
    watermark: Optional[datetime],
    page_size: int,
    log_every_pages: int,
    label: str,
    side: str,
) -> int:
    conn = db_connect(db_path)
    table = "src_partition" if side == "src" else "dst_partition"
    if partition:
        table = f"{table}_{partition}"
    total = 0
    try:
        for page in stream_list_objects(client, bucket, partition, prefix_len, watermark, page_size, log_every_pages, label):
            batch = [(obj.key, obj.size, obj.last_modified, obj.etag) for obj in page]
            conn.executemany(
                f"INSERT OR REPLACE INTO {table}(key, size, last_modified, etag) VALUES(?,?,?,?)",
                batch,
            )
            conn.commit()
            total += len(batch)
    finally:
        conn.close()
    return total


def upsert_partition_summary(conn: sqlite3.Connection, summary: DiffSummary, status: str) -> None:
    conn.execute(
        """
        INSERT INTO partition_progress (
          prefix, status, started_at, finished_at,
          src_count, src_bytes, dst_count, dst_bytes,
          missing_count, missing_bytes, size_mismatch_count, size_mismatch_bytes,
          dst_only_count, dst_only_bytes, db_size_bytes
        ) VALUES (?, ?, COALESCE((SELECT started_at FROM partition_progress WHERE prefix=?), ?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(prefix) DO UPDATE SET
          status=excluded.status,
          finished_at=excluded.finished_at,
          src_count=excluded.src_count,
          src_bytes=excluded.src_bytes,
          dst_count=excluded.dst_count,
          dst_bytes=excluded.dst_bytes,
          missing_count=excluded.missing_count,
          missing_bytes=excluded.missing_bytes,
          size_mismatch_count=excluded.size_mismatch_count,
          size_mismatch_bytes=excluded.size_mismatch_bytes,
          dst_only_count=excluded.dst_only_count,
          dst_only_bytes=excluded.dst_only_bytes,
          db_size_bytes=excluded.db_size_bytes,
          last_error=NULL
        """,
        (
            summary.partition,
            status,
            summary.partition,
            now_utc(),
            now_utc(),
            summary.src_count,
            summary.src_bytes,
            summary.dst_count,
            summary.dst_bytes,
            summary.missing_count,
            summary.missing_bytes,
            summary.size_mismatch_count,
            summary.size_mismatch_bytes,
            summary.dst_only_count,
            summary.dst_only_bytes,
            _db_size_bytes(conn),
        ),
    )
    conn.commit()


def _db_size_bytes(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA database_list").fetchone()
    if not row:
        return 0
    path = row[2]
    if not path:
        return 0
    try:
        main_size = Path(path).stat().st_size
        wal_size = Path(path + "-wal").stat().st_size if Path(path + "-wal").exists() else 0
        return main_size + wal_size
    except OSError:
        return 0


def aggregate_report(conn: sqlite3.Connection) -> Dict[str, Any]:
    partition = conn.execute(
        """
        SELECT
          COUNT(*) AS partitions,
          SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS partitions_done,
          COALESCE(SUM(src_count), 0) AS src_count,
          COALESCE(SUM(src_bytes), 0) AS src_bytes,
          COALESCE(SUM(dst_count), 0) AS dst_count,
          COALESCE(SUM(dst_bytes), 0) AS dst_bytes,
          COALESCE(SUM(missing_count), 0) AS missing_count,
          COALESCE(SUM(missing_bytes), 0) AS missing_bytes,
          COALESCE(SUM(size_mismatch_count), 0) AS size_mismatch_count,
          COALESCE(SUM(size_mismatch_bytes), 0) AS size_mismatch_bytes,
          COALESCE(SUM(dst_only_count), 0) AS dst_only_count,
          COALESCE(SUM(dst_only_bytes), 0) AS dst_only_bytes,
          COALESCE(MAX(db_size_bytes), 0) AS max_db_size_bytes,
          SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS partition_errors
        FROM partition_progress
        """
    ).fetchone()
    return {
        "generated_at": now_utc(),
        "partitions": int(partition["partitions"] or 0),
        "partitions_done": int(partition["partitions_done"] or 0),
        "src_count": int(partition["src_count"] or 0),
        "src_bytes": int(partition["src_bytes"] or 0),
        "dst_count": int(partition["dst_count"] or 0),
        "dst_bytes": int(partition["dst_bytes"] or 0),
        "missing_count": int(partition["missing_count"] or 0),
        "missing_bytes": int(partition["missing_bytes"] or 0),
        "size_mismatch_count": int(partition["size_mismatch_count"] or 0),
        "size_mismatch_bytes": int(partition["size_mismatch_bytes"] or 0),
        "dst_only_count": int(partition["dst_only_count"] or 0),
        "dst_only_bytes": int(partition["dst_only_bytes"] or 0),
        "max_db_size_bytes": int(partition["max_db_size_bytes"] or 0),
        "partition_errors": int(partition["partition_errors"] or 0),
    }


def write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def append_progress(path: Optional[Path], obj: Dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _future_result_with_side(fut: cf.Future, side: str) -> Any:
    try:
        return fut.result()
    except Exception as exc:
        raise RuntimeError(f"{side} listing failed: {exc}") from exc


def scan_partitions(
    db_path: Path,
    prefixes: Sequence[str],
    src_bucket: str,
    dst_bucket: str,
    src_client: Any,
    dst_client: Any,
    prefix_len: int,
    watermark: Optional[datetime],
    progress_file: Optional[Path],
    list_workers: int = 1,
    src_list_workers: Optional[int] = None,
    dst_list_workers: Optional[int] = None,
    src_creds: Optional[Credentials] = None,
    dst_creds: Optional[Credentials] = None,
    s3_read_timeout: int = 300,
    src_s3_read_timeout: Optional[int] = None,
    dst_s3_read_timeout: Optional[int] = None,
    list_page_size: int = 1000,
    src_list_page_size: Optional[int] = None,
    dst_list_page_size: Optional[int] = None,
    list_log_every_pages: int = 0,
) -> Tuple[int, int]:
    conn = db_connect(db_path)
    scanned = 0
    queued = 0
    src_workers = max(1, src_list_workers or list_workers)
    dst_workers = max(1, dst_list_workers or list_workers)
    src_timeout = src_s3_read_timeout or s3_read_timeout
    dst_timeout = dst_s3_read_timeout or s3_read_timeout
    src_page_size = src_list_page_size or list_page_size
    dst_page_size = dst_list_page_size or list_page_size
    src_sem = threading.Semaphore(src_workers)
    dst_sem = threading.Semaphore(dst_workers)

    def _list_side(side: str, prefix_value: str) -> int:
        sem = src_sem if side == "src" else dst_sem
        creds = src_creds if side == "src" else dst_creds
        timeout = src_timeout if side == "src" else dst_timeout
        bucket = src_bucket if side == "src" else dst_bucket
        page_size = src_page_size if side == "src" else dst_page_size
        with sem:
            client = get_thread_s3_client(side, creds, timeout) if creds else (src_client if side == "src" else dst_client)
            return _stream_list_and_insert(
                db_path,
                client,
                bucket,
                prefix_value,
                prefix_len,
                _side_watermark(side, watermark),
                page_size,
                list_log_every_pages,
                side,
                side,
            )

    def _process_one_partition(prefix_value: str) -> DiffSummary:
        thread_conn = db_connect(db_path)
        try:
            prepare_partition_tables(thread_conn, prefix_value)
            with cf.ThreadPoolExecutor(max_workers=2) as ex:
                src_fut = ex.submit(_list_side, "src", prefix_value)
                dst_fut = ex.submit(_list_side, "dst", prefix_value)
                _future_result_with_side(src_fut, "src")
                _future_result_with_side(dst_fut, "dst")
            summary_value = compute_partition_diff(thread_conn, prefix_value)
            upsert_partition_summary(thread_conn, summary_value, "done")
            # The listings are needed only for the diff; keeping them stores both bucket indexes on disk.
            thread_conn.execute(f"DROP TABLE IF EXISTS src_partition_{prefix_value}")
            thread_conn.execute(f"DROP TABLE IF EXISTS dst_partition_{prefix_value}")
            thread_conn.commit()
            append_progress(progress_file, {"event": "partition_done", **asdict(summary_value)})
            log(
                f"[scan] partition={prefix_value} src={summary_value.src_count:,} dst={summary_value.dst_count:,} "
                f"missing={summary_value.missing_count:,} mismatch={summary_value.size_mismatch_count:,} "
                f"dst_only={summary_value.dst_only_count:,}"
            )
        finally:
            thread_conn.close()
        return summary_value

    try:
        for prefix in prefixes:
            conn.execute(
                """
                INSERT INTO partition_progress(prefix, status, started_at)
                VALUES(?, 'pending', ?)
                ON CONFLICT(prefix) DO NOTHING
                """,
                (prefix, now_utc()),
            )
        conn.commit()

        if list_workers <= 1:
            for prefix in prefixes:
                log(f"[scan] partition={prefix}")
                conn.execute(
                    """
                    UPDATE partition_progress
                    SET status='running', started_at=?, last_error=NULL
                    WHERE prefix=?
                    """,
                    (now_utc(), prefix),
                )
                conn.commit()
                try:
                    summary = _process_one_partition(prefix)
                    queued += summary.missing_count + summary.size_mismatch_count
                    scanned += 1
                except Exception as exc:
                    conn.execute(
                        """
                        UPDATE partition_progress
                        SET status='error', finished_at=?, last_error=?
                        WHERE prefix=?
                        """,
                        (now_utc(), str(exc)[:2000], prefix),
                    )
                    conn.commit()
                    log_error(f"[scan] partition={prefix} failed: {exc}")
        else:
            with cf.ThreadPoolExecutor(max_workers=list_workers) as pool:
                futures: Dict[cf.Future, str] = {}
                for prefix in prefixes:
                    log(f"[scan] partition={prefix} queued")
                    conn.execute(
                        """
                        UPDATE partition_progress
                        SET status='running', started_at=?, last_error=NULL
                        WHERE prefix=?
                        """,
                        (now_utc(), prefix),
                    )
                    futures[pool.submit(_process_one_partition, prefix)] = prefix
                conn.commit()
                for fut in cf.as_completed(futures):
                    prefix = futures[fut]
                    try:
                        summary = fut.result()
                        queued += summary.missing_count + summary.size_mismatch_count
                        scanned += 1
                    except Exception as exc:
                        conn.execute(
                            """
                            UPDATE partition_progress
                            SET status='error', finished_at=?, last_error=?
                            WHERE prefix=?
                            """,
                            (now_utc(), str(exc)[:2000], prefix),
                        )
                        conn.commit()
                        log_error(f"[scan] partition={prefix} failed: {exc}")
    finally:
        conn.close()
    return scanned, queued


def load_credentials_or_die(args: argparse.Namespace) -> Tuple[Credentials, Credentials]:
    src_creds = load_mc_alias_credentials(args.mc_bin, args.src_alias)
    dst_creds = load_mc_alias_credentials(args.mc_bin, args.dst_alias)
    if src_creds is None:
        raise RuntimeError(f"cannot read mc alias credentials: {args.src_alias}")
    if dst_creds is None:
        raise RuntimeError(f"cannot read mc alias credentials: {args.dst_alias}")
    return src_creds, dst_creds


def run_verify(
    args: argparse.Namespace,
    src_client: Any,
    dst_client: Any,
    src_creds: Credentials,
    dst_creds: Credentials,
) -> int:
    db_path = Path(args.state_db).resolve()
    conn = db_connect(db_path)
    try:
        conn.execute("DELETE FROM partition_progress")
        conn.execute("DELETE FROM reconcile_queue")
        conn.execute("DELETE FROM dst_only")
        conn.commit()
    finally:
        conn.close()
    prefixes = filter_prefixes_for_run(
        build_prefixes(args.prefix_len),
        start_prefix=args.start_prefix,
        stop_prefix=args.stop_prefix,
        only_prefixes=args.only_prefixes,
    )
    watermark = parse_utc(args.watermark) if args.watermark else datetime.now(timezone.utc) - timedelta(minutes=args.watermark_delay_minutes)
    scan_partitions(
        db_path=db_path,
        prefixes=prefixes,
        src_bucket=args.src_bucket,
        dst_bucket=args.dst_bucket,
        src_client=src_client,
        dst_client=dst_client,
        prefix_len=args.prefix_len,
        watermark=watermark,
        progress_file=Path(args.progress_file).resolve() if args.progress_file else None,
        list_workers=args.list_workers,
        src_list_workers=args.src_list_workers,
        dst_list_workers=args.dst_list_workers,
        src_creds=src_creds,
        dst_creds=dst_creds,
        s3_read_timeout=args.s3_read_timeout,
        src_s3_read_timeout=args.src_s3_read_timeout,
        dst_s3_read_timeout=args.dst_s3_read_timeout,
        list_page_size=args.list_page_size,
        src_list_page_size=args.src_list_page_size,
        dst_list_page_size=args.dst_list_page_size,
        list_log_every_pages=args.list_log_every_pages,
    )
    conn = db_connect(db_path)
    report = aggregate_report(conn)
    conn.close()
    report.update({"phase": "verify", "watermark": format_utc(watermark)})
    report["overall_ok"] = (
        report["missing_count"] == 0
        and report["size_mismatch_count"] == 0
        and report["partition_errors"] == 0
    )
    report_file = Path(args.report_file).resolve() if args.report_file else db_path.with_name("reconcile_verify_report.json")
    write_json_atomic(report_file, report)
    log(
        f"[verify] overall_ok={report['overall_ok']} "
        f"missing={report['missing_count']:,} mismatch={report['size_mismatch_count']:,} "
        f"dst_only={report['dst_only_count']:,} report={report_file}"
    )
    return 0 if report["overall_ok"] else 2


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Stage 5: compare source and destination buckets by listing (key, size, ETag). Copies nothing.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--phase", choices=["verify"], default="verify")
    p.add_argument("--src-alias", required=True)
    p.add_argument("--src-bucket", required=True)
    p.add_argument("--dst-alias", required=True)
    p.add_argument("--dst-bucket", required=True)
    p.add_argument("--state-db", required=True)
    p.add_argument("--report-file", default="")
    p.add_argument("--progress-file", default="")
    p.add_argument("--error-log-file", default="")
    p.add_argument("--mc-bin", default="mc")
    p.add_argument("--prefix-len", type=int, choices=[1, 2], default=1)
    p.add_argument("--list-workers", type=int, default=16)
    p.add_argument("--src-list-workers", type=int, default=0, help="Concurrent source listing operations. 0 inherits --list-workers.")
    p.add_argument("--dst-list-workers", type=int, default=0, help="Concurrent destination listing operations. 0 inherits --list-workers.")
    p.add_argument("--list-page-size", type=int, default=1000)
    p.add_argument("--src-list-page-size", type=int, default=0, help="Source list_objects page size. 0 inherits --list-page-size.")
    p.add_argument("--dst-list-page-size", type=int, default=0, help="Destination list_objects page size. 0 inherits --list-page-size.")
    p.add_argument("--list-log-every-pages", type=int, default=100)
    p.add_argument("--s3-read-timeout", type=int, default=300)
    p.add_argument("--src-s3-read-timeout", type=int, default=0, help="Source S3 read timeout. 0 inherits --s3-read-timeout.")
    p.add_argument("--dst-s3-read-timeout", type=int, default=0, help="Destination S3 read timeout. 0 inherits --s3-read-timeout.")
    p.add_argument("--watermark", default="", help="UTC ISO timestamp. Default: now - watermark delay.")
    p.add_argument("--watermark-delay-minutes", type=int, default=10)
    p.add_argument("--start-prefix", default="", help="Start verify from this partition prefix, inclusive.")
    p.add_argument("--stop-prefix", default="", help="Stop verify at this partition prefix, inclusive.")
    p.add_argument("--only-prefixes", default="", help="Comma-separated partition allow-list for verify.")
    return p


def validate_args(args: argparse.Namespace) -> None:
    if args.list_workers <= 0:
        raise RuntimeError("--list-workers must be > 0")
    if args.src_list_workers < 0 or args.dst_list_workers < 0:
        raise RuntimeError("--src-list-workers/--dst-list-workers must be >= 0")
    if args.list_page_size <= 0 or args.list_page_size > 1000:
        raise RuntimeError("--list-page-size must be between 1 and 1000")
    if args.src_list_page_size < 0 or args.dst_list_page_size < 0:
        raise RuntimeError("--src-list-page-size/--dst-list-page-size must be >= 0")
    if args.src_list_page_size > 1000 or args.dst_list_page_size > 1000:
        raise RuntimeError("--src-list-page-size/--dst-list-page-size must be <= 1000")
    if args.list_log_every_pages < 0:
        raise RuntimeError("--list-log-every-pages must be >= 0")
    if args.s3_read_timeout <= 0:
        raise RuntimeError("--s3-read-timeout must be > 0")
    if args.src_s3_read_timeout < 0 or args.dst_s3_read_timeout < 0:
        raise RuntimeError("--src-s3-read-timeout/--dst-s3-read-timeout must be >= 0")


def main() -> int:
    args = build_parser().parse_args()
    try:
        validate_args(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    set_error_log_file(args.error_log_file)
    log(f"Stage 5 reconcile started phase={args.phase}")
    log(f"  src: {args.src_alias}/{args.src_bucket}")
    log(f"  dst: {args.dst_alias}/{args.dst_bucket}")
    log(f"  state_db: {args.state_db}")
    log(
        "  listing_tuning: "
        f"src_workers={args.src_list_workers or args.list_workers} "
        f"dst_workers={args.dst_list_workers or args.list_workers} "
        f"src_page_size={args.src_list_page_size or args.list_page_size} "
        f"dst_page_size={args.dst_list_page_size or args.list_page_size} "
        f"src_timeout={args.src_s3_read_timeout or args.s3_read_timeout} "
        f"dst_timeout={args.dst_s3_read_timeout or args.s3_read_timeout}"
    )
    if args.start_prefix or args.stop_prefix or args.only_prefixes:
        log(
            "  prefix_filter: "
            f"start={args.start_prefix or '-'} "
            f"stop={args.stop_prefix or '-'} "
            f"only={args.only_prefixes or '-'}"
        )

    try:
        src_creds, dst_creds = load_credentials_or_die(args)
        src_client = common.make_s3_client(src_creds, args.s3_read_timeout, _MAX_POOL_CONNECTIONS)
        dst_client = common.make_s3_client(dst_creds, args.s3_read_timeout, _MAX_POOL_CONNECTIONS)
        validate_bucket_access(src_client, args.src_bucket)
        validate_bucket_access(dst_client, args.dst_bucket)
        return run_verify(args, src_client, dst_client, src_creds, dst_creds)
    except KeyboardInterrupt:
        log_error("Interrupted")
        return 130
    except Exception as exc:
        log_error(f"FATAL: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
