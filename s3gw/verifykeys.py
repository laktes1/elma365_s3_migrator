"""Compare source and destination for the keys the journal touched since a moment.

Closes the gap between the last full reconcile and the cutover window, and checks
the reverse journal before a rollback (PIPELINE.md, stage C). Short list, no listing:

    python -m s3gw.verifykeys --journal /var/lib/s3gw/journal.db --since 2026-11-20T18:00:00Z
    python -m s3gw.verifykeys --keys-file reconcile-keys.txt [--mark-dirty]   # re-check a reconcile's discrepancies

SRC_* / DST_* as for the handler. A key matches when it is absent on both sides, or
present on both with the same size (and the same ETag when neither is multipart).
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from . import journal
from .handler import clients_from_env


def _head(cl: Any, bucket: str, key: str) -> Optional[Dict[str, Any]]:
    try:
        r = cl.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if str(exc.response.get("Error", {}).get("Code", "")) in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return {"size": int(r["ContentLength"]), "etag": str(r.get("ETag", "")).strip('"')}


def compare(key: str, src: Optional[Dict[str, Any]], dst: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """None when the key matches, else a mismatch record."""
    if src is None and dst is None:
        return None
    if src is None or dst is None:
        return {"key": key, "problem": "missing_in_dst" if dst is None else "extra_in_dst", "src": src, "dst": dst}
    if src["size"] != dst["size"]:
        return {"key": key, "problem": "size", "src": src, "dst": dst}
    if "-" not in src["etag"] and "-" not in dst["etag"] and src["etag"] != dst["etag"]:
        return {"key": key, "problem": "etag", "src": src, "dst": dst}
    return None


def read_keys(path: str) -> List[str]:
    """One key per line (a reconcile run's discrepancies, PIPELINE.md C1); empty lines are skipped."""
    with open(path, encoding="utf-8") as fh:
        return sorted({line.rstrip("\r\n") for line in fh if line.strip()})


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Compare source and destination for keys changed since --since or listed in a file")
    p.add_argument("--journal", default=os.environ.get("JOURNAL_DB", "journal.db"))
    which = p.add_mutually_exclusive_group(required=True)
    which.add_argument("--since", help="UTC, as in key_state.updated_at, e.g. 2026-11-20T18:00:00Z")
    which.add_argument("--keys-file", help="one key per line, e.g. the discrepancies of a reconcile run")
    p.add_argument("--mark-dirty", action="store_true",
                   help="hand the mismatched keys to the handler, which brings the destination to the source")
    p.add_argument("--src-bucket", default=os.environ.get("SRC_BUCKET", "s3elma365"))
    p.add_argument("--dst-bucket", default=os.environ.get("DST_BUCKET", "s3elma365"))
    p.add_argument("--workers", type=int, default=8)
    a = p.parse_args(argv)
    if a.keys_file:
        keys = read_keys(a.keys_file)
    else:
        since = journal.utc_iso(a.since)
        if since is None:
            p.error(f"--since is not an RFC 3339 UTC time: {a.since}")
        conn = sqlite3.connect(f"file:{a.journal}?mode=ro", uri=True)
        keys = [r[0] for r in conn.execute("SELECT key FROM key_state WHERE updated_at >= ? ORDER BY key", (since,))]
        conn.close()
    src, dst = clients_from_env()()
    if dst is None:
        raise SystemExit("DST_ENDPOINT is not set")

    def one(key: str) -> Optional[Dict[str, Any]]:
        try:
            return compare(key, _head(src, a.src_bucket, key), _head(dst, a.dst_bucket, key))
        except Exception as exc:  # unreadable is a mismatch, not a pass
            return {"key": key, "problem": "error", "detail": str(exc)}

    with cf.ThreadPoolExecutor(a.workers) as ex:
        bad = [r for r in ex.map(one, keys) if r is not None]
    out: Dict[str, Any] = {"checked": len(keys), "mismatched": len(bad), "mismatches": bad[:50]}
    if a.mark_dirty and bad:
        wconn = journal.connect(Path(a.journal))
        out["marked_dirty"] = journal.mark_dirty(wconn, sorted(r["key"] for r in bad))
        wconn.close()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
