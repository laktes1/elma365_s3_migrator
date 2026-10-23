"""Coverage check: every key change MinIO saw (mc admin trace --json) came through the gateway and has an intent.

    mc admin trace --json --verbose --call s3 local > trace.jsonl     # --verbose: request headers
    python -m s3gw.tracecheck trace.jsonl --journal /var/lib/s3gw/journal.db

The gateway adds X-S3gw-Request-Id to every request it forwards and journals the
intents of that request under the same id. A successful change in the bucket
- without the header reached MinIO not through the gateway (bypass);
- with the header but without an intent of that request for its key is a gateway miss (missed).
Both must be zero. Client addresses are not used: MinIO reports the X-Forwarded-For
set by ingress, so a request through the gateway and a direct one show the same client.

Exit code: 0 - clean; 1 - bypass or missed found; 2 - not verifiable: the trace has
no changes in the bucket, or some changes have no request headers (mc admin trace
without --verbose).

After a rollback of the routing (changes went straight to MinIO), --mark-dirty hands
the keys changed past the gateway to the handler.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import urllib.parse
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from . import journal
from .gateway import REQUEST_ID_HEADER

READ_METHODS = {"GET", "HEAD", "OPTIONS"}
READ_CALL_PREFIXES = ("s3.Get", "s3.Head", "s3.List")  # trace without --verbose has no method
# Write calls that do not change a key by themselves; the gateway journals the Complete.
NO_KEY_CHANGE = {"s3.NewMultipartUpload", "s3.PutObjectPart", "s3.CopyObjectPart", "s3.AbortMultipartUpload"}
_MARK = REQUEST_ID_HEADER.lower()


def _lower(obj: Any) -> Any:
    """mc versions differ in key case (funcName/funcname); compare lower-cased."""
    if isinstance(obj, dict):
        return {str(k).lower(): _lower(v) for k, v in obj.items()}
    return obj


def parse_line(line: str, bucket: str) -> Optional[Dict[str, Any]]:
    """One trace record -> {func, method, client, key, rid, has_headers} for a successful change in `bucket`.

    mc 2025: {"api", "path", "request": {"method", "headers"}, "response": {"statusCode"}} with
    --verbose; without it only {"api", "path", "statusCode"} and no method or headers. Older mc
    nests request/response under "http" and names the call "funcName".
    """
    try:
        rec = _lower(json.loads(line))
    except ValueError:
        return None
    if not isinstance(rec, dict):
        return None
    http = rec.get("http") or {}
    req = rec.get("request") or http.get("request") or {}
    resp = rec.get("response") or http.get("response") or {}
    func = str(rec.get("api") or rec.get("funcname") or "")
    method = str(req.get("method", "")).upper()
    status = int(resp.get("statuscode") or rec.get("statuscode") or 0)
    if not 200 <= status < 300:
        return None
    if method in READ_METHODS or (not method and func.startswith(READ_CALL_PREFIXES)):
        return None
    path = str(rec.get("path") or req.get("path") or "")
    b, _, key = path.lstrip("/").partition("/")
    if urllib.parse.unquote(b) != bucket:
        return None
    headers = req.get("headers")
    rid = ""
    if isinstance(headers, dict):
        value = headers.get(_MARK)
        rid = str(value[0] if isinstance(value, list) and value else value or "")
    return {
        "func": func,
        "method": method,
        "client": str(rec.get("client") or req.get("client") or ""),
        "key": urllib.parse.unquote(key),
        "rid": rid,
        "has_headers": isinstance(headers, dict) and bool(headers),
    }


def check(lines: Iterable[str], intents: Dict[str, Set[str]], bucket: str) -> Dict[str, Any]:
    """intents: request_id -> keys journaled for that request."""
    funcs: Counter = Counter()
    bypass: List[Dict[str, Any]] = []
    missed: List[Dict[str, Any]] = []
    no_headers = 0
    for line in lines:
        ev = parse_line(line, bucket)
        if ev is None:
            continue
        funcs[ev["func"]] += 1
        if not ev.pop("has_headers"):
            no_headers += 1
            continue
        if not ev["rid"]:
            bypass.append(ev)
            continue
        if ev["func"] in NO_KEY_CHANGE:
            continue
        keys = intents.get(ev["rid"])
        # DeleteMultipleObjects has no key in the path (keys are in the body): its request must have intents.
        if keys is None or (ev["key"] and ev["key"] not in keys):
            missed.append(ev)
    return {
        "changes": sum(funcs.values()),
        "by_call": dict(funcs),
        "without_headers": no_headers,
        "bypass": bypass,
        "missed": missed,
    }


def load_intents(journal_path: str) -> Dict[str, Set[str]]:
    conn = sqlite3.connect(f"file:{journal_path}?mode=ro", uri=True)
    try:
        out: Dict[str, Set[str]] = {}
        for rid, key in conn.execute("SELECT request_id, key FROM intents"):
            out.setdefault(rid, set()).add(key)
        return out
    finally:
        conn.close()


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Compare mc admin trace with the gateway journal")
    p.add_argument("trace", help="output of mc admin trace --json")
    p.add_argument("--journal", default=os.environ.get("JOURNAL_DB", "journal.db"))
    p.add_argument("--bucket", default=os.environ.get("GW_BUCKET", "s3elma365"))
    p.add_argument("--mark-dirty", action="store_true",
                   help="hand the keys changed past the gateway to the handler (after a routing rollback)")
    a = p.parse_args(argv)
    intents = load_intents(a.journal)
    with open(a.trace, encoding="utf-8", errors="replace") as fh:
        rep = check(fh, intents, a.bucket)
    if a.mark_dirty:
        changed = sorted({e["key"] for e in rep["bypass"] if e["key"] and e["func"] not in NO_KEY_CHANGE})
        wconn = journal.connect(Path(a.journal))
        rep["marked_dirty"] = journal.mark_dirty(wconn, changed)
        wconn.close()
    print(json.dumps({**rep, "bypass": rep["bypass"][:20], "missed": rep["missed"][:20],
                      "bypass_total": len(rep["bypass"]), "missed_total": len(rep["missed"])},
                     ensure_ascii=False, indent=2))
    # An empty trace (mc died, wrong alias) or records without headers prove nothing about coverage.
    if not rep["changes"]:
        print("trace has no changes in the bucket (mc trace died or watched another alias?): nothing verified",
              file=sys.stderr)
        return 2
    if rep["without_headers"]:
        print(f"{rep['without_headers']} changes have no request headers (run mc admin trace with --verbose): "
              "not verified", file=sys.stderr)
        return 2
    return 1 if rep["bypass"] or rep["missed"] else 0


if __name__ == "__main__":
    sys.exit(main())
