"""Change handler: syncs dirty keys from the journal (docs/gateway-design.md, section 5)."""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sqlite3
import threading
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from . import journal
from .copykey import DELETED, CopyConfig, LeaseExpired, min_lease_seconds, sync_key, write_guard

log = logging.getLogger("s3gw.handler")


@dataclass
class HandlerConfig:
    journal_path: Path
    copy: CopyConfig
    workers: int = 4
    lease_seconds: float = 900.0
    lease_guard_seconds: float = 30.0
    backoff_seconds: float = 5.0
    max_attempts: int = 20
    idle_sleep: float = 0.5
    intent_retention_days: float = 14.0
    purge_every_seconds: float = 3600.0


def process_one(
    conn: sqlite3.Connection,
    lease: journal.Lease,
    src: Any,
    dst: Any,
    cfg: HandlerConfig,
    clock: Callable[[], float] = time.time,
) -> str:
    """Sync one leased key. Returns 'synced', 'deleted', 'changed', 'retry', 'error' or 'lost'."""

    def check_lease(nbytes: int) -> None:
        if journal.lease_remaining(lease, clock()) < write_guard(cfg.lease_guard_seconds, nbytes):
            raise LeaseExpired(lease.key)

    try:
        outcome = sync_key(src, dst, lease.key, cfg.copy, check_lease=check_lease)
    except Exception as exc:  # LeaseExpired too: a key that never fits in the lease must end in 'error', not loop
        state = journal.fail_sync(
            conn, lease, f"{type(exc).__name__}: {exc}", cfg.backoff_seconds, cfg.max_attempts, now=clock()
        )
        if state == "error":
            journal.record_error(conn, "handler", "max_attempts", lease.key, str(exc))
            return "error"
        return "retry" if state == "dirty" else state
    status = "deleted" if outcome.status == DELETED else "synced"
    clean = journal.finish_sync(conn, lease, status, outcome.size, outcome.etag)
    return status if clean else "changed"


def worker_loop(
    name: str,
    cfg: HandlerConfig,
    make_clients: Callable[[], tuple],
    stop: threading.Event,
    once: bool = False,
) -> int:
    conn = journal.connect(cfg.journal_path, synchronous="NORMAL")
    src, dst = make_clients()
    done = 0
    next_purge = time.monotonic() + cfg.purge_every_seconds
    try:
        while not stop.is_set():
            try:
                if name.endswith("-0") and time.monotonic() >= next_purge:
                    next_purge = time.monotonic() + cfg.purge_every_seconds
                    journal.purge_intents(conn, cfg.intent_retention_days)
                leases = journal.claim_dirty(conn, name, cfg.lease_seconds, limit=1)
                if not leases:
                    if once:
                        break
                    stop.wait(cfg.idle_sleep)
                    continue
                for lease in leases:
                    process_one(conn, lease, src, dst, cfg)
                    done += 1
            except sqlite3.Error as exc:
                # Journal busy or failing: keep the worker alive; a held lease expires and is retaken.
                log.error("%s: journal error, retrying: %s", name, exc)
                stop.wait(cfg.backoff_seconds)
    finally:
        conn.close()
    return done


def run(cfg: HandlerConfig, make_clients: Callable[[], tuple], once: bool = False) -> int:
    stop = threading.Event()
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
    results: list = []
    failed: list = []

    def work(name: str) -> None:
        try:
            results.append(worker_loop(name, cfg, make_clients, stop, once))
        except Exception:
            # one dead worker stops all: the process exits non-zero and systemd restarts it
            traceback.print_exc()
            failed.append(name)
            stop.set()

    threads = [threading.Thread(target=work, args=(f"handler-{os.getpid()}-{i}",), daemon=True) for i in range(cfg.workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if failed:
        raise RuntimeError(f"handler workers failed: {failed}")
    return sum(results)


def _make_client(endpoint: str, access_key: str, secret_key: str, region: str) -> Any:
    import boto3
    from botocore.config import Config as BotoConfig

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1, "mode": "standard"},
            read_timeout=300,
            connect_timeout=10,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def clients_from_env() -> Callable[[], tuple]:
    def make() -> tuple:
        region = os.environ.get("S3_REGION", "us-east-1")
        src = _make_client(os.environ["SRC_ENDPOINT"], os.environ["SRC_ACCESS_KEY"], os.environ["SRC_SECRET_KEY"], region)
        if not os.environ.get("DST_ENDPOINT"):
            return src, None  # no destination yet: only `bulk list` runs (PIPELINE.md, stage A)
        dst = _make_client(os.environ["DST_ENDPOINT"], os.environ["DST_ACCESS_KEY"], os.environ["DST_SECRET_KEY"], region)
        return src, dst

    return make


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description="Sync dirty keys from the gateway journal to the destination S3")
    p.add_argument("--journal", default=os.environ.get("JOURNAL_DB", "journal.db"))
    p.add_argument("--src-bucket", default=os.environ.get("SRC_BUCKET", "s3elma365"))
    p.add_argument("--dst-bucket", default=os.environ.get("DST_BUCKET", "s3elma365"))
    p.add_argument("--workers", type=int, default=int(os.environ.get("HANDLER_WORKERS", "4")))
    p.add_argument("--lease-seconds", type=float, default=float(os.environ.get("GW_LEASE_SECONDS", "900")))
    p.add_argument("--max-attempts", type=int, default=int(os.environ.get("HANDLER_MAX_ATTEMPTS", "20")),
                   help="attempts of a key before 'error' (backoff 5, 10, 20 ... s, up to 10 min)")
    p.add_argument("--no-tags", action="store_true")
    p.add_argument("--once", action="store_true", help="exit when no work is left")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if not os.environ.get("DST_ENDPOINT"):
        raise SystemExit("DST_ENDPOINT is not set: the handler needs the destination S3")
    cfg = HandlerConfig(
        journal_path=Path(a.journal),
        copy=CopyConfig(src_bucket=a.src_bucket, dst_bucket=a.dst_bucket, copy_tags=not a.no_tags),
        workers=a.workers,
        lease_seconds=a.lease_seconds,
        max_attempts=a.max_attempts,
    )
    need = min_lease_seconds(cfg.lease_guard_seconds, cfg.copy)
    if cfg.lease_seconds <= need:
        p.error(f"--lease-seconds (GW_LEASE_SECONDS) must be above {need:.0f} s: a write starts only with that much lease left")
    run(cfg, clients_from_env(), once=a.once)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
